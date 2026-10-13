"""Controlled fixtures only: no live prerequisites or Home Assistant access."""
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import tempfile
from threading import Event, Thread
import unittest

from oriel.adapters.ha_execution import HarmlessHaExecutor, WorkerObservation
from oriel.adapters.ha_dry_run import HarmlessHaDryRun
from oriel.adapters.action_ledger import SQLiteActionLedger
from oriel.adapters.bootstrap import InMemoryActionLedger
from oriel.application.ports import ActionExecutionRequest, ActionExecutionResult, ActionLedgerUnavailable, BoundedHomeFact
from oriel.domain.ha_manifest import BuiltInManifest, ExecutionPrerequisites, MANIFEST_REVISION, canonical_ha_proposal, execution_eligibility
from scripts.validate_api_contract import validate_stream
from tests.test_action_fences import gateway, Clock, NOW, STARTUP
from tests.test_action_ledger import record

EVIDENCE = ExecutionPrerequisites(MANIFEST_REVISION, "controlled_fixture", True, True, True, True)
MANIFEST = BuiltInManifest(enabled=True, execution_prerequisites=EVIDENCE)
CANARY = "PRIVATE_PROVIDER_RESPONSE_MUST_NOT_LEAK"


class Provider:
    def __init__(self, clock, mode="confirmed"):
        self.clock, self.mode = clock, mode
        self.calls = []

    def set_power(self, desired_state, deadline):
        self.calls.append(("set", desired_state, deadline))
        self.clock.elapsed += 1
        if self.mode == "transport": raise OSError(CANARY)
        if self.mode == "rejected": return "rejected"
        if self.mode == "no_effect": return "no_effect"
        if self.mode == "dispatch_deadline": self.clock.elapsed += 5
        return "accepted"

    def observe(self, deadline):
        self.calls.append(("observe", deadline))
        self.clock.elapsed += 1
        if self.mode == "lost": raise OSError(CANARY)
        if self.mode == "observation_deadline": self.clock.elapsed += 5
        state = self.calls[0][1] if self.mode != "mismatch" else ("off" if self.calls[0][1] == "on" else "on")
        fact = BoundedHomeFact(state, NOW, "stale" if self.mode == "stale" else "fresh")
        obtained = 0 if self.mode == "cached" else self.clock.elapsed
        return WorkerObservation(fact, obtained)


def setup(mode="confirmed", actions=None, manifest=MANIFEST):
    core = gateway(actions=actions)
    core._ha_manifest = manifest
    core._ha_preview = HarmlessHaDryRun(manifest)
    provider = Provider(core._clock, mode)
    core._ha_execution = HarmlessHaExecutor(provider, manifest, core._clock.monotonic, scope="controlled_fixture")
    return core, provider


def run(core, text="turn on the reviewed harmless light"):
    return list(core.begin_turn(core.create_session().session_id, {"input": text}, STARTUP))


class ExecutionTests(unittest.TestCase):
    def test_on_and_off_require_durable_observation_even_when_already_matching(self):
        for state in ("on", "off"):
            core, provider = setup()
            events = run(core, f"turn {state} the reviewed harmless light")
            self.assertEqual(events[-1].outcome, "completed")
            result = events[-2].action_state["result"]
            self.assertEqual(result["status"], "confirmed")
            self.assertEqual(result["power_state"], state)
            self.assertEqual([c[0] for c in provider.calls], ["set", "observe"])
            self.assertEqual(provider.calls[0][-1], provider.calls[1][-1])
            saved = next(iter(core._action_ledger.records.values()))
            self.assertEqual(saved.result.payload(), result)
            self.assertEqual(validate_stream([e.payload() for e in events]), [])

    def test_failure_matrix_keeps_evidence_and_terminal_consistent_without_retry(self):
        for mode, status, reason in (
            ("transport", "outcome_unknown", "transport_unknown"),
            ("rejected", "denied", "service_rejected"),
            ("no_effect", "failed", "no_effect_failure"),
            ("lost", "outcome_unknown", "observation_missing"),
            ("stale", "outcome_unknown", "observation_stale"),
            ("cached", "outcome_unknown", "observation_stale"),
            ("mismatch", "outcome_unknown", "observation_mismatch"),
            ("dispatch_deadline", "outcome_unknown", "deadline"),
            ("observation_deadline", "outcome_unknown", "deadline"),
        ):
            with self.subTest(mode=mode):
                core, provider = setup(mode)
                events = run(core)
                self.assertEqual(events[-1].outcome, status)
                self.assertEqual(events[-2].action_state["result"]["reason"], reason)
                self.assertEqual(sum(c[0] == "set" for c in provider.calls), 1)
                self.assertNotIn(CANARY, repr(events))
                self.assertEqual(validate_stream([e.payload() for e in events]), [])

    def test_model_proposal_and_explicit_previews_never_authorize_execution(self):
        for text in ("explain this", "create a reviewed harmless light proposal on", "create a reviewed harmless light proposal off"):
            core, provider = setup()
            events = run(core, text)
            self.assertEqual(events[-1].outcome, "completed")
            self.assertEqual(provider.calls, [])

    def test_missing_each_prerequisite_and_default_policy_have_zero_calls(self):
        manifests = [BuiltInManifest(), BuiltInManifest(enabled=True)]
        for field in ("effect_reviewed", "restricted_call_path", "excluded_operation_denied", "confirmation_reviewed"):
            manifests.append(replace(MANIFEST, execution_prerequisites=replace(EVIDENCE, **{field: False})))
        manifests.append(replace(MANIFEST, execution_prerequisites=replace(EVIDENCE, manifest_revision="old")))
        for manifest in manifests:
            core, provider = setup(manifest=manifest)
            events = run(core)
            self.assertEqual(events[-1].outcome, "denied")
            self.assertEqual(provider.calls, [])
            self.assertEqual(core._action_ledger.records, {})

    def test_fixture_evidence_cannot_enable_live_worker(self):
        clock = Clock(NOW)
        provider = Provider(clock)
        proposal = execution_eligibility(canonical_ha_proposal("on"), manifest=MANIFEST).material
        result = HarmlessHaExecutor(provider, MANIFEST, clock.monotonic).execute(ActionExecutionRequest(proposal, 5))
        self.assertEqual(result.status, "denied")
        self.assertEqual(provider.calls, [])

    def test_reservation_or_result_write_failure_fails_closed(self):
        for before in (True, False):
            core, provider = setup(actions=InMemoryActionLedger(fail_reservation=before, fail_result_write=not before))
            events = run(core)
            self.assertEqual(events[-1].outcome, "denied" if before else "outcome_unknown")
            self.assertEqual(sum(c[0] == "set" for c in provider.calls), 0 if before else 1)
            self.assertFalse(any(e.action_state and e.action_state["state"] == "confirmed" for e in events))

    def test_lifecycle_and_policy_changes_before_commit_prevent_dispatch(self):
        for change in ("cancel", "reset", "disable", "exclude", "revision"):
            core, provider = setup()
            session = core.create_session().session_id
            stream = iter(core.begin_turn(session, {"input": "turn on the reviewed harmless light"}, STARTUP))
            request = next(stream).request_id
            self.assertEqual(next(stream).type, "proposal")
            if change == "cancel": core.cancel_request(request)
            elif change == "reset": core.reset_session(session)
            else: core.activate_action_policy(expected_revision=1, manifest=replace(MANIFEST, enabled=change != "disable"), restrictions={"targets": []} if change == "exclude" else None)
            self.assertIn(list(stream)[-1].outcome, {"cancelled", "denied"})
            self.assertEqual(provider.calls, [])

    def test_cancel_and_recovery_after_commit_preserve_evidence_and_suppress_content(self):
        for mode, known_state in (("confirmed", "confirmed"), ("rejected", "denied"), ("no_effect", "failed"), ("lost", "outcome_unknown")):
            for recover in (False, True):
                with self.subTest(mode=mode, recover=recover):
                    core, provider = setup(mode)
                    entered, release = Event(), Event()
                    set_power = provider.set_power
                    def gated(state, deadline):
                        result = set_power(state, deadline)
                        entered.set()
                        if not release.wait(3): raise AssertionError("fixture barrier")
                        return result
                    provider.set_power = gated
                    session = core.create_session().session_id
                    stream = iter(core.begin_turn(session, {"input": "turn on the reviewed harmless light"}, STARTUP))
                    request = next(stream).request_id
                    # Pause before commitment so every subsequent callback is post-cancel.
                    self.assertEqual(next(stream).type, "proposal")
                    events, delivered = [], []
                    def consume():
                        for event in stream:
                            events.append(event)
                            core.deliver_stream_event(event, delivered.append)
                    worker = Thread(target=consume)
                    worker.start()
                    self.assertTrue(entered.wait(2))
                    core.cancel_request(request)
                    if recover: core.recover_interrupted_requests()
                    release.set(); worker.join(3)
                    self.assertFalse(worker.is_alive())
                    expected_state = "outcome_unknown" if recover else known_state
                    expected_outcome = "outcome_unknown" if expected_state == "outcome_unknown" else "cancelled"
                    self.assertEqual([e.type for e in delivered], ["action_state", "action_state", "terminal"])
                    self.assertEqual([e.action_state["state"] for e in delivered if e.action_state], ["reserved", expected_state])
                    self.assertEqual(delivered[-1].outcome, expected_outcome)
                    self.assertEqual(core.request_status(request)["outcome"], delivered[-1].outcome)
                    self.assertEqual(events, delivered)
                    saved = core._action_ledger.records[request]
                    self.assertEqual(saved.state, expected_state)
                    self.assertEqual(sum(c[0] == "set" for c in provider.calls), 1)

    def test_malformed_observation_metadata_is_unknown_and_executor_remains_serviceable(self):
        for metadata in (None, "PRIVATE_CANARY", float("nan"), float("inf"), -float("inf")):
            with self.subTest(metadata=metadata):
                core, provider = setup()
                observe = provider.observe
                def malformed(deadline):
                    return replace(observe(deadline), obtained_at=metadata)
                provider.observe = malformed
                proposal = execution_eligibility(canonical_ha_proposal("on"), manifest=MANIFEST).material
                first = core._ha_execution.execute(ActionExecutionRequest(proposal, core._clock.monotonic() + 5))
                self.assertEqual(first.status, "outcome_unknown")
                self.assertEqual(first.evidence, "accepted")
                self.assertNotIn("PRIVATE_CANARY", repr(first))
                provider.observe = observe
                second = core._ha_execution.execute(ActionExecutionRequest(proposal, core._clock.monotonic() + 5))
                self.assertEqual(second.status, "confirmed")
                self.assertEqual(sum(call[0] == "set" for call in provider.calls), 2)

    def test_request_terminal_write_failure_preserves_known_action_evidence_on_reopen(self):
        from oriel.adapters.bootstrap import InMemoryRequestLedger
        from oriel.application.ports import RequestLedgerUnavailable
        class BrokenRequests(InMemoryRequestLedger):
            def mark_terminal(self, request_id, outcome):
                raise RequestLedgerUnavailable()
        for mode, status in (("confirmed", "confirmed"), ("rejected", "denied"), ("no_effect", "failed")):
            for durable in (False, True):
                with self.subTest(mode=mode, durable=durable), tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "actions.db"
                    actions = SQLiteActionLedger(path) if durable else InMemoryActionLedger()
                    core, _provider = setup(mode, actions=actions)
                    core._ledger = BrokenRequests()
                    events = run(core)
                    self.assertEqual(events[-1].outcome, "outcome_unknown")
                    payload = events[-2].action_state
                    self.assertEqual(payload["state"], status)
                    saved = actions.mark_outcome_unknown(payload["action_id"], NOW)
                    self.assertEqual(saved.state, status)
                    self.assertEqual(saved.result.payload(), payload["result"])
                    reservation = replace(saved, state="reserved", updated_at=saved.reserved_at, result=None)
                    if durable:
                        actions.close()
                        actions = SQLiteActionLedger(path)
                        self.addCleanup(actions.close)
                    reopened = actions.reserve_and_audit(reservation)
                    self.assertEqual(reopened.status, "duplicate")
                    self.assertEqual(reopened.record, saved)
                    audit = actions.audit_events(saved.action_id) if durable else [(event, time) for _id, event, time in actions.audit]
                    self.assertEqual([event for event, _time in audit], ["reserved", status])


class ExecutionLedgerTests(unittest.TestCase):
    def test_v1_migration_preserves_reservation_audit_and_result_on_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"actions.db"
            with sqlite3.connect(path) as connection:
                connection.execute("CREATE TABLE action_ledger (action_id TEXT PRIMARY KEY, request_id TEXT UNIQUE, trace_id TEXT, manifest_revision TEXT, capability_id TEXT, operation_fingerprint TEXT, state TEXT CHECK(state IN ('reserved','fake_attempted','outcome_unknown')), reserved_at TEXT, updated_at TEXT)")
                connection.execute("CREATE TABLE action_audit (audit_id INTEGER PRIMARY KEY, action_id TEXT, event TEXT CHECK(event IN ('reserved','fake_attempted','outcome_unknown')), occurred_at TEXT)")
                r = record()
                connection.execute("INSERT INTO action_ledger VALUES (?,?,?,?,?,?,?,?,?)", tuple(r.__dict__.values())[:9])
                connection.execute("INSERT INTO action_audit VALUES (1,?,?,?)", (r.action_id, "reserved", NOW))
            ledger = SQLiteActionLedger(path)
            result = ActionExecutionResult("confirmed", "observation_confirmed", "observed", "on", NOW)
            ledger.mark_execution_result(r.action_id, NOW, result)
            ledger.close()
            ledger = SQLiteActionLedger(path)
            self.addCleanup(ledger.close)
            duplicate = ledger.reserve_and_audit(r)
            self.assertEqual(duplicate.status, "duplicate")
            self.assertEqual(duplicate.record.result, result)
            self.assertEqual([e[0] for e in ledger.audit_events(r.action_id)], ["reserved", "confirmed"])

    def test_recovery_unknown_cannot_be_overwritten_by_late_confirmation(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteActionLedger(Path(directory)/"actions.db")
            self.addCleanup(ledger.close)
            ledger.reserve_and_audit(record())
            ledger.recover_unresolved(NOW)
            late = ActionExecutionResult("confirmed", "observation_confirmed", "observed", "on", NOW)
            result = ledger.mark_execution_result("action-1", NOW, late)
            self.assertEqual(result.state, "outcome_unknown")
            self.assertIsNone(result.result)
            self.assertEqual([e[0] for e in ledger.audit_events("action-1")], ["reserved", "outcome_unknown"])

    def test_migration_failure_rolls_back_original_tables_and_releases_owner(self):
        class BrokenMigration(SQLiteActionLedger):
            @staticmethod
            def _migrate(connection):
                SQLiteActionLedger._migrate(connection)
                raise sqlite3.DatabaseError("injected migration failure")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"actions.db"
            with sqlite3.connect(path) as connection:
                connection.execute("CREATE TABLE action_ledger (action_id TEXT PRIMARY KEY, request_id TEXT UNIQUE, trace_id TEXT, manifest_revision TEXT, capability_id TEXT, operation_fingerprint TEXT, state TEXT CHECK(state IN ('reserved','fake_attempted','outcome_unknown')), reserved_at TEXT, updated_at TEXT)")
                connection.execute("CREATE TABLE action_audit (audit_id INTEGER PRIMARY KEY, action_id TEXT, event TEXT CHECK(event IN ('reserved','fake_attempted','outcome_unknown')), occurred_at TEXT)")
                connection.execute("INSERT INTO action_ledger VALUES (?,?,?,?,?,?,?,?,?)", tuple(record().__dict__.values())[:9])
                connection.execute("INSERT INTO action_audit VALUES (1,?,?,?)", ("action-1", "reserved", NOW))
            with self.assertRaises(ActionLedgerUnavailable): BrokenMigration(path)
            with sqlite3.connect(path) as connection:
                self.assertNotIn("result_json", [row[1] for row in connection.execute("PRAGMA table_info(action_ledger)")])
                self.assertEqual(connection.execute("SELECT event FROM action_audit").fetchall(), [("reserved",)])
            repaired = SQLiteActionLedger(path)
            self.addCleanup(repaired.close)
            self.assertEqual(repaired.reserve_and_audit(record()).status, "duplicate")
