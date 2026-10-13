"""Deterministic commitment races and crash/reopen recovery, without provider I/O."""
from dataclasses import replace
from pathlib import Path
import os
import subprocess
import sys
import tempfile
from threading import Event, Thread
import unittest

from oriel.adapters.action_ledger import SQLiteActionLedger
from oriel.adapters.request_ledger import SQLiteRequestLedger
from oriel.adapters.bootstrap import (DisabledTools, FixedClock, InMemoryActionLedger, InMemoryRequestLedger,
    NoopTelemetry, SequentialIds, ThreadSafeSynchronization, VolatileState)
from oriel.adapters.ha_dry_run import HarmlessHaDryRun
from oriel.application.action_recovery import recover_actions_and_requests
from oriel.application.configuration import ConfigurationService
from oriel.application.ports import ActionLedgerUnavailable, RequestLedgerUnavailable, RequestStatusRecord
from oriel.application.text_gateway import TextGateway, MAX_SESSION_SECONDS, AdmissionError
from oriel.application.startup import StartupState
from oriel.domain.configuration import CoreConfig
from oriel.domain.ha_manifest import BuiltInManifest, canonical_ha_proposal
from tests.test_action_ledger import _ProposalModel, _FakeDispatch, record

NOW = "2026-10-07T10:00:00Z"
MANIFEST = BuiltInManifest(enabled=True)
STARTUP = StartupState(CoreConfig("fake", {}, ()), None)


class Clock(FixedClock):
    elapsed = 0.0
    def monotonic(self):
        return self.elapsed


class GateDispatch(_FakeDispatch):
    def __init__(self, fail=False):
        super().__init__()
        self.entered, self.release = Event(), Event()
        self.fail = fail
    def attempt(self, action):
        super().attempt(action)
        self.entered.set()
        if not self.release.wait(3):
            raise AssertionError("test barrier timed out")
        if self.fail:
            raise RuntimeError("synthetic uncertainty")


def gateway(actions=None, dispatch=None, requests=None, configuration=None, synchronization=None):
    return TextGateway(_ProposalModel(), Clock(NOW), VolatileState(), NoopTelemetry(), DisabledTools(),
        SequentialIds(), synchronization or ThreadSafeSynchronization(), requests or InMemoryRequestLedger(),
        ha_manifest=MANIFEST, ha_preview=HarmlessHaDryRun(MANIFEST), action_ledger=actions or InMemoryActionLedger(),
        fake_action_dispatch=dispatch or _FakeDispatch(), configuration=configuration)


def crash_gateway(directory, boundary):
    """Crash real gateway processing at durable ledger/fake adapter seams."""
    directory = Path(directory)
    class CrashRequests(SQLiteRequestLedger):
        def recover_interrupted(self, committed_request_ids=()):
            if boundary == "recovery": os._exit(23)
            return super().recover_interrupted(committed_request_ids)
    class CrashLedger(SQLiteActionLedger):
        def reserve_and_audit(self, record):
            result = super().reserve_and_audit(record)
            if boundary in {"reserved", "recovery"}:
                if boundary == "recovery": core.recover_interrupted_requests()
                os._exit(23)
            return result
        def mark_fake_attempt(self, action_id, occurred_at):
            result = super().mark_fake_attempt(action_id, occurred_at)
            if boundary == "persisted": os._exit(23)
            return result
    class DurableFake:
        def attempt(self, action):
            with (directory / "attempts").open("a") as counter:
                counter.write("attempt\n")
                counter.flush()
                os.fsync(counter.fileno())
            if boundary == "attempt": os._exit(23)
    core = gateway(actions=CrashLedger(directory / "actions.db"),
                   requests=CrashRequests(directory / "requests.db"), dispatch=DurableFake())
    stream = iter(core.begin_turn(core.create_session().session_id,
        {"input": "Create a reviewed harmless light proposal on"}, STARTUP))
    accepted = next(stream)
    (directory / "request-id").write_text(accepted.request_id)
    if boundary == "before": os._exit(23)
    list(stream)
    raise AssertionError("crash boundary not reached")


class ActionFenceTests(unittest.TestCase):
    def prepared(self, route, **kwargs):
        core = gateway(**kwargs)
        session = core.create_session().session_id
        stream = iter(core.begin_turn(session, {"input": "Create a reviewed harmless light proposal on" if route == "fast" else "Explain this."}, STARTUP))
        accepted = next(stream)
        self.assertEqual(next(stream).type, "proposal")
        self.assertEqual(next(stream).type, "validation")
        return core, session, stream, accepted.request_id

    def change(self, kind, core, session, request):
        if kind == "cancel": core.cancel_request(request)
        elif kind == "reset": core.reset_session(session)
        elif kind == "end": core.end_session(session)
        elif kind == "expiry":
            core._clock.elapsed = MAX_SESSION_SECONDS
            core.expire_sessions()
        elif kind == "deadline": core._clock.elapsed = 31
        else:
            manifest = replace(MANIFEST, enabled=False) if kind in {"disable", "reenable"} else replace(MANIFEST, revision="changed") if kind == "manifest" else MANIFEST
            restrictions = {"targets": ()} if kind == "exclude" else None
            self.assertTrue(core.activate_action_policy(expected_revision=1, manifest=manifest, restrictions=restrictions))
            if kind == "reenable":
                self.assertTrue(core.activate_action_policy(expected_revision=2, manifest=MANIFEST))

    def test_every_change_before_commit_blocks_both_routes(self):
        for route in ("fast", "model"):
            for change in ("cancel", "reset", "end", "expiry", "deadline", "disable", "exclude", "manifest", "revision", "reenable"):
                with self.subTest(route=route, change=change):
                    core, session, stream, request = self.prepared(route)
                    self.change(change, core, session, request)
                    events = list(stream)
                    self.assertEqual(core._fake_action_dispatch.calls, 0)
                    self.assertEqual(core._action_ledger.records, {})
                    self.assertIn(events[-1].outcome, {"cancelled", "denied", "failed"})

    def test_every_change_after_commit_keeps_one_attempt_and_evidence(self):
        for route in ("fast", "model"):
            for change in ("cancel", "reset", "end", "expiry", "deadline", "disable", "exclude", "manifest", "revision", "reenable"):
                with self.subTest(route=route, change=change):
                    dispatch = GateDispatch()
                    core, session, stream, request = self.prepared(route, dispatch=dispatch)
                    events, delivered = [], []
                    def consume():
                        for event in stream:
                            events.append(event)
                            core.deliver_stream_event(event, delivered.append)
                    worker = Thread(target=consume)
                    worker.start()
                    self.assertTrue(dispatch.entered.wait(2))
                    # This would deadlock if fake I/O held the lifecycle lock.
                    self.change(change, core, session, request)
                    dispatch.release.set()
                    worker.join(3)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(dispatch.calls, 1)
                    self.assertEqual([e.action_state["state"] for e in delivered if e.action_state], ["reserved", "fake_attempted"])
                    self.assertFalse(any(e.type in {"content_delta", "proposal", "validation"} for e in delivered))
                    self.assertEqual(core.request_status(request)["outcome"], events[-1].outcome)

    def test_reservation_and_required_audit_hold_lock_against_lifecycle_and_activation(self):
        class GatedLedger(InMemoryActionLedger):
            def reserve_and_audit(self, record):
                entered.set()
                if not release.wait(3): raise AssertionError("reservation gate timeout")
                return super().reserve_and_audit(record)
        for change in ("cancel", "reset", "end", "disable", "exclude", "manifest", "reenable"):
            with self.subTest(change=change):
                entered, release, changing, changed = Event(), Event(), Event(), Event()
                dispatch = GateDispatch()
                core, session, stream, request = self.prepared("model", actions=GatedLedger(), dispatch=dispatch)
                events = []
                consumer = Thread(target=lambda: events.extend(stream))
                consumer.start()
                self.assertTrue(entered.wait(2))
                def mutate():
                    changing.set()
                    self.change(change, core, session, request)
                    changed.set()
                mutator = Thread(target=mutate)
                mutator.start()
                self.assertTrue(changing.wait(2))
                self.assertFalse(changed.is_set())
                release.set()
                self.assertTrue(changed.wait(2))
                self.assertTrue(dispatch.entered.wait(2))
                dispatch.release.set()
                consumer.join(3); mutator.join(3)
                self.assertFalse(consumer.is_alive())
                self.assertEqual(dispatch.calls, 1)
                self.assertEqual([entry[1] for entry in core._action_ledger.audit], ["reserved", "fake_attempted"])

    def test_unknown_commit_dominates_cancellation_and_disable(self):
        for route in ("fast", "model"):
            for fail_write in (False, True):
                dispatch = GateDispatch(fail=not fail_write)
                core, session, stream, request = self.prepared(route, dispatch=dispatch, actions=InMemoryActionLedger(fail_result_write=fail_write))
                events = []
                worker = Thread(target=lambda: events.extend(stream))
                worker.start()
                self.assertTrue(dispatch.entered.wait(2))
                self.assertEqual(core.cancel_request(request)["state"], "cancellation_requested")
                self.change("disable", core, session, request)
                dispatch.release.set()
                worker.join(3)
                self.assertEqual(events[-1].outcome, "outcome_unknown")
                self.assertEqual(core.request_status(request)["outcome"], "outcome_unknown")
                self.assertEqual(dispatch.calls, 1)

    def test_recovery_fences_a_late_fake_result_and_stream_terminal(self):
        dispatch = GateDispatch()
        core, session, stream, request = self.prepared("model", dispatch=dispatch)
        events = []
        worker = Thread(target=lambda: events.extend(stream))
        worker.start()
        self.assertTrue(dispatch.entered.wait(2))
        core.recover_interrupted_requests()
        dispatch.release.set()
        worker.join(3)
        self.assertEqual(events[-1].outcome, "outcome_unknown")
        self.assertEqual(core.request_status(request)["outcome"], "outcome_unknown")
        self.assertEqual(core._action_ledger.records[request].state, "outcome_unknown")

    def test_operator_configuration_revision_shares_commit_lock(self):
        sync = ThreadSafeSynchronization()
        configuration = ConfigurationService(sync)
        configuration.activate_config(CoreConfig("fake", {}, ()), None, "test")
        core, session, stream, request = self.prepared("model", configuration=configuration, synchronization=sync)
        configuration.activate_config(CoreConfig("fake", {}, ("home_assistant",)), 1, "test")
        configuration.activate_config(CoreConfig("fake", {}, ()), 2, "test")
        self.assertEqual(list(stream)[-1].outcome, "denied")
        self.assertEqual(core._fake_action_dispatch.calls, 0)
        with self.assertRaises(ValueError):
            gateway(configuration=configuration)

    def test_fresh_operator_and_local_restrictions_intersect_on_both_routes(self):
        for route in ("fast", "model"):
            for operator, local, denied in (
                ({"enabled": False}, None, True),
                ({"enabled": True, "targets": []}, None, True),
                ({"enabled": True}, {"targets": []}, True),
                ({"enabled": True, "targets": []}, {"read_fields": ["power_state"]}, True),
                ({"enabled": True, "read_fields": []}, {"targets": []}, True),
                ({"enabled": True}, None, False),
            ):
                with self.subTest(route=route, operator=operator, local=local):
                    sync = ThreadSafeSynchronization()
                    configuration = ConfigurationService(sync)
                    configuration.activate({"api_version": "1.0", "provider": {"connection_ref": "fake"}, "skills": {"home_assistant": operator}}, None, "test")
                    core = gateway(configuration=configuration, synchronization=sync)
                    core.activate_action_policy(expected_revision=1, manifest=MANIFEST, restrictions=local)
                    text = "Create a reviewed harmless light proposal on" if route == "fast" else "Explain this."
                    events = list(core.begin_turn(core.create_session().session_id, {"input": text}, STARTUP))
                    self.assertEqual(events[-1].outcome, "denied" if denied else "completed")
                    self.assertEqual(core._fake_action_dispatch.calls, 0 if denied else 1)

    def test_expiry_during_reservation_never_invokes_fake(self):
        for elapsed in (31, MAX_SESSION_SECONDS):
            class AdvancingLedger(InMemoryActionLedger):
                def reserve_and_audit(self, record):
                    result = super().reserve_and_audit(record)
                    core._clock.elapsed = elapsed
                    return result
            core, session, stream, request = self.prepared("fast", actions=AdvancingLedger())
            if elapsed == MAX_SESSION_SECONDS:
                core._requests[request].deadline_at = MAX_SESSION_SECONDS + 100
            events = list(stream)
            self.assertEqual(core._fake_action_dispatch.calls, 0)
            self.assertEqual(events[-1].outcome, "outcome_unknown")
            self.assertEqual(core._action_ledger.records[request].state, "outcome_unknown")
            self.assertEqual([event.action_state["state"] for event in events if event.action_state], ["reserved", "outcome_unknown"])

    def test_result_failure_degrades_under_same_lock_as_next_commit(self):
        entered, release, contender = Event(), Event(), Event()
        ownership = []
        class GatedDegrade(InMemoryActionLedger):
            def degrade(self):
                ownership.append(core._synchronization._lock._is_owned())
                entered.set()
                if not release.wait(3): raise AssertionError("degradation gate timed out")
                super().degrade()
        dispatch = GateDispatch(fail=True)
        core, session, first, request = self.prepared("model", actions=GatedDegrade(), dispatch=dispatch)
        second = iter(core.begin_turn(core.create_session().session_id, {"input": "Explain this."}, STARTUP))
        for _ in range(3): next(second)
        first_events, second_events = [], []
        first_worker = Thread(target=lambda: first_events.extend(first))
        first_worker.start()
        self.assertTrue(dispatch.entered.wait(2))
        dispatch.release.set()
        self.assertTrue(entered.wait(2))
        def consume_second():
            contender.set()
            second_events.extend(second)
        second_worker = Thread(target=consume_second)
        second_worker.start()
        self.assertTrue(contender.wait(2))
        release.set()
        first_worker.join(3); second_worker.join(3)
        self.assertEqual(ownership, [True])
        self.assertEqual(dispatch.calls, 1)
        self.assertEqual(len(core._action_ledger.records), 1)
        self.assertEqual(second_events[-1].outcome, "denied")

    def test_fresh_turn_after_reenable_requires_and_gets_new_validation(self):
        core, session, stream, request = self.prepared("fast")
        self.change("reenable", core, session, request)
        self.assertEqual(list(stream)[-1].outcome, "denied")
        events = list(core.begin_turn(session, {"input": "Create a reviewed harmless light proposal on"}, STARTUP))
        self.assertEqual(events[-1].outcome, "completed")
        self.assertEqual(core._fake_action_dispatch.calls, 1)

    def test_failed_request_terminal_write_preserves_durable_unknown(self):
        class BrokenTerminal(InMemoryRequestLedger):
            def mark_terminal(self, request_id, outcome): raise RequestLedgerUnavailable()
        core, session, stream, request = self.prepared("fast", requests=BrokenTerminal())
        self.assertEqual(list(stream)[-1].outcome, "outcome_unknown")
        self.assertEqual(core._action_ledger.records[request].state, "outcome_unknown")
        recovered = InMemoryRequestLedger(core._ledger.records)
        recover_actions_and_requests(core._action_ledger, recovered, NOW)
        self.assertEqual(recovered.lookup(request, NOW).outcome, "outcome_unknown")

    def test_double_write_failure_recovers_persisted_fake_attempt_as_unknown(self):
        class BrokenTerminal(InMemoryRequestLedger):
            def mark_terminal(self, request_id, outcome): raise RequestLedgerUnavailable()
        class BrokenCompensation(InMemoryActionLedger):
            def mark_outcome_unknown(self, action_id, occurred_at): raise ActionLedgerUnavailable()
        core, session, stream, request = self.prepared("fast", actions=BrokenCompensation(), requests=BrokenTerminal())
        self.assertEqual(list(stream)[-1].outcome, "outcome_unknown")
        self.assertEqual(core._action_ledger.records[request].state, "fake_attempted")
        recovered = InMemoryRequestLedger(core._ledger.records)
        for _ in range(2): recover_actions_and_requests(core._action_ledger, recovered, NOW)
        self.assertEqual(recovered.lookup(request, NOW).outcome, "outcome_unknown")
        self.assertEqual(core._fake_action_dispatch.calls, 1)

    def test_degraded_readiness_blocks_commit(self):
        core, session, stream, request = self.prepared("fast")
        core._action_ledger.degrade()
        self.assertEqual(list(stream)[-1].outcome, "denied")
        self.assertEqual(core._fake_action_dispatch.calls, 0)


class RecoveryTests(unittest.TestCase):
    def test_real_process_exit_at_each_boundary_reopens_without_replay(self):
        script = "from tests.test_action_fences import crash_gateway; import sys; crash_gateway(sys.argv[1], sys.argv[2])"
        for boundary in ("before", "reserved", "attempt", "persisted", "recovery"):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory:
                result = subprocess.run([sys.executable, "-c", script, directory, boundary], capture_output=True)
                self.assertEqual(result.returncode, 23, result.stderr)
                request_id = (Path(directory) / "request-id").read_text()
                actions = SQLiteActionLedger(Path(directory) / "actions.db")
                requests = SQLiteRequestLedger(Path(directory) / "requests.db")
                try:
                    for _ in range(2): recover_actions_and_requests(actions, requests, NOW)
                    expected = "failed" if boundary == "before" else "outcome_unknown"
                    self.assertEqual(requests.lookup(request_id, NOW).outcome, expected)
                    self.assertEqual(requests.mark_terminal(request_id, "completed"), expected)
                    self.assertEqual((Path(directory) / "attempts").exists(), boundary in {"attempt", "persisted"})
                    if boundary != "before":
                        with actions._connect() as connection:
                            action_id = connection.execute("SELECT action_id FROM action_ledger").fetchone()[0]
                        self.assertEqual(actions.mark_fake_attempt(action_id, NOW).state, "fake_attempted" if boundary == "persisted" else "outcome_unknown")
                        self.assertEqual([event[0] for event in actions.audit_events(action_id)], ["reserved", "fake_attempted" if boundary == "persisted" else "outcome_unknown"])
                finally:
                    actions.close(); requests.close()

    def test_interrupted_reconciliation_repeats_unknowns_and_degrades(self):
        class InterruptedRequests(InMemoryRequestLedger):
            def recover_interrupted(self, committed_request_ids=()):
                raise RequestLedgerUnavailable()
        requests = InterruptedRequests()
        requests.reserve(RequestStatusRecord("request-1", "session-1", "trace-1", 0, "in_progress", None, NOW, "2026-10-08T10:00:00Z"))
        actions = InMemoryActionLedger()
        actions.reserve_and_audit(record())
        with self.assertRaises(RequestLedgerUnavailable):
            recover_actions_and_requests(actions, requests, NOW)
        self.assertEqual(actions.records["request-1"].state, "outcome_unknown")
        self.assertEqual(actions.readiness().state, "degraded")
        recovered = InMemoryRequestLedger(requests.records)
        for _ in range(2): recover_actions_and_requests(actions, recovered, NOW)
        self.assertEqual(recovered.lookup("request-1", NOW).outcome, "outcome_unknown")
        self.assertEqual(len(actions.audit), 2)

    def test_failed_required_recovery_blocks_admission_without_failing_requests(self):
        class BrokenActions(InMemoryActionLedger):
            def recover_unresolved(self, occurred_at): raise ActionLedgerUnavailable()
        core = gateway(actions=BrokenActions())
        with self.assertRaises(ActionLedgerUnavailable): core.recover_interrupted_requests()
        with self.assertRaises(AdmissionError):
            core.begin_turn(core.create_session().session_id, {"input": "hello"}, STARTUP)
        self.assertEqual(core._action_ledger.readiness().state, "degraded")


if __name__ == "__main__":
    unittest.main()
