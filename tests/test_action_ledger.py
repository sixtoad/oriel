from __future__ import annotations

from pathlib import Path
from dataclasses import replace
import tempfile
from threading import Barrier, Thread
import unittest

from oriel.adapters.action_ledger import SQLiteActionLedger
from oriel.adapters.bootstrap import DisabledTools, FixedClock, InMemoryActionLedger, InMemoryRequestLedger, NoopTelemetry, SequentialIds, ThreadSafeSynchronization, VolatileState
from oriel.adapters.ha_dry_run import HarmlessHaDryRun
from oriel.application.ports import ActionLedgerUnavailable, ActionRecord, ModelOutcome, ModelProposal
from oriel.application.startup import StartupState
from oriel.application.text_gateway import TextGateway
from oriel.domain.configuration import CoreConfig
from oriel.domain.ha_manifest import BuiltInManifest, canonical_ha_proposal
from scripts.validate_api_contract import validate_stream


def record(fingerprint: str = "a" * 64) -> ActionRecord:
    return ActionRecord("action-1", "request-1", "trace-1", "home_assistant.harmless_light.v1", "home_assistant.harmless_light", fingerprint, "reserved", "2026-10-07T10:00:00Z", "2026-10-07T10:00:00Z")


class SQLiteActionLedgerTests(unittest.TestCase):
    def test_reservation_and_required_pre_dispatch_audit_are_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteActionLedger(Path(directory) / "actions.sqlite3")
            self.addCleanup(ledger.close)
            reserved = ledger.reserve_and_audit(record())
            self.assertEqual(reserved.status, "reserved")
            self.assertEqual(ledger.audit_events("action-1"), (("reserved", "2026-10-07T10:00:00Z"),))
            attempted = ledger.mark_fake_attempt("action-1", "2026-10-07T10:00:01Z")
            self.assertEqual(attempted.state, "fake_attempted")
            self.assertEqual(ledger.audit_events("action-1"), (("reserved", "2026-10-07T10:00:00Z"), ("fake_attempted", "2026-10-07T10:00:01Z")))

    def test_same_request_is_duplicate_but_changed_fingerprint_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteActionLedger(Path(directory) / "actions.sqlite3")
            self.addCleanup(ledger.close)
            ledger.reserve_and_audit(record())
            self.assertEqual(ledger.reserve_and_audit(record()).status, "duplicate")
            self.assertEqual(ledger.reserve_and_audit(record("b" * 64)).status, "conflict")

    def test_concurrent_equivalent_reservations_create_one_audit_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteActionLedger(Path(directory) / "actions.sqlite3")
            self.addCleanup(ledger.close)
            gate = Barrier(2)
            results: list[str] = []

            def reserve() -> None:
                gate.wait()
                results.append(ledger.reserve_and_audit(record()).status)

            workers = [Thread(target=reserve) for _ in range(2)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
            self.assertEqual(sorted(results), ["duplicate", "reserved"])
            self.assertEqual(ledger.audit_events("action-1"), (("reserved", "2026-10-07T10:00:00Z"),))

    def test_persisted_degradation_wins_before_new_reservation_but_duplicates_survive(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteActionLedger(Path(directory) / "actions.sqlite3")
            self.addCleanup(ledger.close)
            ledger.reserve_and_audit(record())
            self.assertEqual(ledger.readiness().state, "ready")
            # A separate committed storage write lands after the caller's readiness read.
            with ledger._connect() as connection:
                connection.execute("UPDATE action_readiness SET state = 'degraded' WHERE singleton = 1")
            with self.assertRaises(ActionLedgerUnavailable):
                ledger.reserve_and_audit(replace(record(), action_id="action-2", request_id="request-2"))
            self.assertEqual(ledger.reserve_and_audit(record()).status, "duplicate")
            with ledger._connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM action_ledger").fetchone()[0], 1)

    def test_recovery_audit_failure_rolls_back_and_degrades(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteActionLedger(Path(directory) / "actions.sqlite3")
            self.addCleanup(ledger.close)
            ledger.reserve_and_audit(record())
            with ledger._connect() as connection:
                connection.execute("CREATE TRIGGER reject_recovery BEFORE INSERT ON action_audit WHEN NEW.event = 'outcome_unknown' BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
            with self.assertRaises(ActionLedgerUnavailable):
                ledger.recover_unresolved("2026-10-07T10:00:01Z")
            self.assertEqual(ledger.reserve_and_audit(record()).record.state, "reserved")
            self.assertEqual(ledger.readiness().state, "degraded")

    def test_failed_result_write_degrades_readiness_without_creating_a_second_action(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteActionLedger(Path(directory) / "actions.sqlite3")
            self.addCleanup(ledger.close)
            ledger.reserve_and_audit(record())
            with ledger._connect() as connection:
                connection.execute("DROP TABLE action_audit")
            with self.assertRaises(ActionLedgerUnavailable):
                ledger.mark_fake_attempt("action-1", "2026-10-07T10:00:01Z")
            self.assertEqual(ledger.readiness().state, "degraded")
            self.assertEqual(ledger.reserve_and_audit(record()).status, "duplicate")

    def test_malformed_persisted_result_degrades_and_raises_only_ledger_unavailable(self):
        import json
        from oriel.application.ports import ActionExecutionResult
        valid = ActionExecutionResult("confirmed", "observation_confirmed", "observed", "on", "2026-10-07T10:00:00Z")
        malformed = ["[]", "null", "1", '\"PRIVATE_CANARY\"', "{", json.dumps({**valid.payload(), "extra": "PRIVATE_CANARY"}),
                     json.dumps({**valid.payload(), "status": []}), json.dumps({**valid.payload(), "reason": {}})]
        for material in malformed:
            with self.subTest(material=material), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "actions.db"
                ledger = SQLiteActionLedger(path)
                ledger.reserve_and_audit(record())
                ledger.mark_execution_result("action-1", record().updated_at, valid)
                with ledger._connect() as connection:
                    connection.execute("UPDATE action_ledger SET result_json = ?", (material,))
                ledger.close()
                ledger = SQLiteActionLedger(path)
                self.addCleanup(ledger.close)
                for operation in (lambda: ledger.reserve_and_audit(record()), lambda: ledger.mark_outcome_unknown("action-1", record().updated_at)):
                    with self.assertRaises(ActionLedgerUnavailable) as raised:
                        operation()
                    self.assertNotIn("PRIVATE_CANARY", str(raised.exception))
                    self.assertEqual(ledger.readiness().state, "degraded")
                with ledger._connect() as connection:
                    self.assertEqual(connection.execute("SELECT state, result_json FROM action_ledger").fetchone(), ("confirmed", material))



class _ProposalModel:
    def stream(self, input, cancellation):
        del input, cancellation
        yield ModelProposal(canonical_ha_proposal("on"))
        yield ModelOutcome("completed")


class _FakeDispatch:
    def __init__(self) -> None:
        self.calls = 0

    def attempt(self, action: ActionRecord) -> None:
        del action
        self.calls += 1


class _FailingFakeDispatch(_FakeDispatch):
    def attempt(self, action: ActionRecord) -> None:
        super().attempt(action)
        raise RuntimeError("injected fake failure")


class _FailingTelemetry:
    def emit(self, event, fields) -> None:
        del event, fields
        raise RuntimeError("diagnostic export unavailable")


class GatewayActionReservationTests(unittest.TestCase):
    def _gateway(self, ledger, dispatch, telemetry=None):
        manifest = BuiltInManifest(enabled=True)
        return TextGateway(
            _ProposalModel(), FixedClock("2026-10-07T10:00:00Z"), VolatileState(), telemetry or NoopTelemetry(),
            DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(),
            ha_manifest=manifest, ha_preview=HarmlessHaDryRun(manifest),
            action_ledger=ledger, fake_action_dispatch=dispatch,
        )

    def _startup(self):
        return StartupState(CoreConfig("fake", {}, ()), None)

    def test_one_fake_attempt_follows_durable_reservation_and_audit(self) -> None:
        ledger = InMemoryActionLedger()
        dispatch = _FakeDispatch()
        gateway = self._gateway(ledger, dispatch)
        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain this."}, self._startup()))
        self.assertEqual([event.type for event in events], ["accepted", "proposal", "validation", "action_state", "action_state", "terminal"])
        self.assertEqual([event.action_state["state"] for event in events if event.action_state], ["reserved", "fake_attempted"])
        self.assertEqual(dispatch.calls, 1)
        self.assertEqual([entry[1] for entry in ledger.audit], ["reserved", "fake_attempted"])
        self.assertEqual(events[-1].outcome, "completed")
        self.assertEqual(validate_stream([event.payload() for event in events]), [])

    def test_pre_dispatch_failure_blocks_fake_attempt(self) -> None:
        ledger = InMemoryActionLedger(fail_reservation=True)
        dispatch = _FakeDispatch()
        gateway = self._gateway(ledger, dispatch)
        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain this."}, self._startup()))
        self.assertEqual(dispatch.calls, 0)
        self.assertEqual(events[-2].action_state, {"state": "denied", "readiness": "degraded"})
        self.assertEqual(events[-1].outcome, "denied")
        self.assertEqual(validate_stream([event.payload() for event in events]), [])

    def test_post_attempt_write_failure_is_unknown_without_redispatch(self) -> None:
        ledger = InMemoryActionLedger(fail_result_write=True)
        dispatch = _FakeDispatch()
        gateway = self._gateway(ledger, dispatch)
        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain this."}, self._startup()))
        self.assertEqual(dispatch.calls, 1)
        self.assertEqual(events[-2].action_state["state"], "outcome_unknown")
        self.assertEqual(events[-2].action_state["readiness"], "degraded")
        self.assertEqual(events[-1].outcome, "outcome_unknown")

    def test_fake_dispatch_failure_reports_unknown_without_redispatch(self) -> None:
        dispatch = _FailingFakeDispatch()
        gateway = self._gateway(InMemoryActionLedger(), dispatch)
        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain this."}, self._startup()))
        self.assertEqual(dispatch.calls, 1)
        self.assertEqual(events[-1].outcome, "outcome_unknown")

    def test_diagnostic_export_failure_does_not_block_reserved_fake_attempt(self) -> None:
        ledger = InMemoryActionLedger()
        dispatch = _FakeDispatch()
        gateway = self._gateway(ledger, dispatch, _FailingTelemetry())
        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain this."}, self._startup()))
        self.assertEqual(dispatch.calls, 1)
        self.assertEqual(events[-1].outcome, "completed")


if __name__ == "__main__":
    unittest.main()
