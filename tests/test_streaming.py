from __future__ import annotations

from http.client import HTTPConnection
import json
from pathlib import Path
import re
import socket
import struct
import tempfile
import time
from threading import Event, Thread
import unittest
from unittest.mock import patch

from oriel.adapters.bootstrap import AdvanceableClock, DeterministicScheduler, DisabledTools, FakeModel, FixedClock, InMemoryRequestLedger, NoopTelemetry, RecordingTelemetry, RuntimeClock, SecureIds, SequentialIds, ThreadingScheduler, ThreadingTasks, ThreadSafeSynchronization, VolatileState
from oriel.adapters.configuration import ResolvedProviderProfile, StaticProfileResolver, activate_startup
from oriel.adapters.http import HealthServer
from oriel.adapters.request_ledger import SQLiteRequestLedger
from oriel.application.configuration import ConfigurationService
from oriel.application.ports import CancellationSignal, ModelChunk, ModelInput, ModelOperationFailure, ModelOutcome, ModelProposal, RequestLedgerUnavailable
from oriel.application.fast_router import FastRoute
from oriel.application.text_gateway import AdmissionError, TextGateway


VALID_CONFIG = '{"api_version":"1.0","provider":{"connection_ref":"fake"},"skills":{}}'


class CountingModel(FakeModel):
    calls: int = 0

    def stream(self, input: ModelInput, cancellation: CancellationSignal):
        self.calls += 1
        yield from super().stream(input, cancellation)


class MissingOutcomeModel(FakeModel):
    def stream(self, input: ModelInput, cancellation: CancellationSignal):
        del input, cancellation
        yield ModelChunk("ordinary prose")


class ExplodingModel(FakeModel):
    def stream(self, input: ModelInput, cancellation: CancellationSignal):
        del input, cancellation
        raise RuntimeError("private provider detail")
        yield ModelChunk("")


class RecordingTools:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def dispatch(self, name: str, arguments: object) -> None:
        self.calls.append((name, arguments))


class FailingTelemetry:
    def emit(self, event: str, fields: object) -> None:
        del event, fields
        raise RuntimeError("telemetry is unavailable")


class StreamingHttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        config = Path(self._directory.name) / "config.json"
        config.write_text(VALID_CONFIG, encoding="utf-8")
        self.startup, _profile = activate_startup(
            ConfigurationService(ThreadSafeSynchronization()),
            StaticProfileResolver({"fake": ResolvedProviderProfile("test")}),
            explicit_path=config,
        )

    def tearDown(self) -> None:
        self._directory.cleanup()

    def with_server(self, model: FakeModel, ledger: object | None = None, scheduler=None, tasks=None) -> HealthServer:
        gateway = TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), ledger or InMemoryRequestLedger(), scheduler=scheduler, tasks=tasks)
        server = HealthServer(self.startup, gateway)
        server.start()
        self.addCleanup(server.close)
        return server

    def connection(self, server: HealthServer) -> HTTPConnection:
        host, port = server.address
        return HTTPConnection(host, port, timeout=3)

    def create_session(self, server: HealthServer) -> dict[str, object]:
        connection = self.connection(server)
        try:
            connection.request("POST", "/v1/sessions")
            response = connection.getresponse()
            self.assertEqual(response.status, 201)
            return json.loads(response.read())
        finally:
            connection.close()

    def read_frame(self, response) -> tuple[str, dict[str, object]]:
        event_line = response.fp.readline().decode("utf-8").rstrip("\n")
        data_line = response.fp.readline().decode("utf-8").rstrip("\n")
        self.assertEqual(response.fp.readline(), b"\n")
        self.assertTrue(event_line.startswith("event: "))
        self.assertTrue(data_line.startswith("data: "))
        return event_line[7:], json.loads(data_line[6:])

    def turn_response(self, server: HealthServer, session_id: str, body: bytes, headers: dict[str, str] | None = None):
        connection = self.connection(server)
        connection.request("POST", f"/v1/sessions/{session_id}/turns", body=body, headers=headers or {"Content-Type": "application/json"})
        response = connection.getresponse()
        return connection, response

    def start_turn(self, server: HealthServer, session_id: str):
        connection, response = self.turn_response(server, session_id, b'{"input":"hello"}')
        self.assertEqual(response.status, 200)
        self.assertEqual(response.getheader("Content-Type"), "text/event-stream; charset=utf-8")
        return connection, response

    def status(self, server: HealthServer, request_id: str) -> dict[str, object]:
        connection = self.connection(server)
        try:
            connection.request("GET", f"/v1/requests/{request_id}")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            return json.loads(response.read())
        finally:
            connection.close()

    def cancel(self, server: HealthServer, request_id: str):
        connection = self.connection(server)
        connection.request("POST", f"/v1/requests/{request_id}/cancel")
        return connection, connection.getresponse()

    def test_delayed_stream_is_ordered_and_status_is_passive(self):
        model = CountingModel(chunks=("hello", " world"), delay_seconds=0.15)
        server = self.with_server(model)
        session = self.create_session(server)
        connection, response = self.start_turn(server, str(session["session_id"]))
        try:
            event_type, accepted = self.read_frame(response)
            self.assertEqual(event_type, "accepted")
            self.assertEqual(accepted["api_version"], "1.0")
            self.assertEqual(accepted["context_generation"], 0)
            self.assertEqual(accepted["session_id"], session["session_id"])
            in_progress = self.status(server, str(accepted["request_id"]))
            self.assertEqual(in_progress["state"], "in_progress")
            frames = [(event_type, accepted)]
            first_delta_at = None
            terminal_at = None
            while frames[-1][0] != "terminal":
                frame = self.read_frame(response)
                frames.append(frame)
                if frame[0] == "content_delta" and first_delta_at is None:
                    first_delta_at = time.monotonic()
                if frame[0] == "terminal":
                    terminal_at = time.monotonic()
            self.assertEqual([kind for kind, _payload in frames], ["accepted", "content_delta", "content_delta", "terminal"])
            self.assertIsNotNone(first_delta_at)
            self.assertIsNotNone(terminal_at)
            self.assertGreater(float(terminal_at) - float(first_delta_at), 0.08)
            self.assertEqual([payload["seq"] for _kind, payload in frames], [1, 2, 3, 4])
            self.assertTrue(all(payload["request_id"] == accepted["request_id"] and payload["trace_id"] == accepted["trace_id"] for _kind, payload in frames))
            self.assertEqual(frames[-1][1]["outcome"], "completed")
            terminal = self.status(server, str(accepted["request_id"]))
            self.assertEqual(terminal["state"], "terminal")
            self.assertEqual(terminal["outcome"], "completed")
            self.assertEqual(model.calls, 1)
        finally:
            connection.close()

    def test_reservation_precedes_accepted_stream_and_model_work(self):
        model = CountingModel()
        ledger = InMemoryRequestLedger()
        gateway = TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), ledger)
        session = gateway.create_session()
        events = iter(gateway.begin_turn(session.session_id, {"input": "hello"}, self.startup))
        self.assertEqual(model.calls, 0)
        self.assertEqual(len(ledger.records), 1)
        accepted = next(events)
        self.assertEqual(accepted.type, "accepted")
        self.assertEqual(model.calls, 0)
        list(events)
        self.assertEqual(model.calls, 1)
        self.assertEqual(gateway.request_status(accepted.request_id)["state"], "terminal")

    def test_fast_routes_preserve_stream_transcript_telemetry_and_zero_dispatch(self):
        model = CountingModel(response="model reply")
        telemetry = RecordingTelemetry()
        tools = RecordingTools()
        gateway = TextGateway(model, FixedClock(), VolatileState(), telemetry, tools, SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())

        session = gateway.create_session()
        safe = list(gateway.begin_turn(session.session_id, {"input": "  ORIEL\tHELP! "}, self.startup))
        self.assertEqual([(event.type, event.content, event.outcome) for event in safe], [("accepted", None, None), ("content_delta", "Oriel can provide limited deterministic responses.", None), ("terminal", None, "completed")])
        self.assertEqual(model.calls, 0)
        self.assertEqual([(message.role, message.content) for message in gateway._sessions[session.session_id].transcript], [("user", "  ORIEL\tHELP! "), ("assistant", "Oriel can provide limited deterministic responses.")])

        self.assertEqual(telemetry.records[0], ("route_selected", {
            "request_id": safe[0].request_id,
            "session_id": session.session_id,
            "trace_id": safe[0].trace_id,
            "route": "content",
            "rule_revision": "1",
            "duration_ms": "0",
        }))
        self.assertEqual([event for event, _fields in telemetry.records[1:]], ["first_useful_content", "terminal"])
        self.assertTrue(all("content" not in fields for _event, fields in telemetry.records))
        self.assertNotIn("ORIEL", str(telemetry.records))
        self.assertEqual(tools.calls, [])

    def test_fast_route_bypasses_model_queue_and_model_deadline(self):
        class BlockingModel(FakeModel):
            def __init__(self) -> None:
                super().__init__()
                self.started = Event()
                self.release = Event()
                self.calls = 0

            def stream(self, input, cancellation):
                del input
                self.calls += 1
                self.started.set()
                self.release.wait(1)
                if not cancellation.is_cancelled():
                    yield ModelOutcome("completed")

        clock = AdvanceableClock()
        model = BlockingModel()
        gateway = TextGateway(model, clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), turn_deadline_seconds=30)
        sessions = [gateway.create_session() for _ in range(3)]
        active = [iter(gateway.begin_turn(session.session_id, {"input": text}, self.startup)) for session, text in zip(sessions[:2], ("one", "two"), strict=True)]
        for stream in active:
            next(stream)
        workers = [Thread(target=lambda stream=stream: list(stream), daemon=True) for stream in active]
        for worker in workers:
            worker.start()
        self.assertTrue(model.started.wait(0.25))
        clock.advance(30)

        fast = list(gateway.begin_turn(sessions[2].session_id, {"input": "oriel help"}, self.startup))

        self.assertEqual([(event.type, event.content, event.outcome) for event in fast], [("accepted", None, None), ("content_delta", "Oriel can provide limited deterministic responses.", None), ("terminal", None, "completed")])
        self.assertEqual(model.calls, 2)
        model.release.set()
        for worker in workers:
            worker.join(1)

    def test_fast_clarification_limitations_denials_and_proposals_do_not_call_model_or_tools(self):
        model = CountingModel(response="model reply")
        tools = RecordingTools()
        telemetry = RecordingTelemetry()
        gateway = TextGateway(model, FixedClock(), VolatileState(), telemetry, tools, SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())

        def stream(text: str):
            return list(gateway.begin_turn(gateway.create_session().session_id, {"input": text}, self.startup))

        clarification = stream("Turn on the desk lamp")
        limitation = stream("What is the weather now?")
        home_state = stream("Are the lights on?")
        denial = stream("Unlock the front door")
        proposal = stream("Create a synthetic proposal")
        self.assertEqual([(event.type, event.content, event.outcome) for event in clarification], [("accepted", None, None), ("content_delta", "Please clarify your request.", None), ("terminal", None, "completed")])
        self.assertEqual(limitation[1].content, "Live weather lookup is unsupported.")
        self.assertEqual(home_state[1].content, "Live home-state lookup is unsupported.")
        self.assertEqual([(event.type, event.error and event.error["category"], event.outcome) for event in denial], [("accepted", None, None), ("error", "policy_denial", None), ("terminal", None, "denied")])
        self.assertEqual([event.type for event in proposal], ["accepted", "proposal", "terminal"])
        self.assertEqual(proposal[-1].outcome, "completed")
        self.assertEqual(model.calls, 0)
        self.assertEqual(tools.calls, [])
        self.assertEqual(
            [record for record in telemetry.records if record[0] == "route_selected"],
            [
                (
                    "route_selected",
                    {
                        "request_id": events[0].request_id,
                        "session_id": events[0].session_id,
                        "trace_id": events[0].trace_id,
                        "route": route,
                        "rule_revision": "1",
                        "duration_ms": "0",
                    },
                )
                for events, route in (
                    (clarification, "clarification"),
                    (limitation, "limitation"),
                    (home_state, "limitation"),
                    (denial, "denial"),
                    (proposal, "proposal"),
                )
            ],
        )
        self.assertEqual([event for event, _fields in telemetry.records].count("first_useful_content"), 4)
        self.assertEqual([event for event, _fields in telemetry.records].count("terminal"), 5)
        self.assertTrue(all("content" not in fields for _event, fields in telemetry.records))

    def test_route_telemetry_failure_does_not_strand_an_accepted_turn(self):
        gateway = TextGateway(CountingModel(), FixedClock(), VolatileState(), FailingTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())

        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "oriel help"}, self.startup))

        self.assertEqual([(event.type, event.outcome) for event in events], [("accepted", None), ("content_delta", None), ("terminal", "completed")])
        self.assertEqual(gateway.request_status(events[0].request_id)["state"], "terminal")

    def test_invalid_fast_proposal_is_denied_without_model_or_tool_dispatch(self):
        model = CountingModel(response="model reply")
        tools = RecordingTools()
        gateway = TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), tools, SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        with patch("oriel.application.text_gateway.select_fast_route", return_value=FastRoute("proposal", proposal={"invalid": True})):
            events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "any text"}, self.startup))

        self.assertEqual([(event.type, event.error and event.error["category"], event.outcome) for event in events], [("accepted", None, None), ("error", "policy_denial", None), ("terminal", None, "denied")])
        self.assertEqual(model.calls, 0)
        self.assertEqual(tools.calls, [])

    def test_complex_chat_reaches_qwen_route_after_payload_free_telemetry(self):
        model = CountingModel(response="model reply")
        telemetry = RecordingTelemetry()
        gateway = TextGateway(model, FixedClock(), VolatileState(), telemetry, DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain why the sky looks blue."}, self.startup))

        self.assertEqual([event.type for event in events], ["accepted", "content_delta", "terminal"])
        self.assertEqual(model.calls, 1)
        self.assertEqual(telemetry.records[0][1]["route"], "qwen")

    def test_slow_qwen_acknowledges_once_from_durable_admission_with_separate_payload_free_timings(self):
        clock = AdvanceableClock()
        scheduler = DeterministicScheduler()
        telemetry = RecordingTelemetry()
        gateway = TextGateway(
            FakeModel(chunks=("answer",)), clock, VolatileState(), telemetry, DisabledTools(),
            SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=scheduler,
        )
        events = iter(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain the answer."}, self.startup))

        self.assertEqual(len(scheduler._calls), 1)
        accepted = next(events)
        clock.advance(0.5)
        scheduler.advance(0.5)
        acknowledgement = next(events)
        remainder = list(events)

        self.assertEqual((acknowledgement.type, acknowledgement.message, acknowledgement.content, acknowledgement.outcome), ("ack", "Work is continuing.", None, None))
        self.assertEqual([event.type for event in (accepted, acknowledgement, *remainder)], ["accepted", "ack", "content_delta", "terminal"])
        timing = [event for event, _fields in telemetry.records if event != "route_selected"]
        self.assertEqual(timing, ["acknowledgement", "first_model_token", "first_useful_content", "terminal"])
        self.assertTrue(all(set(fields) == {"request_id", "session_id", "trace_id", "duration_ms"} for event, fields in telemetry.records if event != "route_selected"))
        self.assertTrue(all("answer" not in str(fields) for _event, fields in telemetry.records))

    def test_threaded_provider_delivers_ack_before_a_slow_first_model_chunk(self):
        gateway = TextGateway(
            FakeModel(chunks=("answer",), delay_seconds=0.65), RuntimeClock(), VolatileState(), NoopTelemetry(), DisabledTools(),
            SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=ThreadingScheduler(), tasks=ThreadingTasks(),
        )
        events = iter(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain the answer."}, self.startup))

        self.assertEqual(next(events).type, "accepted")
        started = time.monotonic()
        acknowledgement = next(events)

        self.assertEqual((acknowledgement.type, acknowledgement.message), ("ack", "Work is continuing."))
        self.assertGreaterEqual(time.monotonic() - started, 0.4)
        self.assertLess(time.monotonic() - started, 0.75)
        self.assertEqual([event.type for event in events], ["content_delta", "terminal"])

    def test_threaded_cancellation_wins_over_a_pending_acknowledgement(self):
        gateway = TextGateway(
            FakeModel(chunks=("late",), delay_seconds=0.65), RuntimeClock(), VolatileState(), NoopTelemetry(), DisabledTools(),
            SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=ThreadingScheduler(), tasks=ThreadingTasks(),
        )
        events = iter(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain the answer."}, self.startup))

        accepted = next(events)
        self.assertEqual(gateway.cancel_request(accepted.request_id)["state"], "cancellation_requested")
        self.assertEqual([(event.type, event.outcome) for event in events], [("terminal", "cancelled")])

    def test_yielded_acknowledgement_is_fenced_when_cancellation_wins_delivery(self):
        clock = AdvanceableClock()
        scheduler = DeterministicScheduler()
        gateway = TextGateway(
            FakeModel(chunks=("late",)), clock, VolatileState(), NoopTelemetry(), DisabledTools(),
            SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=scheduler,
        )
        events = iter(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain the answer."}, self.startup))
        accepted = next(events)
        clock.advance(0.5)
        scheduler.advance(0.5)
        acknowledgement = next(events)

        self.assertEqual(acknowledgement.type, "ack")
        self.assertEqual(gateway.cancel_request(accepted.request_id)["state"], "cancellation_requested")
        delivered: list[object] = []
        self.assertFalse(gateway.deliver_stream_event(acknowledgement, delivered.append))
        self.assertEqual(delivered, [])

    def test_scheduler_and_task_start_failures_are_safe_accepted_terminals(self):
        class RaisingScheduler:
            def schedule(self, delay_seconds, callback):
                del delay_seconds, callback
                raise RuntimeError("scheduler failure")

        class RaisingTasks:
            def start(self, callback):
                del callback
                raise RuntimeError("task failure")

        def stream(scheduler=None, tasks=None):
            gateway = TextGateway(
                FakeModel(), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(),
                ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=scheduler, tasks=tasks,
            )
            return list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain the answer."}, self.startup))

        for events in (stream(scheduler=RaisingScheduler()), stream(scheduler=DeterministicScheduler(), tasks=RaisingTasks())):
            self.assertEqual([(event.type, event.outcome) for event in events], [("accepted", None), ("error", None), ("terminal", "failed")])
            self.assertEqual(events[1].error["code"], "stream_start_failed")

    def test_http_stream_serializes_one_ack_before_slow_useful_output(self):
        server = self.with_server(FakeModel(chunks=("answer",), delay_seconds=0.65), scheduler=ThreadingScheduler(), tasks=ThreadingTasks())
        session = self.create_session(server)
        connection, response = self.start_turn(server, str(session["session_id"]))
        try:
            frames = []
            while not frames or frames[-1][0] != "terminal":
                frames.append(self.read_frame(response))
            self.assertEqual([kind for kind, _payload in frames], ["accepted", "ack", "content_delta", "terminal"])
            self.assertEqual(frames[1][1]["message"], "Work is continuing.")
            self.assertEqual([payload["seq"] for _kind, payload in frames], [1, 2, 3, 4])
        finally:
            connection.close()

    def test_acknowledgement_keeps_later_failure_and_timeout_terminals_authoritative(self):
        class FailingModel:
            def stream(self, input, cancellation):
                del input, cancellation
                raise ModelOperationFailure("upstream_failure", "dependency_unavailable", "Model is unavailable.", True)

        def acknowledged_events(model):
            clock = AdvanceableClock()
            scheduler = DeterministicScheduler()
            telemetry = RecordingTelemetry()
            gateway = TextGateway(
                model, clock, VolatileState(), telemetry, DisabledTools(), SequentialIds(),
                ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=scheduler,
            )
            events = iter(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain the answer."}, self.startup))
            accepted = next(events)
            clock.advance(0.5)
            scheduler.advance(0.5)
            acknowledgement = next(events)
            return clock, events, accepted, acknowledgement, telemetry

        _clock, failure_events, _accepted, failure_acknowledgement, failure_telemetry = acknowledged_events(FailingModel())
        self.assertEqual((failure_acknowledgement.type, failure_acknowledgement.message), ("ack", "Work is continuing."))
        self.assertEqual([(event.type, event.outcome) for event in failure_events], [("error", None), ("terminal", "failed")])
        self.assertEqual([event for event, _fields in failure_telemetry.records if event != "route_selected"], ["acknowledgement", "terminal"])

        deadline_clock, deadline_events, _accepted, deadline_acknowledgement, deadline_telemetry = acknowledged_events(FakeModel())
        self.assertEqual((deadline_acknowledgement.type, deadline_acknowledgement.message), ("ack", "Work is continuing."))
        deadline_clock.advance(30.0)
        timeout_events = list(deadline_events)
        self.assertEqual([(event.type, event.error and event.error["code"], event.outcome) for event in timeout_events], [("error", "model_deadline", None), ("terminal", None, "failed")])
        self.assertEqual([event for event, _fields in deadline_telemetry.records if event != "route_selected"], ["acknowledgement", "terminal"])

    def test_useful_output_cancellation_and_fast_routes_suppress_pending_acknowledgements(self):
        def gateway_for(model):
            return TextGateway(
                model, AdvanceableClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(),
                ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=DeterministicScheduler(),
            )

        fast = gateway_for(FakeModel())
        self.assertEqual([event.type for event in fast.begin_turn(fast.create_session().session_id, {"input": "oriel help"}, self.startup)], ["accepted", "content_delta", "terminal"])

        useful = gateway_for(FakeModel(chunks=("answer",)))
        useful_events = list(useful.begin_turn(useful.create_session().session_id, {"input": "Explain the answer."}, self.startup))
        self.assertNotIn("ack", [event.type for event in useful_events])

        class ProposalThenFinish(FakeModel):
            def __init__(self):
                super().__init__()
                self.release = Event()

            def stream(self, input, cancellation):
                del input, cancellation
                yield ModelProposal({
                    "proposal_version": "1.0", "proposal_id": "proposal-1", "action": "sample_action",
                    "target": "synthetic:sample-target", "arguments": {"values": ["sample"]}, "dry_run": True,
                    "idempotency": "proposal-1", "deadline": "2030-01-02T12:34:00Z",
                    "confirmation": {"required": True, "evidence": None},
                })
                self.release.wait(1)
                yield ModelOutcome("completed")

        proposal_clock = AdvanceableClock()
        proposal_scheduler = DeterministicScheduler()
        proposal_model = ProposalThenFinish()
        proposal_gateway = TextGateway(proposal_model, proposal_clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=proposal_scheduler)
        proposal_events = iter(proposal_gateway.begin_turn(proposal_gateway.create_session().session_id, {"input": "Explain the answer."}, self.startup))
        self.assertEqual(next(proposal_events).type, "accepted")
        self.assertEqual(next(proposal_events).type, "proposal")
        proposal_clock.advance(0.5)
        proposal_scheduler.advance(0.5)
        proposal_model.release.set()
        self.assertEqual([event.type for event in proposal_events], ["terminal"])

        clock = AdvanceableClock()
        scheduler = DeterministicScheduler()
        cancelled = TextGateway(FakeModel(), clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=scheduler)
        pending = iter(cancelled.begin_turn(cancelled.create_session().session_id, {"input": "Explain the answer."}, self.startup))
        accepted = next(pending)
        clock.advance(0.5)
        scheduler.advance(0.5)
        self.assertEqual(cancelled.cancel_request(accepted.request_id)["state"], "cancellation_requested")
        self.assertEqual([event.type for event in pending], ["terminal"])

        class OutcomeOnly(FakeModel):
            def stream(self, input, cancellation):
                del input, cancellation
                yield ModelOutcome("completed")

        terminal_clock = AdvanceableClock()
        terminal_scheduler = DeterministicScheduler()
        terminal = TextGateway(OutcomeOnly(), terminal_clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), scheduler=terminal_scheduler)
        terminal_events = list(terminal.begin_turn(terminal.create_session().session_id, {"input": "Explain the answer."}, self.startup))
        terminal_clock.advance(0.5)
        terminal_scheduler.advance(0.5)
        self.assertEqual([event.type for event in terminal_events], ["accepted", "terminal"])

    def test_cancelled_turn_keeps_capacity_until_terminal_but_reset_allows_a_new_generation(self):
        gateway = TextGateway(FakeModel(), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        first = gateway.create_session()
        second = gateway.create_session()
        first_events = iter(gateway.begin_turn(first.session_id, {"input": "first"}, self.startup))
        first_request = next(first_events).request_id
        self.assertEqual(gateway.cancel_request(first_request)["state"], "cancellation_requested")
        gateway.reset_session(first.session_id)
        replacement = iter(gateway.begin_turn(first.session_id, {"input": "replacement"}, self.startup))
        self.assertEqual(next(replacement).context_generation, 1)
        queued_sessions = [gateway.create_session() for _ in range(8)]
        for queued_session in queued_sessions:
            next(iter(gateway.begin_turn(queued_session.session_id, {"input": "queued"}, self.startup)))
        with self.assertRaises(AdmissionError) as capacity:
            gateway.begin_turn(second.session_id, {"input": "blocked"}, self.startup)
        self.assertEqual(capacity.exception.status, 429)

    def test_queued_turn_is_accepted_cancelled_without_model_work_and_releases_fifo_capacity(self):
        class BlockingModel(FakeModel):
            calls = 0

            def __init__(self):
                super().__init__()
                self.started = Event()
                self.release = Event()
                self.inputs: list[str] = []

            def stream(self, input, cancellation):
                self.inputs.append(input.messages[-1].content)
                self.calls += 1
                self.started.set()
                self.release.wait(1)
                if not cancellation.is_cancelled():
                    yield ModelChunk("reply")
                    yield ModelOutcome("completed")

        model = BlockingModel()
        gateway = TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        sessions = [gateway.create_session() for _ in range(5)]
        streams = [iter(gateway.begin_turn(session.session_id, {"input": text}, self.startup)) for session, text in zip(sessions, ("one", "two", "three", "four", "five"), strict=True)]
        accepted = [next(stream) for stream in streams]
        active = [Thread(target=lambda stream=stream: list(stream), daemon=True) for stream in streams[:2]]
        for thread in active:
            thread.start()
        self.assertTrue(model.started.wait(0.25))
        self.assertEqual(model.calls, 2)
        queued_events: list[object] = []
        queued_thread = Thread(target=lambda: queued_events.extend(streams[2]), daemon=True)
        queued_thread.start()
        started = time.monotonic()
        self.assertEqual(gateway.cancel_request(accepted[2].request_id)["state"], "cancellation_requested")
        queued_thread.join(0.25)
        self.assertFalse(queued_thread.is_alive())
        self.assertLess(time.monotonic() - started, 0.25)
        self.assertEqual(model.calls, 2)
        self.assertEqual([(event.type, event.outcome) for event in queued_events], [("terminal", "cancelled")])
        fourth_events: list[object] = []
        fourth_thread = Thread(target=lambda: fourth_events.extend(streams[3]), daemon=True)
        fifth_events: list[object] = []
        fifth_thread = Thread(target=lambda: fifth_events.extend(streams[4]), daemon=True)
        fourth_thread.start()
        fifth_thread.start()
        model.release.set()
        for thread in active:
            thread.join(1)
        deadline = time.monotonic() + 0.25
        while model.calls < 4 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(model.calls, 4)
        fourth_thread.join(1)
        fifth_thread.join(1)
        self.assertEqual(fourth_events[-1].outcome, "completed")
        self.assertEqual(fifth_events[-1].outcome, "completed")
        self.assertEqual(model.inputs[2:], ["four", "five"])

    def test_later_queued_provider_waits_for_an_earlier_queued_provider_to_start(self):
        class CoordinatedModel(FakeModel):
            def __init__(self):
                super().__init__()
                self.initial_started = Event()
                self.release_initial = Event()
                self.fourth_entered = Event()
                self.finish_fourth = Event()
                self.fifth_entered = Event()
                self.finish_fifth = Event()
                self._initial_inputs: list[str] = []

            def stream(self, input, cancellation):
                text = input.messages[-1].content
                if text in {"one", "two"}:
                    self._initial_inputs.append(text)
                    if len(self._initial_inputs) == 2:
                        self.initial_started.set()
                    self.release_initial.wait(1)
                elif text == "four":
                    self.fourth_entered.set()
                    self.finish_fourth.wait(1)
                elif text == "five":
                    self.fifth_entered.set()
                    self.finish_fifth.wait(1)
                return [] if cancellation.is_cancelled() else [ModelOutcome("completed")]

        model = CoordinatedModel()
        gateway = TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        sessions = [gateway.create_session() for _ in range(5)]
        streams = [iter(gateway.begin_turn(session.session_id, {"input": text}, self.startup)) for session, text in zip(sessions, ("one", "two", "three", "four", "five"), strict=True)]
        accepted = [next(stream) for stream in streams]
        active = [Thread(target=lambda stream=stream: list(stream), daemon=True) for stream in streams[:2]]
        for thread in active:
            thread.start()
        self.assertTrue(model.initial_started.wait(0.25))

        cancelled_events: list[object] = []
        cancelled_thread = Thread(target=lambda: cancelled_events.extend(streams[2]), daemon=True)
        cancelled_thread.start()
        self.assertEqual(gateway.cancel_request(accepted[2].request_id)["state"], "cancellation_requested")
        cancelled_thread.join(0.25)
        self.assertFalse(cancelled_thread.is_alive())
        self.assertEqual([(event.type, event.outcome) for event in cancelled_events], [("terminal", "cancelled")])

        fifth_events: list[object] = []
        fifth_thread = Thread(target=lambda: fifth_events.extend(streams[4]), daemon=True)
        fifth_thread.start()
        model.release_initial.set()
        for thread in active:
            thread.join(0.25)
        self.assertFalse(any(thread.is_alive() for thread in active))
        self.assertFalse(model.fifth_entered.wait(0.1))

        fourth_events: list[object] = []
        fourth_thread = Thread(target=lambda: fourth_events.extend(streams[3]), daemon=True)
        fourth_thread.start()
        self.assertTrue(model.fourth_entered.wait(0.25))
        self.assertFalse(model.fifth_entered.is_set())
        model.finish_fourth.set()
        self.assertTrue(model.fifth_entered.wait(0.25))
        model.finish_fifth.set()
        fourth_thread.join(0.25)
        fifth_thread.join(0.25)
        self.assertEqual(fourth_events[-1].outcome, "completed")
        self.assertEqual(fifth_events[-1].outcome, "completed")

    def test_unconsumed_cancelled_queue_entry_does_not_block_later_provider_start(self):
        class BlockingModel(FakeModel):
            def __init__(self):
                super().__init__()
                self.started = Event()
                self.release = Event()
                self.inputs: list[str] = []

            def stream(self, input, cancellation):
                self.inputs.append(input.messages[-1].content)
                self.started.set()
                self.release.wait(1)
                if not cancellation.is_cancelled():
                    yield ModelOutcome("completed")

        model = BlockingModel()
        gateway = TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        sessions = [gateway.create_session() for _ in range(4)]
        streams = [iter(gateway.begin_turn(session.session_id, {"input": text}, self.startup)) for session, text in zip(sessions, ("one", "two", "three", "four"), strict=True)]
        accepted = [next(stream) for stream in streams]
        active = [Thread(target=lambda stream=stream: list(stream), daemon=True) for stream in streams[:2]]
        for thread in active:
            thread.start()
        self.assertTrue(model.started.wait(0.25))
        self.assertEqual(gateway.cancel_request(accepted[2].request_id)["state"], "cancellation_requested")

        later_events: list[object] = []
        later = Thread(target=lambda: later_events.extend(streams[3]), daemon=True)
        later.start()
        model.release.set()
        for thread in active:
            thread.join(1)
        later.join(1)

        self.assertFalse(later.is_alive())
        self.assertEqual(model.inputs, ["one", "two", "four"])
        self.assertEqual(later_events[-1].outcome, "completed")

    def test_expired_queued_turn_fails_without_opening_a_third_model_stream(self):
        class BlockingModel(FakeModel):
            calls = 0

            def __init__(self):
                super().__init__()
                self.started = Event()
                self.release = Event()

            def stream(self, input, cancellation):
                del input
                self.calls += 1
                self.started.set()
                self.release.wait(1)
                if not cancellation.is_cancelled():
                    yield ModelOutcome("completed")

        clock = AdvanceableClock()
        model = BlockingModel()
        gateway = TextGateway(model, clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), turn_deadline_seconds=30)
        sessions = [gateway.create_session() for _ in range(3)]
        streams = [iter(gateway.begin_turn(session.session_id, {"input": "hello"}, self.startup)) for session in sessions]
        for stream in streams:
            self.assertEqual(next(stream).type, "accepted")
        active = [Thread(target=lambda stream=stream: list(stream), daemon=True) for stream in streams[:2]]
        for thread in active:
            thread.start()
        self.assertTrue(model.started.wait(0.25))
        self.assertEqual(model.calls, 2)
        clock.advance(30)
        expired = list(streams[2])
        self.assertEqual([(event.type, event.error and event.error["code"], event.outcome) for event in expired], [("error", "model_deadline", None), ("terminal", None, "failed")])
        self.assertEqual(model.calls, 2)
        model.release.set()
        for thread in active:
            thread.join(1)

    def test_expired_unconsumed_queue_entries_release_admission_capacity(self):
        clock = AdvanceableClock()
        gateway = TextGateway(FakeModel(), clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), turn_deadline_seconds=30)
        active_sessions = [gateway.create_session() for _ in range(2)]
        for session in active_sessions:
            self.assertEqual(next(iter(gateway.begin_turn(session.session_id, {"input": "active"}, self.startup))).type, "accepted")
        queued_sessions = [gateway.create_session() for _ in range(8)]
        queued = [iter(gateway.begin_turn(session.session_id, {"input": "queued"}, self.startup)) for session in queued_sessions]
        accepted = [next(stream) for stream in queued]
        clock.advance(30)
        gateway.expire_sessions()
        for event in accepted:
            self.assertEqual(gateway.request_status(event.request_id)["outcome"], "failed")
        replacement = iter(gateway.begin_turn(queued_sessions[0].session_id, {"input": "replacement"}, self.startup))
        self.assertEqual(next(replacement).type, "accepted")

    def test_total_deadline_fails_once_and_fences_transcript_before_model_work(self):
        clock = AdvanceableClock()

        class LateModel(CountingModel):
            def stream(self, input, cancellation):
                self.calls += 1
                del input, cancellation
                yield ModelChunk("early")
                clock.advance(30)
                yield ModelChunk("late")
                yield ModelOutcome("completed")

        model = LateModel()
        gateway = TextGateway(model, clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), turn_deadline_seconds=30)
        session = gateway.create_session()
        events = iter(gateway.begin_turn(session.session_id, {"input": "hello"}, self.startup))
        self.assertEqual(next(events).type, "accepted")
        terminal_events = list(events)
        self.assertEqual([(event.type, event.content, event.error and event.error["code"], event.outcome) for event in terminal_events], [("content_delta", "early", None, None), ("error", None, "model_deadline", None), ("terminal", None, None, "failed")])
        self.assertEqual(model.calls, 1)
        self.assertEqual(gateway._sessions[session.session_id].transcript, ())

    def test_deadline_wins_over_late_provider_failure_without_disclosing_port_fields(self):
        clock = AdvanceableClock()

        class LateFailureModel(FakeModel):
            def stream(self, input, cancellation):
                del input, cancellation
                clock.advance(30)
                raise ModelOperationFailure("PRIVATE_CODE", "PRIVATE_CATEGORY", "PRIVATE_MESSAGE", False)
                yield ModelOutcome("failed")

        gateway = TextGateway(LateFailureModel(), clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), turn_deadline_seconds=30)
        session = gateway.create_session()
        events = iter(gateway.begin_turn(session.session_id, {"input": "hello"}, self.startup))
        self.assertEqual(next(events).type, "accepted")
        terminal_events = list(events)
        self.assertEqual([(event.type, event.error and event.error["code"], event.outcome) for event in terminal_events], [("error", "model_deadline", None), ("terminal", None, "failed")])
        self.assertNotIn("PRIVATE", str(terminal_events))

    def test_model_operation_failure_uses_the_bounded_public_dependency_envelope(self):
        class PrivateFailureModel(FakeModel):
            def stream(self, input, cancellation):
                del input, cancellation
                raise ModelOperationFailure("PRIVATE_CODE", "PRIVATE_CATEGORY", "PRIVATE_MESSAGE", False)
                yield ModelOutcome("failed")

        gateway = TextGateway(PrivateFailureModel(), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        session = gateway.create_session()
        events = iter(gateway.begin_turn(session.session_id, {"input": "hello"}, self.startup))
        self.assertEqual(next(events).type, "accepted")
        terminal_events = list(events)
        self.assertEqual([(event.type, event.error and event.error["code"], event.error and event.error["category"], event.outcome) for event in terminal_events], [("error", "model_unavailable", "dependency_unavailable", None), ("terminal", None, None, "failed")])
        self.assertNotIn("PRIVATE", str(terminal_events))

    def test_completion_wins_before_cancellation_without_leaving_cancelled_context(self):
        class BlockingTerminalLedger(InMemoryRequestLedger):
            def __init__(self) -> None:
                super().__init__()
                self.entered = Event()
                self.release = Event()

            def mark_terminal(self, request_id, outcome):
                self.entered.set()
                self.release.wait(1)
                super().mark_terminal(request_id, outcome)

        ledger = BlockingTerminalLedger()
        gateway = TextGateway(FakeModel(response="reply"), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), ledger)
        session = gateway.create_session()
        events = iter(gateway.begin_turn(session.session_id, {"input": "hello"}, self.startup))
        request_id = next(events).request_id
        received: list[object] = []
        stream_thread = Thread(target=lambda: received.extend(events), daemon=True)
        stream_thread.start()
        self.assertTrue(ledger.entered.wait(1))
        acknowledgement: list[dict[str, object]] = []
        cancel_thread = Thread(target=lambda: acknowledgement.append(gateway.cancel_request(request_id)), daemon=True)
        cancel_thread.start()
        time.sleep(0.03)
        self.assertEqual(acknowledgement, [])
        ledger.release.set()
        stream_thread.join(1)
        cancel_thread.join(1)
        self.assertEqual(received[-1].outcome, "completed")
        self.assertEqual(acknowledgement, [{"request_id": request_id, "state": "already_terminal", "outcome": "completed"}])
        self.assertEqual([(message.role, message.content) for message in gateway._sessions[session.session_id].transcript], [("user", "hello"), ("assistant", "reply")])

    def test_delivery_serializes_an_existing_content_event_with_cancellation(self):
        gateway = TextGateway(FakeModel(response="reply"), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        session = gateway.create_session()
        events = iter(gateway.begin_turn(session.session_id, {"input": "hello"}, self.startup))
        request_id = next(events).request_id
        content = next(events)
        entered = Event()
        release = Event()
        delivered: list[object] = []
        delivery_thread = Thread(target=lambda: gateway.deliver_stream_event(content, lambda event: (entered.set(), release.wait(1), delivered.append(event))), daemon=True)
        delivery_thread.start()
        self.assertTrue(entered.wait(1))
        acknowledgement: list[dict[str, object]] = []
        cancel_thread = Thread(target=lambda: acknowledgement.append(gateway.cancel_request(request_id)), daemon=True)
        cancel_thread.start()
        time.sleep(0.03)
        self.assertEqual(acknowledgement, [])
        release.set()
        delivery_thread.join(1)
        cancel_thread.join(1)
        self.assertEqual(delivered, [content])
        self.assertEqual(acknowledgement, [{"request_id": request_id, "state": "cancellation_requested"}])
        self.assertEqual([event.type for event in events], ["terminal"])

    def test_ledger_reservation_failure_returns_safe_http_error_without_model_work(self):
        class UnavailableLedger:
            def reserve(self, record):
                del record
                raise RequestLedgerUnavailable()

            def mark_terminal(self, request_id, outcome):
                del request_id, outcome
                raise AssertionError("terminal transition must not be attempted")

            def lookup(self, request_id, now):
                del request_id, now
                raise RequestLedgerUnavailable()

            def recover_interrupted(self):
                raise AssertionError("recovery is not part of admission")

        model = CountingModel()
        server = self.with_server(model, UnavailableLedger())
        session = self.create_session(server)
        connection, response = self.turn_response(server, str(session["session_id"]), b'{"input":"hello"}')
        try:
            self.assertEqual(response.status, 503)
            self.assertEqual(json.loads(response.read())["error"], {"code": "request_ledger_unavailable", "category": "dependency_unavailable", "message": "Request status storage is unavailable.", "retryable": True})
            self.assertEqual(model.calls, 0)
        finally:
            connection.close()

        status_connection = self.connection(server)
        try:
            status_connection.request("GET", "/v1/requests/unknown-request")
            status_response = status_connection.getresponse()
            self.assertEqual(status_response.status, 503)
            self.assertEqual(json.loads(status_response.read())["error"]["category"], "dependency_unavailable")
        finally:
            status_connection.close()

    def test_terminal_storage_failure_still_emits_one_failed_terminal_and_releases_request(self):
        class FailingTerminalLedger(InMemoryRequestLedger):
            def mark_terminal(self, request_id, outcome):
                del request_id, outcome
                raise RequestLedgerUnavailable()

        gateway = TextGateway(FakeModel(), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), FailingTerminalLedger())
        session = gateway.create_session()
        events = list(gateway.begin_turn(session.session_id, {"input": "hello"}, self.startup))
        self.assertEqual(events[-1].outcome, "failed")
        self.assertEqual(sum(event.type == "terminal" for event in events), 1)
        self.assertEqual(gateway._requests, {})
        self.assertEqual(gateway.request_status(events[0].request_id)["state"], "in_progress")

    def test_lifecycle_cancellation_leaves_terminal_storage_to_the_stream_owner(self):
        class FailingTerminalLedger(InMemoryRequestLedger):
            def mark_terminal(self, request_id, outcome):
                del request_id, outcome
                raise RequestLedgerUnavailable()

        server = self.with_server(FakeModel(delay_seconds=0.2), FailingTerminalLedger())
        session = self.create_session(server)
        connection, response = self.start_turn(server, str(session["session_id"]))
        try:
            self.assertEqual(self.read_frame(response)[0], "accepted")
            reset_connection = self.connection(server)
            try:
                reset_connection.request("POST", f"/v1/sessions/{session['session_id']}/reset")
                reset_response = reset_connection.getresponse()
                self.assertEqual(reset_response.status, 200)
                reset_response.read()
            finally:
                reset_connection.close()
            terminal = self.read_frame(response)
            self.assertEqual((terminal[0], terminal[1]["outcome"]), ("terminal", "failed"))
        finally:
            connection.close()

    def test_restart_recovery_fails_admitted_request_without_rerunning_model(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.sqlite3"
            first_model = CountingModel()
            first_ledger = SQLiteRequestLedger(path)
            first = TextGateway(first_model, FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), first_ledger)
            session = first.create_session()
            pending = first.begin_turn(session.session_id, {"input": "hello"}, self.startup)
            request_id = next(iter(pending)).request_id
            self.assertEqual(first_model.calls, 0)
            first_ledger.close()

            recovered_model = CountingModel()
            recovered_ledger = SQLiteRequestLedger(path)
            self.addCleanup(recovered_ledger.close)
            recovered = TextGateway(recovered_model, FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), recovered_ledger)
            recovered.recover_interrupted_requests()
            status = recovered.request_status(request_id)
            self.assertEqual((status["state"], status["outcome"]), ("terminal", "failed"))
            self.assertEqual(recovered_model.calls, 0)

    def test_explicit_fixture_outcomes_have_one_terminal_and_safe_error(self):
        outcomes = {
            "completed": ["accepted", "content_delta", "terminal"],
            "denied": ["accepted", "content_delta", "error", "terminal"],
            "failed": ["accepted", "content_delta", "error", "terminal"],
            "outcome_unknown": ["accepted", "content_delta", "error", "terminal"],
        }
        for outcome, expected_types in outcomes.items():
            with self.subTest(outcome=outcome):
                server = self.with_server(FakeModel(chunks=("ordinary prose",), outcome=outcome))
                session = self.create_session(server)
                connection, response = self.start_turn(server, str(session["session_id"]))
                try:
                    frames = []
                    while not frames or frames[-1][0] != "terminal":
                        frames.append(self.read_frame(response))
                    self.assertEqual([kind for kind, _payload in frames], expected_types)
                    self.assertEqual(frames[-1][1]["outcome"], outcome)
                    self.assertEqual(sum(kind == "terminal" for kind, _payload in frames), 1)
                    if outcome != "completed":
                        error = frames[-2][1]["error"]
                        self.assertEqual(set(error), {"code", "category", "message", "retryable", "request_id", "session_id", "trace_id"})
                        self.assertEqual(error["request_id"], frames[0][1]["request_id"])
                        self.assertEqual(error["session_id"], frames[0][1]["session_id"])
                        self.assertEqual(error["trace_id"], frames[0][1]["trace_id"])
                finally:
                    connection.close()

    def test_missing_model_outcome_is_uncertain_and_exception_is_safe(self):
        for model, expected_code, expected_outcome in (
            (MissingOutcomeModel(), "missing_outcome", "outcome_unknown"),
            (ExplodingModel(), "model_failure", "failed"),
        ):
            with self.subTest(model=type(model).__name__):
                server = self.with_server(model)
                session = self.create_session(server)
                connection, response = self.start_turn(server, str(session["session_id"]))
                try:
                    frames = []
                    while not frames or frames[-1][0] != "terminal":
                        frames.append(self.read_frame(response))
                    self.assertEqual(frames[-2][0], "error")
                    self.assertEqual(frames[-2][1]["error"]["code"], expected_code)
                    self.assertEqual(frames[-1][1]["outcome"], expected_outcome)
                    self.assertNotIn("private provider detail", json.dumps(frames[-2][1]))
                finally:
                    connection.close()

    def test_invalid_and_missing_turns_are_safe_preaccept_errors(self):
        server = self.with_server(FakeModel())
        connection = self.connection(server)
        try:
            connection.request("POST", "/v1/sessions/missing/turns", body=b'{"input":"hello"}', headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            self.assertEqual(response.status, 404)
            self.assertEqual(json.loads(response.read())["error"]["category"], "conflict_or_expired_reference")
            session = self.create_session(server)
            connection.request("POST", f"/v1/sessions/{session['session_id']}/turns", body=b'{"input":""}')
            response = connection.getresponse()
            self.assertEqual(response.status, 400)
            self.assertEqual(json.loads(response.read())["error"], {"code": "invalid_turn", "category": "invalid_input", "message": "Turn input is invalid.", "retryable": False})
        finally:
            connection.close()

    def test_decoder_context_and_framing_failures_are_safe(self):
        server = self.with_server(FakeModel())
        session = self.create_session(server)
        invalid_bodies = (
            b'{"input":"first","input":"second"}',
            b'{"input":"hello","context":[{"role":"system","content":"no"}]}',
            b'{"input":"hello","context":[' + b','.join(b'{"role":"user","content":"x"}' for _ in range(33)) + b']}',
            b'{"input":"hello","context":[' + b','.join(b'{"role":"user","content":"' + b'x' * 16384 + b'"}' for _ in range(5)) + b']}',
            b'{"input":' + b'[' * 1200 + b']' * 1200 + b'}',
        )
        for body in invalid_bodies:
            with self.subTest(body=body[:32]):
                connection, response = self.turn_response(server, str(session["session_id"]), body)
                try:
                    self.assertEqual(response.status, 400)
                    self.assertEqual(json.loads(response.read())["error"]["category"], "invalid_input")
                finally:
                    connection.close()
        for headers in ({"Content-Type": "text/plain"}, {"Content-Type": "application/json; charset=utf-8", "Transfer-Encoding": "chunked"}):
            with self.subTest(headers=headers):
                connection, response = self.turn_response(server, str(session["session_id"]), b'{"input":"hello"}', headers)
                try:
                    self.assertEqual(response.status, 400)
                    self.assertEqual(json.loads(response.read())["error"]["code"], "invalid_turn")
                finally:
                    connection.close()

    def test_admission_limits_conflict_and_unavailable_status(self):
        server = self.with_server(FakeModel(chunks=("slow",), delay_seconds=0.4))
        sessions = [self.create_session(server) for _ in range(10)]
        extra = self.connection(server)
        try:
            extra.request("POST", "/v1/sessions")
            response = extra.getresponse()
            self.assertEqual(response.status, 429)
            self.assertEqual(json.loads(response.read())["error"]["category"], "overload")
        finally:
            extra.close()
        first_connection, first_response = self.start_turn(server, str(sessions[0]["session_id"]))
        second_connection = None
        third_connection = None
        try:
            self.assertEqual(self.read_frame(first_response)[0], "accepted")
            second_connection, second_response = self.turn_response(server, str(sessions[0]["session_id"]), b'{"input":"again"}')
            self.assertEqual(second_response.status, 409)
            self.assertEqual(json.loads(second_response.read())["error"]["category"], "conflict_or_expired_reference")
            third_connection, third_response = self.start_turn(server, str(sessions[1]["session_id"]))
            self.assertEqual(self.read_frame(third_response)[0], "accepted")
            capacity_connection, capacity_response = self.turn_response(server, str(sessions[2]["session_id"]), b'{"input":"third"}')
            try:
                self.assertEqual(capacity_response.status, 200)
                queued_kind, queued = self.read_frame(capacity_response)
                self.assertEqual(queued_kind, "accepted")
                self.assertEqual(self.status(server, str(queued["request_id"]))["state"], "in_progress")
            finally:
                capacity_connection.close()
            self.assertEqual(self.status(server, "unknown-request"), {"request_id": "unknown-request", "state": "unavailable"})
        finally:
            first_connection.close()
            if second_connection is not None:
                second_connection.close()
            if third_connection is not None:
                third_connection.close()

    def test_secure_identifier_source_is_url_safe_and_not_sequential(self):
        identifiers = SecureIds()
        values = [identifiers.next_id("request") for _ in range(8)]
        self.assertEqual(len(set(values)), len(values))
        self.assertTrue(all(re.fullmatch(r"oriel-request-[A-Za-z0-9_-]{32}", value) for value in values))
        self.assertTrue(all("fake-request" not in value for value in values))

    def test_disconnect_cancels_the_request_without_a_second_invocation(self):
        model = CountingModel(chunks=("one",) * 20, delay_seconds=0.05)
        server = self.with_server(model)
        session = self.create_session(server)
        connection, response = self.start_turn(server, str(session["session_id"]))
        event_type, accepted = self.read_frame(response)
        self.assertEqual(event_type, "accepted")
        stream_socket = response.fp.raw._sock
        stream_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        response.close()
        connection.close()
        time.sleep(0.3)
        terminal = self.status(server, str(accepted["request_id"]))
        self.assertEqual(terminal["state"], "terminal")
        self.assertEqual(terminal["outcome"], "cancelled")
        self.assertLessEqual(model.calls, 1)

    def test_disconnect_during_silent_provider_work_cancels_before_completed_outcome(self):
        class SilentOutcomeModel(FakeModel):
            def stream(self, input: ModelInput, cancellation: CancellationSignal):
                del input, cancellation
                time.sleep(0.25)
                yield ModelOutcome("completed")

        server = self.with_server(SilentOutcomeModel())
        session = self.create_session(server)
        connection, response = self.start_turn(server, str(session["session_id"]))
        accepted = self.read_frame(response)[1]
        stream_socket = response.fp.raw._sock
        stream_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        response.close()
        connection.close()
        time.sleep(0.4)
        terminal = self.status(server, str(accepted["request_id"]))
        self.assertEqual((terminal["state"], terminal["outcome"]), ("terminal", "cancelled"))

    def test_cancel_route_acknowledges_live_terminal_and_unknown_requests(self):
        server = self.with_server(FakeModel(chunks=("one", "two"), delay_seconds=0.2))
        session = self.create_session(server)
        stream_connection, stream = self.start_turn(server, str(session["session_id"]))
        try:
            accepted = self.read_frame(stream)[1]
            cancel_connection, cancellation = self.cancel(server, str(accepted["request_id"]))
            try:
                self.assertEqual(cancellation.status, 202)
                self.assertEqual(json.loads(cancellation.read()), {"request_id": accepted["request_id"], "state": "cancellation_requested"})
            finally:
                cancel_connection.close()
            repeat_connection, repeated = self.cancel(server, str(accepted["request_id"]))
            try:
                self.assertEqual(repeated.status, 202)
                self.assertEqual(json.loads(repeated.read()), {"request_id": accepted["request_id"], "state": "cancellation_requested"})
            finally:
                repeat_connection.close()
            kind, terminal = self.read_frame(stream)
            self.assertEqual(kind, "terminal")
            self.assertEqual(terminal["outcome"], "cancelled")
            self.assertEqual(terminal["type"], "terminal")
            self.assertEqual(terminal["seq"], 2)
            completed_connection, completed = self.cancel(server, str(accepted["request_id"]))
            try:
                self.assertEqual(completed.status, 202)
                self.assertEqual(json.loads(completed.read()), {"request_id": accepted["request_id"], "state": "already_terminal", "outcome": "cancelled"})
            finally:
                completed_connection.close()
        finally:
            stream_connection.close()
        unknown_connection, unknown = self.cancel(server, "unknown-request")
        try:
            self.assertEqual(unknown.status, 404)
            self.assertEqual(json.loads(unknown.read())["error"]["category"], "conflict_or_expired_reference")
        finally:
            unknown_connection.close()
        malformed_connection, malformed = self.cancel(server, "bad/request")
        try:
            self.assertEqual(malformed.status, 404)
            self.assertEqual(json.loads(malformed.read()), {"error": {"code": "request_unavailable", "category": "conflict_or_expired_reference", "message": "Request is unavailable.", "retryable": False}})
        finally:
            malformed_connection.close()

    def test_cancel_route_returns_a_safe_dependency_error_when_lookup_is_unavailable(self):
        class LookupUnavailableLedger(InMemoryRequestLedger):
            def lookup(self, request_id, now):
                del request_id, now
                raise RequestLedgerUnavailable()

        server = self.with_server(FakeModel(), LookupUnavailableLedger())
        connection, response = self.cancel(server, "unknown-request")
        try:
            self.assertEqual(response.status, 503)
            self.assertEqual(json.loads(response.read()), {"error": {"code": "request_ledger_unavailable", "category": "dependency_unavailable", "message": "Request status storage is unavailable.", "retryable": True}})
        finally:
            connection.close()

    def test_disconnect_before_acceptance_has_no_automatic_replay(self):
        model = CountingModel(chunks=("one", "two"), delay_seconds=0.05)
        server = self.with_server(model)
        session = self.create_session(server)
        connection, response = self.start_turn(server, str(session["session_id"]))
        self.assertEqual(response.status, 200)
        connection.close()

        time.sleep(0.2)

        self.assertEqual(model.calls, 1)

    def test_reset_and_end_routes_only_translate_lifecycle_outcomes(self):
        server = self.with_server(FakeModel())
        session = self.create_session(server)
        connection = self.connection(server)
        try:
            connection.request("POST", f"/v1/sessions/{session['session_id']}/reset")
            reset = connection.getresponse()
            self.assertEqual(reset.status, 200)
            self.assertEqual(json.loads(reset.read()), {"session_id": session["session_id"], "context_generation": 1})
            connection.request("DELETE", f"/v1/sessions/{session['session_id']}")
            deleted = connection.getresponse()
            self.assertEqual(deleted.status, 204)
            self.assertEqual(deleted.read(), b"")
            connection.request("POST", f"/v1/sessions/{session['session_id']}/turns", body=b'{"input":"again"}', headers={"Content-Type": "application/json"})
            unavailable = connection.getresponse()
            self.assertEqual(unavailable.status, 404)
            self.assertEqual(json.loads(unavailable.read())["error"]["category"], "conflict_or_expired_reference")
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
