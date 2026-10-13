from __future__ import annotations

import unittest
import time
from threading import Event, Thread
from unittest.mock import patch

from oriel.adapters.bootstrap import AdvanceableClock, CleanupTrigger, DisabledTools, InMemoryRequestLedger, NoopTelemetry, RecordingModel, RuntimeClock, SequentialIds, ThreadSafeSynchronization, VolatileState
from oriel.application.ports import ModelChunk, ModelMessage, ModelProposal
from oriel.application.text_gateway import AdmissionError, IDLE_SESSION_SECONDS, MAX_SESSION_SECONDS, TextGateway
from oriel.application.startup import StartupState
from oriel.domain.configuration import parse_core_config


READY = StartupState(parse_core_config({"api_version": "1.0", "provider": {"connection_ref": "fake"}, "skills": {}}), None)


class SessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = AdvanceableClock()
        self.model = RecordingModel(response="reply")
        self.gateway = TextGateway(self.model, self.clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())

    def stream(self, session_id: str, document: object) -> list[object]:
        return list(self.gateway.begin_turn(session_id, document, READY))

    def test_isolated_followups_and_initial_context_are_structured_and_bounded(self) -> None:
        first = self.gateway.create_session()
        second = self.gateway.create_session()
        self.stream(first.session_id, {"input": "first", "context": [{"role": "user", "content": "alpha"}]})
        self.stream(second.session_id, {"input": "first", "context": [{"role": "user", "content": "beta"}]})
        self.stream(first.session_id, {"input": "follow-up"})
        self.stream(second.session_id, {"input": "follow-up"})
        first_followup = self.model.inputs[2]
        second_followup = self.model.inputs[3]
        self.assertEqual([(message.role, message.content) for message in first_followup.messages], [("user", "alpha"), ("user", "first"), ("assistant", "reply"), ("user", "follow-up")])
        self.assertEqual([(message.role, message.content) for message in second_followup.messages], [("user", "beta"), ("user", "first"), ("assistant", "reply"), ("user", "follow-up")])
        self.assertNotIn("beta", [message.content for message in first_followup.messages])
        with self.assertRaises(AdmissionError) as failure:
            self.gateway.begin_turn(first.session_id, {"input": "replace", "context": [{"role": "user", "content": "other"}]}, READY)
        self.assertEqual(failure.exception.status, 409)
        self.assertEqual(len(self.model.inputs), 4)

    def test_context_caps_reject_without_model_or_transcript_eviction(self) -> None:
        session = self.gateway.create_session()
        too_many = [{"role": "user", "content": "x"} for _ in range(32)]
        with self.assertRaises(AdmissionError) as failure:
            self.gateway.begin_turn(session.session_id, {"input": "turn", "context": too_many}, READY)
        self.assertEqual(failure.exception.status, 400)
        self.assertEqual(failure.exception.code, "context_limit")
        self.assertEqual(self.model.inputs, [])
        self.assertEqual(self.gateway._sessions[session.session_id].transcript, ())

    def test_reset_end_and_expiry_fence_old_streams_with_one_cancelled_terminal(self) -> None:
        for transition in ("reset", "end", "expiry"):
            with self.subTest(transition=transition):
                clock = AdvanceableClock()
                model = RecordingModel(response="reply")
                gateway = TextGateway(model, clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
                session = gateway.create_session()
                events = iter(gateway.begin_turn(session.session_id, {"input": "slow"}, READY))
                accepted = next(events)
                if transition == "reset":
                    reset = gateway.reset_session(session.session_id)
                    self.assertEqual(reset.context_generation, 1)
                elif transition == "end":
                    gateway.end_session(session.session_id)
                else:
                    clock.advance(MAX_SESSION_SECONDS)
                    gateway.expire_sessions()
                self.assertEqual(gateway.request_status(accepted.request_id)["state"], "in_progress")
                if transition == "reset":
                    self.assertEqual(model.inputs, [])
                    replacement = list(gateway.begin_turn(session.session_id, {"input": "again"}, READY))
                    self.assertEqual(replacement[0].context_generation, 1)
                    self.assertEqual([(message.role, message.content) for message in model.inputs[-1].messages], [("user", "again")])
                inputs_before_resume = list(model.inputs)
                remainder = list(events)
                self.assertEqual([event.type for event in remainder], ["terminal"])
                self.assertEqual(remainder[0].outcome, "cancelled")
                self.assertEqual(model.inputs, inputs_before_resume)
                self.assertEqual(gateway.request_status(accepted.request_id)["outcome"], "cancelled")
                if transition != "reset":
                    with self.assertRaises(AdmissionError) as unavailable:
                        gateway.begin_turn(session.session_id, {"input": "again"}, READY)
                    self.assertEqual(unavailable.exception.status, 404)

    def test_explicit_cancel_is_idempotent_and_discards_late_content_and_proposals(self) -> None:
        class LateOutputModel:
            def __init__(self, late_item: ModelChunk | ModelProposal) -> None:
                self.waiting = Event()
                self.release = Event()
                self.cancelled = Event()
                self.late_item = late_item

            def respond(self, text: str) -> str:
                del text
                return "unused"

            def stream(self, input, cancellation):
                del input
                yield ModelChunk("early")
                self.waiting.set()
                self.release.wait(1)
                if cancellation.is_cancelled():
                    self.cancelled.set()
                yield self.late_item

        late_items = (
            ModelChunk("late"),
            ModelProposal({
                "proposal_version": "1.0", "proposal_id": "late", "action": "sample_action",
                "target": "synthetic:sample-target", "arguments": {"values": ["sample"]},
                "dry_run": True, "idempotency": "late", "deadline": "2030-01-02T12:34:00Z",
                "confirmation": {"required": True, "evidence": None},
            }),
        )
        for late_item in late_items:
            with self.subTest(late_item=type(late_item).__name__):
                model = LateOutputModel(late_item)
                gateway = TextGateway(model, self.clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
                session = gateway.create_session()
                events = iter(gateway.begin_turn(session.session_id, {"input": "slow"}, READY))
                accepted = next(events)
                self.assertEqual(next(events).type, "content_delta")

                received: list[object] = []
                worker = Thread(target=lambda: received.extend(events), daemon=True)
                worker.start()
                self.assertTrue(model.waiting.wait(1))
                started = time.monotonic()
                acknowledgement = gateway.cancel_request(accepted.request_id)
                self.assertEqual(acknowledgement, {"request_id": accepted.request_id, "state": "cancellation_requested"})
                self.assertEqual(gateway.cancel_request(accepted.request_id), acknowledgement)
                model.release.set()
                worker.join(1)
                self.assertFalse(worker.is_alive())
                self.assertLess(time.monotonic() - started, 0.25)
                self.assertTrue(model.cancelled.is_set())
                self.assertEqual([event.type for event in received], ["terminal"])
                self.assertEqual(received[0].outcome, "cancelled")
                self.assertEqual(gateway.request_status(accepted.request_id)["outcome"], "cancelled")
                self.assertEqual(gateway._sessions[session.session_id].transcript, ())

    def test_cancel_after_completion_and_unknown_request_are_safe(self) -> None:
        session = self.gateway.create_session()
        events = self.stream(session.session_id, {"input": "done"})
        request_id = events[0].request_id
        self.assertEqual(self.gateway.cancel_request(request_id), {"request_id": request_id, "state": "already_terminal", "outcome": "completed"})
        with self.assertRaises(AdmissionError) as missing:
            self.gateway.cancel_request("unknown-request")
        self.assertEqual((missing.exception.status, missing.exception.category), (404, "conflict_or_expired_reference"))

    def test_expiry_has_idle_and_absolute_lifetime_bounds_and_fresh_gateway_has_no_state(self) -> None:
        session = self.gateway.create_session()
        self.clock.advance(MAX_SESSION_SECONDS)
        self.gateway.expire_sessions()
        with self.assertRaises(AdmissionError) as expired:
            self.gateway.begin_turn(session.session_id, {"input": "again"}, READY)
        self.assertEqual(expired.exception.status, 404)
        fresh = TextGateway(self.model, AdvanceableClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        with self.assertRaises(AdmissionError) as missing:
            fresh.begin_turn(session.session_id, {"input": "again"}, READY)
        self.assertEqual(missing.exception.status, 404)

    def test_runtime_clock_uses_real_monotonic_time(self) -> None:
        with patch("oriel.adapters.bootstrap.time.monotonic", return_value=123.5):
            self.assertEqual(RuntimeClock().monotonic(), 123.5)

    def test_active_turn_does_not_idle_expire_and_completion_renews_idle_time(self) -> None:
        gateway = TextGateway(self.model, self.clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), turn_deadline_seconds=IDLE_SESSION_SECONDS + 1)
        session = gateway.create_session()
        active = iter(gateway.begin_turn(session.session_id, {"input": "slow"}, READY))
        next(active)
        self.clock.advance(IDLE_SESSION_SECONDS)
        gateway.expire_sessions()
        self.assertEqual(gateway.request_status("fake-request-2")["state"], "in_progress")
        list(active)
        self.clock.advance(IDLE_SESSION_SECONDS - 1)
        gateway.expire_sessions()
        self.assertIn(session.session_id, gateway._sessions)
        self.clock.advance(1)
        gateway.expire_sessions()
        self.assertNotIn(session.session_id, gateway._sessions)

    def test_near_limit_response_fails_without_replacing_prior_transcript(self) -> None:
        model = RecordingModel(response="r" * 2000)
        gateway = TextGateway(model, self.clock, VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        session = gateway.create_session()
        prior = tuple(ModelMessage("user" if index % 2 == 0 else "assistant", "x" * 2100) for index in range(30))
        gateway._sessions[session.session_id].transcript = prior
        events = list(gateway.begin_turn(session.session_id, {"input": "y" * 2000}, READY))
        self.assertEqual([event.type for event in events], ["accepted", "content_delta", "error", "terminal"])
        self.assertEqual(events[-2].error["code"], "transcript_limit")
        self.assertEqual(events[-1].outcome, "failed")
        self.assertEqual(gateway._sessions[session.session_id].transcript, prior)

    def test_cleanup_trigger_is_idempotent_and_restartable(self) -> None:
        calls: list[int] = []
        first = Event()
        second = Event()

        def cleanup() -> None:
            calls.append(len(calls))
            (first if len(calls) == 1 else second).set()

        trigger = CleanupTrigger(cleanup, interval_seconds=60)
        trigger.start()
        trigger.start()
        self.assertTrue(first.wait(1))
        trigger.close()
        self.assertEqual(calls, [0])
        trigger.start()
        self.assertTrue(second.wait(1))
        trigger.close()
        self.assertEqual(calls, [0, 1])


if __name__ == "__main__":
    unittest.main()
