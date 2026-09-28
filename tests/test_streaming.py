from __future__ import annotations

from http.client import HTTPConnection
import json
from pathlib import Path
import re
import tempfile
import time
import unittest

from oriel.adapters.bootstrap import DisabledTools, FakeModel, FixedClock, NoopTelemetry, SecureIds, SequentialIds, ThreadSafeSynchronization, VolatileState
from oriel.adapters.configuration import load_startup
from oriel.adapters.http import HealthServer
from oriel.application.ports import ModelChunk
from oriel.application.text_gateway import TextGateway


VALID_CONFIG = '{"api_version":"1.0","provider":{"connection_ref":"fake"},"skills":{}}'


class CountingModel(FakeModel):
    calls: int = 0

    def stream(self, text: str):
        self.calls += 1
        yield from super().stream(text)


class MissingOutcomeModel(FakeModel):
    def stream(self, text: str):
        del text
        yield ModelChunk("ordinary prose")


class ExplodingModel(FakeModel):
    def stream(self, text: str):
        del text
        raise RuntimeError("private provider detail")
        yield ModelChunk("")


class StreamingHttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        config = Path(self._directory.name) / "config.json"
        config.write_text(VALID_CONFIG, encoding="utf-8")
        self.startup = load_startup(config)

    def tearDown(self) -> None:
        self._directory.cleanup()

    def with_server(self, model: FakeModel) -> HealthServer:
        gateway = TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization())
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
                self.assertEqual(capacity_response.status, 429)
                self.assertEqual(json.loads(capacity_response.read())["error"]["category"], "overload")
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

    def test_disconnect_leaves_queryable_terminal_status_without_a_second_invocation(self):
        model = CountingModel(chunks=("one", "two"), delay_seconds=0.05)
        server = self.with_server(model)
        session = self.create_session(server)
        connection, response = self.start_turn(server, str(session["session_id"]))
        event_type, accepted = self.read_frame(response)
        self.assertEqual(event_type, "accepted")
        connection.close()
        time.sleep(0.2)
        terminal = self.status(server, str(accepted["request_id"]))
        self.assertEqual(terminal["state"], "terminal")
        self.assertEqual(terminal["outcome"], "completed")
        self.assertEqual(model.calls, 1)


if __name__ == "__main__":
    unittest.main()
