from __future__ import annotations

from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
from pathlib import Path
from threading import Thread
import unittest
from unittest.mock import patch


_SCRIPT = Path(__file__).parents[1] / "scripts" / "text_client.py"
_SPEC = importlib.util.spec_from_file_location("text_client", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
text_client = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(text_client)


def frame(event_type: str, payload: dict[str, object]) -> bytes:
    return f"event: {event_type}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n".encode("utf-8")


def event(event_type: str, request_id: str = "req-1", **extra: object) -> dict[str, object]:
    return {
        "api_version": "1.0",
        "type": event_type,
        "request_id": request_id,
        "session_id": "ses-1",
        "trace_id": "tr-1",
        "context_generation": 0,
        "seq": 1,
        **extra,
    }


class ScriptedServer:
    def __init__(self, responses: list[tuple[int, bytes, str]]) -> None:
        self.responses = deque(responses)
        self.records: list[tuple[str, str, bytes, dict[str, str]]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802
                self.respond()

            def do_POST(self) -> None:  # noqa: N802
                self.respond()

            def respond(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                outer.records.append((self.command, self.path, self.rfile.read(length), dict(self.headers.items())))
                status, body, content_type = outer.responses.popleft()
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_unused: object) -> None:
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()


def json_response(payload: object, status: int = 200) -> tuple[int, bytes, str]:
    return status, json.dumps(payload).encode("utf-8"), "application/json"


class TrackingWriter(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.operations: list[tuple[str, str]] = []

    def write(self, value: str) -> int:
        self.operations.append(("write", value))
        return super().write(value)

    def flush(self) -> None:
        self.operations.append(("flush", ""))
        super().flush()


class TextClientTests(unittest.TestCase):
    def run_client(self, server: ScriptedServer, *arguments: str) -> tuple[int, str, str]:
        output, errors = io.StringIO(), io.StringIO()
        result = text_client.main(["--endpoint", server.endpoint, *arguments], output, errors)
        return result, output.getvalue(), errors.getvalue()

    def test_start_renders_ids_ack_deltas_and_terminal_incrementally(self):
        stream = b"".join(
            [
                frame("accepted", event("accepted", future_hint="compatible")),
                frame("ack", event("ack", message="Work is continuing.", seq=2)),
                frame("content_delta", event("content_delta", content="hello", seq=3)),
                frame("content_delta", event("content_delta", content=" world", seq=4)),
                frame("terminal", event("terminal", outcome="completed", seq=5)),
            ]
        )
        with ScriptedServer([json_response({"session_id": "ses-1", "context_generation": 0}, 201), (200, stream, "text/event-stream")]) as server:
            writer, errors = TrackingWriter(), io.StringIO()
            result = text_client.main(["--endpoint", server.endpoint, "start", "hello"], writer, errors)
            output = writer.getvalue()
        self.assertEqual(result, 0)
        self.assertEqual(errors.getvalue(), "")
        self.assertIn("accepted: session_id=ses-1 request_id=req-1 trace_id=tr-1", output)
        self.assertIn("acknowledgement: Work is continuing.", output)
        self.assertIn("hello world\nterminal: outcome=completed", output)
        self.assertEqual([(method, path) for method, path, _body, _headers in server.records], [("POST", "/v1/sessions"), ("POST", "/v1/sessions/ses-1/turns")])
        self.assertEqual(json.loads(server.records[1][2]), {"input": "hello"})
        self.assertEqual(server.records[1][3]["Accept"], "text/event-stream")
        hello_write = writer.operations.index(("write", "hello"))
        terminal_write = writer.operations.index(("write", "terminal: outcome=completed\n"))
        self.assertIn(("flush", ""), writer.operations[hello_write + 1 : terminal_write])

    def test_structured_outcomes_never_promote_a_proposal_to_an_action(self):
        stream = b"".join(
            [
                frame("accepted", event("accepted")),
                frame("proposal", event("proposal", seq=2, proposal={"proposal_id": "prop-1", "action": "synthetic_action", "target": "synthetic:target", "dry_run": True})),
                frame("error", event("error", seq=3, error={"code": "policy_denied", "category": "policy_denial", "retryable": False, "message": "denied"})),
                frame("terminal", event("terminal", seq=4, outcome="denied")),
            ]
        )
        with ScriptedServer([(200, stream, "text/event-stream")]) as server:
            result, output, _errors = self.run_client(server, "continue", "ses-1", "do a protected action")
        self.assertEqual(result, 0)
        self.assertIn("proposal: UNTRUSTED DRY-RUN=True proposal_id=prop-1", output)
        self.assertIn("error: code=policy_denied category=policy_denial retryable=False message=denied", output)
        self.assertIn("terminal: outcome=denied", output)
        self.assertNotIn("completed action", output)

    def test_validation_preview_renders_its_simulated_or_limited_meaning(self):
        stream = b"".join(
            [
                frame("accepted", event("accepted")),
                frame("validation", event("validation", seq=2, preview={"status": "simulated", "operation": "home_assistant.light.set_power.v1", "target": "synthetic:reviewed-harmless-light", "manifest_revision": "home_assistant.harmless_light.v1"})),
                frame("validation", event("validation", seq=3, preview={"status": "unavailable", "reason": "adapter_unavailable"})),
                frame("terminal", event("terminal", seq=4, outcome="completed")),
            ]
        )
        with ScriptedServer([(200, stream, "text/event-stream")]) as server:
            result, output, _errors = self.run_client(server, "continue", "ses-1", "preview")
        self.assertEqual(result, 0)
        self.assertIn("preview: simulated operation=home_assistant.light.set_power.v1", output)
        self.assertIn("preview: unavailable reason=adapter_unavailable", output)

    def test_typed_terminal_outcomes_are_displayed_from_their_structured_field(self):
        outcomes = ("failed", "cancelled", "outcome_unknown")
        responses = []
        for index, outcome in enumerate(outcomes, start=1):
            request_id = f"req-{index}"
            responses.append((200, b"".join([frame("accepted", event("accepted", request_id=request_id)), frame("terminal", event("terminal", request_id=request_id, seq=2, outcome=outcome))]), "text/event-stream"))
        with ScriptedServer(responses) as server:
            rendered = [self.run_client(server, "continue", "ses-1", "hello") for _outcome in outcomes]
        self.assertEqual([result for result, _output, _errors in rendered], [0, 0, 0])
        for outcome, (_result, output, _errors) in zip(outcomes, rendered):
            self.assertIn(f"terminal: outcome={outcome}", output)

    def test_broken_stream_after_acceptance_uses_one_passive_status_lookup(self):
        stream = frame("accepted", event("accepted", request_id="req-broken"))
        with ScriptedServer([(200, stream, "text/event-stream"), json_response({"request_id": "req-broken", "state": "terminal", "outcome": "failed"})]) as server:
            result, output, _errors = self.run_client(server, "continue", "ses-1", "hello")
        self.assertEqual(result, 0)
        self.assertIn("stream_interrupted: request_id=req-broken", output)
        self.assertIn("status: request_id=req-broken state=terminal outcome=failed", output)
        self.assertEqual([(method, path) for method, path, _body, _headers in server.records], [("POST", "/v1/sessions/ses-1/turns"), ("GET", "/v1/requests/req-broken")])

    def test_broken_stream_before_acceptance_reports_unknown_without_lookup_or_replay(self):
        with ScriptedServer([(200, b"", "text/event-stream")]) as server:
            result, output, _errors = self.run_client(server, "continue", "ses-1", "hello")
        self.assertEqual(result, 1)
        self.assertIn("outcome_unknown: stream ended before accepted; no replay was attempted", output)
        self.assertEqual([(method, path) for method, path, _body, _headers in server.records], [("POST", "/v1/sessions/ses-1/turns")])

    def test_cancel_and_status_render_structured_responses(self):
        with ScriptedServer([
            json_response({"request_id": "req-live", "state": "cancellation_requested"}, 202),
            json_response({"request_id": "req-finished", "state": "already_terminal", "outcome": "cancelled"}, 202),
            json_response({"request_id": "req-progress", "state": "in_progress"}),
            json_response({"request_id": "req-unavailable", "state": "unavailable"}),
        ]) as server:
            cancel_result, cancel_output, _errors = self.run_client(server, "cancel", "req-live")
            terminal_cancel_result, terminal_cancel_output, _errors = self.run_client(server, "cancel", "req-finished")
            status_result, status_output, _errors = self.run_client(server, "status", "req-progress")
            unavailable_result, unavailable_output, _errors = self.run_client(server, "status", "req-unavailable")
        self.assertEqual(cancel_result, 0)
        self.assertIn("cancellation: request_id=req-live state=cancellation_requested", cancel_output)
        self.assertEqual(terminal_cancel_result, 0)
        self.assertIn("cancellation: request_id=req-finished state=already_terminal outcome=cancelled", terminal_cancel_output)
        self.assertEqual(status_result, 0)
        self.assertIn("status: request_id=req-progress state=in_progress", status_output)
        self.assertNotIn("outcome=", status_output)
        self.assertEqual(unavailable_result, 0)
        self.assertIn("status: request_id=req-unavailable state=unavailable", unavailable_output)
        self.assertNotIn("outcome=", unavailable_output)
        self.assertEqual([(method, path) for method, path, _body, _headers in server.records], [("POST", "/v1/requests/req-live/cancel"), ("POST", "/v1/requests/req-finished/cancel"), ("GET", "/v1/requests/req-progress"), ("GET", "/v1/requests/req-unavailable")])

    def test_preaccept_http_error_is_rendered_without_a_replay(self):
        with ScriptedServer([json_response({"error": {"code": "session_unavailable", "category": "conflict_or_expired_reference", "message": "reference is unavailable", "retryable": False}}, 404)]) as server:
            result, output, _errors = self.run_client(server, "continue", "ses-missing", "hello")
        self.assertEqual(result, 1)
        self.assertIn("http_error: status=404 code=session_unavailable category=conflict_or_expired_reference retryable=False", output)
        self.assertEqual([(method, path) for method, path, _body, _headers in server.records], [("POST", "/v1/sessions/ses-missing/turns")])

    def test_dynamic_identifiers_are_encoded_as_single_path_segments(self):
        with ScriptedServer([
            json_response({"request_id": "req/a?b", "state": "cancellation_requested"}, 202),
            (200, frame("accepted", event("accepted", request_id="req-safe")) + frame("terminal", event("terminal", request_id="req-safe", seq=2, outcome="completed")), "text/event-stream"),
        ]) as server:
            cancel_result, _output, _errors = self.run_client(server, "cancel", "req/a?b")
            turn_result, _output, _errors = self.run_client(server, "continue", "ses/a?b", "hello")
        self.assertEqual((cancel_result, turn_result), (0, 0))
        self.assertEqual([(method, path) for method, path, _body, _headers in server.records], [("POST", "/v1/requests/req%2Fa%3Fb/cancel"), ("POST", "/v1/sessions/ses%2Fa%3Fb/turns")])

    def test_successful_non_sse_turn_response_is_a_protocol_error_without_recovery(self):
        with ScriptedServer([json_response({"state": "not-a-stream"})]) as server:
            result, output, _errors = self.run_client(server, "continue", "ses-1", "hello")
        self.assertEqual(result, 1)
        self.assertIn("protocol_error: expected text/event-stream response", output)
        self.assertEqual([(method, path) for method, path, _body, _headers in server.records], [("POST", "/v1/sessions/ses-1/turns")])

    def test_malformed_terminal_is_not_a_successful_completion(self):
        stream = b"".join([frame("accepted", event("accepted", request_id="req-1")), frame("terminal", event("terminal", request_id="req-other", seq=2, outcome="completed"))])
        with ScriptedServer([(200, stream, "text/event-stream"), json_response({"request_id": "req-1", "state": "terminal", "outcome": "failed"})]) as server:
            result, output, _errors = self.run_client(server, "continue", "ses-1", "hello")
        self.assertEqual(result, 0)
        self.assertIn("terminal: malformed event", output)
        self.assertIn("status: request_id=req-1 state=terminal outcome=failed", output)

    def test_control_characters_are_escaped_in_server_values(self):
        stream = b"".join([frame("accepted", event("accepted", trace_id="tr-1\nterminal: outcome=completed")), frame("content_delta", event("content_delta", content="hello\rterminal: outcome=completed", seq=2)), frame("terminal", event("terminal", seq=3, outcome="completed"))])
        with ScriptedServer([(200, stream, "text/event-stream")]) as server:
            result, output, _errors = self.run_client(server, "continue", "ses-1", "hello")
        self.assertEqual(result, 0)
        self.assertIn("trace_id=tr-1\\nterminal: outcome=completed", output)
        self.assertIn("hello\\rterminal: outcome=completed", output)
        self.assertEqual(output.count("terminal: outcome=completed"), 3)

    def test_invalid_endpoint_returns_the_controlled_argument_error(self):
        for endpoint in ("http://host name:8080", "http://localhost:99999", "http://[bad"):
            output, errors = io.StringIO(), io.StringIO()
            result = text_client.main(["--endpoint", endpoint, "status", "req-1"], output, errors)
            self.assertEqual(result, 2)
            self.assertIn("invalid endpoint:", errors.getvalue())

    def test_interrupt_after_acceptance_returns_recovery_instruction_without_traceback(self):
        def interrupt(client: object, session_id: str, text: str) -> int:
            del session_id, text
            client._active_request_id = "req-1"  # type: ignore[attr-defined]
            raise KeyboardInterrupt

        output, errors = io.StringIO(), io.StringIO()
        with patch.object(text_client.TextClient, "_stream_turn", interrupt):
            result = text_client.main(["continue", "ses-1", "hello"], output, errors)
        self.assertEqual(result, 130)
        self.assertIn("interrupted: run status req-1 to recover the authoritative outcome.", output.getvalue())
        self.assertEqual(errors.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
