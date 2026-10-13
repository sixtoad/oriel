from __future__ import annotations

from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import time
from threading import Thread
import unittest
from urllib.error import URLError

from oriel.adapters.bootstrap import FixedClock, InMemoryRequestLedger, NoopTelemetry, SequentialIds, ThreadSafeSynchronization, VolatileState
from oriel.adapters.configuration import OpenAICompatibleProfile, ProfileUnavailable, provider_profile_resolver
from oriel.adapters.http import HealthServer
from oriel.adapters.qwen import OpenAICompatibleStreamingModel, _decode_openai_sse
from oriel.application.ports import CancellationSignal, ModelInput, ModelMessage, ModelOperationFailure
from oriel.application.startup import StartupState
from oriel.application.text_gateway import TextGateway
from oriel.domain.configuration import parse_core_config
from scripts.validate_api_contract import validate_stream


READY = StartupState(parse_core_config({"api_version": "1.0", "provider": {"connection_ref": "worker"}, "skills": {}}), None)


class RecordingTools:
    def __init__(self) -> None:
        self.dispatched = False

    def dispatch(self, name: str, arguments: dict[str, str]) -> None:
        del name, arguments
        self.dispatched = True


class ScriptedWorker:
    def __init__(self, records: list[bytes], pause_seconds: float = 0.0) -> None:
        self.records = records
        self.pause_seconds = pause_seconds
        self.requests: list[dict[str, object]] = []

        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - HTTP method spelling is prescribed.
                length = int(self.headers["Content-Length"])
                owner.requests.append(json.loads(self.rfile.read(length)))
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.end_headers()
                for index, record in enumerate(owner.records):
                    self.wfile.write(record)
                    self.wfile.flush()
                    if index == 0 and owner.pause_seconds:
                        time.sleep(owner.pause_seconds)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)

    @property
    def request_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/v1/chat/completions"

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._thread.join(timeout=2)
        self._server.server_close()


def data(payload: object) -> bytes:
    return b"data: " + json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n\n"


def completed_stream(*chunks: str) -> list[bytes]:
    records = [data({"choices": [{"delta": {"content": chunk}, "finish_reason": None}]}) for chunk in chunks]
    return [*records, data({"choices": [{"delta": {}, "finish_reason": "stop"}]}), b"data: [DONE]\n\n"]


class QwenAdapterTests(unittest.TestCase):
    def worker_server(self, records: list[bytes], pause_seconds: float = 0.0) -> ScriptedWorker:
        worker = ScriptedWorker(records, pause_seconds)
        worker.start()
        self.addCleanup(worker.close)
        return worker

    def gateway_server(self, worker: ScriptedWorker, tools: RecordingTools | None = None) -> HealthServer:
        profile = OpenAICompatibleProfile(worker.request_url, "qwen-test-revision", 128)
        gateway = TextGateway(
            OpenAICompatibleStreamingModel(profile),
            FixedClock(),
            VolatileState(),
            NoopTelemetry(),
            tools or RecordingTools(),
            SequentialIds(),
            ThreadSafeSynchronization(),
            InMemoryRequestLedger(),
        )
        server = HealthServer(READY, gateway)
        server.start()
        self.addCleanup(server.close)
        return server

    def request(self, server: HealthServer, method: str, path: str, body: bytes | None = None) -> tuple[HTTPConnection, object]:
        host, port = server.address
        connection = HTTPConnection(host, port, timeout=3)
        headers = {"Content-Type": "application/json"} if body is not None else {}
        connection.request(method, path, body=body, headers=headers)
        return connection, connection.getresponse()

    def session_id(self, server: HealthServer) -> str:
        connection, response = self.request(server, "POST", "/v1/sessions")
        try:
            self.assertEqual(response.status, 201)
            return str(json.loads(response.read())["session_id"])
        finally:
            connection.close()

    def read_frame(self, response: object) -> tuple[str, dict[str, object]]:
        event_line = response.fp.readline().decode("utf-8").rstrip("\n")  # type: ignore[attr-defined]
        data_line = response.fp.readline().decode("utf-8").rstrip("\n")  # type: ignore[attr-defined]
        self.assertEqual(response.fp.readline(), b"\n")  # type: ignore[attr-defined]
        return event_line[7:], json.loads(data_line[6:])

    def stream_turn(self, server: HealthServer, session_id: str, text: str) -> list[tuple[str, dict[str, object]]]:
        connection, response = self.request(server, "POST", f"/v1/sessions/{session_id}/turns", json.dumps({"input": text}).encode("utf-8"))
        try:
            self.assertEqual(response.status, 200)
            frames: list[tuple[str, dict[str, object]]] = []
            while not frames or frames[-1][0] != "terminal":
                frames.append(self.read_frame(response))
            return frames
        finally:
            connection.close()

    def test_streams_early_deltas_and_preserves_attributed_followup_transcript(self) -> None:
        worker = self.worker_server(completed_stream("hello", " again"), pause_seconds=0.12)
        server = self.gateway_server(worker)
        session = self.session_id(server)

        connection, response = self.request(server, "POST", f"/v1/sessions/{session}/turns", b'{"input":"first"}')
        self.assertEqual(response.status, 200)
        try:
            first = [self.read_frame(response), self.read_frame(response)]
            first_delta_at = time.monotonic()
            while first[-1][0] != "terminal":
                first.append(self.read_frame(response))
            terminal_at = time.monotonic()
        finally:
            connection.close()
        second = self.stream_turn(server, session, "follow-up")

        self.assertEqual([kind for kind, _ in first], ["accepted", "content_delta", "content_delta", "terminal"])
        self.assertEqual([kind for kind, _ in second], ["accepted", "content_delta", "content_delta", "terminal"])
        self.assertGreater(terminal_at - first_delta_at, 0.1)
        self.assertEqual(worker.requests[0]["stream"], True)
        self.assertEqual(worker.requests[0]["model"], "qwen-test-revision")
        self.assertEqual(worker.requests[1]["messages"], [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "hello again"},
            {"role": "user", "content": "follow-up"},
        ])

    def test_private_registry_selects_only_the_opaque_reference(self) -> None:
        worker = ScriptedWorker([])
        worker.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                registry = Path(directory) / "profiles.json"
                registry.write_text(json.dumps({"profiles": {"worker": {
                    "request_url": worker.request_url,
                    "model_revision": "qwen-test-revision",
                    "generation": {"max_tokens": 128},
                    "credential_ref": "TEST_WORKER_CREDENTIAL",
                }}}), encoding="utf-8")
                resolver = provider_profile_resolver({"ORIEL_PROVIDER_PROFILES_PATH": str(registry)})
                profile = resolver.resolve("worker")
                self.assertIsInstance(profile, OpenAICompatibleProfile)
                self.assertEqual(profile.model_revision, "qwen-test-revision")
                with self.assertRaises(ProfileUnavailable):
                    resolver.resolve("other")
        finally:
            worker.close()

    def test_malformed_worker_frame_is_a_sanitized_terminal_failure(self) -> None:
        worker = self.worker_server([b"data: {not-json}\n\n"])
        server = self.gateway_server(worker)
        frames = self.stream_turn(server, self.session_id(server), "hello")

        self.assertEqual([kind for kind, _ in frames], ["accepted", "error", "terminal"])
        self.assertEqual(frames[-2][1]["error"]["code"], "model_unavailable")
        self.assertEqual(frames[-1][1]["outcome"], "failed")
        self.assertNotIn(worker.request_url, json.dumps(frames))

    def test_complete_valid_tool_call_becomes_proposal_without_dispatch(self) -> None:
        proposal = {
            "proposal_version": "1.0",
            "proposal_id": "proposal-1",
            "action": "sample_action",
            "target": "synthetic:sample-target",
            "arguments": {"values": ["sample"]},
            "dry_run": True,
            "idempotency": "idem-1",
            "deadline": "2030-01-02T12:34:00Z",
            "confirmation": {"required": True, "evidence": None},
        }
        arguments = json.dumps(proposal, separators=(",", ":"))
        split = len(arguments) // 2
        worker = self.worker_server([
            data({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call-1", "type": "function", "function": {"name": "oriel-proposal-v1", "arguments": arguments[:split]}}]}, "finish_reason": None}]}),
            data({"choices": [{"delta": {"tool_calls": [{"index": 0, "function": {"arguments": arguments[split:]}}]}, "finish_reason": None}]}),
            data({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}),
            b"data: [DONE]\n\n",
        ])
        tools = RecordingTools()
        server = self.gateway_server(worker, tools)
        frames = self.stream_turn(server, self.session_id(server), "propose")

        self.assertEqual([kind for kind, _ in frames], ["accepted", "proposal", "terminal"])
        self.assertEqual(frames[1][1]["proposal"], proposal)
        self.assertEqual(frames[-1][1]["outcome"], "completed")
        self.assertFalse(tools.dispatched)
        self.assertEqual(validate_stream([payload for _kind, payload in frames]), [])

    def test_typed_or_control_bearing_ha_candidates_are_denied_without_dispatch(self) -> None:
        for proposal in (
            {"operation": "home_assistant.light.set_power.v1", "target": "synthetic:reviewed-harmless-light", "arguments": {"desired_state": "on"}},
            {"provider_url": "model-selected"},
        ):
            with self.subTest(proposal=proposal):
                arguments = json.dumps(proposal, separators=(",", ":"))
                worker = self.worker_server([
                    data({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call-1", "type": "function", "function": {"name": "oriel-proposal-v1", "arguments": arguments}}]}, "finish_reason": None}]}),
                    data({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}),
                    b"data: [DONE]\n\n",
                ])
                tools = RecordingTools()
                server = self.gateway_server(worker, tools)
                frames = self.stream_turn(server, self.session_id(server), "propose")
                self.assertEqual([kind for kind, _ in frames], ["accepted", "error", "terminal"])
                self.assertEqual(frames[1][1]["error"]["category"], "policy_denial")
                self.assertEqual(frames[-1][1]["outcome"], "denied")
                self.assertFalse(tools.dispatched)

    def test_malformed_generic_tool_call_reaches_application_admission_without_dispatch(self) -> None:
        arguments = json.dumps({"proposal_version": "1.0"}, separators=(",", ":"))
        worker = self.worker_server([
            data({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call-1", "type": "function", "function": {"name": "oriel-proposal-v1", "arguments": arguments}}]}, "finish_reason": None}]}),
            data({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}),
            b"data: [DONE]\n\n",
        ])
        tools = RecordingTools()
        server = self.gateway_server(worker, tools)
        frames = self.stream_turn(server, self.session_id(server), "propose")

        self.assertEqual([kind for kind, _ in frames], ["accepted", "error", "terminal"])
        self.assertEqual(frames[1][1]["error"]["code"], "invalid_proposal")
        self.assertEqual(frames[-1][1]["outcome"], "outcome_unknown")
        self.assertFalse(tools.dispatched)

    def test_mixed_tool_content_fails_safely_without_dispatch(self) -> None:
        worker = self.worker_server([
            data({"choices": [{"delta": {"content": "ordinary", "tool_calls": []}, "finish_reason": None}]}),
        ])
        tools = RecordingTools()
        server = self.gateway_server(worker, tools)
        frames = self.stream_turn(server, self.session_id(server), "hello")

        self.assertEqual([kind for kind, _ in frames], ["accepted", "error", "terminal"])
        self.assertEqual(frames[-1][1]["outcome"], "failed")
        self.assertFalse(tools.dispatched)

    def test_invalid_profile_url_is_a_sanitized_unavailable_registry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            registry = Path(directory) / "profiles.json"
            registry.write_text(json.dumps({"profiles": {"worker": {
                "request_url": "http://[",
                "model_revision": "qwen-test-revision",
                "generation": {"max_tokens": 128},
            }}}), encoding="utf-8")
            with self.assertRaises(ProfileUnavailable):
                provider_profile_resolver({"ORIEL_PROVIDER_PROFILES_PATH": str(registry)})

    def test_cancellation_signal_stops_decoding_without_a_provider_outcome(self) -> None:
        cancellation = CancellationSignal()
        cancellation.cancel()
        self.assertEqual(list(_decode_openai_sse(["[DONE]"], cancellation)), [])

    def test_cancellation_between_records_stops_decoding_before_provider_completion(self) -> None:
        cancellation = CancellationSignal()
        first = json.dumps({"choices": [{"delta": {"content": "early"}, "finish_reason": None}]}, separators=(",", ":"))

        def records():
            yield first
            if cancellation.is_cancelled():
                raise AssertionError("decoder read a provider record after cancellation")
            yield json.dumps({"choices": [{"delta": {}, "finish_reason": "stop"}]}, separators=(",", ":"))
            yield "[DONE]"

        decoded = _decode_openai_sse(records(), cancellation)
        self.assertEqual(next(decoded).content, "early")
        cancellation.cancel()
        self.assertEqual(list(decoded), [])

    def test_provider_connect_timeout_is_bounded_and_sanitized(self) -> None:
        received: list[float] = []

        def timed_out(_request, timeout):
            received.append(timeout)
            raise TimeoutError("private worker address")

        model = OpenAICompatibleStreamingModel(OpenAICompatibleProfile("http://private.invalid/stream", "revision", 1), opener=timed_out)
        with self.assertRaises(ModelOperationFailure) as failure:
            list(model.stream(ModelInput((ModelMessage("user", "hello"),)), CancellationSignal()))
        self.assertEqual(received, [5])
        self.assertEqual((failure.exception.code, failure.exception.category, failure.exception.message), ("model_operation_timeout", "timeout", "Model operation timed out."))
        self.assertNotIn("private", str(failure.exception))

    def test_provider_read_timeout_becomes_a_sanitized_accepted_stream_failure(self) -> None:
        class Headers:
            def get_content_type(self):
                return "text/event-stream"

        class ReadTimeoutResponse:
            status = 200
            headers = Headers()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                del exc_type, exc, traceback
                return False

            def readline(self, limit):
                del limit
                raise URLError(TimeoutError("private provider read"))

        model = OpenAICompatibleStreamingModel(
            OpenAICompatibleProfile("http://private.invalid/stream", "revision", 1),
            opener=lambda _request, timeout: ReadTimeoutResponse(),
        )
        gateway = TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), RecordingTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
        server = HealthServer(READY, gateway)
        server.start()
        self.addCleanup(server.close)
        frames = self.stream_turn(server, self.session_id(server), "hello")
        self.assertEqual([kind for kind, _ in frames], ["accepted", "error", "terminal"])
        self.assertEqual(frames[1][1]["error"], {
            "code": "model_operation_timeout",
            "category": "timeout",
            "message": "Model operation timed out.",
            "retryable": True,
            "request_id": frames[0][1]["request_id"],
            "session_id": frames[0][1]["session_id"],
            "trace_id": frames[0][1]["trace_id"],
        })
        self.assertEqual(frames[2][1]["outcome"], "failed")
        self.assertNotIn("private", json.dumps(frames))


if __name__ == "__main__":
    unittest.main()
