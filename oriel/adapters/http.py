"""HTTP and SSE translation for the application-owned text gateway."""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Iterable

from ..application.startup import StartupState
from ..application.text_gateway import AdmissionError, StreamEvent, TextGateway
from ..domain.configuration import API_VERSION


class _HealthHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False


def live_payload() -> dict[str, str]:
    return {"api_version": API_VERSION, "state": "live"}


def ready_payload(startup: StartupState) -> dict[str, str]:
    if startup.ready:
        return {"api_version": API_VERSION, "state": "ready"}
    return {"api_version": API_VERSION, "state": "unready", "code": startup.code or "config_unavailable"}


def _canonical_json(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _decode_json(raw: bytes) -> object:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        del value
        raise ValueError("non-finite number")

    return json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object, parse_constant=reject_constant)


def _opaque_path_id(value: str) -> bool:
    return bool(value) and len(value) <= 128 and all(character.isascii() and (character.isalnum() or character in "._~-") for character in value)


def _is_json_content_type(value: str | None) -> bool:
    return value is not None and value.split(";", 1)[0].strip().lower() == "application/json"


def _handler(startup: StartupState, gateway: TextGateway | None) -> type[BaseHTTPRequestHandler]:
    class GatewayHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - HTTP method spelling is prescribed.
            if self.path == "/live":
                self._send_json(200, live_payload())
            elif self.path == "/ready":
                self._send_json(200 if startup.ready else 503, ready_payload(startup))
            elif self.path.startswith("/v1/requests/") and gateway is not None and self.path.count("/") == 3 and _opaque_path_id(self.path.rsplit("/", 1)[-1]):
                self._send_json(200, gateway.request_status(self.path.rsplit("/", 1)[-1]))
            else:
                self._not_found()

        def do_POST(self) -> None:  # noqa: N802
            if gateway is None:
                self._not_found()
                return
            if self.path == "/v1/sessions":
                try:
                    self._send_json(201, gateway.create_session().payload())
                except AdmissionError as failure:
                    self._send_admission_error(failure)
                return
            prefix = "/v1/sessions/"
            suffix = "/turns"
            if self.path.startswith(prefix) and self.path.endswith(suffix):
                session_id = self.path[len(prefix) : -len(suffix)]
                if not _opaque_path_id(session_id):
                    self._not_found()
                    return
                try:
                    if not _is_json_content_type(self.headers.get("Content-Type")) or self.headers.get("Transfer-Encoding") is not None:
                        raise ValueError("invalid turn framing")
                    document = _decode_json(self._read_body())
                    events = gateway.begin_turn(session_id, document, startup)
                except AdmissionError as failure:
                    self._send_admission_error(failure)
                except (RecursionError, UnicodeError, ValueError, json.JSONDecodeError):
                    self._send_invalid_turn()
                else:
                    self._send_stream(events)
                return
            self._not_found()

        def _read_body(self) -> bytes:
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                raise ValueError("invalid content length") from None
            if length < 0 or length > 128 * 1024:
                raise ValueError("invalid content length")
            return self.rfile.read(length)

        def _send_stream(self, events: Iterable[StreamEvent]) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            disconnected = False
            for event in events:
                if disconnected:
                    continue
                try:
                    self._send_frame(event)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    disconnected = True
            self.close_connection = True

        def _send_frame(self, event: StreamEvent) -> None:
            frame = b"event: " + event.type.encode("ascii") + b"\n" + b"data: " + _canonical_json(event.payload()) + b"\n\n"
            self.wfile.write(frame)
            self.wfile.flush()

        def _send_admission_error(self, failure: AdmissionError) -> None:
            self._send_json(failure.status, {"error": {"code": failure.code, "category": failure.category, "message": failure.message, "retryable": failure.retryable}})

        def _send_invalid_turn(self) -> None:
            self._send_json(400, {"error": {"code": "invalid_turn", "category": "invalid_input", "message": "Turn input is invalid.", "retryable": False}})

        def _send_json(self, status: int, payload: object) -> None:
            body = _canonical_json(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _not_found(self) -> None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    return GatewayHandler


class HealthServer:
    """A disposable local health and stream server suitable for CLI and tests."""

    def __init__(self, startup: StartupState, gateway: TextGateway | None = None, host: str = "127.0.0.1", port: int = 0) -> None:
        self._server = _HealthHTTPServer((host, port), _handler(startup, gateway))
        self._thread: Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    def start(self) -> None:
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def serve_forever(self) -> None:
        self._server.serve_forever()

    def close(self) -> None:
        if self._thread is not None:
            self._server.shutdown()
            self._thread.join(timeout=2)
            self._thread = None
        self._server.server_close()
