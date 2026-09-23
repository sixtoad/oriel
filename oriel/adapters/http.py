"""Health-only HTTP server for the bootstrap core."""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from ..application.startup import StartupState
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


def _handler(startup: StartupState) -> type[BaseHTTPRequestHandler]:
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - HTTP method spelling is prescribed.
            if self.path == "/live":
                self._send_json(200, live_payload())
            elif self.path == "/ready":
                self._send_json(200 if startup.ready else 503, ready_payload(startup))
            else:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

        def do_POST(self) -> None:  # noqa: N802
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _send_json(self, status: int, payload: dict[str, str]) -> None:
            body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    return HealthHandler


class HealthServer:
    """A disposable local health server suitable for CLI and tests."""

    def __init__(self, startup: StartupState, host: str = "127.0.0.1", port: int = 0) -> None:
        self._server = _HealthHTTPServer((host, port), _handler(startup))
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
