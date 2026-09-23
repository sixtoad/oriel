"""CLI wiring for the health-only bootstrap server and local self-test."""
from __future__ import annotations

import argparse
import json
from http.client import HTTPConnection

from .adapters.bootstrap import DisabledTools, FakeModel, FixedClock, NoopTelemetry, VolatileState
from .adapters.configuration import load_startup
from .adapters.http import HealthServer
from .application.text_gateway import TextGateway
from .domain.configuration import API_VERSION


def _get_health(server: HealthServer, path: str) -> tuple[int, dict[str, str]]:
    host, port = server.address
    connection = HTTPConnection(host, port, timeout=2)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, json.loads(response.read().decode("utf-8"))
    finally:
        connection.close()


def run_self_test(config_path: str | None = None) -> dict[str, object]:
    """Exercise local health and the injected fake model without external I/O."""
    startup = load_startup(config_path)
    server = HealthServer(startup)
    server.start()
    try:
        live_status, live = _get_health(server, "/live")
        ready_status, ready = _get_health(server, "/ready")
    finally:
        server.close()
    if live_status != 200 or ready_status != 200 or not startup.ready:
        raise RuntimeError("self-test requires valid configuration")
    core = TextGateway(FakeModel(), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools())
    turn = core.run_fake_turn("self-test", startup)
    return {"api_version": API_VERSION, "fake_turn": {"text": turn.text}, "live": live, "ready": ready}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Oriel's health-only text bootstrap.")
    parser.add_argument("--config", help="Path to the immutable core configuration")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        print(json.dumps(run_self_test(args.config), sort_keys=True, separators=(",", ":")))
        return 0
    server = HealthServer(load_startup(args.config), args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
