"""CLI wiring for the health-only bootstrap server and local self-test."""
from __future__ import annotations

import argparse
import json
from http.client import HTTPConnection

from .adapters.bootstrap import CleanupTrigger, DisabledTools, FakeModel, FixedClock, NoopTelemetry, RuntimeClock, SecureIds, SequentialIds, ThreadSafeSynchronization, VolatileState
from .adapters.configuration import ResolvedProviderProfile, StaticProfileResolver, activate_startup
from .adapters.http import HealthServer
from .application.configuration import ConfigurationService
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


def _compose_startup(config_path: str | None = None):
    """Select the one local profile resolver and activate configuration once."""
    configuration = ConfigurationService(ThreadSafeSynchronization())
    resolver = StaticProfileResolver(
        {
            "fake-model": ResolvedProviderProfile("bootstrap-fake"),
            "fake": ResolvedProviderProfile("bootstrap-fake"),
        }
    )
    return activate_startup(configuration, resolver, explicit_path=config_path)


def run_self_test(config_path: str | None = None) -> dict[str, object]:
    """Exercise local health and the injected fake model without external I/O."""
    startup, _profile = _compose_startup(config_path)
    core = TextGateway(FakeModel(), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization())
    server = HealthServer(startup, core)
    server.start()
    try:
        live_status, live = _get_health(server, "/live")
        ready_status, ready = _get_health(server, "/ready")
    finally:
        server.close()
    if live_status != 200 or ready_status != 200 or not startup.ready:
        raise RuntimeError("self-test requires valid configuration")
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
    startup, _profile = _compose_startup(args.config)
    gateway = TextGateway(FakeModel(), RuntimeClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SecureIds(), ThreadSafeSynchronization())
    server = HealthServer(startup, gateway, args.host, args.port)
    cleanup = CleanupTrigger(gateway.expire_sessions)
    cleanup.start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        cleanup.close()
        server.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
