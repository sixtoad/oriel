"""CLI wiring for the health-only bootstrap server and local self-test."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from http.client import HTTPConnection
from typing import Mapping

from .adapters.bootstrap import CleanupTrigger, DisabledTools, FakeModel, FixedClock, InMemoryRequestLedger, NoopTelemetry, RuntimeClock, SecureIds, SequentialIds, ThreadSafeSynchronization, UnavailableRequestLedger, VolatileState
from .adapters.configuration import OpenAICompatibleProfile, ProfileUnavailable, activate_startup, provider_profile_resolver
from .adapters.http import HealthServer
from .adapters.qwen import EnvironmentCredentialResolver, OpenAICompatibleStreamingModel
from .adapters.request_ledger import SQLiteRequestLedger
from .application.configuration import ConfigurationService
from .application.ports import RequestLedgerUnavailable
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


def _compose_startup(config_path: str | None = None, environ: Mapping[str, str] | None = None):
    """Select the one local profile resolver and activate configuration once."""
    configuration = ConfigurationService(ThreadSafeSynchronization())
    try:
        resolver = provider_profile_resolver(environ)
    except ProfileUnavailable:
        # Preserve the config adapter's sanitized unavailable-profile outcome.
        resolver = _UnavailableProfileResolver()
    return activate_startup(configuration, resolver, explicit_path=config_path, environ=environ)


class _UnavailableProfileResolver:
    def resolve(self, connection_ref: str):
        del connection_ref
        raise ProfileUnavailable("provider profile is unavailable")


def _model_for_profile(profile: object, environ: Mapping[str, str] | None = None):
    if isinstance(profile, OpenAICompatibleProfile):
        return OpenAICompatibleStreamingModel(profile, EnvironmentCredentialResolver(environ))
    return FakeModel()


def run_self_test(config_path: str | None = None) -> dict[str, object]:
    """Exercise local health and the injected fake model without external I/O."""
    startup, _profile = _compose_startup(config_path)
    core = TextGateway(FakeModel(), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger())
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
    parser.add_argument("--ledger", type=Path, default=Path("oriel-request-ledger.sqlite3"), help="Path to the local durable request-status ledger")
    args = parser.parse_args(argv)
    if args.self_test:
        print(json.dumps(run_self_test(args.config), sort_keys=True, separators=(",", ":")))
        return 0
    startup, _profile = _compose_startup(args.config)
    try:
        ledger = SQLiteRequestLedger(args.ledger)
        gateway = TextGateway(_model_for_profile(_profile), RuntimeClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SecureIds(), ThreadSafeSynchronization(), ledger)
        gateway.recover_interrupted_requests()
    except RequestLedgerUnavailable:
        ledger = UnavailableRequestLedger()
        gateway = TextGateway(_model_for_profile(_profile), RuntimeClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SecureIds(), ThreadSafeSynchronization(), ledger)
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
        if isinstance(ledger, SQLiteRequestLedger):
            ledger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
