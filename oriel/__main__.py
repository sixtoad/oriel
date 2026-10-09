"""CLI wiring for the health-only bootstrap server and local self-test."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from http.client import HTTPConnection
from typing import Callable, Mapping
from types import MappingProxyType

from .adapters.bootstrap import CleanupTrigger, DisabledTools, FakeModel, FixedClock, InMemoryRequestLedger, NoopTelemetry, RuntimeClock, SecureIds, SequentialIds, ThreadingScheduler, ThreadingTasks, ThreadSafeSynchronization, UnavailableRequestLedger, VolatileState
from .adapters.action_ledger import SQLiteActionLedger
from .adapters.configuration import OpenAICompatibleProfile, ProfileUnavailable, ResolvedProviderProfile, activate_startup, provider_profile_resolver, select_ha_worker_channel, synthetic_ha_fact_reader
from .adapters.ha_worker import UnixHaWorkerClient
from .adapters.ha_dry_run import HarmlessHaDryRun
from .adapters.http import HealthServer
from .adapters.qwen import EnvironmentCredentialResolver, OpenAICompatibleStreamingModel
from .adapters.request_ledger import SQLiteRequestLedger
from .application.configuration import ConfigurationService
from .application.ports import ActionLedgerUnavailable, CancellationSignal, ModelChunk, ModelInput, ModelMessage, RequestLedgerUnavailable
from .application.text_gateway import TextGateway
from .domain.configuration import API_VERSION, CoreConfig
from .domain.ha_manifest import BUILT_IN_MANIFEST


def _get_health(server: HealthServer, path: str) -> tuple[int, dict[str, str]]:
    host, port = server.address
    connection = HTTPConnection(host, port, timeout=2)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, json.loads(response.read().decode("utf-8"))
    finally:
        connection.close()


def _compose_startup(
    config_path: str | None = None,
    environ: Mapping[str, str] | None = None,
    model_probe: Callable[[object], bool] | None = None,
    optional_ha_probe: Callable[[], bool] | None = None,
    configuration: ConfigurationService | None = None,
):
    """Select the one local profile resolver and activate configuration once."""
    configuration = configuration or ConfigurationService(ThreadSafeSynchronization())
    channel = select_ha_worker_channel(environ)
    try:
        resolver = provider_profile_resolver(environ)
    except ProfileUnavailable:
        # Preserve the config adapter's sanitized unavailable-profile outcome.
        resolver = _UnavailableProfileResolver()
    return activate_startup(
        configuration,
        resolver,
        explicit_path=config_path,
        environ=environ,
        model_probe=model_probe or (lambda profile: _model_ready(profile, environ)),
        optional_ha_probe=optional_ha_probe,
        optional_ha_worker=None if channel is None else UnixHaWorkerClient(channel),
    )


class _UnavailableProfileResolver:
    def resolve(self, connection_ref: str):
        del connection_ref
        raise ProfileUnavailable("provider profile is unavailable")


def _ha_restrictions(config: CoreConfig | None) -> Mapping[str, object] | None:
    """Pass only validated optional-skill restrictions into the gateway."""
    if config is None:
        return None
    skill = config.skills.get("home_assistant")
    if skill is None:
        return None
    return MappingProxyType({key: skill[key] for key in ("targets", "read_fields") if key in skill})


def _model_for_profile(profile: object, environ: Mapping[str, str] | None = None):
    if isinstance(profile, OpenAICompatibleProfile):
        return OpenAICompatibleStreamingModel(profile, EnvironmentCredentialResolver(environ))
    return FakeModel()


def _model_ready(profile: object, environ: Mapping[str, str] | None = None) -> bool:
    """Probe the selected local model through its adapter without publishing provider details."""
    if isinstance(profile, ResolvedProviderProfile):
        return True
    if not isinstance(profile, OpenAICompatibleProfile):
        return False
    try:
        model = OpenAICompatibleStreamingModel(profile, EnvironmentCredentialResolver(environ))
        stream = model.stream(ModelInput((ModelMessage("user", "Reply with exactly OK."),)), CancellationSignal())
        return any(isinstance(item, ModelChunk) and item.content.strip() for item in stream)
    except Exception:
        return False


def run_self_test(config_path: str | None = None) -> dict[str, object]:
    """Exercise local health and the injected fake model without external I/O."""
    startup, _profile = _compose_startup(config_path)
    core = TextGateway(FakeModel(), FixedClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), ha_restrictions=_ha_restrictions(startup.config), fact_reader=synthetic_ha_fact_reader(startup.optional_ha_state), ha_preview=HarmlessHaDryRun(BUILT_IN_MANIFEST))
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
    parser.add_argument("--action-ledger", type=Path, default=Path("oriel-action-ledger.sqlite3"), help="Path to the local payload-free action reservation ledger")
    args = parser.parse_args(argv)
    if args.self_test:
        print(json.dumps(run_self_test(args.config), sort_keys=True, separators=(",", ":")))
        return 0
    synchronization = ThreadSafeSynchronization()
    configuration = ConfigurationService(synchronization)
    startup, _profile = _compose_startup(args.config, configuration=configuration)
    action_ledger = None
    try:
        action_ledger = SQLiteActionLedger(args.action_ledger)
    except ActionLedgerUnavailable:
        action_ledger = None
    owned_ledger = None
    try:
        if action_ledger is None:
            raise ActionLedgerUnavailable()
        ledger = owned_ledger = SQLiteRequestLedger(args.ledger)
        gateway = TextGateway(_model_for_profile(_profile), RuntimeClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SecureIds(), synchronization, ledger, scheduler=ThreadingScheduler(), tasks=ThreadingTasks(), ha_restrictions=_ha_restrictions(startup.config), fact_reader=synthetic_ha_fact_reader(startup.optional_ha_state), ha_preview=HarmlessHaDryRun(BUILT_IN_MANIFEST), action_ledger=action_ledger, configuration=configuration, ha_execution=None if select_ha_worker_channel(os.environ) is None else UnixHaWorkerClient(select_ha_worker_channel(os.environ)))
        gateway.recover_interrupted_requests()
    except (RequestLedgerUnavailable, ActionLedgerUnavailable):
        if action_ledger is not None:
            action_ledger.degrade()
        if owned_ledger is not None:
            owned_ledger.close()
        ledger = UnavailableRequestLedger()
        gateway = TextGateway(_model_for_profile(_profile), RuntimeClock(), VolatileState(), NoopTelemetry(), DisabledTools(), SecureIds(), synchronization, ledger, scheduler=ThreadingScheduler(), tasks=ThreadingTasks(), ha_restrictions=_ha_restrictions(startup.config), fact_reader=synthetic_ha_fact_reader(startup.optional_ha_state), ha_preview=HarmlessHaDryRun(BUILT_IN_MANIFEST), action_ledger=action_ledger, configuration=configuration, ha_execution=None if select_ha_worker_channel(os.environ) is None else UnixHaWorkerClient(select_ha_worker_channel(os.environ)))
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
        if action_ledger is not None:
            action_ledger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
