from __future__ import annotations

from pathlib import Path
import tempfile
import time
from threading import Barrier, Thread
import unittest
from unittest.mock import patch

from oriel.adapters.bootstrap import ThreadSafeSynchronization
from oriel.adapters.configuration import DEFAULT_CONFIG_PATH, ResolvedProviderProfile, StaticProfileResolver, activate_startup, select_config_path, select_ha_worker_channel
from oriel.adapters.request_ledger import SQLiteRequestLedger
from oriel.application.configuration import ActivationConflict, ActivationRejected, ActivationSucceeded, ConfigurationService, READY_PROFILE_LABEL
from oriel.application.ports import RequestStatusRecord
from oriel.application.startup import UNREADY_CODE
from oriel.application.text_gateway import AdmissionError
from oriel.domain.configuration import CoreConfig, parse_core_config
from oriel.__main__ import _compose_startup, _ha_restrictions, main


VALID_CONFIG = '{"api_version":"1.0","provider":{"connection_ref":"fake"},"skills":{}}'


def canonical_startup(
    explicit_path: str | Path | None = None,
    environ: dict[str, str] | None = None,
    default_path: Path = DEFAULT_CONFIG_PATH,
):
    state, _profile = activate_startup(
        ConfigurationService(ThreadSafeSynchronization()),
        StaticProfileResolver(
            {
                "fake": ResolvedProviderProfile("test"),
                "fake-model": ResolvedProviderProfile("test"),
            }
        ),
        explicit_path=explicit_path,
        environ=environ,
        default_path=default_path,
    )
    return state


class StartupTests(unittest.TestCase):
    def write(self, directory: Path, name: str, content: str) -> Path:
        path = directory / name
        path.write_text(content, encoding="utf-8")
        return path

    def test_runnable_composition_recovers_closed_ledger_and_fails_closed_when_unavailable(self):
        class CapturingServer:
            instances: list[object] = []

            def __init__(self, startup, gateway, host, port):
                del host, port
                self.startup = startup
                self.gateway = gateway
                self.instances.append(self)

            def serve_forever(self):
                return None

            def close(self):
                return None

        with tempfile.TemporaryDirectory() as directory, patch("oriel.__main__.HealthServer", CapturingServer):
            root = Path(directory)
            config = self.write(root, "config.json", VALID_CONFIG)
            ledger_path = root / "ledger.sqlite3"
            ledger = SQLiteRequestLedger(ledger_path)
            ledger.reserve(RequestStatusRecord("request-1", "session-1", "trace-1", 0, "in_progress", None, "2099-09-26T10:00:00Z", "2099-09-27T10:00:00Z"))
            ledger.close()

            self.assertEqual(main(["--config", str(config), "--ledger", str(ledger_path)]), 0)
            recovered = CapturingServer.instances[-1].gateway.request_status("request-1")
            self.assertEqual((recovered["state"], recovered["outcome"]), ("terminal", "failed"))

            self.assertEqual(main(["--config", str(config), "--ledger", str(root)]), 0)
            with self.assertRaises(AdmissionError) as unavailable:
                CapturingServer.instances[-1].gateway.request_status("request-1")
            self.assertEqual(unavailable.exception.status, 503)

            with patch("oriel.__main__._model_ready", return_value=False):
                self.assertEqual(main(["--config", str(config), "--ledger", str(ledger_path)]), 0)
            self.assertFalse(CapturingServer.instances[-1].startup.ready)
            self.assertEqual(CapturingServer.instances[-1].startup.code, "model_unavailable")

    def test_explicit_path_wins_over_environment_then_packaged_default(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            default = self.write(root, "default.json", VALID_CONFIG)
            env = self.write(root, "env.json", VALID_CONFIG)
            explicit = self.write(root, "explicit.json", VALID_CONFIG)
            self.assertEqual(select_config_path(None, {"ORIEL_CONFIG_PATH": str(env)}, default), env)
            self.assertEqual(select_config_path(explicit, {"ORIEL_CONFIG_PATH": str(env)}, default), explicit)
            self.assertTrue(canonical_startup(None, {}, default).ready)

    def test_selected_missing_or_invalid_config_is_live_but_unready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid_default = self.write(root, "default.json", VALID_CONFIG)
            malformed = self.write(root, "duplicate.json", '{"api_version":"1.0","api_version":"1.0"}')
            for selected in (root / "missing.json", malformed):
                with self.subTest(selected=selected.name):
                    startup = canonical_startup(selected, {}, valid_default)
                    self.assertFalse(startup.ready)
                    self.assertEqual(startup.code, UNREADY_CODE)
                    self.assertNotIn(str(selected), startup.code or "")

    def test_nonfinite_and_bad_core_shapes_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            default = self.write(root, "default.json", VALID_CONFIG)
            for content in (
                '{"api_version":NaN,"provider":{"connection_ref":"fake"},"skills":{}}',
                '{"api_version":"2.0","provider":{"connection_ref":"fake"},"skills":{}}',
                '{"api_version":"1.0","provider":{"connection_ref":"fake","secret":"x"},"skills":{}}',
            ):
                selected = self.write(root, "invalid.json", content)
                self.assertFalse(canonical_startup(selected, {}, default).ready)

    def test_home_assistant_restrictions_are_immutable_and_invalid_expansion_disables_only_that_skill(self):
        config = parse_core_config({
            "api_version": "1.0", "provider": {"connection_ref": "fake"},
            "skills": {"home_assistant": {"enabled": True, "targets": ["synthetic:reviewed-harmless-light"], "read_fields": ["power_state"]}},
        })
        self.assertEqual(config.skills["home_assistant"]["targets"], ("synthetic:reviewed-harmless-light",))
        self.assertEqual(config.skills["home_assistant"]["read_fields"], ("power_state",))
        restrictions = _ha_restrictions(config)
        self.assertEqual(dict(restrictions), {"targets": ("synthetic:reviewed-harmless-light",), "read_fields": ("power_state",)})
        with self.assertRaises(TypeError):
            restrictions["targets"] = ()  # type: ignore[index]
        invalid = parse_core_config({
            "api_version": "1.0", "provider": {"connection_ref": "fake"},
            "skills": {"home_assistant": {"enabled": True, "targets": ["synthetic:added-target"]}},
        })
        self.assertEqual(invalid.disabled_skills, ("home_assistant",))
        self.assertEqual(dict(invalid.skills), {})

    def test_invalid_optional_skill_is_disabled_without_unready_core(self):
        config = parse_core_config(
            {
                "api_version": "1.0",
                "provider": {"connection_ref": "fake"},
                "skills": {
                    "malformed": {"enabled": "yes"},
                    "valid": {"enabled": True, "connection_ref": "optional-ref"},
                    "disabled": {"enabled": False},
                },
            }
        )
        self.assertEqual(config.disabled_skills, ("malformed", "disabled"))
        self.assertEqual(set(config.skills), {"valid"})

    def test_environment_config_does_not_fall_back_when_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            default = self.write(root, "default.json", VALID_CONFIG)
            startup = canonical_startup(None, {"ORIEL_CONFIG_PATH": str(root / "not-there.json")}, default)
            self.assertFalse(startup.ready)

    def test_empty_present_environment_config_does_not_fall_back(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            default = self.write(root, "default.json", VALID_CONFIG)
            self.assertEqual(select_config_path(None, {"ORIEL_CONFIG_PATH": ""}, default), Path("."))
            self.assertFalse(canonical_startup(None, {"ORIEL_CONFIG_PATH": ""}, default).ready)

    def test_parsed_config_and_skills_are_deeply_immutable_and_default_is_packaged(self):
        config = parse_core_config({"api_version": "1.0", "provider": {"connection_ref": "fake"}, "skills": {"one": {"enabled": True}}})
        with self.assertRaises(TypeError):
            config.skills["new"] = {}  # type: ignore[index]
        with self.assertRaises(TypeError):
            config.skills["one"]["enabled"] = False  # type: ignore[index]
        self.assertEqual(DEFAULT_CONFIG_PATH.parent.name, "adapters")
        self.assertTrue(DEFAULT_CONFIG_PATH.is_file())

    def test_activation_is_compare_and_swap_and_keeps_one_immutable_winner(self):
        service = ConfigurationService(ThreadSafeSynchronization())
        initial = service.activate({"api_version": "1.0", "provider": {"connection_ref": "first"}, "skills": {}}, None, "first-profile")
        self.assertIsInstance(initial, ActivationSucceeded)
        barrier = Barrier(2)
        results: list[object] = []

        def activate(ref: str) -> None:
            barrier.wait()
            results.append(service.activate({"api_version": "1.0", "provider": {"connection_ref": ref}, "skills": {}}, 1, f"{ref}-profile"))

        threads = [Thread(target=activate, args=(ref,)) for ref in ("one", "two")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(isinstance(result, ActivationSucceeded) for result in results), 1)
        self.assertEqual(sum(isinstance(result, ActivationConflict) for result in results), 1)
        self.assertEqual(service.active.revision if service.active else None, 2)

    def test_invalid_and_unresolved_candidates_preserve_active_sanitized_view(self):
        service = ConfigurationService(ThreadSafeSynchronization())
        first = service.activate(
            {"api_version": "1.0", "provider": {"connection_ref": "PRIVATE_REFERENCE"}, "skills": {"bad": {"enabled": "yes"}}},
            None,
            "safe-profile",
        )
        self.assertIsInstance(first, ActivationSucceeded)
        rejected = service.activate({"api_version": "wrong", "provider": {}, "skills": {}}, 1, "other-profile")
        self.assertIsInstance(rejected, ActivationRejected)
        self.assertEqual(rejected.active, first.active)
        self.assertEqual(
            first.active.effective.payload() if first.active else None,
            {"api_version": "1.0", "revision": 1, "provider_ready": True, "provider_profile": READY_PROFILE_LABEL, "disabled_skills": ["bad"]},
        )
        self.assertNotIn("connection_ref", first.active.effective.payload() if first.active else {})
        self.assertNotIn("PRIVATE_REFERENCE", str(first.active.effective.payload() if first.active else {}))

        with tempfile.TemporaryDirectory() as directory:
            unresolved = self.write(Path(directory), "unresolved.json", VALID_CONFIG)
            state, profile = activate_startup(
                service,
                StaticProfileResolver({"different": ResolvedProviderProfile("other-profile")}),
                expected_revision=1,
                explicit_path=unresolved,
            )
        self.assertIsNone(profile)
        self.assertTrue(state.ready)
        self.assertEqual(state.revision, 1)
        self.assertIsInstance(state.activation_result, ActivationRejected)

    def test_stale_update_is_typed_without_resolving_a_profile(self):
        service = ConfigurationService(ThreadSafeSynchronization())
        self.assertIsInstance(service.activate({"api_version": "1.0", "provider": {"connection_ref": "fake"}, "skills": {}}, None, "safe"), ActivationSucceeded)

        class Resolver:
            def resolve(self, connection_ref: str) -> ResolvedProviderProfile:
                del connection_ref
                raise AssertionError("stale candidates must not resolve profiles")

        with tempfile.TemporaryDirectory() as directory:
            config = self.write(Path(directory), "config.json", VALID_CONFIG)
            state, profile = activate_startup(service, Resolver(), expected_revision=0, explicit_path=config)
        self.assertIsNone(profile)
        self.assertTrue(state.ready)
        self.assertIsInstance(state.activation_result, ActivationConflict)
        self.assertEqual(state.revision, 1)

    def test_unavailable_profile_on_fresh_service_is_unready_and_sanitized(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write(
                Path(directory),
                "PRIVATE_PATH.json",
                '{"api_version":"1.0","provider":{"connection_ref":"PRIVATE_REFERENCE"},"skills":{}}',
            )
            state, profile = activate_startup(
                ConfigurationService(ThreadSafeSynchronization()),
                StaticProfileResolver({}),
                explicit_path=config,
            )
        self.assertFalse(state.ready)
        self.assertEqual(state.code, UNREADY_CODE)
        self.assertIsNone(state.revision)
        self.assertIsNone(state.effective_view)
        self.assertIsNone(profile)
        self.assertNotIn("PRIVATE_REFERENCE", str(state))

    def test_manually_constructed_config_is_revalidated_and_copied(self):
        service = ConfigurationService(ThreadSafeSynchronization())
        mutable_skills = {"valid": {"enabled": True}}
        candidate = CoreConfig("fake", mutable_skills, ())
        result = service.activate_config(candidate, None, "private resolver material")
        self.assertIsInstance(result, ActivationSucceeded)
        mutable_skills["valid"]["enabled"] = False
        self.assertTrue(result.active.config.skills["valid"]["enabled"] if result.active else False)
        self.assertEqual(result.active.effective.provider_profile if result.active else None, READY_PROFILE_LABEL)

    def test_malformed_manually_constructed_config_is_rejected_without_changing_active(self):
        service = ConfigurationService(ThreadSafeSynchronization())
        first = service.activate({"api_version": "1.0", "provider": {"connection_ref": "fake"}, "skills": {}}, None, "safe")
        malformed = service.activate_config(CoreConfig("fake", {}, (object(),)), 1, "safe")
        self.assertIsInstance(malformed, ActivationRejected)
        self.assertEqual(malformed.active, first.active)

    def test_malformed_static_profile_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write(Path(directory), "config.json", VALID_CONFIG)
            state, profile = activate_startup(
                ConfigurationService(ThreadSafeSynchronization()),
                StaticProfileResolver({"fake": object()}),  # type: ignore[arg-type]
                explicit_path=config,
            )
        self.assertFalse(state.ready)
        self.assertIsNone(profile)
        self.assertIsInstance(state.activation_result, ActivationRejected)

    def test_composition_selects_profile_resolver_and_unavailable_profile_is_unready(self):
        startup, profile = _compose_startup()
        self.assertTrue(startup.ready)
        self.assertEqual(profile, ResolvedProviderProfile("bootstrap-fake"))
        with tempfile.TemporaryDirectory() as directory:
            config = self.write(
                Path(directory),
                "config.json",
                '{"api_version":"1.0","provider":{"connection_ref":"not-selected"},"skills":{}}',
            )
            unavailable, unavailable_profile = _compose_startup(str(config))
        self.assertFalse(unavailable.ready)
        self.assertEqual(unavailable.code, UNREADY_CODE)
        self.assertIsNone(unavailable_profile)

    def test_composition_uses_sanitized_model_and_optional_dependency_probe_states(self):
        model_down, _profile = _compose_startup(model_probe=lambda _profile: False, optional_ha_probe=lambda: False)
        model_up, _profile = _compose_startup(model_probe=lambda _profile: True, optional_ha_probe=lambda: True)
        self.assertFalse(model_down.ready)
        self.assertEqual(model_down.code, "model_unavailable")
        self.assertEqual(model_down.components, {"core": {"state": "ready"}, "model": {"state": "unready", "code": "model_unavailable"}, "ha": {"state": "degraded"}})
        self.assertTrue(model_up.ready)
        self.assertEqual(model_up.components["ha"], {"state": "ready"})

    def test_ha_worker_channel_selection_reads_only_its_non_secret_private_setting(self):
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "worker.sock"
            selected = select_ha_worker_channel({"ORIEL_HA_WORKER_CHANNEL": str(channel), "ORIEL_HA_WORKER_CONNECTION_REF": "CANARY"})
        self.assertEqual(selected, channel)
        self.assertIsNone(select_ha_worker_channel({"ORIEL_HA_WORKER_CHANNEL": "relative.sock"}))
        self.assertIsNone(select_ha_worker_channel({"ORIEL_HA_WORKER_CHANNEL": "\x00invalid"}))

    def test_blocked_probe_makes_startup_unready_without_delaying_liveness(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write(Path(directory), "config.json", VALID_CONFIG)
            started = time.monotonic()
            state, _profile = activate_startup(
                ConfigurationService(ThreadSafeSynchronization()),
                StaticProfileResolver({"fake": ResolvedProviderProfile("test")}),
                explicit_path=config,
                model_probe=lambda _profile: time.sleep(1) or True,
                probe_timeout_seconds=0.01,
            )
        self.assertLess(time.monotonic() - started, 0.25)
        self.assertFalse(state.ready)

    def test_profile_resolution_is_selected_at_restart_not_live_rewired(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write(Path(directory), "config.json", VALID_CONFIG)
            first_service = ConfigurationService(ThreadSafeSynchronization())
            first, first_profile = activate_startup(
                first_service,
                StaticProfileResolver({"fake": ResolvedProviderProfile("first-profile")}),
                explicit_path=config,
            )
            self.assertTrue(first.ready)
            self.assertEqual(first_profile, ResolvedProviderProfile("first-profile"))
            self.assertEqual(first.effective_view and first.effective_view["provider_profile"], READY_PROFILE_LABEL)

            fresh_service = ConfigurationService(ThreadSafeSynchronization())
            restarted, restarted_profile = activate_startup(
                fresh_service,
                StaticProfileResolver({"fake": ResolvedProviderProfile("second-profile")}),
                explicit_path=config,
            )
        self.assertEqual(first.effective_view and first.effective_view["provider_profile"], READY_PROFILE_LABEL)
        self.assertEqual(restarted_profile, ResolvedProviderProfile("second-profile"))
        self.assertEqual(restarted.effective_view and restarted.effective_view["provider_profile"], READY_PROFILE_LABEL)


if __name__ == "__main__":
    unittest.main()
