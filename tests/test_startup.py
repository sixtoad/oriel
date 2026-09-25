from __future__ import annotations

from pathlib import Path
import tempfile
from threading import Barrier, Thread
import unittest

from oriel.adapters.bootstrap import ThreadSafeSynchronization
from oriel.adapters.configuration import DEFAULT_CONFIG_PATH, ResolvedProviderProfile, StaticProfileResolver, activate_startup, select_config_path
from oriel.application.configuration import ActivationConflict, ActivationRejected, ActivationSucceeded, ConfigurationService, READY_PROFILE_LABEL
from oriel.application.startup import UNREADY_CODE
from oriel.domain.configuration import CoreConfig, parse_core_config
from oriel.__main__ import _compose_startup


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
