from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from oriel.config import DEFAULT_CONFIG_PATH, UNREADY_CODE, load_startup, parse_core_config, select_config_path


VALID_CONFIG = '{"api_version":"1.0","provider":{"connection_ref":"fake"},"skills":{}}'


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
            self.assertTrue(load_startup(None, {}, default).ready)

    def test_selected_missing_or_invalid_config_is_live_but_unready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid_default = self.write(root, "default.json", VALID_CONFIG)
            malformed = self.write(root, "duplicate.json", '{"api_version":"1.0","api_version":"1.0"}')
            for selected in (root / "missing.json", malformed):
                with self.subTest(selected=selected.name):
                    startup = load_startup(selected, {}, valid_default)
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
                self.assertFalse(load_startup(selected, {}, default).ready)

    def test_invalid_optional_skill_is_disabled_without_unready_core(self):
        config = parse_core_config(
            {
                "api_version": "1.0",
                "provider": {"connection_ref": "fake"},
                "skills": {
                    "malformed": {"enabled": "yes"},
                    "valid": {"enabled": True, "connection_ref": "optional-ref"},
                },
            }
        )
        self.assertEqual(config.disabled_skills, ("malformed",))
        self.assertEqual(set(config.skills), {"valid"})

    def test_environment_config_does_not_fall_back_when_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            default = self.write(root, "default.json", VALID_CONFIG)
            startup = load_startup(None, {"ORIEL_CONFIG_PATH": str(root / "not-there.json")}, default)
            self.assertFalse(startup.ready)

    def test_empty_present_environment_config_does_not_fall_back(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            default = self.write(root, "default.json", VALID_CONFIG)
            self.assertEqual(select_config_path(None, {"ORIEL_CONFIG_PATH": ""}, default), Path("."))
            self.assertFalse(load_startup(None, {"ORIEL_CONFIG_PATH": ""}, default).ready)

    def test_parsed_config_and_skills_are_deeply_immutable_and_default_is_packaged(self):
        config = parse_core_config({"api_version": "1.0", "provider": {"connection_ref": "fake"}, "skills": {"one": {"enabled": True}}})
        with self.assertRaises(TypeError):
            config.skills["new"] = {}  # type: ignore[index]
        with self.assertRaises(TypeError):
            config.skills["one"]["enabled"] = False  # type: ignore[index]
        self.assertEqual(DEFAULT_CONFIG_PATH.parent.name, "oriel")
        self.assertTrue(DEFAULT_CONFIG_PATH.is_file())


if __name__ == "__main__":
    unittest.main()
