"""Offline checks for the frozen Oriel text-contract artifacts."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
API = ROOT / "api"
EXAMPLES = API / "examples"
CLI = ROOT / "scripts" / "validate_api_contract.py"
sys.path.insert(0, str(ROOT))
from scripts.validate_api_contract import load_json, validate_directory, validate_fixture, validate_stream, validate_turn


class ApiContractTests(unittest.TestCase):
    def cli(self, directory=EXAMPLES, offline_guard=False):
        if offline_guard:
            guard = (
                "import runpy, socket, sys; "
                "deny = lambda *a, **k: (_ for _ in ()).throw(AssertionError('network attempted')); "
                "socket.socket = socket.create_connection = socket.getaddrinfo = deny; "
                "sys.argv = [sys.argv[1], sys.argv[2]]; runpy.run_path(sys.argv[0], run_name='__main__')"
            )
            command = [sys.executable, "-c", guard, str(CLI), str(directory)]
        else:
            command = [sys.executable, str(CLI), str(directory)]
        return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=10)

    def test_fixture_matrix_is_validated_offline(self):
        result = self.cli(offline_guard=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout, "Valid API contract fixtures: 12 valid; 8 invalid.\n")
        self.assertEqual(result.stderr, "")

    def test_every_matrix_fixture_has_its_expected_result(self):
        for path in sorted((EXAMPLES / "valid").glob("*.json")):
            with self.subTest(path=path.name):
                self.assertEqual(validate_fixture(load_json(path)), [])
        for path in sorted((EXAMPLES / "invalid").glob("*.json")):
            with self.subTest(path=path.name):
                self.assertTrue(validate_fixture(load_json(path)))

    def test_stream_fences_identity_ack_and_terminal_rules(self):
        valid = load_json(EXAMPLES / "valid" / "accepted-stream.json")["events"]
        mutations = [
            (lambda e: e[0].pop("api_version"), "api_version"),
            (lambda e: e.__setitem__(0, dict(e[0], type="content_delta")), "first event"),
            (lambda e: e.append(dict(e[-1], seq=5, type="content_delta", content="late")), "after terminal"),
            (lambda e: e.__setitem__(1, dict(e[1], seq=1)), "strictly increasing"),
            (lambda e: e.__setitem__(2, dict(e[2], session_id="other")), "identity"),
            (lambda e: e.__setitem__(-1, dict(e[-1], type="action_state")), "terminal"),
            (lambda e: e.__setitem__(1, dict(e[1], outcome="completed")), "ack cannot"),
            (lambda e: e.__setitem__(2, dict(e[2], content="x" * 8192)), "serialized event"),
        ]
        for mutate, expected in mutations:
            with self.subTest(expected=expected):
                events = copy.deepcopy(valid)
                mutate(events)
                self.assertIn(expected, "\n".join(validate_stream(events)))

        missing_content = copy.deepcopy(valid)
        missing_content[2].pop("content")
        self.assertIn("requires content", "\n".join(validate_stream(missing_content)))
        over_content = copy.deepcopy(valid)
        over_content[2]["content"] = "x" * 65537
        self.assertIn("cumulative streamed", "\n".join(validate_stream(over_content)))

    def test_closed_documents_and_caps_reject_extensions_or_overflow(self):
        config = load_json(EXAMPLES / "valid" / "config-precedence.json")
        config["config"]["provider"]["secret"] = "PRIVATE_PAYLOAD_MUST_NOT_APPEAR"
        self.assertIn("unknown field", "\n".join(validate_fixture(config)))
        manifest = load_json(EXAMPLES / "valid" / "generic-action.json")
        manifest["manifest"]["actions"][0]["enabled"] = True
        self.assertIn("disabled by default", "\n".join(validate_fixture(manifest)))
        caps = load_json(EXAMPLES / "valid" / "caps.json")
        caps["limits"]["input_bytes"] += 1
        self.assertIn("frozen cap", "\n".join(validate_fixture(caps)))
        optional = load_json(EXAMPLES / "valid" / "invalid-optional-skill.json")
        self.assertEqual(validate_fixture(optional), [])
        proposal = load_json(EXAMPLES / "valid" / "generic-action.json")
        proposal["proposal"]["arguments"]["authority"] = "not-allowed"
        self.assertIn("unknown field", "\n".join(validate_fixture(proposal)))

    def test_error_envelope_and_passive_status_cancellation_semantics(self):
        errors = load_json(EXAMPLES / "valid" / "preaccept-errors.json")
        self.assertEqual(validate_fixture(errors), [])
        errors["responses"][0]["body"]["error"].pop("retryable")
        self.assertIn("required field missing", "\n".join(validate_fixture(errors)))
        status = load_json(EXAMPLES / "valid" / "passive-status.json")
        self.assertEqual(validate_fixture(status), [])
        status["statuses"][0]["outcome"] = "completed"
        self.assertIn("nonterminal status", "\n".join(validate_fixture(status)))
        cancellation = load_json(EXAMPLES / "valid" / "cancellation-race.json")
        self.assertEqual(validate_fixture(cancellation), [])
        cancellation["acknowledgements"][1].pop("outcome")
        self.assertIn("terminal race requires outcome", "\n".join(validate_fixture(cancellation)))
        categories = load_json(EXAMPLES / "valid" / "accepted-error-categories.json")
        self.assertEqual(validate_fixture(categories), [])
        self.assertEqual({stream[1]["error"]["category"] for stream in categories["streams"]},
                         {"policy_denial", "dependency_unavailable", "cancellation", "uncertainty", "internal_failure"})

    def test_turn_utf8_byte_caps_and_canonical_key_order(self):
        self.assertEqual(validate_turn({"input": "é" * 8192}), [])
        self.assertIn("UTF-8 input exceeds cap", "\n".join(validate_turn({"input": "é" * 8193})))
        context = [{"content": "é" * 32760, "role": "user"}]
        self.assertIn("UTF-8 context exceeds cap", "\n".join(validate_turn({"input": "ok", "context": context})))
        fixture = load_json(EXAMPLES / "valid" / "accepted-stream.json")
        document = {"fixture": fixture["fixture"], "events": fixture["events"]}
        self.assertIn("lexicographically", "\n".join(validate_fixture(document)))

    def test_fixture_names_counts_and_empty_directory_are_strict(self):
        with tempfile.TemporaryDirectory() as directory:
            empty = Path(directory)
            self.assertIn("missing required fixture", "\n".join(validate_directory(empty)))
            unexpected = empty / "valid"
            unexpected.mkdir()
            (unexpected / "extra.json").write_text('{"fixture":"extra"}', encoding="utf-8")
            self.assertIn("unexpected fixture file", "\n".join(validate_directory(empty)))

    def test_schemas_are_draft_2020_12_and_public_artifacts_hold_no_private_values(self):
        for path in sorted((API / "schemas").glob("*.json")):
            with self.subTest(schema=path.name):
                self.assertEqual(load_json(path)["$schema"], "https://json-schema.org/draft/2020-12/schema")
        public = "\n".join(path.read_text(encoding="utf-8").lower() for path in API.rglob("*") if path.is_file())
        for forbidden in ("api_key", "password", "bearer "):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, public)
        config = load_json(API / "schemas" / "config.json")
        self.assertEqual(set(config["properties"]["provider"]["properties"]), {"connection_ref"})

    def test_loader_never_echoes_malformed_or_private_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "PRIVATE_PAYLOAD_MUST_NOT_APPEAR.json"
            path.write_text('{"secret":"PRIVATE_PAYLOAD_MUST_NOT_APPEAR","secret":2}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate object key") as raised:
                load_json(path)
            self.assertNotIn("PRIVATE_PAYLOAD_MUST_NOT_APPEAR", str(raised.exception))
            path.write_text('{"number":NaN}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "non-finite"):
                load_json(path)


if __name__ == "__main__":
    unittest.main()
