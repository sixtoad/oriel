"""Offline CLI coverage for the versioned measurement-result format."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "evaluation/fixtures/synthetic-results.json"
GOLDEN = ROOT / "evaluation/fixtures/synthetic-summary.json"
CORPUS = ROOT / "evaluation/corpus.json"
CLI = ROOT / "scripts/score_results.py"
SENTINEL = "PRIVATE_PAYLOAD_MUST_NOT_APPEAR"


class ScoringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = json.loads(RESULTS.read_text(encoding="utf-8"))

    def cli(self, path, corpus=CORPUS, offline_guard=False):
        if offline_guard:
            guard = (
                "import runpy, socket, sys; "
                "deny = lambda *a, **k: (_ for _ in ()).throw(AssertionError('network attempted')); "
                "socket.socket = socket.create_connection = socket.getaddrinfo = deny; "
                "sys.argv = [sys.argv[1], sys.argv[2], '--corpus', sys.argv[3]]; "
                "runpy.run_path(sys.argv[0], run_name='__main__')"
            )
            command = [sys.executable, "-c", guard, str(CLI), str(path), str(corpus)]
        else:
            command = [sys.executable, str(CLI), str(path), "--corpus", str(corpus)]
        return subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=10)

    def reject(self, document, expected):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "mutated.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            result = self.cli(path, offline_guard=True)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(expected, result.stdout)
        self.assertNotIn("Traceback", result.stdout + result.stderr)
        self.assertNotIn(SENTINEL, result.stdout + result.stderr)
        self.assertEqual(result.stderr, "")

    def test_committed_fixture_matches_golden_twice_and_never_uses_network(self):
        first = self.cli(RESULTS, offline_guard=True)
        second = self.cli(RESULTS)
        expected = GOLDEN.read_text(encoding="utf-8")
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        self.assertEqual(first.stderr, "")
        self.assertEqual(first.stdout, expected)
        self.assertEqual(first.stdout, second.stdout)

    def test_groups_denominators_and_unavailable_stages_are_preserved(self):
        result = self.cli(RESULTS)
        summary = json.loads(result.stdout)
        groups = {(group["population"], group["thermal"]): group for group in summary["groups"]}
        self.assertEqual(set(groups), {("text", "cold"), ("text", "warm"), ("voice", "cold"), ("voice", "warm")})
        self.assertEqual(groups[("text", "warm")]["counts"]["failure"], 1)
        self.assertEqual(groups[("text", "warm")]["correctness"], {"numerator": 0, "denominator": 1, "rate": 0.0})
        self.assertEqual(groups[("text", "cold")]["stages"]["first_useful_content"]["p95_ms"], 24)
        self.assertEqual(groups[("voice", "warm")]["counts"]["unsupported"], 1)
        self.assertEqual(groups[("voice", "warm")]["reliability"]["denominator"], 0)
        audio = groups[("voice", "cold")]["stages"]["first_audio"]
        self.assertEqual(audio["observed"], 0)
        self.assertEqual(audio["unavailable_reasons"], {"audio-unavailable": 1})
        acknowledgement = groups[("voice", "warm")]["stages"]["acknowledgement"]
        self.assertEqual(acknowledgement["uncertain"], 1)
        self.assertEqual(acknowledgement["unavailable_reasons"], {})
        self.assertEqual(acknowledgement["uncertainty_reasons"], {"cross-system-event": 1})
        self.assertIsNone(acknowledgement["p50_ms"])

    def test_rejects_structure_semantics_and_nonmonotonic_time(self):
        mutations = [
            (lambda d: d["samples"].append(copy.deepcopy(d["samples"][0])), "duplicate sample ID"),
            (lambda d: d["samples"][0].update({"unknown": SENTINEL}), "unknown fields"),
            (lambda d: d["samples"][0]["case"].update({"id": "unknown-case"}), "unknown corpus case"),
            (lambda d: d["samples"][0]["case"].update({"revision": 99}), "corpus case revision mismatch"),
            (lambda d: d["samples"][0]["stages"]["wake"].update({"uncertainty": "mixed"}), "exactly one stage state"),
            (lambda d: d["samples"][0]["stages"]["wake"].update({"observed_monotonic_ms": 999}), "must be at or after anchor"),
            (lambda d: d["samples"][0]["stages"]["first_audio"].update({"unavailable_reason": SENTINEL}), "expected bounded lowercase hyphenated identifier"),
            (lambda d: d["samples"][3]["stages"]["acknowledgement"].update({"uncertainty": SENTINEL}), "expected bounded lowercase hyphenated identifier"),
            (lambda d: (d["samples"][0].__setitem__("anchor_monotonic_ms", -1.7e308), d["samples"][0]["stages"]["wake"].__setitem__("observed_monotonic_ms", 1.7e308)), "computed offset must be finite"),
            (lambda d: d["samples"][1].update({"assessment": {"expected_correct": None, "observed_correct": None, "side_effect_outcome": "not_assessable"}}), "failure requires boolean expected correctness and observed false"),
            (lambda d: d["samples"][0]["assessment"].update({"observed_correct": None}), "both be booleans or both null"),
            (lambda d: d["samples"][0]["assessment"].update({"side_effect_outcome": "not_assessable"}), "assessable result cannot use not_assessable"),
        ]
        for mutate, expected in mutations:
            with self.subTest(expected=expected):
                document = copy.deepcopy(self.document)
                mutate(document)
                self.reject(document, expected)

    def test_type_invalid_anchor_timestamp_and_case_id_are_sanitized(self):
        mutations = [
            (("anchor_monotonic_ms",), SENTINEL, "expected finite number"),
            (("stages", "wake", "observed_monotonic_ms"), SENTINEL, "expected finite number"),
            (("case", "id"), [], "expected stable lowercase hyphenated identifier"),
        ]
        for path, value, expected in mutations:
            with self.subTest(path=path):
                document = copy.deepcopy(self.document)
                target = document["samples"][0]
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                self.reject(document, expected)

    def test_rejects_duplicate_json_key_and_invalid_custom_corpus(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            duplicate = root / "duplicate.json"
            duplicate.write_text('{"schema_version":"1.0","schema_version":"1.0"}', encoding="utf-8")
            result = self.cli(duplicate, offline_guard=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("duplicate object key", result.stdout)
            self.assertNotIn("Traceback", result.stdout + result.stderr)
            bad_corpus = root / "bad-corpus.json"
            bad_corpus.write_text('{"corpus_id":"oriel-synthetic-baseline","revision":2}', encoding="utf-8")
            result = self.cli(RESULTS, bad_corpus, offline_guard=True)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stdout, "Invalid results: corpus: referenced corpus is invalid\n")

    def test_nearest_rank_percentiles_use_real_cli_output(self):
        document = copy.deepcopy(self.document)
        for sample_id, offset in (("text-cold-percentile-two", 10), ("text-cold-percentile-three", 100)):
            sample = copy.deepcopy(document["samples"][0])
            sample["sample_id"] = sample_id
            sample["stages"]["wake"] = {"observed_monotonic_ms": sample["anchor_monotonic_ms"] + offset}
            document["samples"].append(sample)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "percentiles.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            result = self.cli(path)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        group = next(group for group in json.loads(result.stdout)["groups"] if (group["population"], group["thermal"]) == ("text", "cold"))
        self.assertEqual(group["stages"]["wake"]["p50_ms"], 10)
        self.assertEqual(group["stages"]["wake"]["p95_ms"], 100)

    def test_nonfinite_input_and_captured_or_alternate_corpus_inputs_use_real_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nonfinite = root / "nonfinite.json"
            nonfinite.write_text('{"schema_version": 1e999}', encoding="utf-8")
            result = self.cli(nonfinite)
            self.assertEqual(result.returncode, 1)
            self.assertIn("non-finite numbers", result.stdout)
            self.assertNotIn("Traceback", result.stdout + result.stderr)

            captured = copy.deepcopy(self.document)
            captured["provenance"] = "captured_sanitized"
            captured_path = root / "captured.json"
            captured_path.write_text(json.dumps(captured), encoding="utf-8")
            result = self.cli(captured_path)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(json.loads(result.stdout)["measurement"]["provenance"], "captured_sanitized")

            alternate_corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
            alternate_corpus["corpus_id"] = "alternate-corpus"
            alternate_corpus["revision"] = 3
            alternate_path = root / "alternate-corpus.json"
            alternate_path.write_text(json.dumps(alternate_corpus), encoding="utf-8")
            alternate_results = copy.deepcopy(self.document)
            alternate_results["corpus"] = {"id": "alternate-corpus", "revision": 3}
            alternate_results_path = root / "alternate-results.json"
            alternate_results_path.write_text(json.dumps(alternate_results), encoding="utf-8")
            result = self.cli(alternate_results_path, alternate_path)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
