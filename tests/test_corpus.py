"""Definition validation only: no execution, scoring, providers or measurements."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.validate_corpus import (  # noqa: E402
    CONTRACTS, REQUIRED_CATEGORIES, load_corpus, summary, validate_corpus,
)

CORPUS = ROOT / "evaluation/corpus.json"
CLI = ROOT / "scripts/validate_corpus.py"
SENTINEL = "PRIVATE_PAYLOAD_MUST_NOT_APPEAR"


class CorpusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.corpus = load_corpus(CORPUS)

    def cli(self, path, offline_guard=False):
        if offline_guard:
            guard = (
                "import runpy, socket, sys; "
                "deny = lambda *a, **k: (_ for _ in ()).throw(AssertionError('network attempted')); "
                "socket.socket = socket.create_connection = socket.getaddrinfo = deny; "
                "sys.argv = [sys.argv[1], sys.argv[2]]; "
                "runpy.run_path(sys.argv[0], run_name='__main__')"
            )
            command = [sys.executable, "-c", guard, str(CLI), str(path)]
        else:
            command = [sys.executable, str(CLI), str(path)]
        return subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=10)

    def reject(self, doc, location):
        errors = validate_corpus(doc)
        self.assertTrue(errors)
        self.assertIn(location, "\n".join(errors))
        self.assertNotIn(SENTINEL, "\n".join(errors))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "mutated.json"
            path.write_text(json.dumps(doc), encoding="utf-8")
            result = self.cli(path, offline_guard=True)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(location, result.stdout)
        self.assertNotIn("Traceback", result.stdout + result.stderr)
        self.assertNotIn(SENTINEL, result.stdout + result.stderr)
        self.assertEqual(result.stderr, "")

    def case(self, doc, category):
        return next(case for case in doc["cases"] if case["category"] == category)

    def test_committed_corpus_and_deterministic_offline_cli(self):
        self.assertEqual(validate_corpus(self.corpus), [])
        self.assertEqual({c["category"] for c in self.corpus["cases"]}, REQUIRED_CATEGORIES)
        first = self.cli(CORPUS, offline_guard=True)
        second = self.cli(CORPUS)
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        self.assertEqual(first.stderr, "")
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(first.stdout, summary(self.corpus) + "\n")
        self.assertIn("16 cases; 12 fixtures", first.stdout)
        self.assertIn("label supported: 7", first.stdout)
        self.assertIn("label negative_security: 5", first.stdout)
        self.assertIn("label deferred_voice_skill: 4", first.stdout)

    def test_documented_valid_alternatives(self):
        variants = []
        for arguments in (None, ['on']):
            doc = copy.deepcopy(self.corpus)
            self.case(doc, 'malformed_proposal')['input']['proposal']['arguments'] = arguments
            variants.append(doc)
        doc = copy.deepcopy(self.corpus)
        doc['corpus_id'] = 'alternate-corpus'
        doc['revision'] += 1
        for fixture in doc['fixtures']:
            fixture['id'] += '-alternate'
            fixture['revision'] += 1
        for case in doc['cases']:
            case['id'] += '-alternate'
            case['revision'] += 1
            for ref in case['preconditions']['fixture_refs']:
                ref['id'] += '-alternate'
                ref['revision'] += 1
        variants.append(doc)
        doc = copy.deepcopy(self.corpus)
        extra = copy.deepcopy(self.case(doc, 'chat'))
        extra['id'] = 'additional-chat-case'
        doc['cases'].append(extra)
        doc['revision'] += 1
        variants.append(doc)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'valid-alternative.json'
            for index, doc in enumerate(variants):
                with self.subTest(variant=index):
                    path.write_text(json.dumps(doc), encoding='utf-8')
                    self.assertEqual(validate_corpus(load_corpus(path)), [])
                    result = self.cli(path, offline_guard=True)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertEqual(result.stdout, summary(doc) + '\n')
                    self.assertEqual(result.stderr, '')

    def test_missing_fields_at_each_schema_level(self):
        paths = [(), ("cases", 0), ("cases", 0, "input"),
                 ("cases", 0, "preconditions"), ("cases", 0, "preconditions", "fixture_refs", 0),
                 ("cases", 0, "expected"), ("cases", 0, "expected", "result"),
                 ("fixtures", 0), ("fixtures", 0, "data")]
        for path in paths:
            source = self.corpus
            for part in path:
                source = source[part]
            for key in source:
                with self.subTest(path=path, field=key):
                    doc = copy.deepcopy(self.corpus)
                    target = doc
                    for part in path:
                        target = target[part]
                    del target[key]
                    self.reject(doc, "." + key)

    def test_structure_mutations_via_cli(self):
        changes = [
            (("schema_version",), "2.0"), (("revision",), True), (("synthetic",), 1),
            (("corpus_id",), ""), (("fixtures",), {}), (("cases",), []),
            (("cases", 0), None), (("cases", 0, "id"), SENTINEL),
            (("cases", 0, "revision"), 0), (("cases", 0, "category"), []),
            (("cases", 0, "label"), SENTINEL), (("cases", 0, "input"), SENTINEL),
            (("cases", 0, "input", "text"), " "), (("cases", 0, "input", "kind"), "event"),
            (("cases", 0, "input", "proposal"), {}),
            (("cases", 0, "preconditions", "fixture_refs"), []),
            (("cases", 0, "preconditions", "fixture_refs", 0, "id"), "unknown-fixture"),
            (("cases", 0, "preconditions", "fixture_refs", 0, "revision"), 2),
            (("cases", 0, "preconditions", "requested_state"), "on"),
            (("cases", 0, "expected", "result", "rubric"), []),
            (("cases", 0, "expected", "result", "facts"), {}),
            (("cases", 0, "expected", "result", "state"), {}),
            (("cases", 0, "expected", "live_dispatch_count"), False),
            (("fixtures", 0, "kind"), SENTINEL),
            (("fixtures", 0, "data", "history"), {}),
            (("fixtures", 0, "data", "answer_facts"), []),
        ]
        for path, value in changes:
            with self.subTest(path=path, value=value):
                doc = copy.deepcopy(self.corpus)
                target = doc
                for part in path[:-1]:
                    target = target[part]
                target[path[-1]] = value
                self.reject(doc, "corpus")

    def test_duplicates_unknown_fields_and_coverage(self):
        for mutation, diagnostic in [
            (lambda d: d["cases"].append(copy.deepcopy(d["cases"][0])), "duplicate case ID"),
            (lambda d: d["fixtures"].append(copy.deepcopy(d["fixtures"][0])), "duplicate fixture ID"),
            (lambda d: d["cases"][0].update({SENTINEL: SENTINEL}), "unknown fields"),
            (lambda d: d["cases"].pop(0), "missing required category: chat"),
        ]:
            with self.subTest(diagnostic=diagnostic):
                doc = copy.deepcopy(self.corpus)
                mutation(doc)
                self.reject(doc, diagnostic)

    def test_every_category_enforces_label_route_status_and_zero_dispatch(self):
        for category in CONTRACTS:
            for path, value in [(('label',), SENTINEL), (('expected', 'route'), SENTINEL),
                                (('expected', 'result', 'status'), SENTINEL),
                                (('expected', 'live_dispatch_count'), 1)]:
                with self.subTest(category=category, path=path):
                    doc = copy.deepcopy(self.corpus)
                    target = self.case(doc, category)
                    for part in path[:-1]:
                        target = target[part]
                    target[path[-1]] = value
                    self.reject(doc, "." + path[-1])

    def test_semantic_contradictions(self):
        changes = [
            ('audio_interruption', ('label',), 'supported'),
            ('weather', ('expected', 'route'), 'conversation'),
            ('chat', ('expected', 'result', 'status'), 'unsupported'),
            ('time', ('expected', 'result', 'facts'), ['invented time']),
            ('follow_up', ('preconditions', 'fixture_refs'), [{'id':'chat-context', 'revision':1}]),
            ('weather', ('preconditions', 'fixture_refs'), [{'id':'music-unavailable', 'revision':1}]),
            ('chat', ('preconditions', 'fixture_refs'), [{'id':'fixed-clock', 'revision':1}]),
            ('text_cancellation', ('preconditions', 'fixture_refs'), [{'id':'audio-interrupt-event', 'revision':1}]),
            ('dependency_failure', ('expected', 'result', 'error_code'), None),
            ('audio_interruption', ('expected', 'result', 'limitation'), None),
            ('music', ('expected', 'result', 'limitation'), ''),
            ('light_state', ('preconditions', 'requested_state'), 'toggle'),
            ('light_state', ('preconditions', 'requested_state'), 'off'),
            ('light_state', ('expected', 'side_effects'), []),
            ('light_state', ('expected', 'result', 'state'), {'target':'excluded_test_target', 'state':'on'}),
            ('deliberate_denial', ('expected', 'side_effects'), [{'scope':'live'}]),
            ('malformed_proposal', ('input', 'proposal', 'arguments'), {'state':'on'}),
            ('malformed_proposal', ('input', 'proposal'), []),
            ('malformed_proposal', ('input', 'proposal', 'operation'), 'excluded_operation'),
            ('malformed_proposal', ('input', 'proposal', 'target'), 'excluded_test_target'),
            ('injected_proposal', ('input', 'proposal', 'operation'), 'excluded_operation'),
            ('injected_proposal', ('input', 'proposal', 'target'), 'excluded_test_target'),
            ('excluded_operation', ('input', 'proposal', 'operation'), 'set_light'),
            ('excluded_target', ('input', 'proposal', 'target'), 'test_light_alpha'),
        ]
        for category, path, value in changes:
            with self.subTest(category=category, path=path):
                doc = copy.deepcopy(self.corpus)
                target = self.case(doc, category)
                for part in path[:-1]:
                    target = target[part]
                target[path[-1]] = value
                self.reject(doc, "corpus.cases")

    def test_fixture_semantics(self):
        changes = [
            ('fixed-clock', 'instant', '2030-01-02T00:00:00Z'),
            ('fixed-clock', 'timezone', 'local'),
            ('weather-unavailable', 'available', True),
            ('light-initially-off', 'target', 'real_target'),
            ('light-initially-off', 'execution_boundary', 'live'),
            ('light-initially-off', 'allowed_states', ['toggle']),
            ('synthetic-light-policy', 'trust', 'trusted'),
            ('text-cancel-event', 'status', 'complete'),
            ('backend-timeout', 'fault', None),
        ]
        for fixture_id, key, value in changes:
            with self.subTest(fixture=fixture_id, key=key):
                doc = copy.deepcopy(self.corpus)
                fixture = next(f for f in doc['fixtures'] if f['id'] == fixture_id)
                fixture['data'][key] = value
                self.reject(doc, '.data.' + key)

    def test_loading_errors_are_sanitized(self):
        payloads = [
            ('{"secret": "' + SENTINEL + '",}', 'malformed document at line 1, column'),
            ('{"nested": {"secret": 1, "secret": "' + SENTINEL + '"}}', 'duplicate object key'),
            ('{"secret": NaN}', 'non-finite'), ('{"secret": Infinity}', 'non-finite'),
            ('{"secret": -Infinity}', 'non-finite'), ('{"secret": 1e999}', 'non-finite'),
            ('"' + SENTINEL + '"', 'corpus: expected object'),
            ('[' * 10000 + '0' + ']' * 10000, 'parser limits'),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / (SENTINEL + '.json')
            for payload, diagnostic in payloads:
                with self.subTest(diagnostic=diagnostic):
                    path.write_text(payload, encoding='utf-8')
                    result = self.cli(path, offline_guard=True)
                    self.assertEqual(result.returncode, 1)
                    self.assertIn(diagnostic, result.stdout)
                    self.assertNotIn(SENTINEL, result.stdout + result.stderr)
                    self.assertNotIn('Traceback', result.stdout + result.stderr)
            path.write_bytes(b'\xff')
            self.assertIn('cannot read a UTF-8', self.cli(path).stdout)
            path.unlink()
            missing = self.cli(path)
            self.assertEqual(missing.returncode, 1)
            self.assertIn('cannot read a UTF-8', missing.stdout)
            self.assertNotIn(SENTINEL, missing.stdout + missing.stderr)
            self.assertEqual(self.cli(Path(directory)).returncode, 1)

    def test_validator_function_does_not_open_files(self):
        with patch('builtins.open', side_effect=AssertionError('unexpected file I/O')):
            self.assertEqual(validate_corpus(self.corpus), [])


if __name__ == '__main__':
    unittest.main()
