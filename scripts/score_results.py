#!/usr/bin/env python3
"""Validate and score sanitized measurement results offline."""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.validate_corpus import CorpusLoadError, load_corpus, validate_corpus

VERSION = "1.0"
IDENTIFIER = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
STAGES = (
    "wake", "end_of_turn", "final_transcript", "route", "first_model_token",
    "first_useful_content", "acknowledgement", "first_audio", "static_validation",
    "action_completion", "final_audio",
)
POPULATIONS = ("text", "voice")
THERMAL = ("cold", "warm")
TERMINAL = ("success", "failure", "deferred", "unsupported")
SIDE_EFFECTS = ("none", "expected", "unexpected", "not_assessable")


class ResultError(ValueError):
    """A deterministic diagnostic that never includes supplied values."""


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ResultError("JSON: duplicate object key")
        result[key] = value
    return result


def _reject_constant(_value):
    raise ResultError("JSON: non-finite numbers are not allowed")


def _finite_float(value):
    try:
        result = float(value)
    except OverflowError:
        raise ResultError("JSON: number exceeds supported range") from None
    if not math.isfinite(result):
        raise ResultError("JSON: non-finite numbers are not allowed")
    return result


def load_results(path: str | Path):
    """Load strict UTF-8 JSON without returning path or payload in errors."""
    try:
        with Path(path).open(encoding="utf-8") as source:
            return json.load(source, object_pairs_hook=_unique_object,
                             parse_constant=_reject_constant, parse_float=_finite_float)
    except json.JSONDecodeError as exc:
        raise ResultError(f"JSON: malformed document at line {exc.lineno}, column {exc.colno}") from None
    except (OSError, UnicodeError):
        raise ResultError("file: cannot read a UTF-8 result document") from None
    except (RecursionError, ValueError, OverflowError) as exc:
        if isinstance(exc, ResultError):
            raise
        raise ResultError("JSON: document exceeds parser limits") from None


class _Check:
    def __init__(self):
        self.errors: list[str] = []

    def require(self, condition, path, message):
        if not condition:
            self.errors.append(f"{path}: {message}")
        return condition

    def obj(self, value, keys, path):
        if not self.require(type(value) is dict, path, "expected object"):
            return False
        expected = set(keys)
        valid = True
        for key in keys:
            valid = self.require(key in value, f"{path}.{key}", "required field missing") and valid
        valid = self.require(not (value.keys() - expected), path, "unknown fields are not allowed") and valid
        return valid

    def text(self, value, path):
        return self.require(type(value) is str and bool(value.strip()), path, "expected nonempty string")

    def reason_identifier(self, value, path):
        return self.require(
            type(value) is str and len(value) <= 64 and IDENTIFIER.fullmatch(value) is not None,
            path, "expected bounded lowercase hyphenated identifier",
        )

    def identifier(self, value, path):
        return self.require(type(value) is str and IDENTIFIER.fullmatch(value) is not None,
                            path, "expected stable lowercase hyphenated identifier")

    def revision(self, value, path):
        return self.require(type(value) is int and value > 0, path, "expected positive integer revision")

    def enum(self, value, choices, path):
        return self.require(type(value) is str and value in choices, path, "invalid enum value")

    def number(self, value, path):
        valid_type = type(value) in (int, float)
        if not self.require(valid_type, path, "expected finite number"):
            return False
        try:
            return self.require(math.isfinite(value), path, "expected finite number")
        except OverflowError:
            return self.require(False, path, "number exceeds supported range")


def _metadata(check, document):
    if not check.obj(document, ("schema_version", "provenance", "corpus", "component", "model", "generation", "conditions", "resource_profile", "method", "samples"), "results"):
        return None
    check.require(document["schema_version"] == VERSION, "results.schema_version", "unsupported schema version")
    check.enum(document["provenance"], ("synthetic", "captured_sanitized"), "results.provenance")
    for field in ("corpus", "component", "model"):
        value = document[field]
        path = f"results.{field}"
        if check.obj(value, ("id", "revision"), path):
            check.identifier(value["id"], path + ".id")
            check.revision(value["revision"], path + ".revision")
    generation = document["generation"]
    if check.obj(generation, ("strategy", "max_output_tokens"), "results.generation"):
        check.identifier(generation["strategy"], "results.generation.strategy")
        check.revision(generation["max_output_tokens"], "results.generation.max_output_tokens")
    conditions = document["conditions"]
    if check.obj(conditions, ("load",), "results.conditions"):
        check.identifier(conditions["load"], "results.conditions.load")
    check.identifier(document["resource_profile"], "results.resource_profile")
    check.identifier(document["method"], "results.method")
    if not check.require(type(document["samples"]) is list and bool(document["samples"]), "results.samples", "expected nonempty array"):
        return None
    return document


def _stage(check, value, path, anchor_valid, anchor):
    if type(value) is not dict:
        check.require(False, path, "expected exactly one stage state")
        return None
    keys = set(value)
    if keys == {"unavailable_reason"}:
        check.reason_identifier(value["unavailable_reason"], path + ".unavailable_reason")
        return ("unavailable", None, value["unavailable_reason"])
    if keys == {"uncertainty"}:
        check.reason_identifier(value["uncertainty"], path + ".uncertainty")
        return ("uncertain", None, value["uncertainty"])
    if keys != {"observed_monotonic_ms"}:
        check.require(False, path, "expected exactly one stage state")
        return None
    observed = value["observed_monotonic_ms"]
    if not check.number(observed, path + ".observed_monotonic_ms"):
        return None
    if anchor_valid:
        check.require(observed >= anchor, path + ".observed_monotonic_ms", "must be at or after anchor")
        offset = observed - anchor
        if not check.require(math.isfinite(offset), path + ".observed_monotonic_ms", "computed offset must be finite"):
            return None
        return ("observed", offset, None)
    return None


def _sample(check, sample, index, cases):
    path = f"results.samples[{index}]"
    keys = ("sample_id", "case", "population", "thermal", "terminal", "assessment", "anchor_monotonic_ms", "stages")
    if not check.obj(sample, keys, path):
        return None
    check.identifier(sample["sample_id"], path + ".sample_id")
    if check.obj(sample["case"], ("id", "revision"), path + ".case"):
        case_id = sample["case"]["id"]
        if check.identifier(case_id, path + ".case.id"):
            known = cases.get(case_id)
            if check.require(known is not None, path + ".case.id", "unknown corpus case"):
                check.require(sample["case"]["revision"] == known, path + ".case.revision", "corpus case revision mismatch")
        check.revision(sample["case"]["revision"], path + ".case.revision")
    check.enum(sample["population"], POPULATIONS, path + ".population")
    check.enum(sample["thermal"], THERMAL, path + ".thermal")
    check.enum(sample["terminal"], TERMINAL, path + ".terminal")
    assessment = sample["assessment"]
    if check.obj(assessment, ("expected_correct", "observed_correct", "side_effect_outcome"), path + ".assessment"):
        expected, observed = assessment["expected_correct"], assessment["observed_correct"]
        assessable = type(expected) is bool and type(observed) is bool
        both_none = expected is None and observed is None
        check.require(assessable or both_none, path + ".assessment", "expected and observed correctness must both be booleans or both null")
        check.enum(assessment["side_effect_outcome"], SIDE_EFFECTS, path + ".assessment.side_effect_outcome")
        if assessable:
            check.require(assessment["side_effect_outcome"] != "not_assessable", path + ".assessment.side_effect_outcome", "assessable result cannot use not_assessable")
        if both_none:
            check.require(assessment["side_effect_outcome"] == "not_assessable", path + ".assessment.side_effect_outcome", "unassessable result must use not_assessable")
        if sample["terminal"] == "failure":
            check.require(type(expected) is bool and observed is False, path + ".assessment", "failure requires boolean expected correctness and observed false")
    anchor = sample["anchor_monotonic_ms"]
    anchor_valid = check.number(anchor, path + ".anchor_monotonic_ms")
    stages = sample["stages"]
    if not check.obj(stages, STAGES, path + ".stages"):
        return None
    calculated = {}
    for name in STAGES:
        calculated[name] = _stage(check, stages[name], f"{path}.stages.{name}", anchor_valid, anchor)
    return calculated


def corpus_is_valid(document) -> bool:
    """Keep malformed custom corpus structures from escaping as tracebacks."""
    try:
        return not validate_corpus(document)
    except (AttributeError, IndexError, KeyError, TypeError, OverflowError, RecursionError):
        return False


def validate_results(document, corpus_document) -> list[str]:
    """Return deterministic semantic errors for a loaded results document."""
    check = _Check()
    if _metadata(check, document) is None:
        return check.errors
    if check.errors:
        return check.errors
    if not corpus_is_valid(corpus_document):
        check.require(False, "corpus", "referenced corpus is invalid")
        return check.errors
    if document["corpus"]["id"] != corpus_document.get("corpus_id"):
        check.require(False, "results.corpus.id", "does not match referenced corpus")
    if document["corpus"]["revision"] != corpus_document.get("revision"):
        check.require(False, "results.corpus.revision", "does not match referenced corpus")
    cases = {}
    for case in corpus_document.get("cases", []):
        if type(case) is dict and type(case.get("id")) is str and type(case.get("revision")) is int:
            cases[case["id"]] = case["revision"]
    ids = set()
    for index, sample in enumerate(document["samples"]):
        before = len(check.errors)
        _sample(check, sample, index, cases)
        if type(sample) is dict and type(sample.get("sample_id")) is str:
            if check.require(sample["sample_id"] not in ids, f"results.samples[{index}].sample_id", "duplicate sample ID"):
                ids.add(sample["sample_id"])
        # Continue after structure errors to give stable useful diagnostics, but never
        # dereference malformed values while calculating the later summary.
        del before
    return check.errors


def _percentile(values, quantile):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(quantile * len(ordered)) - 1]


def _stage_summary(entries):
    statuses = Counter(status for status, _offset, _reason in entries)
    values = [offset for status, offset, _reason in entries if status == "observed"]
    unavailable_reasons = Counter(reason for status, _offset, reason in entries if status == "unavailable")
    uncertainty_reasons = Counter(reason for status, _offset, reason in entries if status == "uncertain")
    result = {
        "eligible": len(entries), "observed": statuses["observed"],
        "unavailable": statuses["unavailable"], "uncertain": statuses["uncertain"],
        "unavailable_reasons": dict(sorted(unavailable_reasons.items())),
        "uncertainty_reasons": dict(sorted(uncertainty_reasons.items())),
        "p50_ms": _percentile(values, 0.50), "p95_ms": _percentile(values, 0.95),
    }
    return result


def score_results(document):
    """Build a stable, JSON-serializable summary from already validated input."""
    groups = defaultdict(list)
    for sample in document["samples"]:
        groups[(sample["population"], sample["thermal"])].append(sample)
    report_groups = []
    for population, thermal in sorted(groups, key=lambda item: (POPULATIONS.index(item[0]), THERMAL.index(item[1]))):
        samples = groups[(population, thermal)]
        terminals = Counter(sample["terminal"] for sample in samples)
        assessable = [sample for sample in samples if type(sample["assessment"]["expected_correct"]) is bool]
        reliable = [sample for sample in samples if sample["terminal"] != "unsupported"]
        successes = sum(sample["terminal"] == "success" for sample in reliable)
        correct = sum(sample["assessment"]["expected_correct"] == sample["assessment"]["observed_correct"] for sample in assessable)
        stage_entries = {name: [] for name in STAGES}
        for sample in samples:
            anchor = sample["anchor_monotonic_ms"]
            for name in STAGES:
                stage = sample["stages"][name]
                if "observed_monotonic_ms" in stage:
                    stage_entries[name].append(("observed", stage["observed_monotonic_ms"] - anchor, None))
                elif "unavailable_reason" in stage:
                    stage_entries[name].append(("unavailable", None, stage["unavailable_reason"]))
                else:
                    stage_entries[name].append(("uncertain", None, stage["uncertainty"]))
        report_groups.append({
            "population": population, "thermal": thermal,
            "counts": {"submitted": len(samples), **{outcome: terminals[outcome] for outcome in TERMINAL}},
            "reliability": {"numerator": successes, "denominator": len(reliable), "rate": successes / len(reliable) if reliable else None},
            "correctness": {"numerator": correct, "denominator": len(assessable), "rate": correct / len(assessable) if assessable else None},
            "side_effect_outcomes": {outcome: sum(sample["assessment"]["side_effect_outcome"] == outcome for sample in samples) for outcome in SIDE_EFFECTS},
            "stages": {name: _stage_summary(stage_entries[name]) for name in STAGES},
        })
    return {
        "schema_version": VERSION,
        "measurement": {key: document[key] for key in ("provenance", "corpus", "component", "model", "generation", "conditions", "resource_profile", "method")},
        "groups": report_groups,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", help="path to a UTF-8 measurement results JSON file")
    parser.add_argument("--corpus", default=str(ROOT / "evaluation/corpus.json"), help="path to a UTF-8 corpus JSON file")
    args = parser.parse_args(argv)
    try:
        corpus = load_corpus(args.corpus)
    except CorpusLoadError:
        print("Invalid results: corpus: cannot load a valid referenced corpus")
        return 1
    if not corpus_is_valid(corpus):
        print("Invalid results: corpus: referenced corpus is invalid")
        return 1
    try:
        document = load_results(args.results)
    except ResultError as exc:
        print(f"Invalid results: {exc}")
        return 1
    errors = validate_results(document, corpus)
    if errors:
        print("Invalid results:")
        print("\n".join(errors))
        return 1
    print(json.dumps(score_results(document), indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
