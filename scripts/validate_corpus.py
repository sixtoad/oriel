#!/usr/bin/env python3
"""Validate synthetic corpus definitions offline; never execute their contents."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

VERSION = "1.0"
# category: (label, route, result status, required fixture kind)
CONTRACTS = {
    "chat": ("supported", "conversation", "answer", "conversation"),
    "time": ("supported", "clock", "answer", "clock"),
    "time_unavailable": ("deferred_voice_skill", "limitation", "unsupported", "unavailable"),
    "weather": ("deferred_voice_skill", "limitation", "unsupported", "unavailable"),
    "light_state": ("supported", "fixture_light", "simulated", "light"),
    "follow_up": ("supported", "conversation", "answer", "conversation"),
    "music": ("deferred_voice_skill", "limitation", "unsupported", "unavailable"),
    "audio_interruption": ("deferred_voice_skill", "limitation", "unsupported", "in_flight"),
    "deliberate_denial": ("negative_security", "policy", "denied", "policy"),
    "injected_proposal": ("negative_security", "policy", "denied", "policy"),
    "malformed_proposal": ("negative_security", "policy", "denied", "policy"),
    "excluded_operation": ("negative_security", "policy", "denied", "policy"),
    "excluded_target": ("negative_security", "policy", "denied", "policy"),
    "text_cancellation": ("supported", "cancel", "cancelled", "in_flight"),
    "dependency_failure": ("supported", "dependency_error", "error", "dependency"),
}
REQUIRED_CATEGORIES = frozenset(CONTRACTS)
LABELS = ("supported", "negative_security", "deferred_voice_skill")
ID = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
TARGET = "test_light_alpha"


class CorpusLoadError(ValueError):
    """A sanitized diagnostic safe for CLI output."""


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CorpusLoadError("JSON: duplicate object key")
        result[key] = value
    return result


def _reject_constant(_value):
    raise CorpusLoadError("JSON: non-finite numbers are not allowed")


def _finite_float(value):
    # Reject overflow (e.g. 1e999), as well as the nonstandard constants above.
    import math
    result = float(value)
    if not math.isfinite(result):
        raise CorpusLoadError("JSON: non-finite numbers are not allowed")
    return result


def load_corpus(path: str | Path):
    """Load strict UTF-8 JSON without echoing filenames or input payloads."""
    try:
        with Path(path).open(encoding="utf-8") as source:
            return json.load(source, object_pairs_hook=_unique_object,
                             parse_constant=_reject_constant, parse_float=_finite_float)
    except json.JSONDecodeError as exc:
        raise CorpusLoadError(f"JSON: malformed document at line {exc.lineno}, column {exc.colno}") from None
    except (OSError, UnicodeError):
        raise CorpusLoadError("file: cannot read a UTF-8 corpus document") from None
    except (RecursionError, ValueError) as exc:
        if isinstance(exc, CorpusLoadError):
            raise
        raise CorpusLoadError("JSON: document exceeds parser limits") from None


class _Check:
    def __init__(self):
        self.errors = []

    def require(self, condition, path, message):
        if not condition:
            self.errors.append(f"{path}: {message}")
        return condition

    def obj(self, value, keys, path):
        if not self.require(type(value) is dict, path, "expected object"):
            return False
        valid = True
        for key in keys:
            valid = self.require(key in value, f"{path}.{key}", "required field missing") and valid
        valid = self.require(not (value.keys() - set(keys)), path, "unknown fields are not allowed") and valid
        return valid

    def text(self, value, path):
        return self.require(type(value) is str and bool(value.strip()), path, "expected nonempty string")

    def enum(self, value, choices, path):
        return self.require(type(value) is str and value in choices, path, "invalid enum value")

    def identifier(self, value, path):
        return self.require(type(value) is str and ID.fullmatch(value) is not None,
                            path, "expected stable lowercase hyphenated identifier")

    def revision(self, value, path):
        return self.require(type(value) is int and value > 0, path, "expected positive integer revision")

    def strings(self, value, path, nonempty=False):
        if not self.require(type(value) is list, path, "expected array"):
            return False
        valid = self.require(bool(value) or not nonempty, path, "expected nonempty array")
        for index, item in enumerate(value):
            valid = self.text(item, f"{path}[{index}]") and valid
        return valid


def _fixture(check, fixture, path):
    if not check.obj(fixture, ("id", "revision", "kind", "data"), path):
        return False
    check.identifier(fixture["id"], path + ".id")
    check.revision(fixture["revision"], path + ".revision")
    kind, data = fixture["kind"], fixture["data"]
    fields = {
        "clock": ("instant", "timezone"),
        "unavailable": ("source", "available"),
        "light": ("target", "initial_state", "allowed_states", "execution_boundary"),
        "conversation": ("history", "answer_facts"),
        "in_flight": ("request_id", "status", "event"),
        "dependency": ("backend", "fault", "request_state"),
        "policy": ("allowed_operation", "allowed_target", "allowed_states", "trust"),
    }
    if not check.enum(kind, fields, path + ".kind"):
        return False
    path += ".data"
    if not check.obj(data, fields[kind], path):
        return False
    if kind == "clock":
        # Intentionally a single fixed instant, not a wall-clock read or live source.
        check.require(data["instant"] == "2030-01-02T12:34:00Z", path + ".instant", "expected fixed synthetic instant")
        check.require(data["timezone"] == "UTC", path + ".timezone", "expected UTC")
    elif kind == "unavailable":
        check.enum(data["source"], ("time", "weather", "music"), path + ".source")
        check.require(data["available"] is False, path + ".available", "source must be unavailable")
    elif kind in ("light", "policy"):
        target_key = "target" if kind == "light" else "allowed_target"
        check.require(data[target_key] == TARGET, path + "." + target_key, "expected harmless synthetic light alias")
        check.require(data["allowed_states"] == ["off", "on"], path + ".allowed_states", "expected explicit off/on states")
        if kind == "light":
            check.enum(data["initial_state"], ("off", "on"), path + ".initial_state")
            check.require(data["execution_boundary"] == "fixture_only", path + ".execution_boundary", "must be fixture_only")
        else:
            check.require(data["allowed_operation"] == "set_light", path + ".allowed_operation", "expected fixture operation")
            check.require(data["trust"] == "untrusted_proposal", path + ".trust", "proposals must be untrusted")
    elif kind == "conversation":
        if check.require(type(data["history"]) is list, path + ".history", "expected array"):
            for index, message in enumerate(data["history"]):
                loc = f"{path}.history[{index}]"
                if check.obj(message, ("role", "text"), loc):
                    check.enum(message["role"], ("user", "assistant"), loc + ".role")
                    check.text(message["text"], loc + ".text")
        check.strings(data["answer_facts"], path + ".answer_facts", nonempty=True)
    elif kind == "in_flight":
        check.require(data["request_id"] == "synthetic-request-1", path + ".request_id", "expected synthetic request reference")
        check.require(data["status"] == "streaming", path + ".status", "expected in-flight streaming state")
        check.enum(data["event"], ("text_cancel", "audio_interrupt"), path + ".event")
    elif kind == "dependency":
        check.require(data["backend"] == "synthetic_backend", path + ".backend", "expected synthetic backend alias")
        check.require(data["fault"] == "timeout", path + ".fault", "expected injected timeout")
        check.require(data["request_state"] == "pending", path + ".request_state", "expected pending request")
    return True


def _case(check, case, path, fixtures):
    if not check.obj(case, ("id", "revision", "category", "label", "input", "preconditions", "expected"), path):
        return
    check.identifier(case["id"], path + ".id")
    check.revision(case["revision"], path + ".revision")
    category = case["category"]
    if not check.enum(category, CONTRACTS, path + ".category"):
        return
    label, route, status, fixture_kind = CONTRACTS[category]
    check.enum(case["label"], LABELS, path + ".label")
    check.require(case["label"] == label, path + ".label", "label contradicts category contract")
    inp, pre, expected = case["input"], case["preconditions"], case["expected"]
    if not check.obj(inp, ("kind", "text", "proposal"), path + ".input"):
        return
    check.text(inp["text"], path + ".input.text")
    input_kind = "proposal" if category in ("injected_proposal", "malformed_proposal", "excluded_operation", "excluded_target") else (
        "event" if category in ("audio_interruption", "text_cancellation") else "text")
    check.require(inp["kind"] == input_kind, path + ".input.kind", "input kind contradicts category contract")
    proposal = inp["proposal"]
    if input_kind == "proposal":
        if not check.obj(proposal, ("operation", "target", "arguments"), path + ".input.proposal"):
            return
        check.text(proposal["operation"], path + ".input.proposal.operation")
        check.text(proposal["target"], path + ".input.proposal.target")
        # Arguments deliberately remain untrusted JSON data; malformed cases need it.
        valid_args = type(proposal["arguments"]) is dict and set(proposal["arguments"]) == {"state"} and proposal["arguments"]["state"] in ("on", "off")
        if category == "malformed_proposal":
            check.require(not valid_args, path + ".input.proposal.arguments", "malformed case must contain invalid arguments")
        elif category != "injected_proposal":
            check.require(valid_args, path + ".input.proposal.arguments", "expected valid arguments to isolate excluded field")
        if category == "excluded_operation":
            check.require(proposal["operation"] == "excluded_operation" and proposal["target"] == TARGET,
                          path + ".input.proposal", "expected synthetic excluded operation and allowed target")
        elif category == "excluded_target":
            check.require(proposal["operation"] == "set_light" and proposal["target"] == "excluded_test_target",
                          path + ".input.proposal", "expected fixture operation and synthetic excluded target")
        else:
            check.require(proposal["operation"] == "set_light" and proposal["target"] == TARGET,
                          path + ".input.proposal", "expected allowed synthetic operation and target")
    else:
        check.require(proposal is None, path + ".input.proposal", "must be null for text/event cases")
    if not check.obj(pre, ("fixture_refs", "requested_state"), path + ".preconditions"):
        return
    refs = pre["fixture_refs"]
    if not check.require(type(refs) is list and len(refs) == 1, path + ".preconditions.fixture_refs", "expected exactly one pinned fixture reference"):
        return
    ref = refs[0]
    ref_path = path + ".preconditions.fixture_refs[0]"
    if not check.obj(ref, ("id", "revision"), ref_path):
        return
    if not check.identifier(ref["id"], ref_path + ".id"):
        return
    check.revision(ref["revision"], ref_path + ".revision")
    fixture = fixtures.get(ref["id"])
    if not check.require(fixture is not None, ref_path + ".id", "unknown fixture reference"):
        return
    check.require(ref["revision"] == fixture["revision"], ref_path + ".revision", "fixture revision mismatch")
    if not check.require(fixture["kind"] == fixture_kind, ref_path, "fixture kind contradicts category contract"):
        return
    data = fixture["data"]
    if category == "light_state":
        check.enum(pre["requested_state"], ("on", "off"), path + ".preconditions.requested_state")
        check.require(pre["requested_state"] != data["initial_state"], path + ".preconditions.requested_state", "fixture must describe a state change")
    else:
        check.require(pre["requested_state"] is None, path + ".preconditions.requested_state", "must be null outside light cases")
    if category in ("time_unavailable", "weather", "music"):
        check.require(data["source"] == ("time" if category == "time_unavailable" else category), ref_path, "unavailable source contradicts category")
    elif category == "follow_up":
        roles = [message["role"] for message in data["history"]]
        check.require(len(roles) >= 2 and roles[-2:] == ["user", "assistant"], ref_path, "follow-up requires prior user and assistant turns")
    elif category in ("audio_interruption", "text_cancellation"):
        check.require(data["event"] == ("text_cancel" if category == "text_cancellation" else "audio_interrupt"), ref_path, "event contradicts category")
    if not check.obj(expected, ("route", "result", "side_effects", "live_dispatch_count"), path + ".expected"):
        return
    check.require(expected["route"] == route, path + ".expected.route", "route contradicts category contract")
    check.require(type(expected["live_dispatch_count"]) is int and expected["live_dispatch_count"] == 0,
                  path + ".expected.live_dispatch_count", "live dispatch must be zero")
    result = expected["result"]
    loc = path + ".expected.result"
    if not check.obj(result, ("status", "facts", "rubric", "limitation", "error_code", "state"), loc):
        return
    check.require(result["status"] == status, loc + ".status", "status contradicts category contract")
    check.strings(result["facts"], loc + ".facts")
    check.strings(result["rubric"], loc + ".rubric", nonempty=True)
    facts = data["answer_facts"] if fixture_kind == "conversation" else ([data["instant"], "UTC"] if category == "time" else [])
    check.require(result["facts"] == facts, loc + ".facts", "facts must match deterministic fixture facts (empty for non-answer cases)")
    if status == "unsupported":
        check.text(result["limitation"], loc + ".limitation")
    else:
        check.require(result["limitation"] is None, loc + ".limitation", "must be null for non-deferred cases")
    error_code = "policy_denied" if status == "denied" else "dependency_timeout" if status == "error" else None
    check.require(result["error_code"] == error_code, loc + ".error_code", "error code contradicts category contract")
    state = {"target": TARGET, "state": pre["requested_state"]} if category == "light_state" else None
    check.require(result["state"] == state, loc + ".state", "state must match fixture target and request, or be null")
    effects = [{"scope": "fixture_only", "target": TARGET, "from": data["initial_state"], "to": pre["requested_state"]}] if category == "light_state" else []
    check.require(expected["side_effects"] == effects, path + ".expected.side_effects", "expected one fixture-only state change for light, otherwise zero effects")


def validate_corpus(document) -> list[str]:
    """Validate a strict JSON document obtained from load_corpus.

    Arbitrary Python objects are outside the supported input domain. Return
    deterministic, sanitized errors; no file, network or runtime effects.
    """
    check = _Check()
    if not check.obj(document, ("schema_version", "corpus_id", "revision", "synthetic", "fixtures", "cases"), "corpus"):
        return check.errors
    check.require(document["schema_version"] == VERSION, "corpus.schema_version", "unsupported schema version")
    check.identifier(document["corpus_id"], "corpus.corpus_id")
    check.revision(document["revision"], "corpus.revision")
    check.require(document["synthetic"] is True, "corpus.synthetic", "must explicitly be true")
    fixtures = {}
    if check.require(type(document["fixtures"]) is list and bool(document["fixtures"]), "corpus.fixtures", "expected nonempty array"):
        for index, fixture in enumerate(document["fixtures"]):
            loc = f"corpus.fixtures[{index}]"
            before = len(check.errors)
            _fixture(check, fixture, loc)
            if len(check.errors) == before:
                if check.require(fixture["id"] not in fixtures, loc + ".id", "duplicate fixture ID"):
                    fixtures[fixture["id"]] = fixture
    # Do not use invalid fixture structures for semantic validation.
    if check.errors:
        return check.errors
    if not check.require(type(document["cases"]) is list and bool(document["cases"]), "corpus.cases", "expected nonempty array"):
        return check.errors
    ids, categories = set(), set()
    for index, case in enumerate(document["cases"]):
        loc = f"corpus.cases[{index}]"
        _case(check, case, loc, fixtures)
        if type(case) is dict:
            case_id, category = case.get("id"), case.get("category")
            if type(case_id) is str:
                check.require(case_id not in ids, loc + ".id", "duplicate case ID")
                ids.add(case_id)
            if type(category) is str:
                categories.add(category)
    for category in sorted(REQUIRED_CATEGORIES - categories):
        check.require(False, "corpus.cases", f"missing required category: {category}")
    return check.errors


def summary(document) -> str:
    """Summarize only a previously validated document, in stable order."""
    cases = document["cases"]
    lines = [f"Valid corpus: schema {VERSION}; {len(cases)} cases; {len(document['fixtures'])} fixtures."]
    lines.extend(f"label {label}: {sum(case['label'] == label for case in cases)}" for label in LABELS)
    lines.extend(f"category {category}: {sum(case['category'] == category for case in cases)}" for category in sorted(REQUIRED_CATEGORIES))
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("corpus", help="path to a UTF-8 corpus JSON file")
    args = parser.parse_args(argv)
    try:
        document = load_corpus(args.corpus)
    except CorpusLoadError as exc:
        print(f"Invalid corpus: {exc}")
        return 1
    errors = validate_corpus(document)
    if errors:
        print("Invalid corpus:")
        print("\n".join(errors))
        return 1
    print(summary(document))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
