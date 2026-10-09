#!/usr/bin/env python3
"""Validate Oriel's frozen public-contract fixtures without network access."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import math
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API = ROOT / "api"
LIMITS = {"input_bytes": 16384, "context_messages": 32, "context_bytes": 65536,
          "open_sessions": 10, "active_turns": 2, "queue_depth": 8,
          "model_deadline_seconds": 30, "streamed_content_bytes": 65536,
          "event_data_bytes": 8192}
EVENTS = frozenset(("accepted", "ack", "content_delta", "proposal", "fact", "validation",
                    "action_state", "error", "terminal"))
OUTCOMES = frozenset(("completed", "denied", "failed", "cancelled", "outcome_unknown"))
IDENTIFIER = re.compile(r"^[A-Za-z0-9._~-]{1,128}$")
ACTION = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
SYNTHETIC_TARGET = re.compile(r"^synthetic:[a-z][a-z0-9_-]{0,63}$")
FIXTURES = {
    "valid": {"accepted-error-categories": "accepted_error_categories", "accepted-failure": "accepted_failure", "accepted-stream": "accepted_stream",
              "cancelled-stream": "cancelled_stream", "caps": "caps",
              "cancellation-race": "cancellation_race", "config-precedence": "config_precedence", "extension": "extension_event",
              "home-fact-stream": "home_fact_stream",
              "generic-action": "generic_action", "invalid-optional-skill": "invalid_optional_skill",
              "action-reservation": "action_reservation", "cancelled-action-known": "cancelled_action_known", "cancelled-action-unknown": "cancelled_action_unknown", "passive-status": "passive_status", "preaccept-errors": "preaccept_errors", "simulated-preview": "simulated_preview"},
    "invalid": {"ack-after-content": "ack_after_content", "cap-overflow": "cap_overflow",
                "cancellation-outcome": "cancellation_outcome", "out-of-order-sequence": "invalid_sequence", "passive-status-outcome": "passive_status_outcome", "post-terminal": "post_terminal",
                "unknown-config-field": "unknown_config_field", "unknown-manifest-field": "unknown_manifest_field", "invalid-preview": "invalid_preview", "invalid-action-state": "invalid_action_state", "cancelled-action-hidden": "cancelled_action_hidden", "cancelled-action-content": "cancelled_action_content", "cancelled-action-repeat": "cancelled_action_repeat"},
}
FIXTURE_NAMES = frozenset(name for group in FIXTURES.values() for name in group.values())


class ContractLoadError(ValueError):
    """A deterministic diagnostic that never includes a fixture payload or path."""


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContractLoadError("JSON: duplicate object key")
        result[key] = value
    return result


def _reject_constant(_value):
    raise ContractLoadError("JSON: non-finite numbers are not allowed")


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ContractLoadError("JSON: non-finite numbers are not allowed")
    return result


def load_json(path: str | Path):
    """Load strict UTF-8 JSON with safe, stable errors."""
    try:
        with Path(path).open(encoding="utf-8") as source:
            return json.load(source, object_pairs_hook=_unique_object,
                             parse_constant=_reject_constant, parse_float=_finite_float)
    except json.JSONDecodeError as exc:
        raise ContractLoadError(f"JSON: malformed document at line {exc.lineno}, column {exc.colno}") from None
    except (OSError, UnicodeError):
        raise ContractLoadError("file: cannot read a UTF-8 contract document") from None
    except (RecursionError, ValueError) as exc:
        if isinstance(exc, ContractLoadError):
            raise
        raise ContractLoadError("JSON: document exceeds parser limits") from None


def _keys(value, required, path, errors, allow_extra=False, optional=()):
    if type(value) is not dict:
        errors.append(f"{path}: expected object")
        return False
    missing = sorted(set(required) - set(value))
    if missing:
        errors.append(f"{path}: required field missing: {missing[0]}")
    allowed = set(required) | set(optional)
    if not allow_extra:
        extra = sorted(set(value) - allowed)
        if extra:
            errors.append(f"{path}: unknown field: {extra[0]}")
    return not missing and (allow_extra or not (set(value) - allowed))


def _canonical_keys(value, path, errors):
    if type(value) is dict:
        keys = list(value)
        if keys != sorted(keys):
            errors.append(path + ": object keys must be lexicographically ordered")
        for key, child in value.items():
            _canonical_keys(child, path + "." + key, errors)
    elif type(value) is list:
        for index, child in enumerate(value):
            _canonical_keys(child, f"{path}[{index}]", errors)


def _id(value, path, errors):
    if type(value) is not str or IDENTIFIER.fullmatch(value) is None:
        errors.append(f"{path}: expected opaque identifier")


def _utf8_bytes(value):
    return len(value.encode("utf-8"))


def _deadline(value, path, errors):
    if not (type(value) is str and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value)):
        errors.append(path + ": expected UTC deadline")


def _error(value, path, errors):
    if not _keys(value, ("error",), path, errors):
        return
    error = value["error"]
    if not _keys(error, ("code", "category", "message", "retryable"), path + ".error", errors,
                 optional=("request_id", "session_id", "trace_id", "action_outcome")):
        return
    if error["category"] not in ("invalid_input", "conflict_or_expired_reference", "overload", "policy_denial", "dependency_unavailable", "timeout", "cancellation", "uncertainty", "internal_failure"):
        errors.append(path + ".error.category: invalid category")
    if not (type(error["code"]) is str and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", error["code"])):
        errors.append(path + ".error.code: invalid code")
    if not (type(error["message"]) is str and 0 < len(error["message"]) <= 256):
        errors.append(path + ".error.message: expected bounded message")
    if type(error["retryable"]) is not bool:
        errors.append(path + ".error.retryable: expected boolean")
    for key in ("request_id", "session_id", "trace_id"):
        if key in error:
            _id(error[key], path + ".error." + key, errors)
    if "action_outcome" in error and error["action_outcome"] not in OUTCOMES:
        errors.append(path + ".error.action_outcome: invalid action outcome")


def validate_stream(events, path="events", cancellation_after_seq=None):
    errors = []
    if type(events) is not list or not events:
        return [f"{path}: expected nonempty event array"]
    if cancellation_after_seq is not None and (type(cancellation_after_seq) is not int or cancellation_after_seq < 1):
        return [f"{path}: invalid cancellation acknowledgement boundary"]
    if cancellation_after_seq is not None:
        boundary = next((event for event in events if isinstance(event, dict) and event.get("seq") == cancellation_after_seq), None)
        terminal_sequences = [event.get("seq") for event in events if isinstance(event, dict) and event.get("type") == "terminal" and type(event.get("seq")) is int]
        if boundary is None or boundary.get("type") == "terminal" or not terminal_sequences or cancellation_after_seq >= min(terminal_sequences):
            errors.append(path + ": cancellation boundary must identify an event before terminal")
    late_cancellation_errors = 0
    request = session = trace = generation = None
    seq = 0
    acked = useful = terminal = False
    streamed_content_bytes = 0
    preview_proposal = None
    action_id = action_state = None
    action_evidence_count = 0
    for index, event in enumerate(events):
        loc = f"{path}[{index}]"
        if not _keys(event, ("api_version", "type", "request_id", "session_id", "trace_id", "context_generation", "seq"), loc, errors, allow_extra=True):
            continue
        if event["api_version"] != "1.0":
            errors.append(loc + ".api_version: expected 1.0")
        kind = event["type"]
        if kind not in EVENTS:
            errors.append(loc + ".type: unsupported event")
            continue
        for key in ("request_id", "session_id", "trace_id"):
            _id(event[key], loc + "." + key, errors)
        if type(event["context_generation"]) is not int or event["context_generation"] < 0:
            errors.append(loc + ".context_generation: expected nonnegative integer")
        if type(event["seq"]) is not int or event["seq"] <= seq:
            errors.append(loc + ".seq: must be strictly increasing")
        seq = event["seq"] if type(event["seq"]) is int else seq
        if index == 0 and kind != "accepted":
            errors.append(loc + ".type: first event must be accepted")
        if terminal:
            errors.append(loc + ": event appears after terminal")
        if request is None:
            request, session, trace, generation = (event["request_id"], event["session_id"], event["trace_id"], event["context_generation"])
        elif (event["request_id"], event["session_id"], event["trace_id"], event["context_generation"]) != (request, session, trace, generation):
            errors.append(loc + ": event identity or context generation changed")
        if cancellation_after_seq is not None and type(event["seq"]) is int and event["seq"] > cancellation_after_seq:
            if kind == "error":
                late_cancellation_errors += 1
                error = event.get("error")
                if late_cancellation_errors > 1 or not isinstance(error, dict) or error.get("category") != "cancellation":
                    errors.append(loc + ": at most one typed cancellation error may follow acknowledgement")
                elif any(error.get(key) != event[key] for key in ("request_id", "session_id", "trace_id")):
                    errors.append(loc + ": cancellation error must match stream identity")
            if kind in {"ack", "content_delta", "proposal", "fact", "validation"}:
                errors.append(loc + ": generated output after cancellation acknowledgement")
            if kind == "action_state" and (not isinstance(event.get("action_state"), dict) or event["action_state"].get("state") not in {"reserved", "fake_attempted", "outcome_unknown"}):
                errors.append(loc + ": only committed action evidence survives cancellation")
        if kind == "ack":
            if acked:
                errors.append(loc + ".type: ack may occur at most once")
            if useful:
                errors.append(loc + ".type: ack must precede useful content")
            if "outcome" in event:
                errors.append(loc + ".outcome: ack cannot state completion or approval")
            if event.get("message") != "Work is continuing.":
                errors.append(loc + ".message: ack requires the fixed bounded message")
            for field in set(event) - {"api_version", "type", "request_id", "session_id", "trace_id", "context_generation", "seq", "message"}:
                errors.append(loc + f".{field}: ack may only carry its fixed message")
            acked = True
        elif "message" in event:
            errors.append(loc + ".message: only ack may carry a message")
        if kind in ("content_delta", "proposal", "fact", "validation", "action_state"):
            useful = True
        if kind == "content_delta":
            if type(event.get("content")) is not str:
                errors.append(loc + ".content: content_delta requires content")
            else:
                streamed_content_bytes += _utf8_bytes(event["content"])
                if streamed_content_bytes > LIMITS["streamed_content_bytes"]:
                    errors.append(loc + ".content: cumulative streamed content exceeds cap")
        if kind == "proposal":
            if "proposal" not in event:
                errors.append(loc + ".proposal: proposal event requires proposal")
            else:
                _validate_proposal(event["proposal"], loc + ".proposal", errors)
                preview_proposal = event["proposal"] if _is_ha_material(event["proposal"]) else None
        if kind == "validation":
            if "preview" not in event:
                errors.append(loc + ".preview: validation requires preview")
            else:
                _validate_preview(event["preview"], loc + ".preview", errors)
                preview = event["preview"]
                if type(preview) is dict and preview.get("status") == "simulated":
                    material = {key: preview.get(key) for key in ("operation", "target", "arguments", "manifest_revision")}
                    if preview_proposal is None or material != preview_proposal:
                        errors.append(loc + ".preview: simulated preview must follow its identical harmless-light proposal")
                preview_proposal = None
        elif "preview" in event:
            errors.append(loc + ".preview: only validation may carry preview")
        if kind != "fact" and "fact" in event:
            errors.append(loc + ".fact: only fact events may carry a fact")
        if kind == "fact":
            _validate_fact(event.get("fact"), loc + ".fact", errors)
        if kind == "action_state":
            if "action_state" not in event:
                errors.append(loc + ".action_state: action_state event requires payload")
            else:
                _validate_action_state(event["action_state"], loc + ".action_state", errors)
                for key in ("content", "proposal", "preview", "error", "outcome"):
                    if key in event:
                        errors.append(loc + "." + key + ": action_state may not carry other payload")
                state = event["action_state"]
                if type(state) is dict and state.get("state") != "denied":
                    action_evidence_count += 1
                    if action_evidence_count > 2:
                        errors.append(loc + ": action evidence exceeds bounded commitment/result pair")
                    if action_id is not None and state.get("action_id") != action_id:
                        errors.append(loc + ": action identity changed")
                    next_state = state.get("state")
                    if action_state is None and next_state in {"fake_attempted", "outcome_unknown"}:
                        errors.append(loc + ": action result requires prior reservation")
                    if action_state is not None and (action_state != "reserved" or next_state not in {"fake_attempted", "outcome_unknown"}):
                        errors.append(loc + ": action evidence regressed or repeated")
                    action_id = state.get("action_id")
                    action_state = state.get("existing_state") if next_state in {"duplicate", "conflict"} else next_state
                    for key in ("request_id", "trace_id"):
                        if state.get(key) != event[key]:
                            errors.append(loc + ".action_state." + key + ": must match stream identity")
        elif "action_state" in event:
            errors.append(loc + ".action_state: only action_state may carry action state")
        if kind == "error":
            if "error" not in event:
                errors.append(loc + ".error: error event requires error")
            _error({"error": event.get("error")}, loc, errors)
        if kind == "terminal":
            if "outcome" not in event:
                errors.append(loc + ".outcome: terminal requires outcome")
            if event.get("outcome") not in OUTCOMES:
                errors.append(loc + ".outcome: invalid terminal outcome")
            if action_state in {"reserved", "outcome_unknown"} and event.get("outcome") != "outcome_unknown":
                errors.append(loc + ": unresolved commitment requires outcome_unknown")
            terminal = True
        event_bytes = len(json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        if event_bytes > LIMITS["event_data_bytes"]:
            errors.append(loc + ": serialized event data exceeds cap")
    if not terminal:
        errors.append(path + ": exactly one terminal event is required")
    return errors


def _validate_fact(fact, path, errors):
    if not _keys(fact, ("power_state", "observed_at", "freshness"), path, errors):
        return
    freshness = fact["freshness"]
    if freshness not in ("fresh", "stale", "unavailable"):
        errors.append(path + ".freshness: invalid freshness")
        return
    if freshness == "unavailable":
        if fact["power_state"] is not None or fact["observed_at"] is not None:
            errors.append(path + ": unavailable fact must carry no observation")
        return
    if fact["power_state"] not in ("on", "off"):
        errors.append(path + ".power_state: expected observed power state")
    if not _utc_observation(fact["observed_at"]):
        errors.append(path + ".observed_at: expected UTC observation time")


def _utc_observation(value):
    if not (type(value) is str and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", value)):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True


def _validate_config(config, path, errors, disabled_skills=None):
    if not _keys(config, ("api_version", "provider", "skills"), path, errors): return
    if config["api_version"] != "1.0": errors.append(path + ".api_version: expected 1.0")
    if _keys(config["provider"], ("connection_ref",), path + ".provider", errors):
        reference = config["provider"]["connection_ref"]
        if not (type(reference) is str and 0 < len(reference) <= 256): errors.append(path + ".provider.connection_ref: expected bounded reference")
    if type(config["skills"]) is not dict: errors.append(path + ".skills: expected object")
    else:
        for name, skill in config["skills"].items():
            skill_errors = []
            if _keys(skill, ("enabled", "connection_ref") if type(skill) is dict and "connection_ref" in skill else ("enabled",), path + ".skills." + name, skill_errors):
                if type(skill["enabled"]) is not bool: errors.append(path + ".skills." + name + ".enabled: expected boolean")
                if "connection_ref" in skill and not (type(skill["connection_ref"]) is str and 0 < len(skill["connection_ref"]) <= 256): errors.append(path + ".skills." + name + ".connection_ref: expected bounded reference")
            if skill_errors:
                if disabled_skills is None:
                    errors.extend(skill_errors)
                else:
                    disabled_skills.append(name)


def _validate_manifest(manifest, path, errors):
    if not _keys(manifest, ("manifest_version", "actions"), path, errors): return
    if manifest["manifest_version"] != "1.0": errors.append(path + ".manifest_version: expected 1.0")
    if type(manifest["actions"]) is not list: errors.append(path + ".actions: expected array"); return
    for index, action in enumerate(manifest["actions"]):
        loc = f"{path}.actions[{index}]"
        if _keys(action, ("name", "target", "enabled", "dry_run", "idempotency", "deadline"), loc, errors):
            if action["enabled"] is not False: errors.append(loc + ".enabled: actions are disabled by default")
            if not (type(action["name"]) is str and ACTION.fullmatch(action["name"])): errors.append(loc + ".name: invalid action")
            if not (type(action["target"]) is str and SYNTHETIC_TARGET.fullmatch(action["target"])): errors.append(loc + ".target: expected synthetic target")
            if type(action["dry_run"]) is not bool: errors.append(loc + ".dry_run: expected boolean")
            if not (type(action["idempotency"]) is str and action["idempotency"]): errors.append(loc + ".idempotency: expected key")
            _deadline(action["deadline"], loc + ".deadline", errors)


def _validate_proposal(proposal, path, errors):
    if type(proposal) is dict and set(proposal) == {"operation", "target", "arguments", "manifest_revision"}:
        _validate_ha_material(proposal, path, errors)
        return
    required = ("proposal_version", "proposal_id", "action", "target", "arguments", "dry_run", "idempotency", "deadline", "confirmation")
    if not _keys(proposal, required, path, errors, optional=("state", "result")): return
    if proposal["proposal_version"] != "1.0": errors.append(path + ".proposal_version: expected 1.0")
    _id(proposal["proposal_id"], path + ".proposal_id", errors)
    if not (type(proposal["action"]) is str and ACTION.fullmatch(proposal["action"])): errors.append(path + ".action: invalid action")
    if not (type(proposal["target"]) is str and SYNTHETIC_TARGET.fullmatch(proposal["target"])): errors.append(path + ".target: expected synthetic target")
    if not _keys(proposal["arguments"], ("values",), path + ".arguments", errors): return
    values = proposal["arguments"]["values"]
    if type(values) is not list or len(values) > 16 or any(type(value) not in (str, int, float, bool, type(None)) for value in values): errors.append(path + ".arguments.values: expected bounded scalar array")
    if type(proposal["dry_run"]) is not bool: errors.append(path + ".dry_run: expected boolean")
    if not (type(proposal["idempotency"]) is str and proposal["idempotency"]): errors.append(path + ".idempotency: expected key")
    _deadline(proposal["deadline"], path + ".deadline", errors)
    if not _keys(proposal["confirmation"], ("required", "evidence"), path + ".confirmation", errors): return
    if type(proposal["confirmation"]["required"]) is not bool: errors.append(path + ".confirmation.required: expected boolean")
    if proposal["confirmation"]["evidence"] is not None and not (type(proposal["confirmation"]["evidence"]) is str and len(proposal["confirmation"]["evidence"]) <= 256): errors.append(path + ".confirmation.evidence: expected bounded evidence")
    if "state" in proposal and proposal["state"] not in ("proposed", "validated", "denied", "expired", "cancelled", "failed", "completed"):
        errors.append(path + ".state: invalid action state")
    if "result" in proposal:
        result = proposal["result"]
        if _keys(result, ("state",), path + ".result", errors, optional=("detail",)):
            if result["state"] not in ("denied", "expired", "cancelled", "failed", "completed"):
                errors.append(path + ".result.state: invalid result state")
            if "detail" in result and not (type(result["detail"]) is str and len(result["detail"]) <= 256):
                errors.append(path + ".result.detail: expected bounded detail")


def _validate_ha_material(value, path, errors):
    if not _keys(value, ("operation", "target", "arguments", "manifest_revision"), path, errors):
        return
    if value["operation"] != "home_assistant.light.set_power.v1": errors.append(path + ".operation: unsupported operation")
    if value["target"] != "synthetic:reviewed-harmless-light": errors.append(path + ".target: unsupported target")
    if value["manifest_revision"] != "home_assistant.harmless_light.v1": errors.append(path + ".manifest_revision: unsupported revision")
    if _keys(value["arguments"], ("desired_state",), path + ".arguments", errors) and value["arguments"]["desired_state"] not in ("on", "off"): errors.append(path + ".arguments.desired_state: invalid desired state")


def _is_ha_material(value):
    return type(value) is dict and set(value) == {"operation", "target", "arguments", "manifest_revision"}


def _validate_preview(value, path, errors):
    if not _keys(value, ("status",), path, errors, optional=("operation", "target", "arguments", "manifest_revision", "reason")):
        return
    status = value["status"]
    if status not in ("simulated", "denied", "unavailable"):
        errors.append(path + ".status: invalid preview status")
        return
    material = {key: value[key] for key in ("operation", "target", "arguments", "manifest_revision") if key in value}
    if status == "simulated":
        if "reason" in value:
            errors.append(path + ".reason: simulated preview has no reason")
        _validate_ha_material(material, path, errors)
    else:
        if material:
            errors.append(path + ": unavailable or denied preview has no canonical material")
        expected_reason = "adapter_rejected" if status == "denied" else "adapter_unavailable"
        if value.get("reason") != expected_reason:
            errors.append(path + ".reason: invalid preview reason")


def _validate_action_state(value, path, errors):
    if not _keys(value, ("state", "readiness"), path, errors,
                 optional=("action_id", "request_id", "trace_id", "manifest_revision", "capability_id", "existing_state")):
        return
    state = value["state"]
    if state not in ("reserved", "duplicate", "conflict", "fake_attempted", "outcome_unknown", "denied"):
        errors.append(path + ".state: invalid action state")
        return
    if value["readiness"] not in ("ready", "degraded"):
        errors.append(path + ".readiness: invalid readiness")
    identifiers = ("action_id", "request_id", "trace_id")
    if state == "denied":
        for key in identifiers:
            if key in value:
                errors.append(path + "." + key + ": denied action state must not identify an action")
    else:
        for key in identifiers:
            if key not in value:
                errors.append(path + "." + key + ": action state requires identifier")
            else:
                _id(value[key], path + "." + key, errors)
        if value.get("manifest_revision") != "home_assistant.harmless_light.v1":
            errors.append(path + ".manifest_revision: unsupported revision")
        if value.get("capability_id") != "home_assistant.harmless_light":
            errors.append(path + ".capability_id: unsupported capability")
    if state in ("duplicate", "conflict"):
        if value.get("existing_state") not in ("reserved", "fake_attempted", "outcome_unknown"):
            errors.append(path + ".existing_state: duplicate/conflict requires known state")
    elif "existing_state" in value:
        errors.append(path + ".existing_state: only duplicate/conflict may carry existing state")


def validate_turn(turn, path="turn"):
    errors = []
    required = ("input", "context") if type(turn) is dict and "context" in turn else ("input",)
    if not _keys(turn, required, path, errors):
        return errors
    if type(turn["input"]) is not str or not turn["input"]:
        errors.append(path + ".input: expected nonempty string")
    elif _utf8_bytes(turn["input"]) > LIMITS["input_bytes"]:
        errors.append(path + ".input: UTF-8 input exceeds cap")
    context = turn.get("context", [])
    if type(context) is not list:
        errors.append(path + ".context: expected array")
    elif len(context) > LIMITS["context_messages"]:
        errors.append(path + ".context: message count exceeds cap")
    else:
        for index, message in enumerate(context):
            loc = f"{path}.context[{index}]"
            if _keys(message, ("role", "content"), loc, errors):
                if message["role"] not in ("user", "assistant"): errors.append(loc + ".role: invalid role")
                if type(message["content"]) is not str: errors.append(loc + ".content: expected string")
        if _utf8_bytes(json.dumps(context, sort_keys=True, separators=(",", ":"), ensure_ascii=False)) > LIMITS["context_bytes"]:
            errors.append(path + ".context: UTF-8 context exceeds cap")
    return errors


def _validate_status(status, path, errors):
    if not _keys(status, ("request_id", "state"), path, errors,
                 optional=("session_id", "trace_id", "context_generation", "outcome")):
        return
    _id(status["request_id"], path + ".request_id", errors)
    if status["state"] not in ("in_progress", "terminal", "unavailable"):
        errors.append(path + ".state: invalid passive state")
        return
    if status["state"] == "terminal":
        if status.get("outcome") not in OUTCOMES:
            errors.append(path + ".outcome: terminal status requires outcome")
    elif "outcome" in status:
        errors.append(path + ".outcome: nonterminal status cannot have outcome")
    for key in ("session_id", "trace_id"):
        if key in status: _id(status[key], path + "." + key, errors)
    if "context_generation" in status and (type(status["context_generation"]) is not int or status["context_generation"] < 0):
        errors.append(path + ".context_generation: expected nonnegative integer")


def _validate_cancellation(acknowledgement, path, errors):
    if not _keys(acknowledgement, ("request_id", "state"), path, errors, optional=("outcome",)):
        return
    _id(acknowledgement["request_id"], path + ".request_id", errors)
    state = acknowledgement["state"]
    if state not in ("cancellation_requested", "already_terminal"):
        errors.append(path + ".state: invalid cancellation acknowledgement")
    elif state == "already_terminal":
        if acknowledgement.get("outcome") not in OUTCOMES:
            errors.append(path + ".outcome: terminal race requires outcome")
    elif "outcome" in acknowledgement:
        errors.append(path + ".outcome: cancellation request cannot have outcome")


def validate_fixture(document):
    errors = []
    _canonical_keys(document, "fixture", errors)
    if type(document) is not dict or type(document.get("fixture")) is not str:
        return ["fixture: expected named object"]
    fixture = document["fixture"]
    if fixture not in FIXTURE_NAMES:
        errors.append("fixture: unknown fixture name")
    combined_cancellation = fixture.startswith("cancelled_action_") or ("events" in document and ("acknowledgements" in document or "cancellation_after_seq" in document))
    if combined_cancellation:
        if document.get("cancellation_after_seq") is None:
            errors.append("cancellation_after_seq: combined fixture requires cancellation boundary")
        events = document.get("events")
        request_id = events[0].get("request_id") if isinstance(events, list) and events and isinstance(events[0], dict) else None
        acknowledgements = document.get("acknowledgements")
        if not isinstance(acknowledgements, list) or len(acknowledgements) != 1 or not isinstance(acknowledgements[0], dict) or acknowledgements[0].get("state") != "cancellation_requested" or acknowledgements[0].get("request_id") != request_id:
            errors.append("acknowledgements: combined fixture requires matching live cancellation acknowledgement")
    if "events" in document: errors.extend(validate_stream(document["events"], cancellation_after_seq=document.get("cancellation_after_seq")))
    if fixture == "accepted_error_categories":
        streams = document.get("streams")
        if type(streams) is not list or len(streams) != 5:
            errors.append("streams: expected five accepted failure streams")
        else:
            for index, stream in enumerate(streams): errors.extend(validate_stream(stream, f"streams[{index}]"))
    if fixture == "preaccept_errors":
        responses = document.get("responses")
        if type(responses) is not list: errors.append("responses: expected array")
        else:
            mapping = {400: "invalid_input", 404: "conflict_or_expired_reference", 409: "conflict_or_expired_reference", 429: "overload"}
            for index, response in enumerate(responses):
                loc = f"responses[{index}]"
                if _keys(response, ("status", "body"), loc, errors):
                    _error(response["body"], loc + ".body", errors)
                    if response["status"] not in mapping: errors.append(loc + ".status: unsupported mapping")
                    elif response["body"].get("error", {}).get("category") != mapping[response["status"]]: errors.append(loc + ".body: category contradicts status")
    if fixture in ("passive_status", "passive_status_outcome"):
        statuses = document.get("statuses")
        if type(statuses) is not list or not statuses: errors.append("statuses: expected nonempty array")
        else:
            for index, status in enumerate(statuses): _validate_status(status, f"statuses[{index}]", errors)
    if fixture in ("cancellation_race", "cancellation_outcome", "cancelled_action_known", "cancelled_action_unknown", "cancelled_action_hidden", "cancelled_action_content", "cancelled_action_repeat"):
        acknowledgements = document.get("acknowledgements")
        if type(acknowledgements) is not list or not acknowledgements: errors.append("acknowledgements: expected nonempty array")
        else:
            for index, acknowledgement in enumerate(acknowledgements): _validate_cancellation(acknowledgement, f"acknowledgements[{index}]", errors)
    if "config" in document and fixture != "invalid_optional_skill": _validate_config(document["config"], "config", errors)
    if "manifest" in document: _validate_manifest(document["manifest"], "manifest", errors)
    if "proposal" in document: _validate_proposal(document["proposal"], "proposal", errors)
    if fixture == "config_precedence":
        if document.get("selected_path") != document.get("sources", {}).get("explicit"): errors.append("selected_path: explicit path must win")
    if fixture == "invalid_optional_skill":
        disabled = []
        _validate_config(document.get("config"), "config", errors, disabled)
        if document.get("expected") != {"disabled_skills": ["sample"]} or disabled != ["sample"]:
            errors.append("expected: invalid optional skill must be disabled")
    if fixture in ("caps", "cap_overflow"):
        limits = document.get("limits")
        if type(limits) is not dict: errors.append("limits: expected object")
        else:
            for name, value in LIMITS.items():
                if limits.get(name) != value: errors.append("limits." + name + ": must equal frozen cap")
    return errors


def validate_directory(directory: str | Path):
    directory = Path(directory)
    errors = []
    seen = {"valid": set(), "invalid": set()}
    if not directory.is_dir():
        return ["fixtures: directory is unavailable"]
    for path in sorted(directory.rglob("*.json")):
        relative = path.relative_to(directory).as_posix()
        try:
            fixture_errors = validate_fixture(load_json(path))
        except ContractLoadError as exc:
            fixture_errors = [str(exc)]
        if relative.startswith("valid/"):
            seen["valid"].add(path.stem)
            if isinstance(document := (load_json(path) if not fixture_errors else None), dict) and document.get("fixture") != FIXTURES["valid"].get(path.stem):
                fixture_errors.append("fixture: name does not match fixture file")
            errors.extend(f"{relative}: {error}" for error in fixture_errors)
        elif relative.startswith("invalid/"):
            seen["invalid"].add(path.stem)
            if isinstance(document := (load_json(path) if not fixture_errors else None), dict) and document.get("fixture") != FIXTURES["invalid"].get(path.stem):
                fixture_errors.append("fixture: name does not match fixture file")
            if not fixture_errors: errors.append(f"{relative}: invalid fixture unexpectedly passed")
        else:
            errors.append(f"{relative}: fixture must be under valid or invalid")
    for kind in ("valid", "invalid"):
        missing = set(FIXTURES[kind]) - seen[kind]
        extra = seen[kind] - set(FIXTURES[kind])
        if missing: errors.append(f"{kind}: missing required fixture: {sorted(missing)[0]}")
        if extra: errors.append(f"{kind}: unexpected fixture file: {sorted(extra)[0]}")
    return errors


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("directory", nargs="?", default=API / "examples")
    args = parser.parse_args(argv)
    errors = validate_directory(args.directory)
    if errors:
        print("Invalid API contract:")
        print("\n".join(errors))
        return 1
    print(f"Valid API contract fixtures: {len(FIXTURES['valid'])} valid; {len(FIXTURES['invalid'])} invalid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
