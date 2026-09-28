"""Pure validation for generic, untrusted proposal stream payloads."""
from __future__ import annotations

import math
import re
from typing import Mapping


_IDENTIFIER = re.compile(r"^[A-Za-z0-9._~-]{1,128}$")
_ACTION = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SYNTHETIC_TARGET = re.compile(r"^synthetic:[a-z][a-z0-9_-]{0,63}$")
_DEADLINE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_REQUIRED = frozenset(("proposal_version", "proposal_id", "action", "target", "arguments", "dry_run", "idempotency", "deadline", "confirmation"))
_OPTIONAL = frozenset(("state", "result"))
_STATES = frozenset(("proposed", "validated", "denied", "expired", "cancelled", "failed", "completed"))
_RESULT_STATES = frozenset(("denied", "expired", "cancelled", "failed", "completed"))


def validate_proposal(value: object) -> bool:
    """Validate the existing generic proposal schema without selecting an action."""
    if not isinstance(value, Mapping) or set(value) - (_REQUIRED | _OPTIONAL):
        return False
    if set(value) & _REQUIRED != _REQUIRED:
        return False
    if value.get("proposal_version") != "1.0" or not _identifier(value.get("proposal_id")):
        return False
    if not _matches(_ACTION, value.get("action")) or not _matches(_SYNTHETIC_TARGET, value.get("target")):
        return False
    if type(value.get("dry_run")) is not bool or not _bounded_string(value.get("idempotency"), 128):
        return False
    if not _matches(_DEADLINE, value.get("deadline")) or not _arguments(value.get("arguments")):
        return False
    if not _confirmation(value.get("confirmation")):
        return False
    if "state" in value and (not isinstance(value["state"], str) or value["state"] not in _STATES):
        return False
    return "result" not in value or _result(value["result"])


def proposal_event_size_is_bounded(value: Mapping[str, object]) -> bool:
    """Conservatively keep an emitted proposal below the frozen event-data cap."""
    return _worst_case_json_bytes(value) <= 7 * 1024


def _identifier(value: object) -> bool:
    return isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None


def _matches(pattern: re.Pattern[str], value: object) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _bounded_string(value: object, maximum: int) -> bool:
    return isinstance(value, str) and 0 < len(value) <= maximum


def _arguments(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"values"}:
        return False
    values = value.get("values")
    return isinstance(values, list) and len(values) <= 16 and all(_scalar(item) for item in values)


def _scalar(value: object) -> bool:
    return value is None or type(value) in (str, int, bool) or (type(value) is float and math.isfinite(value))


def _confirmation(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"required", "evidence"}:
        return False
    evidence = value.get("evidence")
    return type(value.get("required")) is bool and (evidence is None or isinstance(evidence, str) and len(evidence) <= 256)


def _result(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) - {"state", "detail"} or "state" not in value:
        return False
    detail = value.get("detail")
    return isinstance(value["state"], str) and value["state"] in _RESULT_STATES and ("detail" not in value or isinstance(detail, str) and len(detail) <= 256)


def _worst_case_json_bytes(value: object) -> int:
    if value is None:
        return 4
    if type(value) is bool:
        return 5
    if type(value) in (int, float):
        return 32
    if isinstance(value, str):
        return 2 + 6 * len(value.encode("utf-8"))
    if isinstance(value, Mapping):
        return 2 + sum(_worst_case_json_bytes(str(key)) + 1 + _worst_case_json_bytes(item) + 1 for key, item in value.items())
    if isinstance(value, list):
        return 2 + sum(_worst_case_json_bytes(item) + 1 for item in value)
    return 8 * 1024
