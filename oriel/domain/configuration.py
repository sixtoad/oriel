"""Provider-neutral configuration values and pure validation."""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from .ha_manifest import READ_FIELDS, TARGET_ALIAS

API_VERSION = "1.0"


class ConfigError(ValueError):
    """A configuration failure whose public diagnostic is always sanitized."""


@dataclass(frozen=True)
class CoreConfig:
    provider_connection_ref: str
    skills: Mapping[str, Mapping[str, object]]
    disabled_skills: tuple[str, ...]


def _is_reference(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256


def _validate_skill(name: str, value: object) -> Mapping[str, object] | None:
    allowed = {"enabled", "connection_ref"}
    if name == "home_assistant":
        allowed |= {"targets", "read_fields"}
    if not isinstance(value, dict) or set(value) - allowed:
        return None
    if "enabled" not in value or not isinstance(value["enabled"], bool):
        return None
    if "connection_ref" in value and not _is_reference(value["connection_ref"]):
        return None
    material: dict[str, object] = {key: value[key] for key in ("enabled", "connection_ref") if key in value}
    if name == "home_assistant":
        for key, permitted in (("targets", frozenset((TARGET_ALIAS,))), ("read_fields", READ_FIELDS)):
            if key in value:
                members = value[key]
                if not isinstance(members, (list, tuple)) or not all(isinstance(item, str) for item in members):
                    return None
                frozen = tuple(members)
                if not set(frozen).issubset(permitted):
                    return None
                material[key] = frozen
    return MappingProxyType(material)


def parse_core_config(document: object) -> CoreConfig:
    """Validate core fields and disable malformed optional skills."""
    if not isinstance(document, dict) or set(document) != {"api_version", "provider", "skills"}:
        raise ConfigError("invalid configuration")
    if document["api_version"] != API_VERSION:
        raise ConfigError("invalid configuration")
    provider = document["provider"]
    if not isinstance(provider, dict) or set(provider) != {"connection_ref"} or not _is_reference(provider.get("connection_ref")):
        raise ConfigError("invalid configuration")
    skills = document["skills"]
    if not isinstance(skills, dict):
        raise ConfigError("invalid configuration")
    enabled_skills: dict[str, Mapping[str, object]] = {}
    disabled: list[str] = []
    for name, material in skills.items():
        valid = _validate_skill(name, material)
        if valid is None:
            disabled.append(name)
        elif valid["enabled"]:
            enabled_skills[name] = valid
        else:
            disabled.append(name)
    return CoreConfig(str(provider["connection_ref"]), MappingProxyType(enabled_skills), tuple(disabled))
