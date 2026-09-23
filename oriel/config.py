"""Strict, fail-closed loading for the bootstrap configuration."""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

API_VERSION = "1.0"
UNREADY_CODE = "config_unavailable"
DEFAULT_CONFIG_PATH = Path(__file__).with_name("fake-model.json")


class ConfigError(ValueError):
    """A configuration failure whose public diagnostic is always sanitized."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ConfigError("duplicate object key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    del value
    raise ConfigError("non-finite number")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ConfigError("non-finite number")
    return number


def load_json(path: Path) -> Any:
    """Load UTF-8 JSON while never returning a path or source payload in errors."""
    try:
        with path.open("r", encoding="utf-8") as source:
            return json.load(
                source,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
                parse_float=_finite_float,
            )
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        if isinstance(exc, ConfigError):
            raise
        raise ConfigError("invalid configuration") from None


@dataclass(frozen=True)
class CoreConfig:
    provider_connection_ref: str
    skills: Mapping[str, Mapping[str, object]]
    disabled_skills: tuple[str, ...]


def _is_reference(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256


def _validate_skill(value: object) -> Mapping[str, object] | None:
    if not isinstance(value, dict) or set(value) - {"enabled", "connection_ref"}:
        return None
    if "enabled" not in value or not isinstance(value["enabled"], bool):
        return None
    if "connection_ref" in value and not _is_reference(value["connection_ref"]):
        return None
    return MappingProxyType({key: value[key] for key in ("enabled", "connection_ref") if key in value})


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
        valid = _validate_skill(material)
        if valid is None:
            disabled.append(name)
        elif valid["enabled"]:
            enabled_skills[name] = valid
    return CoreConfig(str(provider["connection_ref"]), MappingProxyType(enabled_skills), tuple(disabled))


def select_config_path(
    explicit_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    default_path: Path = DEFAULT_CONFIG_PATH,
) -> Path:
    """Use explicit config, then environment, then exactly the packaged default."""
    source = os.environ if environ is None else environ
    if explicit_path is not None:
        return Path(explicit_path)
    if "ORIEL_CONFIG_PATH" in source:
        return Path(source["ORIEL_CONFIG_PATH"])
    return default_path


@dataclass(frozen=True)
class StartupState:
    config: CoreConfig | None
    code: str | None

    @property
    def ready(self) -> bool:
        return self.config is not None


def load_startup(
    explicit_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    default_path: Path = DEFAULT_CONFIG_PATH,
) -> StartupState:
    """Return a live-but-unready state for every selected-config failure."""
    try:
        path = select_config_path(explicit_path, environ, default_path)
        return StartupState(parse_core_config(load_json(path)), None)
    except ConfigError:
        return StartupState(None, UNREADY_CODE)
