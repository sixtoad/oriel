"""Filesystem and environment configuration adapter for the bootstrap."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from ..application.configuration import ActivationResult, ActivationSucceeded, ConfigurationService
from ..application.startup import StartupState, UNREADY_CODE
from ..domain.configuration import ConfigError, parse_core_config

DEFAULT_CONFIG_PATH = Path(__file__).with_name("fake-model.json")


class ProfileUnavailable(ValueError):
    """A sanitized failure to resolve an opaque provider connection reference."""


@dataclass(frozen=True)
class ResolvedProviderProfile:
    """Adapter-private provider profile selected only by the composition root."""

    label: str


class ProviderProfileResolver(Protocol):
    """Resolves an opaque connection reference without changing core policy."""

    def resolve(self, connection_ref: str) -> ResolvedProviderProfile: ...


class StaticProfileResolver:
    """Deterministic local mapping used until the Qwen adapter owns real profiles."""

    def __init__(self, profiles: Mapping[str, ResolvedProviderProfile]) -> None:
        self._profiles = MappingProxyType(dict(profiles))

    def resolve(self, connection_ref: str) -> ResolvedProviderProfile:
        try:
            profile = self._profiles[connection_ref]
        except KeyError:
            raise ProfileUnavailable("provider profile is unavailable") from None
        if not isinstance(profile, ResolvedProviderProfile) or not isinstance(profile.label, str):
            raise ProfileUnavailable("provider profile is unavailable")
        return profile


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
    """Load UTF-8 JSON without returning a path or source payload in errors."""
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


def activate_startup(
    service: ConfigurationService,
    resolver: ProviderProfileResolver,
    expected_revision: int | None = None,
    explicit_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    default_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[StartupState, ResolvedProviderProfile | None]:
    """Select, validate, resolve, and atomically activate one startup document.

    The resolver is supplied by the composition root. A missing profile is a
    sanitized rejection, so an earlier active revision remains usable.
    """
    try:
        path = select_config_path(explicit_path, environ, default_path)
        candidate = parse_core_config(load_json(path))
    except ConfigError:
        return _startup_from_result(service.reject()), None
    stale = service.stale_result(expected_revision)
    if stale is not None:
        return _startup_from_result(stale), None
    try:
        profile = resolver.resolve(candidate.provider_connection_ref)
    except ProfileUnavailable:
        return _startup_from_result(service.reject()), None
    result = service.activate_config(candidate, expected_revision, profile.label)
    return _startup_from_result(result), profile if isinstance(result, ActivationSucceeded) else None


def _startup_from_result(result: ActivationResult) -> StartupState:
    active = result.active
    if active is None:
        return StartupState(None, UNREADY_CODE, activation_result=result)
    return StartupState(active.config, None, active.revision, active.effective, result)
