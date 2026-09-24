"""Filesystem and environment configuration adapter for the bootstrap."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Mapping

from ..application.startup import StartupState, UNREADY_CODE
from ..domain.configuration import ConfigError, parse_core_config

DEFAULT_CONFIG_PATH = Path(__file__).with_name("fake-model.json")


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
