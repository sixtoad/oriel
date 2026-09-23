"""Narrow, provider-neutral ports used by the text gateway core."""
from __future__ import annotations

from typing import Mapping, Protocol


class ModelPort(Protocol):
    """Produces text for an already-admitted, bounded input."""

    def respond(self, text: str) -> str: ...


class Clock(Protocol):
    """Supplies an opaque timestamp for local state and telemetry."""

    def now(self) -> str: ...


class StatePort(Protocol):
    """Records a completed internal turn without defining persistence."""

    def record_turn(self, input_text: str, output_text: str, occurred_at: str) -> None: ...


class TelemetryPort(Protocol):
    """Receives bounded operational facts without defining their storage."""

    def emit(self, event: str, fields: Mapping[str, str]) -> None: ...


class ToolPort(Protocol):
    """A future tool seam. Bootstrap adapters must deny every dispatch."""

    def dispatch(self, name: str, arguments: Mapping[str, str]) -> None: ...
