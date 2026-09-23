"""Local adapters for the bootstrap core; none call a provider or service."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class FakeModel:
    """A deterministic model adapter for the clean-checkout demo."""

    response: str = "The fake model is ready."

    def respond(self, text: str) -> str:
        del text
        return self.response


@dataclass(frozen=True)
class FixedClock:
    """A deterministic clock for tests and the demo."""

    value: str = "1970-01-01T00:00:00Z"

    def now(self) -> str:
        return self.value


@dataclass
class VolatileState:
    """A fake-state adapter that deliberately retains no turn material."""

    def record_turn(self, input_text: str, output_text: str, occurred_at: str) -> None:
        del input_text, output_text, occurred_at


@dataclass
class NoopTelemetry:
    """A no-op telemetry adapter that retains no operational data."""

    def emit(self, event: str, fields: Mapping[str, str]) -> None:
        del event, fields


class ToolDenied(RuntimeError):
    """Raised when bootstrap code is asked to execute a tool."""


class DisabledTools:
    """The bootstrap tool adapter denies every requested operation."""

    def dispatch(self, name: str, arguments: Mapping[str, str]) -> None:
        del name, arguments
        raise ToolDenied("tools are disabled")
