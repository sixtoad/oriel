"""Narrow, provider-neutral ports used by the text gateway core."""
from __future__ import annotations

from dataclasses import dataclass
from typing import ContextManager, Iterable, Mapping, Protocol


class ModelPort(Protocol):
    """Produces text for an already-admitted, bounded input."""

    def respond(self, text: str) -> str: ...


@dataclass(frozen=True)
class ModelChunk:
    """One non-authoritative piece of model text."""

    content: str


@dataclass(frozen=True)
class ModelOutcome:
    """An explicit provider-neutral outcome, never inferred from prose."""

    outcome: str


ModelStreamItem = ModelChunk | ModelOutcome


class StreamingModelPort(Protocol):
    """Produces bounded text pieces and one explicit outcome for an admitted turn."""

    def stream(self, text: str) -> Iterable[ModelStreamItem]: ...


class IdentifierPort(Protocol):
    """Mints opaque correlation handles for application-owned records."""

    def next_id(self, kind: str) -> str: ...


class SynchronizationPort(Protocol):
    """Provides a narrow critical section for volatile application records."""

    def locked(self) -> ContextManager[None]: ...


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
