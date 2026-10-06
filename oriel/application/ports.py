"""Narrow, provider-neutral ports used by the text gateway core."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, ContextManager, Iterable, Mapping, Protocol


@dataclass(frozen=True)
class HaWorkerAvailability:
    """The only HA-worker fact allowed to enter application readiness."""

    state: str

    def __post_init__(self) -> None:
        if self.state not in {"ready", "unavailable"}:
            raise ValueError("invalid HA worker availability")


class HaWorkerAvailabilityPort(Protocol):
    """Reports bounded HA-worker availability without provider material."""

    def availability(self) -> HaWorkerAvailability: ...


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


@dataclass(frozen=True)
class ModelProposal:
    """One untrusted, provider-neutral generic proposal from a model stream."""

    proposal: Mapping[str, object]


ModelStreamItem = ModelChunk | ModelOutcome | ModelProposal


@dataclass(frozen=True)
class ModelMessage:
    """One attributed conversation message supplied to a model adapter."""

    role: str
    content: str


@dataclass(frozen=True)
class ModelInput:
    """The complete bounded transcript for one isolated model turn."""

    messages: tuple[ModelMessage, ...]


@dataclass
class CancellationSignal:
    """Application-owned cooperative cancellation intent for one live turn."""

    _cancelled: bool = False

    def cancel(self) -> None:
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled


class StreamingModelPort(Protocol):
    """Produces bounded text pieces and one explicit outcome for an admitted turn."""

    def stream(self, input: ModelInput, cancellation: CancellationSignal) -> Iterable[ModelStreamItem]: ...


@dataclass(frozen=True)
class ModelOperationFailure(RuntimeError):
    """A sanitized, provider-neutral failure from one bounded model operation."""

    code: str
    category: str
    message: str
    retryable: bool = True


class IdentifierPort(Protocol):
    """Mints opaque correlation handles for application-owned records."""

    def next_id(self, kind: str) -> str: ...


class SynchronizationPort(Protocol):
    """Provides a narrow critical section for volatile application records."""

    def locked(self) -> ContextManager[None]: ...

    def model_start_locked(self) -> ContextManager[None]: ...

    def wait(self, timeout: float | None = None) -> None: ...

    def notify_all(self) -> None: ...


class Clock(Protocol):
    """Supplies an opaque timestamp for local state and telemetry."""

    def now(self) -> str: ...


class MonotonicClock(Protocol):
    """Supplies monotonic seconds for volatile lifecycle decisions."""

    def monotonic(self) -> float: ...


class ScheduledCall(Protocol):
    """Allows application lifecycle code to retract a scheduled callback."""

    def cancel(self) -> None: ...


class SchedulerPort(Protocol):
    """Schedules application-owned callbacks without choosing timer infrastructure."""

    def schedule(self, delay_seconds: float, callback: Callable[[], None]) -> ScheduledCall: ...


class BackgroundTaskPort(Protocol):
    """Runs provider work outside the stream-event serialization loop."""

    def start(self, callback: Callable[[], None]) -> None: ...


class StatePort(Protocol):
    """Records a completed internal turn without defining persistence."""

    def record_turn(self, input_text: str, output_text: str, occurred_at: str) -> None: ...


@dataclass(frozen=True)
class RequestStatusRecord:
    """The payload-free durable correlation state for one admitted turn."""

    request_id: str
    session_id: str
    trace_id: str
    context_generation: int
    state: str
    outcome: str | None
    admitted_at: str
    expires_at: str


class RequestLedgerUnavailable(RuntimeError):
    """A storage adapter could not complete a durable ledger operation."""


class RequestLedgerPort(Protocol):
    """Durably reserves and reports server-generated request identities."""

    def reserve(self, record: RequestStatusRecord) -> None: ...

    def mark_terminal(self, request_id: str, outcome: str) -> None: ...

    def lookup(self, request_id: str, now: str) -> RequestStatusRecord | None: ...

    def recover_interrupted(self) -> None: ...


class TelemetryPort(Protocol):
    """Receives bounded operational facts without defining their storage."""

    def emit(self, event: str, fields: Mapping[str, str]) -> None: ...


@dataclass(frozen=True)
class RouteTelemetry:
    """Payload-free, correlated facts emitted once for one routing decision."""

    request_id: str
    session_id: str
    trace_id: str
    route: str
    rule_revision: str
    duration_ms: int

    def fields(self) -> Mapping[str, str]:
        return {
            "request_id": self.request_id,
            "session_id": self.session_id,
            "trace_id": self.trace_id,
            "route": self.route,
            "rule_revision": self.rule_revision,
            "duration_ms": str(self.duration_ms),
        }


class ToolPort(Protocol):
    """A future tool seam. Bootstrap adapters must deny every dispatch."""

    def dispatch(self, name: str, arguments: Mapping[str, str]) -> None: ...
