"""Local adapters for the bootstrap core; none call a provider or service."""
from __future__ import annotations

from dataclasses import dataclass, field
import secrets
from threading import Event, RLock, Thread
import time
from typing import Callable, Iterable, Mapping

from ..application.ports import ModelChunk, ModelInput, ModelOutcome, ModelStreamItem


@dataclass(frozen=True)
class FakeModel:
    """A deterministic model adapter for the clean-checkout demo."""

    response: str = "The fake model is ready."
    chunks: tuple[str, ...] | None = None
    outcome: str = "completed"
    delay_seconds: float = 0.0

    def respond(self, text: str) -> str:
        del text
        return self.response

    def stream(self, input: ModelInput) -> Iterable[ModelStreamItem]:
        del input
        for chunk in self.chunks if self.chunks is not None else (self.response,):
            if self.delay_seconds:
                time.sleep(self.delay_seconds)
            yield ModelChunk(chunk)
        yield ModelOutcome(self.outcome)


@dataclass
class SequentialIds:
    """Deterministic opaque handles for local tests and composition."""

    number: int = 0

    def next_id(self, kind: str) -> str:
        self.number += 1
        return f"fake-{kind}-{self.number}"


class SecureIds:
    """Cryptographically unguessable URL-safe handles for public composition."""

    def next_id(self, kind: str) -> str:
        return f"oriel-{kind}-{secrets.token_urlsafe(24)}"


class ThreadSafeSynchronization:
    """Serializes access to one gateway's volatile application records."""

    def __init__(self) -> None:
        self._lock = RLock()

    def locked(self) -> RLock:
        return self._lock


@dataclass(frozen=True)
class FixedClock:
    """A deterministic clock for tests and the demo."""

    value: str = "1970-01-01T00:00:00Z"

    def now(self) -> str:
        return self.value

    def monotonic(self) -> float:
        return 0.0


class RuntimeClock:
    """Wall and monotonic time for the long-running local composition."""

    def now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def monotonic(self) -> float:
        return time.monotonic()


@dataclass
class AdvanceableClock:
    """Deterministic monotonic time for lifecycle tests."""

    seconds: float = 0.0
    value: str = "1970-01-01T00:00:00Z"

    def now(self) -> str:
        return self.value

    def monotonic(self) -> float:
        return self.seconds

    def advance(self, seconds: float) -> None:
        self.seconds += seconds


@dataclass(frozen=True)
class RecordingModel(FakeModel):
    """A deterministic stream fake that retains structured inputs for tests."""

    inputs: list[ModelInput] = field(default_factory=list)

    def stream(self, input: ModelInput) -> Iterable[ModelStreamItem]:
        self.inputs.append(input)
        yield from super().stream(input)


class CleanupTrigger:
    """Outer runtime trigger that invokes application-owned expiry at most each minute."""

    def __init__(self, cleanup: Callable[[], None], interval_seconds: float = 60.0) -> None:
        self._cleanup = cleanup
        self._interval_seconds = interval_seconds
        self._stop = Event()
        self._lock = RLock()
        self._thread: Thread | None = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop = Event()
            self._thread = Thread(target=self._run, daemon=True)
            self._thread.start()

    def close(self) -> None:
        with self._lock:
            thread = self._thread
            self._stop.set()
        if thread is not None:
            thread.join(timeout=2)
        with self._lock:
            if self._thread is thread:
                self._thread = None

    def _run(self) -> None:
        self._cleanup()
        while not self._stop.wait(self._interval_seconds):
            self._cleanup()


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
