"""Application-owned text turns, ordering, and volatile request status."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Mapping

from .ports import (
    Clock,
    IdentifierPort,
    ModelChunk,
    ModelOutcome,
    ModelPort,
    StatePort,
    SynchronizationPort,
    TelemetryPort,
    ToolPort,
)
from .startup import StartupState
from ..domain.configuration import API_VERSION

MAX_FAKE_TURN_INPUT_BYTES = 1024
MAX_FAKE_TURN_OUTPUT_BYTES = 4096
MAX_TURN_INPUT_BYTES = 16 * 1024
MAX_CONTEXT_MESSAGES = 32
MAX_CONTEXT_BYTES = 64 * 1024
MAX_STREAM_CONTENT_BYTES = 64 * 1024
MAX_STREAM_EVENT_CONTENT_BYTES = 7 * 1024
MAX_OPEN_SESSIONS = 10
MAX_ACTIVE_TURNS = 2


@dataclass(frozen=True)
class TurnResult:
    """The bounded result used only by the local fake-model exercise."""

    text: str


@dataclass(frozen=True)
class AdmissionError(Exception):
    """A typed, safe pre-accept failure for an HTTP adapter to map."""

    status: int
    code: str
    category: str
    message: str
    retryable: bool = False


@dataclass(frozen=True)
class StreamEvent:
    """A public event whose order and terminal fence belong to the application."""

    type: str
    request_id: str
    session_id: str
    trace_id: str
    context_generation: int
    seq: int
    content: str | None = None
    error: Mapping[str, object] | None = None
    outcome: str | None = None

    def payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "api_version": API_VERSION,
            "type": self.type,
            "request_id": self.request_id,
            "session_id": self.session_id,
            "trace_id": self.trace_id,
            "context_generation": self.context_generation,
            "seq": self.seq,
        }
        if self.content is not None:
            payload["content"] = self.content
        if self.error is not None:
            payload["error"] = dict(self.error)
        if self.outcome is not None:
            payload["outcome"] = self.outcome
        return payload


@dataclass(frozen=True)
class Session:
    session_id: str
    context_generation: int = 0

    def payload(self) -> dict[str, object]:
        return {"session_id": self.session_id, "context_generation": self.context_generation}


@dataclass
class _Request:
    request_id: str
    session_id: str
    trace_id: str
    context_generation: int
    outcome: str | None = None
    seq: int = 0
    terminal_emitted: bool = False


class TextGateway:
    """Composition boundary for provider-neutral ports."""

    def __init__(
        self,
        model: ModelPort,
        clock: Clock,
        state: StatePort,
        telemetry: TelemetryPort,
        tools: ToolPort,
        identifiers: IdentifierPort,
        synchronization: SynchronizationPort,
    ) -> None:
        self._model = model
        self._clock = clock
        self._state = state
        self._telemetry = telemetry
        self._tools = tools
        self._identifiers = identifiers
        self._synchronization = synchronization
        self._sessions: dict[str, Session] = {}
        self._requests: dict[str, _Request] = {}

    def run_fake_turn(self, text: str, startup: StartupState) -> TurnResult:
        """Run a ready-gated limited internal turn; it never dispatches a tool."""
        if not startup.ready:
            raise RuntimeError("fake turn is unavailable while unready")
        input_bytes = _utf8_length(text)
        if input_bytes is None or not text or input_bytes > MAX_FAKE_TURN_INPUT_BYTES:
            raise ValueError("fake turn input is invalid")
        output = self._model.respond(text)
        output_bytes = _utf8_length(output)
        if output_bytes is None or not output or output_bytes > MAX_FAKE_TURN_OUTPUT_BYTES:
            raise ValueError("model returned invalid text")
        occurred_at = self._clock.now()
        self._state.record_turn(text, output, occurred_at)
        self._telemetry.emit("fake_turn_completed", {"input_bytes": str(input_bytes)})
        return TurnResult(text=output)

    def create_session(self) -> Session:
        """Create the small volatile handle needed by the initial turn route."""
        with self._synchronization.locked():
            if len(self._sessions) >= MAX_OPEN_SESSIONS:
                raise AdmissionError(429, "session_limit", "overload", "Session capacity is reached.", True)
            session = Session(self._identifiers.next_id("session"))
            self._sessions[session.session_id] = session
            return session

    def begin_turn(self, session_id: str, document: object, startup: StartupState) -> Iterable[StreamEvent]:
        """Admit a non-replayable turn before the returned stream invokes the model."""
        text = _validate_turn_document(document)
        with self._synchronization.locked():
            session = self._sessions.get(session_id)
            if session is None:
                raise AdmissionError(404, "session_unavailable", "conflict_or_expired_reference", "Session is unavailable.")
            if not startup.ready:
                raise AdmissionError(409, "service_unready", "conflict_or_expired_reference", "Service is unavailable.", True)
            if any(request.session_id == session_id and request.outcome is None for request in self._requests.values()):
                raise AdmissionError(409, "turn_conflict", "conflict_or_expired_reference", "A turn is already active for this session.", True)
            if sum(request.outcome is None for request in self._requests.values()) >= MAX_ACTIVE_TURNS:
                raise AdmissionError(429, "turn_capacity", "overload", "Turn capacity is reached.", True)
            request = _Request(
                request_id=self._identifiers.next_id("request"),
                session_id=session.session_id,
                trace_id=self._identifiers.next_id("trace"),
                context_generation=session.context_generation,
            )
            self._requests[request.request_id] = request
        return self._stream(request, text)

    def request_status(self, request_id: str) -> dict[str, object]:
        """Read retained status without dispatching, replaying, or mutating a turn."""
        with self._synchronization.locked():
            request = self._requests.get(request_id)
            if request is None:
                return {"request_id": request_id, "state": "unavailable"}
            payload: dict[str, object] = {
                "request_id": request.request_id,
                "session_id": request.session_id,
                "trace_id": request.trace_id,
                "context_generation": request.context_generation,
                "state": "terminal" if request.outcome is not None else "in_progress",
            }
            if request.outcome is not None:
                payload["outcome"] = request.outcome
            return payload

    def _stream(self, request: _Request, text: str) -> Iterator[StreamEvent]:
        yield self._event(request, "accepted")
        try:
            model = self._model
            if not hasattr(model, "stream"):
                yield self._error(request, "model_unavailable", "dependency_unavailable", "Model streaming is unavailable.", True)
                yield self._terminal(request, "failed")
                return
            saw_outcome = False
            streamed_bytes = 0
            for item in model.stream(text):  # type: ignore[union-attr]
                if isinstance(item, ModelChunk):
                    byte_count = _utf8_length(item.content)
                    if byte_count is None or byte_count == 0 or byte_count > MAX_STREAM_EVENT_CONTENT_BYTES or streamed_bytes + byte_count > MAX_STREAM_CONTENT_BYTES:
                        yield self._error(request, "stream_limit", "internal_failure", "Stream output exceeded a limit.", False)
                        yield self._terminal(request, "failed")
                        return
                    streamed_bytes += byte_count
                    yield self._event(request, "content_delta", content=item.content)
                elif isinstance(item, ModelOutcome):
                    if saw_outcome:
                        yield self._error(request, "invalid_outcome", "uncertainty", "Model outcome is unavailable.", False)
                        yield self._terminal(request, "outcome_unknown")
                        return
                    saw_outcome = True
                    if item.outcome not in {"completed", "denied", "failed", "outcome_unknown"}:
                        yield self._error(request, "invalid_outcome", "uncertainty", "Model outcome is unavailable.", False)
                        yield self._terminal(request, "outcome_unknown")
                        return
                    if item.outcome != "completed":
                        category = {"denied": "policy_denial", "failed": "internal_failure", "outcome_unknown": "uncertainty"}[item.outcome]
                        yield self._error(request, f"model_{item.outcome}", category, "Model did not complete the turn.", item.outcome == "failed")
                    yield self._terminal(request, item.outcome)
                    return
                else:
                    yield self._error(request, "invalid_stream", "uncertainty", "Model output is unavailable.", False)
                    yield self._terminal(request, "outcome_unknown")
                    return
            if not saw_outcome:
                yield self._error(request, "missing_outcome", "uncertainty", "Model outcome is unavailable.", False)
                yield self._terminal(request, "outcome_unknown")
        except Exception:
            yield self._error(request, "model_failure", "internal_failure", "Model did not complete the turn.", True)
            yield self._terminal(request, "failed")

    def _event(self, request: _Request, type: str, **fields: object) -> StreamEvent:
        request.seq += 1
        return StreamEvent(type, request.request_id, request.session_id, request.trace_id, request.context_generation, request.seq, **fields)

    def _error(self, request: _Request, code: str, category: str, message: str, retryable: bool) -> StreamEvent:
        return self._event(request, "error", error={"code": code, "category": category, "message": message, "retryable": retryable, "request_id": request.request_id, "session_id": request.session_id, "trace_id": request.trace_id})

    def _terminal(self, request: _Request, outcome: str) -> StreamEvent:
        with self._synchronization.locked():
            if request.terminal_emitted:
                raise RuntimeError("terminal event already emitted")
            request.terminal_emitted = True
            request.outcome = outcome
            return self._event(request, "terminal", outcome=outcome)


def _utf8_length(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return len(value.encode("utf-8"))
    except UnicodeError:
        return None


def _validate_turn_document(document: object) -> str:
    if not isinstance(document, dict) or set(document) - {"input", "context"} or "input" not in document:
        raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn input is invalid.")
    text = document["input"]
    input_bytes = _utf8_length(text)
    if not isinstance(text, str) or not text or input_bytes is None or input_bytes > MAX_TURN_INPUT_BYTES:
        raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn input is invalid.")
    context = document.get("context", [])
    if not isinstance(context, list) or len(context) > MAX_CONTEXT_MESSAGES:
        raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn context is invalid.")
    context_bytes = 0
    for message in context:
        if not isinstance(message, dict) or set(message) != {"role", "content"} or message.get("role") not in {"user", "assistant"}:
            raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn context is invalid.")
        content = message.get("content")
        size = _utf8_length(content)
        if not isinstance(content, str) or size is None or size > MAX_TURN_INPUT_BYTES:
            raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn context is invalid.")
        context_bytes += size
    if context_bytes > MAX_CONTEXT_BYTES:
        raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn context is invalid.")
    return text
