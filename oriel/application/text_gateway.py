"""Application-owned text turns, volatile transcripts, and lifecycle fences."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Mapping

from .ports import Clock, IdentifierPort, ModelChunk, ModelInput, ModelMessage, ModelOutcome, ModelPort, ModelProposal, StatePort, SynchronizationPort, TelemetryPort, ToolPort
from .startup import StartupState
from ..domain.configuration import API_VERSION
from ..domain.proposals import proposal_event_size_is_bounded, validate_proposal

MAX_FAKE_TURN_INPUT_BYTES = 1024
MAX_FAKE_TURN_OUTPUT_BYTES = 4096
MAX_TURN_INPUT_BYTES = 16 * 1024
MAX_CONTEXT_MESSAGES = 32
MAX_CONTEXT_BYTES = 64 * 1024
MAX_STREAM_CONTENT_BYTES = 64 * 1024
MAX_STREAM_EVENT_CONTENT_BYTES = 7 * 1024
MAX_OPEN_SESSIONS = 10
MAX_ACTIVE_TURNS = 2
IDLE_SESSION_SECONDS = 30 * 60
MAX_SESSION_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class TurnResult:
    text: str


@dataclass(frozen=True)
class AdmissionError(Exception):
    status: int
    code: str
    category: str
    message: str
    retryable: bool = False


@dataclass(frozen=True)
class StreamEvent:
    type: str
    request_id: str
    session_id: str
    trace_id: str
    context_generation: int
    seq: int
    content: str | None = None
    proposal: Mapping[str, object] | None = None
    error: Mapping[str, object] | None = None
    outcome: str | None = None

    def payload(self) -> dict[str, object]:
        payload: dict[str, object] = {"api_version": API_VERSION, "type": self.type, "request_id": self.request_id, "session_id": self.session_id, "trace_id": self.trace_id, "context_generation": self.context_generation, "seq": self.seq}
        if self.content is not None:
            payload["content"] = self.content
        if self.proposal is not None:
            payload["proposal"] = dict(self.proposal)
        if self.error is not None:
            payload["error"] = dict(self.error)
        if self.outcome is not None:
            payload["outcome"] = self.outcome
        return payload


@dataclass
class Session:
    """Private volatile session state; payloads disclose only correlation data."""

    session_id: str
    context_generation: int = 0
    transcript: tuple[ModelMessage, ...] = ()
    created_at: float = 0.0
    last_active_at: float = 0.0

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
    """The sole owner of volatile session lifecycle and transcript policy."""

    def __init__(self, model: ModelPort, clock: Clock, state: StatePort, telemetry: TelemetryPort, tools: ToolPort, identifiers: IdentifierPort, synchronization: SynchronizationPort) -> None:
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
        if not startup.ready:
            raise RuntimeError("fake turn is unavailable while unready")
        input_bytes = _utf8_length(text)
        if input_bytes is None or not text or input_bytes > MAX_FAKE_TURN_INPUT_BYTES:
            raise ValueError("fake turn input is invalid")
        output = self._model.respond(text)
        output_bytes = _utf8_length(output)
        if output_bytes is None or not output or output_bytes > MAX_FAKE_TURN_OUTPUT_BYTES:
            raise ValueError("model returned invalid text")
        self._state.record_turn(text, output, self._clock.now())
        self._telemetry.emit("fake_turn_completed", {"input_bytes": str(input_bytes)})
        return TurnResult(text=output)

    def create_session(self) -> Session:
        with self._synchronization.locked():
            self._expire_sessions_locked()
            if len(self._sessions) >= MAX_OPEN_SESSIONS:
                raise AdmissionError(429, "session_limit", "overload", "Session capacity is reached.", True)
            now = self._monotonic()
            session = Session(self._identifiers.next_id("session"), created_at=now, last_active_at=now)
            self._sessions[session.session_id] = session
            return session

    def begin_turn(self, session_id: str, document: object, startup: StartupState) -> Iterable[StreamEvent]:
        text, supplied_context = _validate_turn_document(document)
        with self._synchronization.locked():
            self._expire_sessions_locked()
            session = self._sessions.get(session_id)
            if session is None:
                raise AdmissionError(404, "session_unavailable", "conflict_or_expired_reference", "Session is unavailable.")
            if not startup.ready:
                raise AdmissionError(409, "service_unready", "conflict_or_expired_reference", "Service is unavailable.", True)
            if any(request.session_id == session_id and request.outcome is None for request in self._requests.values()):
                raise AdmissionError(409, "turn_conflict", "conflict_or_expired_reference", "A turn is already active for this session.", True)
            if sum(request.outcome is None for request in self._requests.values()) >= MAX_ACTIVE_TURNS:
                raise AdmissionError(429, "turn_capacity", "overload", "Turn capacity is reached.", True)
            if supplied_context and session.transcript:
                raise AdmissionError(409, "context_conflict", "conflict_or_expired_reference", "Session context is already established.")
            model_input = ModelInput((*(session.transcript or supplied_context), ModelMessage("user", text)))
            _validate_transcript_capacity(model_input.messages)
            session.last_active_at = self._monotonic()
            request = _Request(self._identifiers.next_id("request"), session.session_id, self._identifiers.next_id("trace"), session.context_generation)
            self._requests[request.request_id] = request
        return self._stream(request, model_input, text, supplied_context)

    def reset_session(self, session_id: str) -> Session:
        with self._synchronization.locked():
            self._expire_sessions_locked()
            session = self._sessions.get(session_id)
            if session is None:
                raise AdmissionError(404, "session_unavailable", "conflict_or_expired_reference", "Session is unavailable.")
            self._cancel_generation_locked(session_id, session.context_generation)
            session.context_generation += 1
            session.transcript = ()
            session.last_active_at = self._monotonic()
            return session

    def end_session(self, session_id: str) -> None:
        with self._synchronization.locked():
            self._expire_sessions_locked()
            session = self._sessions.get(session_id)
            if session is None:
                raise AdmissionError(404, "session_unavailable", "conflict_or_expired_reference", "Session is unavailable.")
            self._cancel_generation_locked(session_id, session.context_generation)
            del self._sessions[session_id]

    def expire_sessions(self) -> None:
        with self._synchronization.locked():
            self._expire_sessions_locked()

    def request_status(self, request_id: str) -> dict[str, object]:
        with self._synchronization.locked():
            request = self._requests.get(request_id)
            if request is None:
                return {"request_id": request_id, "state": "unavailable"}
            payload: dict[str, object] = {"request_id": request.request_id, "session_id": request.session_id, "trace_id": request.trace_id, "context_generation": request.context_generation, "state": "terminal" if request.outcome is not None else "in_progress"}
            if request.outcome is not None:
                payload["outcome"] = request.outcome
            return payload

    def _stream(self, request: _Request, model_input: ModelInput, text: str, supplied_context: tuple[ModelMessage, ...]) -> Iterator[StreamEvent]:
        yield self._event(request, "accepted")
        if self._is_fenced(request):
            yield self._terminal(request, "cancelled")
            return
        try:
            model = self._model
            if not hasattr(model, "stream"):
                yield self._error(request, "model_unavailable", "dependency_unavailable", "Model streaming is unavailable.", True)
                yield self._terminal(request, "failed")
                return
            saw_outcome = False
            saw_content = False
            saw_proposal = False
            streamed_bytes = 0
            response_parts: list[str] = []
            provider_stream = iter(model.stream(model_input))  # type: ignore[union-attr]
            while True:
                if self._is_fenced(request):
                    yield self._terminal(request, "cancelled")
                    return
                try:
                    item = next(provider_stream)
                except StopIteration:
                    break
                if isinstance(item, ModelChunk):
                    if saw_proposal:
                        yield self._error(request, "invalid_stream", "uncertainty", "Model output is unavailable.", False)
                        yield self._terminal(request, "outcome_unknown")
                        return
                    byte_count = _utf8_length(item.content)
                    if byte_count is None or byte_count == 0 or byte_count > MAX_STREAM_EVENT_CONTENT_BYTES or streamed_bytes + byte_count > MAX_STREAM_CONTENT_BYTES:
                        yield self._error(request, "stream_limit", "internal_failure", "Stream output exceeded a limit.", False)
                        yield self._terminal(request, "failed")
                        return
                    streamed_bytes += byte_count
                    saw_content = True
                    delta = self._content_delta(request, item.content)
                    if delta is None:
                        yield self._terminal(request, "cancelled")
                        return
                    response_parts.append(item.content)
                    yield delta
                elif isinstance(item, ModelProposal):
                    if saw_content or saw_proposal or not validate_proposal(item.proposal) or not proposal_event_size_is_bounded(item.proposal):
                        yield self._error(request, "invalid_proposal", "uncertainty", "Model proposal is unavailable.", False)
                        yield self._terminal(request, "outcome_unknown")
                        return
                    saw_proposal = True
                    yield self._event(request, "proposal", proposal=item.proposal)
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
                    record_result = self._record_completed_turn(request, text, supplied_context, "".join(response_parts))
                    if record_result == "fenced":
                        yield self._terminal(request, "cancelled")
                        return
                    if record_result == "limit":
                        yield self._error(request, "transcript_limit", "internal_failure", "Response exceeded a context limit.", False)
                        yield self._terminal(request, "failed")
                        return
                    yield self._terminal(request, "completed")
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
            session = self._sessions.get(request.session_id)
            if request.outcome == "cancelled" or (outcome != "cancelled" and (session is None or session.context_generation != request.context_generation)):
                outcome = "cancelled"
            request.terminal_emitted = True
            request.outcome = outcome
            return self._event(request, "terminal", outcome=outcome)

    def _content_delta(self, request: _Request, content: str) -> StreamEvent | None:
        with self._synchronization.locked():
            if self._is_fenced_locked(request):
                return None
            return self._event(request, "content_delta", content=content)

    def _record_completed_turn(self, request: _Request, text: str, supplied_context: tuple[ModelMessage, ...], response: str) -> str:
        with self._synchronization.locked():
            if self._is_fenced_locked(request):
                return "fenced"
            session = self._sessions[request.session_id]
            updated = (*(session.transcript or supplied_context), ModelMessage("user", text), ModelMessage("assistant", response))
            try:
                _validate_transcript_capacity(updated)
            except AdmissionError:
                return "limit"
            session.transcript = updated
            session.last_active_at = self._monotonic()
            return "recorded"

    def _is_fenced(self, request: _Request) -> bool:
        with self._synchronization.locked():
            return self._is_fenced_locked(request)

    def _is_fenced_locked(self, request: _Request) -> bool:
        session = self._sessions.get(request.session_id)
        return request.outcome == "cancelled" or session is None or session.context_generation != request.context_generation

    def _expire_sessions_locked(self) -> None:
        now = self._monotonic()
        expired = [
            session_id
            for session_id, session in self._sessions.items()
            if now - session.created_at >= MAX_SESSION_SECONDS
            or (not self._generation_active_locked(session_id, session.context_generation) and now - session.last_active_at >= IDLE_SESSION_SECONDS)
        ]
        for session_id in expired:
            self._cancel_generation_locked(session_id, self._sessions[session_id].context_generation)
            del self._sessions[session_id]

    def _generation_active_locked(self, session_id: str, generation: int) -> bool:
        return any(request.session_id == session_id and request.context_generation == generation and request.outcome is None for request in self._requests.values())

    def _cancel_generation_locked(self, session_id: str, generation: int) -> None:
        for request in self._requests.values():
            if request.session_id == session_id and request.context_generation == generation and request.outcome is None:
                request.outcome = "cancelled"

    def _monotonic(self) -> float:
        if not hasattr(self._clock, "monotonic"):
            raise RuntimeError("monotonic clock is unavailable")
        return float(self._clock.monotonic())  # type: ignore[union-attr]


def _utf8_length(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return len(value.encode("utf-8"))
    except UnicodeError:
        return None


def _validate_turn_document(document: object) -> tuple[str, tuple[ModelMessage, ...]]:
    if not isinstance(document, dict) or set(document) - {"input", "context"} or "input" not in document:
        raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn input is invalid.")
    text = document["input"]
    input_bytes = _utf8_length(text)
    if not isinstance(text, str) or not text or input_bytes is None or input_bytes > MAX_TURN_INPUT_BYTES:
        raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn input is invalid.")
    context = document.get("context", [])
    if not isinstance(context, list):
        raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn context is invalid.")
    messages: list[ModelMessage] = []
    for message in context:
        if not isinstance(message, dict) or set(message) != {"role", "content"} or message.get("role") not in {"user", "assistant"}:
            raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn context is invalid.")
        content = message.get("content")
        size = _utf8_length(content)
        if not isinstance(content, str) or size is None or size > MAX_TURN_INPUT_BYTES:
            raise AdmissionError(400, "invalid_turn", "invalid_input", "Turn context is invalid.")
        messages.append(ModelMessage(message["role"], content))
    _validate_transcript_capacity(messages)
    return text, tuple(messages)


def _validate_transcript_capacity(messages: Iterable[ModelMessage]) -> None:
    materialized = tuple(messages)
    if len(materialized) > MAX_CONTEXT_MESSAGES or sum(_utf8_length(message.content) or 0 for message in materialized) > MAX_CONTEXT_BYTES:
        raise AdmissionError(400, "context_limit", "invalid_input", "Turn context exceeded a limit.")
