"""Application-owned text turns, volatile transcripts, and lifecycle fences."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Iterator, Mapping
from types import MappingProxyType

from .fast_router import FastRoute, route as select_fast_route
from .ports import BackgroundTaskPort, CancellationSignal, Clock, DryRunPreview, DryRunPreviewPort, IdentifierPort, ModelChunk, ModelInput, ModelMessage, ModelOperationFailure, ModelOutcome, ModelPort, ModelProposal, RequestLedgerPort, RequestLedgerUnavailable, RequestStatusRecord, RouteTelemetry, ScheduledCall, SchedulerPort, StatePort, SynchronizationPort, TelemetryPort, ToolPort
from .startup import StartupState
from ..domain.configuration import API_VERSION
from ..domain.ha_manifest import BUILT_IN_MANIFEST, BuiltInManifest, CanonicalProposal, is_ha_shaped_candidate, preview_eligibility
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
MAX_QUEUED_TURNS = 8
MAX_PENDING_PROVIDER_ITEMS = 16
TURN_DEADLINE_SECONDS = 30.0
ACK_DELAY_SECONDS = 0.5
ACK_MESSAGE = "Work is continuing."
_ACK_WAKE = object()
_PUMP_DONE = object()
_PUMP_CANCELLED = object()
_PUMP_DEADLINE = object()
IDLE_SESSION_SECONDS = 30 * 60
MAX_SESSION_SECONDS = 24 * 60 * 60
REQUEST_RETENTION_SECONDS = 24 * 60 * 60


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
    message: str | None = None
    content: str | None = None
    proposal: Mapping[str, object] | None = None
    preview: Mapping[str, object] | None = None
    error: Mapping[str, object] | None = None
    outcome: str | None = None

    def payload(self) -> dict[str, object]:
        payload: dict[str, object] = {"api_version": API_VERSION, "type": self.type, "request_id": self.request_id, "session_id": self.session_id, "trace_id": self.trace_id, "context_generation": self.context_generation, "seq": self.seq}
        if self.message is not None:
            payload["message"] = self.message
        if self.content is not None:
            payload["content"] = self.content
        if self.proposal is not None:
            payload["proposal"] = dict(self.proposal)
        if self.preview is not None:
            payload["preview"] = dict(self.preview)
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
    cancellation: CancellationSignal = field(default_factory=CancellationSignal)
    queued: bool = False
    queued_at_admission: bool = False
    queued_start_order: int | None = None
    provider_start_claimed: bool = False
    deadline_at: float = 0.0
    admitted_monotonic: float = 0.0
    acknowledgement: ScheduledCall | None = None
    acknowledgement_pending: bool = False
    acknowledgement_emitted: bool = False
    useful_output_emitted: bool = False
    first_model_token_recorded: bool = False
    first_useful_content_recorded: bool = False
    pending_provider_items: deque[object] = field(default_factory=deque)
    startup_failure: str | None = None


class TextGateway:
    """The sole owner of volatile session lifecycle and transcript policy."""

    def __init__(self, model: ModelPort, clock: Clock, state: StatePort, telemetry: TelemetryPort, tools: ToolPort, identifiers: IdentifierPort, synchronization: SynchronizationPort, ledger: RequestLedgerPort, turn_deadline_seconds: float = TURN_DEADLINE_SECONDS, scheduler: SchedulerPort | None = None, tasks: BackgroundTaskPort | None = None, ha_restrictions: Mapping[str, object] | None = None, ha_manifest: BuiltInManifest = BUILT_IN_MANIFEST, ha_preview: DryRunPreviewPort | None = None) -> None:
        self._model = model
        self._clock = clock
        self._state = state
        self._telemetry = telemetry
        self._tools = tools
        self._identifiers = identifiers
        self._synchronization = synchronization
        self._ledger = ledger
        self._sessions: dict[str, Session] = {}
        self._requests: dict[str, _Request] = {}
        self._queued_request_ids: deque[str] = deque()
        self._next_queued_start_order = 0
        self._turn_deadline_seconds = turn_deadline_seconds
        self._scheduler = scheduler
        self._tasks = tasks
        self._ha_restrictions = None if ha_restrictions is None else MappingProxyType({key: tuple(value) if isinstance(value, (list, tuple)) else value for key, value in ha_restrictions.items()})
        self._ha_manifest = ha_manifest
        self._ha_preview = ha_preview

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
            if any(request.session_id == session_id and request.context_generation == session.context_generation and request.outcome is None for request in self._requests.values()):
                raise AdmissionError(409, "turn_conflict", "conflict_or_expired_reference", "A turn is already active for this session.", True)
            active = sum(request.outcome is None and not request.queued for request in self._requests.values())
            if active >= MAX_ACTIVE_TURNS and len(self._queued_request_ids) >= MAX_QUEUED_TURNS:
                raise AdmissionError(429, "turn_capacity", "overload", "Turn capacity is reached.", True)
            if supplied_context and session.transcript:
                raise AdmissionError(409, "context_conflict", "conflict_or_expired_reference", "Session context is already established.")
            model_input = ModelInput((*(session.transcript or supplied_context), ModelMessage("user", text)))
            _validate_transcript_capacity(model_input.messages)
            session.last_active_at = self._monotonic()
            queued = active >= MAX_ACTIVE_TURNS
            queued_start_order = self._next_queued_start_order if queued else None
            if queued:
                self._next_queued_start_order += 1
            request = _Request(
                self._identifiers.next_id("request"), session.session_id, self._identifiers.next_id("trace"), session.context_generation,
                queued=queued,
                queued_at_admission=queued,
                queued_start_order=queued_start_order,
                deadline_at=self._monotonic() + self._turn_deadline_seconds,
            )
            admitted_at = self._clock.now()
            try:
                self._ledger.reserve(
                    RequestStatusRecord(
                        request.request_id,
                        request.session_id,
                        request.trace_id,
                        request.context_generation,
                        "in_progress",
                        None,
                        admitted_at,
                        _request_expiry(admitted_at),
                    )
                )
            except RequestLedgerUnavailable:
                raise AdmissionError(503, "request_ledger_unavailable", "dependency_unavailable", "Request status storage is unavailable.", True) from None
            request.admitted_monotonic = self._monotonic()
            route_started = self._monotonic()
            decision = select_fast_route(text, model_input.messages[:-1], self._proposal_deadline())
            duration_ms = max(0, round((self._monotonic() - route_started) * 1000))
            self._requests[request.request_id] = request
            if request.queued:
                self._queued_request_ids.append(request.request_id)
        try:
            self._telemetry.emit("route_selected", RouteTelemetry(request.request_id, request.session_id, request.trace_id, decision.route, decision.rule_revision, duration_ms).fields())
        except Exception:
            pass
        if decision.route == "qwen":
            try:
                self._schedule_acknowledgement(request)
            except Exception:
                request.startup_failure = "scheduler"
        return self._stream(request, model_input, text, supplied_context, decision)

    def reset_session(self, session_id: str) -> Session:
        with self._synchronization.locked():
            self._expire_sessions_locked()
            session = self._sessions.get(session_id)
            if session is None:
                raise AdmissionError(404, "session_unavailable", "conflict_or_expired_reference", "Session is unavailable.")
            try:
                self._cancel_generation_locked(session_id, session.context_generation)
            except RequestLedgerUnavailable:
                raise _ledger_unavailable_error() from None
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
            try:
                self._cancel_generation_locked(session_id, session.context_generation)
            except RequestLedgerUnavailable:
                raise _ledger_unavailable_error() from None
            del self._sessions[session_id]

    def expire_sessions(self) -> None:
        with self._synchronization.locked():
            self._expire_sessions_locked()

    def request_status(self, request_id: str) -> dict[str, object]:
        try:
            record = self._ledger.lookup(request_id, self._clock.now())
        except RequestLedgerUnavailable:
            raise _ledger_unavailable_error() from None
        if record is None:
            return {"request_id": request_id, "state": "unavailable"}
        payload: dict[str, object] = {"request_id": record.request_id, "session_id": record.session_id, "trace_id": record.trace_id, "context_generation": record.context_generation, "state": record.state}
        if record.outcome is not None:
            payload["outcome"] = record.outcome
        return payload

    def cancel_request(self, request_id: str) -> dict[str, object]:
        """Request cooperative cancellation without assigning a terminal outcome."""
        with self._synchronization.locked():
            request = self._requests.get(request_id)
            if request is not None and request.outcome is None:
                request.cancellation.cancel()
                self._cancel_acknowledgement_locked(request)
                if request.queued:
                    self._remove_queued_locked(request.request_id)
                self._synchronization.notify_all()
                return {"request_id": request_id, "state": "cancellation_requested"}
            try:
                record = self._ledger.lookup(request_id, self._clock.now())
            except RequestLedgerUnavailable:
                raise _ledger_unavailable_error() from None
            if record is None or record.state != "terminal" or record.outcome is None:
                raise AdmissionError(404, "request_unavailable", "conflict_or_expired_reference", "Request is unavailable.")
            return {"request_id": request_id, "state": "already_terminal", "outcome": record.outcome}

    def deliver_stream_event(self, event: StreamEvent, deliver: Callable[[StreamEvent], None]) -> bool:
        """Serialize useful stream delivery with cancellation acknowledgement."""
        with self._synchronization.locked():
            if event.type in {"ack", "content_delta", "proposal", "validation"}:
                request = self._requests.get(event.request_id)
                if request is None or self._is_fenced_locked(request):
                    return False
            deliver(event)
            return True

    def recover_interrupted_requests(self) -> None:
        """Fail admitted work from an earlier process without replaying it."""
        self._ledger.recover_interrupted()

    def _stream(self, request: _Request, model_input: ModelInput, text: str, supplied_context: tuple[ModelMessage, ...], decision: FastRoute) -> Iterator[StreamEvent]:
        yield self._event(request, "accepted")
        if request.startup_failure is not None:
            yield self._error(request, "stream_start_failed", "internal_failure", "Request processing is unavailable.", True)
            yield self._terminal(request, "failed")
            return
        if decision.route != "qwen":
            yield from self._stream_fast_route(request, text, supplied_context, decision)
            return
        model = self._model
        if not hasattr(model, "stream"):
            yield self._error(request, "model_unavailable", "dependency_unavailable", "Model streaming is unavailable.", True)
            yield self._terminal(request, "failed")
            return
        if self._tasks is None:
            admission = self._await_start(request)
            if admission == "cancelled":
                yield self._terminal(request, "cancelled")
                return
            if admission == "deadline":
                yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                yield self._terminal(request, "failed")
                return
        else:
            try:
                self._tasks.start(lambda: self._pump_provider_stream(request, model, model_input))
            except Exception:
                yield self._error(request, "stream_start_failed", "internal_failure", "Request processing is unavailable.", True)
                yield self._terminal(request, "failed")
                return
        try:
            saw_outcome = False
            saw_content = False
            saw_proposal = False
            streamed_bytes = 0
            response_parts: list[str] = []
            provider_stream: Iterator[object] | None = None
            first_provider_item = True
            while True:
                acknowledgement = self._pending_acknowledgement(request)
                if acknowledgement is not None:
                    yield acknowledgement
                    continue
                if self._is_fenced(request):
                    yield self._terminal(request, "cancelled")
                    return
                if self._deadline_expired(request):
                    yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                    yield self._terminal(request, "failed")
                    return
                try:
                    if self._tasks is not None:
                        item = self._next_pumped_item(request)
                        if item is _ACK_WAKE:
                            acknowledgement = self._pending_acknowledgement(request)
                            if acknowledgement is not None:
                                yield acknowledgement
                            continue
                        if item is _PUMP_CANCELLED:
                            yield self._terminal(request, "cancelled")
                            return
                        if item is _PUMP_DEADLINE:
                            yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                            yield self._terminal(request, "failed")
                            return
                        if item is _PUMP_DONE:
                            break
                        if isinstance(item, BaseException):
                            raise item
                    elif first_provider_item and request.queued_at_admission:
                        provider_stream = self._start_queued_provider_stream(request, model, model_input)
                        item = next(provider_stream)
                        first_provider_item = False
                    elif self._tasks is None:
                        provider_stream = iter(model.stream(model_input, request.cancellation)) if provider_stream is None else provider_stream  # type: ignore[union-attr]
                        item = next(provider_stream)
                        first_provider_item = False
                except StopIteration:
                    if self._is_fenced(request):
                        yield self._terminal(request, "cancelled")
                        return
                    if self._deadline_expired(request):
                        yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                        yield self._terminal(request, "failed")
                        return
                    break
                if self._is_fenced(request):
                    yield self._terminal(request, "cancelled")
                    return
                if self._deadline_expired(request):
                    yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                    yield self._terminal(request, "failed")
                    return
                acknowledgement = self._pending_acknowledgement(request)
                if acknowledgement is not None:
                    yield acknowledgement
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
                    self._record_first_model_token(request)
                    saw_content = True
                    delta = self._content_delta(request, item.content)
                    if delta is None:
                        yield self._terminal(request, "cancelled")
                        return
                    response_parts.append(item.content)
                    yield delta
                elif isinstance(item, ModelProposal):
                    if saw_content or saw_proposal:
                        yield self._error(request, "invalid_proposal", "uncertainty", "Model proposal is unavailable.", False)
                        yield self._terminal(request, "outcome_unknown")
                        return
                    admitted = self._admit_proposal(request, item.proposal)
                    if admitted is None:
                        if is_ha_shaped_candidate(item.proposal):
                            yield self._error(request, "ha_proposal_denied", "policy_denial", "The proposed action is unavailable.", False)
                            yield self._terminal(request, "denied")
                        else:
                            yield self._error(request, "invalid_proposal", "uncertainty", "Model proposal is unavailable.", False)
                            yield self._terminal(request, "outcome_unknown")
                        return
                    saw_proposal = True
                    proposal, canonical_preview = admitted
                    yield proposal
                    if canonical_preview is not None:
                        preview_event, terminal_outcome = self._preview_ha_proposal(request, canonical_preview)
                        if preview_event is None:
                            yield self._terminal(request, "cancelled")
                            return
                        yield preview_event
                        if terminal_outcome is not None:
                            yield self._terminal(request, terminal_outcome)
                            return
                        record_result, terminal = self._complete_turn(request, text, supplied_context, "")
                        if record_result == "fenced":
                            yield self._terminal(request, "cancelled")
                        elif record_result == "deadline":
                            yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                            yield self._terminal(request, "failed")
                        elif record_result == "limit":
                            yield self._error(request, "transcript_limit", "internal_failure", "Response exceeded a context limit.", False)
                            yield self._terminal(request, "failed")
                        elif terminal is not None:
                            yield terminal
                        else:
                            raise RuntimeError("completed preview is missing its terminal event")
                        return
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
                    record_result, terminal = self._complete_turn(request, text, supplied_context, "".join(response_parts))
                    if record_result == "fenced":
                        yield self._terminal(request, "cancelled")
                        return
                    if record_result == "deadline":
                        yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                        yield self._terminal(request, "failed")
                        return
                    if record_result == "limit":
                        yield self._error(request, "transcript_limit", "internal_failure", "Response exceeded a context limit.", False)
                        yield self._terminal(request, "failed")
                        return
                    if terminal is None:
                        raise RuntimeError("completed turn is missing its terminal event")
                    yield terminal
                    return
                else:
                    yield self._error(request, "invalid_stream", "uncertainty", "Model output is unavailable.", False)
                    yield self._terminal(request, "outcome_unknown")
                    return
            if not saw_outcome:
                yield self._error(request, "missing_outcome", "uncertainty", "Model outcome is unavailable.", False)
                yield self._terminal(request, "outcome_unknown")
        except ModelOperationFailure as failure:
            if self._is_fenced(request):
                yield self._terminal(request, "cancelled")
                return
            if self._deadline_expired(request):
                yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                yield self._terminal(request, "failed")
                return
            code, category, message, retryable = _safe_model_failure(failure)
            yield self._error(request, code, category, message, retryable)
            yield self._terminal(request, "failed")
        except Exception:
            if self._is_fenced(request):
                yield self._terminal(request, "cancelled")
                return
            if self._deadline_expired(request):
                yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
                yield self._terminal(request, "failed")
                return
            yield self._error(request, "model_failure", "internal_failure", "Model did not complete the turn.", True)
            yield self._terminal(request, "failed")

    def _stream_fast_route(self, request: _Request, text: str, supplied_context: tuple[ModelMessage, ...], decision: FastRoute) -> Iterator[StreamEvent]:
        """Use the ordinary stream and transcript rules for a rule-owned response."""
        if self._is_fenced(request):
            yield self._terminal(request, "cancelled")
            return
        if decision.route == "denial":
            yield self._error(request, decision.error_code or "fast_route_denied", decision.error_category or "policy_denial", decision.error_message or "This request is not allowed.", False)
            yield self._terminal(request, "denied")
            return
        if decision.route == "proposal":
            admitted = self._admit_proposal(request, decision.proposal or {})
            if admitted is None:
                yield self._error(request, "fast_proposal_denied", "policy_denial", "The proposed action is unavailable.", False)
                yield self._terminal(request, "denied")
                return
            proposal, canonical_preview = admitted
            yield proposal
            if canonical_preview is not None:
                preview_event, terminal_outcome = self._preview_ha_proposal(request, canonical_preview)
                if preview_event is None:
                    yield self._terminal(request, "cancelled")
                    return
                yield preview_event
                if terminal_outcome is not None:
                    yield self._terminal(request, terminal_outcome)
                    return
            result, terminal = self._complete_turn(request, text, supplied_context, "", enforce_deadline=False)
        else:
            content = decision.content
            if decision.route not in {"content", "clarification", "limitation"} or content is None:
                yield self._error(request, "fast_route_failure", "internal_failure", "The deterministic response is unavailable.", False)
                yield self._terminal(request, "failed")
                return
            delta = self._content_delta(request, content)
            if delta is None:
                yield self._terminal(request, "cancelled")
                return
            yield delta
            result, terminal = self._complete_turn(request, text, supplied_context, content, enforce_deadline=False)
        if result == "fenced":
            yield self._terminal(request, "cancelled")
        elif result == "deadline":
            yield self._error(request, "model_deadline", "timeout", "Model did not complete before its deadline.", True)
            yield self._terminal(request, "failed")
        elif result == "limit":
            yield self._error(request, "transcript_limit", "internal_failure", "Response exceeded a context limit.", False)
            yield self._terminal(request, "failed")
        elif terminal is not None:
            yield terminal
        else:
            raise RuntimeError("completed fast route is missing its terminal event")

    def _event(self, request: _Request, type: str, **fields: object) -> StreamEvent:
        request.seq += 1
        return StreamEvent(type, request.request_id, request.session_id, request.trace_id, request.context_generation, request.seq, **fields)

    def _error(self, request: _Request, code: str, category: str, message: str, retryable: bool) -> StreamEvent:
        return self._event(request, "error", error={"code": code, "category": category, "message": message, "retryable": retryable, "request_id": request.request_id, "session_id": request.session_id, "trace_id": request.trace_id})

    def _terminal(self, request: _Request, outcome: str) -> StreamEvent:
        with self._synchronization.locked():
            return self._terminal_locked(request, outcome)

    def _terminal_locked(self, request: _Request, outcome: str) -> StreamEvent:
        if request.terminal_emitted:
            raise RuntimeError("terminal event already emitted")
        session = self._sessions.get(request.session_id)
        if request.cancellation.is_cancelled() or request.outcome == "cancelled" or (outcome != "cancelled" and (session is None or session.context_generation != request.context_generation)):
            outcome = "cancelled"
        try:
            self._ledger.mark_terminal(request.request_id, outcome)
        except RequestLedgerUnavailable:
            outcome = "failed"
        request.terminal_emitted = True
        request.outcome = outcome
        request.cancellation.cancel()
        self._cancel_acknowledgement_locked(request)
        request.pending_provider_items.clear()
        self._synchronization.notify_all()
        event = self._event(request, "terminal", outcome=outcome)
        self._emit_timing("terminal", request)
        self._requests.pop(request.request_id, None)
        self._remove_queued_locked(request.request_id)
        self._promote_queued_locked()
        return event

    def _content_delta(self, request: _Request, content: str) -> StreamEvent | None:
        with self._synchronization.locked():
            if self._is_fenced_locked(request):
                return None
            self._mark_useful_output_locked(request)
            return self._event(request, "content_delta", content=content)

    def _proposal_event(self, request: _Request, proposal: Mapping[str, object]) -> StreamEvent | None:
        with self._synchronization.locked():
            if self._is_fenced_locked(request):
                return None
            self._mark_useful_output_locked(request)
            return self._event(request, "proposal", proposal=proposal)

    def _admit_proposal(self, request: _Request, proposal: Mapping[str, object]) -> tuple[StreamEvent, CanonicalProposal | None] | None:
        """Apply the one application proposal boundary shared by every route."""
        if is_ha_shaped_candidate(proposal):
            result = preview_eligibility(proposal, self._ha_restrictions, self._ha_manifest)
            if result.denial_code is not None:
                try:
                    self._telemetry.emit("ha_proposal_denied", {"code": result.denial_code})
                except Exception:
                    pass
                return None
            if result.material is None:
                return None
            event = self._proposal_event(request, _canonical_proposal_payload(result.material))
            if event is None:
                return None
            return event, result.material
        if not validate_proposal(proposal) or not proposal_event_size_is_bounded(proposal):
            return None
        event = self._proposal_event(request, proposal)
        return None if event is None else (event, None)

    def _preview_ha_proposal(self, request: _Request, proposal: CanonicalProposal) -> tuple[StreamEvent | None, str | None]:
        """Preview through the inward adapter; this path never reaches tools."""
        if self._is_fenced(request):
            return None, "cancelled"
        if self._ha_preview is None:
            result = DryRunPreview("unavailable", None, None, None, None, "adapter_unavailable")
        else:
            try:
                result = self._ha_preview.preview(proposal)
            except Exception:
                result = DryRunPreview("unavailable", None, None, None, None, "adapter_unavailable")
        if type(result) is not DryRunPreview or not _preview_matches_proposal(result, proposal):
            result = DryRunPreview("unavailable", None, None, None, None, "adapter_unavailable")
        if self._is_fenced(request):
            return None, "cancelled"
        preview = _preview_payload(result)
        event = self._validation_event(request, preview)
        if result.status == "simulated":
            return event, None
        return event, "denied" if result.status == "denied" else "failed"

    def _validation_event(self, request: _Request, preview: Mapping[str, object]) -> StreamEvent | None:
        with self._synchronization.locked():
            if self._is_fenced_locked(request):
                return None
            self._mark_useful_output_locked(request)
            return self._event(request, "validation", preview=preview)

    def _schedule_acknowledgement(self, request: _Request) -> None:
        """Arm the one non-authoritative acknowledgement from durable admission."""
        if self._scheduler is None:
            return
        delay = max(0.0, request.admitted_monotonic + ACK_DELAY_SECONDS - self._monotonic())
        request.acknowledgement = self._scheduler.schedule(delay, lambda: self._acknowledgement_due(request))

    def _acknowledgement_due(self, request: _Request) -> None:
        with self._synchronization.locked():
            if request.terminal_emitted or self._is_fenced_locked(request) or request.useful_output_emitted:
                return
            request.acknowledgement_pending = True
            if self._tasks is not None and len(request.pending_provider_items) < MAX_PENDING_PROVIDER_ITEMS:
                request.pending_provider_items.append(_ACK_WAKE)
            self._synchronization.notify_all()

    def _pump_provider_stream(self, request: _Request, model: ModelPort, model_input: ModelInput) -> None:
        """Move blocking provider reads outside application event serialization."""
        try:
            admission = self._await_start(request)
            if admission == "cancelled":
                self._append_pumped_item(request, _PUMP_CANCELLED)
                return
            if admission == "deadline":
                self._append_pumped_item(request, _PUMP_DEADLINE)
                return
            if request.queued_at_admission:
                stream = self._start_queued_provider_stream(request, model, model_input)
            else:
                stream = iter(model.stream(model_input, request.cancellation))  # type: ignore[union-attr]
            for item in stream:
                if not self._append_pumped_item(request, item):
                    return
        except Exception as failure:
            self._append_pumped_item(request, failure)
        finally:
            self._append_pumped_item(request, _PUMP_DONE)

    def _append_pumped_item(self, request: _Request, item: object) -> bool:
        with self._synchronization.locked():
            while len(request.pending_provider_items) >= MAX_PENDING_PROVIDER_ITEMS:
                if request.terminal_emitted or self._is_fenced_locked(request):
                    return False
                self._synchronization.wait()
            if request.terminal_emitted or self._is_fenced_locked(request):
                return False
            if _provider_item_is_useful(item):
                self._mark_useful_output_locked(request)
            elif item is _PUMP_DONE or isinstance(item, (ModelOutcome, BaseException)) or not isinstance(item, (ModelChunk, ModelProposal)):
                self._cancel_acknowledgement_locked(request)
            request.pending_provider_items.append(item)
            self._synchronization.notify_all()
            return True

    def _next_pumped_item(self, request: _Request) -> object:
        with self._synchronization.locked():
            while not request.pending_provider_items:
                if self._is_fenced_locked(request):
                    return _PUMP_CANCELLED
                remaining = request.deadline_at - self._monotonic()
                if remaining <= 0:
                    return _PUMP_DEADLINE
                self._synchronization.wait(remaining)
            item = request.pending_provider_items.popleft()
            self._synchronization.notify_all()
            return item

    def _pending_acknowledgement(self, request: _Request) -> StreamEvent | None:
        with self._synchronization.locked():
            if not request.acknowledgement_pending or request.acknowledgement_emitted:
                return None
            if self._is_fenced_locked(request) or request.terminal_emitted or request.useful_output_emitted:
                self._cancel_acknowledgement_locked(request)
                return None
            request.acknowledgement_pending = False
            request.acknowledgement_emitted = True
            event = self._event(request, "ack", message=ACK_MESSAGE)
            self._emit_timing("acknowledgement", request)
            return event

    def _mark_useful_output_locked(self, request: _Request) -> None:
        request.useful_output_emitted = True
        self._cancel_acknowledgement_locked(request)
        if not request.first_useful_content_recorded:
            request.first_useful_content_recorded = True
            self._emit_timing("first_useful_content", request)

    def _record_first_model_token(self, request: _Request) -> None:
        with self._synchronization.locked():
            if request.first_model_token_recorded:
                return
            request.first_model_token_recorded = True
            self._emit_timing("first_model_token", request)

    def _cancel_acknowledgement_locked(self, request: _Request) -> None:
        request.acknowledgement_pending = False
        if request.acknowledgement is not None:
            request.acknowledgement.cancel()
            request.acknowledgement = None

    def _emit_timing(self, event: str, request: _Request) -> None:
        """Emit correlation and elapsed time only; never include streamed material."""
        try:
            self._telemetry.emit(event, {
                "request_id": request.request_id,
                "session_id": request.session_id,
                "trace_id": request.trace_id,
                "duration_ms": str(max(0, round((self._monotonic() - request.admitted_monotonic) * 1000))),
            })
        except Exception:
            pass

    def _complete_turn(self, request: _Request, text: str, supplied_context: tuple[ModelMessage, ...], response: str, enforce_deadline: bool = True) -> tuple[str, StreamEvent | None]:
        with self._synchronization.locked():
            if self._is_fenced_locked(request):
                return "fenced", None
            if enforce_deadline and self._deadline_expired_locked(request):
                return "deadline", None
            session = self._sessions[request.session_id]
            updated = (*(session.transcript or supplied_context), ModelMessage("user", text), ModelMessage("assistant", response))
            try:
                _validate_transcript_capacity(updated)
            except AdmissionError:
                return "limit", None
            session.transcript = updated
            session.last_active_at = self._monotonic()
            return "recorded", self._terminal_locked(request, "completed")

    def _is_fenced(self, request: _Request) -> bool:
        with self._synchronization.locked():
            return self._is_fenced_locked(request)

    def _is_fenced_locked(self, request: _Request) -> bool:
        session = self._sessions.get(request.session_id)
        return request.cancellation.is_cancelled() or request.outcome == "cancelled" or session is None or session.context_generation != request.context_generation

    def _await_start(self, request: _Request) -> str:
        """Wait at the application boundary; queued work never reaches the model."""
        with self._synchronization.locked():
            while request.queued and not request.cancellation.is_cancelled():
                remaining = request.deadline_at - self._monotonic()
                if remaining <= 0:
                    self._remove_queued_locked(request.request_id)
                    return "deadline"
                self._synchronization.wait(remaining)
            if request.cancellation.is_cancelled() or self._is_fenced_locked(request):
                return "cancelled"
            if self._deadline_expired_locked(request):
                return "deadline"
            return "started"

    def _deadline_expired(self, request: _Request) -> bool:
        with self._synchronization.locked():
            return self._deadline_expired_locked(request)

    def _deadline_expired_locked(self, request: _Request) -> bool:
        return self._monotonic() >= request.deadline_at

    def _proposal_deadline(self) -> str:
        """Bound a synthetic proposal from the injected application clock."""
        timestamp = datetime.fromisoformat(self._clock.now().replace("Z", "+00:00"))
        return (timestamp + timedelta(minutes=5)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _start_queued_provider_stream(self, request: _Request, model: ModelPort, model_input: ModelInput) -> Iterator[object]:
        """Acquire FIFO permission before a provider can begin any stream work."""
        with self._synchronization.locked():
            while self._has_earlier_unstarted_queued_request_locked(request):
                if self._is_fenced_locked(request) or self._deadline_expired_locked(request):
                    raise StopIteration
                self._synchronization.wait(request.deadline_at - self._monotonic())
        with self._synchronization.model_start_locked():
            with self._synchronization.locked():
                if self._is_fenced_locked(request) or self._deadline_expired_locked(request):
                    raise StopIteration
                request.provider_start_claimed = True
                self._synchronization.notify_all()
            return iter(model.stream(model_input, request.cancellation))  # type: ignore[union-attr]

    def _has_earlier_unstarted_queued_request_locked(self, request: _Request) -> bool:
        if request.queued_start_order is None:
            return False
        return any(
            other.queued_start_order is not None
            and other.queued_start_order < request.queued_start_order
            and other.outcome is None
            and not other.cancellation.is_cancelled()
            and not other.provider_start_claimed
            for other in self._requests.values()
        )

    def _remove_queued_locked(self, request_id: str) -> None:
        try:
            self._queued_request_ids.remove(request_id)
        except ValueError:
            return

    def _promote_queued_locked(self) -> None:
        if self._active_turn_count_locked() >= MAX_ACTIVE_TURNS:
            self._synchronization.notify_all()
            return
        while self._queued_request_ids:
            request_id = self._queued_request_ids.popleft()
            request = self._requests.get(request_id)
            if request is None or request.outcome is not None or request.cancellation.is_cancelled():
                continue
            request.queued = False
            self._synchronization.notify_all()
            return
        self._synchronization.notify_all()

    def _prune_expired_queued_locked(self) -> None:
        """Release expired queued admissions even when their stream is never resumed."""
        for request_id in tuple(self._queued_request_ids):
            request = self._requests.get(request_id)
            if request is None or request.outcome is not None:
                self._remove_queued_locked(request_id)
                continue
            if self._deadline_expired_locked(request):
                self._remove_queued_locked(request_id)
                request.outcome = "failed"
                try:
                    self._ledger.mark_terminal(request.request_id, "failed")
                except RequestLedgerUnavailable:
                    pass

    def _active_turn_count_locked(self) -> int:
        return sum(request.outcome is None and not request.queued for request in self._requests.values())

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
        self._prune_expired_queued_locked()

    def _generation_active_locked(self, session_id: str, generation: int) -> bool:
        return any(request.session_id == session_id and request.context_generation == generation and request.outcome is None for request in self._requests.values())

    def _cancel_generation_locked(self, session_id: str, generation: int) -> None:
        for request in self._requests.values():
            if request.session_id == session_id and request.context_generation == generation and request.outcome is None:
                request.cancellation.cancel()
                self._cancel_acknowledgement_locked(request)
                if request.queued:
                    self._remove_queued_locked(request.request_id)
        self._synchronization.notify_all()

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


def _canonical_proposal_payload(proposal: CanonicalProposal) -> dict[str, object]:
    """Publish only the canonical reviewed operation material."""
    return {
        "operation": proposal.operation,
        "target": proposal.target,
        "arguments": proposal.argument_object(),
        "manifest_revision": proposal.manifest_revision,
    }


def _preview_payload(result: DryRunPreview) -> dict[str, object]:
    """Publish the closed simulated result without a state-change claim."""
    payload: dict[str, object] = {"status": result.status}
    if result.status == "simulated":
        payload.update({
            "operation": result.operation,
            "target": result.target,
            "arguments": {"desired_state": result.desired_state},
            "manifest_revision": result.manifest_revision,
        })
    else:
        payload["reason"] = result.reason
    return payload


def _preview_matches_proposal(result: DryRunPreview, proposal: CanonicalProposal) -> bool:
    """Keep an adapter result from contradicting the admitted canonical proposal."""
    return result.status != "simulated" or (
        result.operation == proposal.operation
        and result.target == proposal.target
        and result.desired_state == proposal.argument_object().get("desired_state")
        and result.manifest_revision == proposal.manifest_revision
    )


def _provider_item_is_useful(item: object) -> bool:
    """Recognize only output that can become a useful public stream event."""
    if isinstance(item, ModelChunk):
        size = _utf8_length(item.content)
        return size is not None and 0 < size <= MAX_STREAM_EVENT_CONTENT_BYTES
    return isinstance(item, ModelProposal) and (
        is_ha_shaped_candidate(item.proposal)
        or validate_proposal(item.proposal) and proposal_event_size_is_bounded(item.proposal)
    )


def _safe_model_failure(failure: ModelOperationFailure) -> tuple[str, str, str, bool]:
    """Keep a port implementation from selecting arbitrary public error fields."""
    if failure.category == "timeout":
        return "model_operation_timeout", "timeout", "Model operation timed out.", True
    return "model_unavailable", "dependency_unavailable", "Model streaming is unavailable.", True


def _request_expiry(admitted_at: str) -> str:
    """Return the fixed 24-hour retention expiry for a UTC clock timestamp."""
    try:
        parsed = datetime.fromisoformat(admitted_at.replace("Z", "+00:00"))
    except ValueError:
        raise RequestLedgerUnavailable() from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (parsed.astimezone(timezone.utc) + timedelta(seconds=REQUEST_RETENTION_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ledger_unavailable_error() -> AdmissionError:
    return AdmissionError(503, "request_ledger_unavailable", "dependency_unavailable", "Request status storage is unavailable.", True)


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
