"""A small dependency-injected text core with no public HTTP work API."""
from __future__ import annotations

from dataclasses import dataclass

from .config import StartupState
from .contracts import Clock, ModelPort, StatePort, TelemetryPort, ToolPort

MAX_FAKE_TURN_INPUT_BYTES = 1024
MAX_FAKE_TURN_OUTPUT_BYTES = 4096


@dataclass(frozen=True)
class TurnResult:
    """The bounded result used only by the local fake-model exercise."""

    text: str


class TextGateway:
    """Composition boundary for provider-neutral ports."""

    def __init__(
        self,
        model: ModelPort,
        clock: Clock,
        state: StatePort,
        telemetry: TelemetryPort,
        tools: ToolPort,
    ) -> None:
        self._model = model
        self._clock = clock
        self._state = state
        self._telemetry = telemetry
        self._tools = tools

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


def _utf8_length(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return len(value.encode("utf-8"))
    except UnicodeError:
        return None
