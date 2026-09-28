from __future__ import annotations

import ast
from pathlib import Path
import unittest

from oriel.adapters.bootstrap import DisabledTools, SequentialIds, ThreadSafeSynchronization, ToolDenied, VolatileState
from oriel.application.startup import StartupState
from oriel.application.text_gateway import MAX_FAKE_TURN_OUTPUT_BYTES, TextGateway
from oriel.domain.configuration import parse_core_config


class RecordingModel:
    def __init__(self) -> None:
        self.inputs: list[str] = []

    def respond(self, text: str) -> str:
        self.inputs.append(text)
        return "model output"


class RecordingClock:
    def now(self) -> str:
        return "2000-01-01T00:00:00Z"


class RecordingState:
    def __init__(self) -> None:
        self.turns: list[tuple[str, str, str]] = []

    def record_turn(self, input_text: str, output_text: str, occurred_at: str) -> None:
        self.turns.append((input_text, output_text, occurred_at))


class RecordingTelemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, str]]] = []

    def emit(self, event: str, fields: dict[str, str]) -> None:
        self.events.append((event, dict(fields)))


class RecordingTools:
    def __init__(self) -> None:
        self.dispatched = False

    def dispatch(self, name: str, arguments: dict[str, str]) -> None:
        self.dispatched = True


class OutputModel:
    def __init__(self, output: object) -> None:
        self.output = output

    def respond(self, text: str) -> object:
        del text
        return self.output


class CoreTests(unittest.TestCase):
    def ready(self) -> StartupState:
        return StartupState(parse_core_config({"api_version": "1.0", "provider": {"connection_ref": "fake"}, "skills": {}}), None)

    def test_turn_uses_each_non_tool_injected_port_and_never_dispatches(self):
        model = RecordingModel()
        clock = RecordingClock()
        state = RecordingState()
        telemetry = RecordingTelemetry()
        tools = RecordingTools()
        core = TextGateway(model, clock, state, telemetry, tools, SequentialIds(), ThreadSafeSynchronization())

        self.assertEqual(core.run_fake_turn("hello", self.ready()).text, "model output")
        self.assertEqual(model.inputs, ["hello"])
        self.assertEqual(state.turns, [("hello", "model output", "2000-01-01T00:00:00Z")])
        self.assertEqual(telemetry.events, [("fake_turn_completed", {"input_bytes": "5"})])
        self.assertFalse(tools.dispatched)

    def test_turn_is_bounded(self):
        core = TextGateway(RecordingModel(), RecordingClock(), RecordingState(), RecordingTelemetry(), RecordingTools(), SequentialIds(), ThreadSafeSynchronization())
        with self.assertRaises(ValueError):
            core.run_fake_turn("", self.ready())
        with self.assertRaises(ValueError):
            core.run_fake_turn("é" * 513, self.ready())

    def test_turn_rejects_unready_startup_invalid_utf8_and_bad_model_output(self):
        unready = StartupState(None, "config_unavailable")
        core = TextGateway(RecordingModel(), RecordingClock(), RecordingState(), RecordingTelemetry(), RecordingTools(), SequentialIds(), ThreadSafeSynchronization())
        with self.assertRaises(RuntimeError):
            core.run_fake_turn("hello", unready)
        with self.assertRaises(ValueError):
            core.run_fake_turn("\ud800", self.ready())
        for output in ("\ud800", "x" * (MAX_FAKE_TURN_OUTPUT_BYTES + 1), "", 1):
            with self.subTest(output=repr(output)):
                rejecting_core = TextGateway(OutputModel(output), RecordingClock(), RecordingState(), RecordingTelemetry(), RecordingTools(), SequentialIds(), ThreadSafeSynchronization())
                with self.assertRaises(ValueError):
                    rejecting_core.run_fake_turn("hello", self.ready())

    def test_volatile_state_retains_no_fake_turn_material(self):
        state = VolatileState()
        state.record_turn("private input", "private output", "now")
        self.assertEqual(vars(state), {})

    def test_disabled_tools_deny_every_dispatch(self):
        with self.assertRaisesRegex(ToolDenied, "tools are disabled"):
            DisabledTools().dispatch("anything", {})

    def test_core_has_no_provider_or_home_assistant_import(self):
        source = Path("oriel/application/text_gateway.py").read_text(encoding="utf-8")
        modules = [node.module or "" for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ImportFrom)]
        self.assertTrue(all("homeassistant" not in module.lower() and "provider" not in module.lower() for module in modules))


if __name__ == "__main__":
    unittest.main()
