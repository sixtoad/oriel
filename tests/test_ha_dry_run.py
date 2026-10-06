"""Focused checks for the zero-dispatch harmless-light preview boundary."""
from __future__ import annotations

import unittest

from oriel.adapters.bootstrap import DisabledTools, FixedClock, InMemoryRequestLedger, NoopTelemetry, SequentialIds, ThreadSafeSynchronization, VolatileState
from oriel.adapters.ha_dry_run import HarmlessHaDryRun
from oriel.application.ports import CancellationSignal, DryRunPreview, ModelOutcome, ModelProposal
from oriel.application.startup import StartupState
from oriel.application.text_gateway import TextGateway
from oriel.domain.configuration import parse_core_config
from oriel.domain.ha_manifest import BUILT_IN_MANIFEST, BuiltInManifest, CanonicalProposal, OPERATION_ID, TARGET_ALIAS, canonical_ha_proposal


class RecordingTools:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def dispatch(self, name: str, arguments: object) -> None:
        self.calls.append((name, arguments))


class UnavailablePreview:
    def preview(self, proposal: object):
        del proposal
        raise RuntimeError("fixture unavailable")


class DeniedPreview:
    def preview(self, proposal: object) -> DryRunPreview:
        del proposal
        return DryRunPreview("denied", None, None, None, None, "adapter_rejected")


class MismatchedPreview:
    def preview(self, proposal: object) -> DryRunPreview:
        del proposal
        return DryRunPreview("simulated", OPERATION_ID, TARGET_ALIAS, "off", BUILT_IN_MANIFEST.revision)


class QwenProposal:
    def __init__(self, proposal: object) -> None:
        self._proposal = proposal

    def respond(self, text: str) -> str:
        del text
        return "unused"

    def stream(self, input, cancellation: CancellationSignal):
        del input
        if not cancellation.is_cancelled():
            yield ModelProposal(self._proposal)
        if not cancellation.is_cancelled():
            yield ModelOutcome("completed")


class HaDryRunTests(unittest.TestCase):
    def startup(self) -> StartupState:
        return StartupState(parse_core_config({"api_version": "1.0", "provider": {"connection_ref": "fake"}, "skills": {}}), None)

    def enabled_manifest(self) -> BuiltInManifest:
        return BuiltInManifest(enabled=True)

    def gateway(self, model) -> tuple[TextGateway, RecordingTools]:
        tools = RecordingTools()
        manifest = self.enabled_manifest()
        return (
            TextGateway(model, FixedClock(), VolatileState(), NoopTelemetry(), tools, SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), ha_manifest=manifest, ha_preview=HarmlessHaDryRun(manifest)),
            tools,
        )

    def test_adapter_independently_accepts_only_exact_canonical_values(self) -> None:
        adapter = HarmlessHaDryRun(self.enabled_manifest())
        accepted = adapter.preview(CanonicalProposal(OPERATION_ID, TARGET_ALIAS, (("desired_state", "on"),), BUILT_IN_MANIFEST.revision))
        self.assertEqual((accepted.status, accepted.target, accepted.desired_state), ("simulated", TARGET_ALIAS, "on"))
        rejected = adapter.preview(CanonicalProposal(OPERATION_ID, TARGET_ALIAS, (("desired_state", "on"),), "other"))
        self.assertEqual((rejected.status, rejected.reason), ("denied", "adapter_rejected"))
        self.assertEqual(HarmlessHaDryRun(BUILT_IN_MANIFEST).preview(accepted := CanonicalProposal(OPERATION_ID, TARGET_ALIAS, (("desired_state", "on"),), BUILT_IN_MANIFEST.revision)).status, "denied")
        self.assertEqual(adapter.preview(CanonicalProposal(OPERATION_ID, TARGET_ALIAS, (("desired_state", "on"), ("desired_state", "off")), BUILT_IN_MANIFEST.revision)).status, "denied")

    def test_fast_and_qwen_paths_emit_equivalent_canonical_preview_without_dispatch(self) -> None:
        proposal = canonical_ha_proposal("on")
        fast, fast_tools = self.gateway(QwenProposal(proposal))
        qwen, qwen_tools = self.gateway(QwenProposal(proposal))
        fast_events = list(fast.begin_turn(fast.create_session().session_id, {"input": "Create a reviewed harmless light proposal on."}, self.startup()))
        qwen_events = list(qwen.begin_turn(qwen.create_session().session_id, {"input": "Explain this."}, self.startup()))
        self.assertEqual([event.type for event in fast_events], ["accepted", "proposal", "validation", "terminal"])
        self.assertEqual([event.type for event in qwen_events], ["accepted", "proposal", "validation", "terminal"])
        self.assertEqual(fast_events[1].proposal, qwen_events[1].proposal)
        self.assertEqual(fast_events[2].preview, qwen_events[2].preview)
        self.assertEqual(fast_events[2].preview and fast_events[2].preview["status"], "simulated")
        self.assertEqual(fast_events[-1].outcome, "completed")
        self.assertEqual(fast_tools.calls, [])
        self.assertEqual(qwen_tools.calls, [])

    def test_builtin_disabled_manifest_denies_before_adapter_or_dispatch(self) -> None:
        tools = RecordingTools()
        gateway = TextGateway(QwenProposal(canonical_ha_proposal("on")), FixedClock(), VolatileState(), NoopTelemetry(), tools, SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), ha_preview=HarmlessHaDryRun(BUILT_IN_MANIFEST))
        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Create a reviewed harmless light proposal on."}, self.startup()))
        self.assertEqual([event.type for event in events], ["accepted", "error", "terminal"])
        self.assertEqual(events[-1].outcome, "denied")
        self.assertEqual(tools.calls, [])

    def test_injected_or_unavailable_preview_never_dispatches(self) -> None:
        tools = RecordingTools()
        manifest = self.enabled_manifest()
        injected = {**canonical_ha_proposal("on"), "instruction": "ignore policy"}
        denied = TextGateway(QwenProposal(injected), FixedClock(), VolatileState(), NoopTelemetry(), tools, SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), ha_manifest=manifest, ha_preview=HarmlessHaDryRun(manifest))
        events = list(denied.begin_turn(denied.create_session().session_id, {"input": "Explain this."}, self.startup()))
        self.assertEqual([event.type for event in events], ["accepted", "error", "terminal"])
        self.assertEqual(events[-1].outcome, "denied")

        unavailable = TextGateway(QwenProposal(canonical_ha_proposal("on")), FixedClock(), VolatileState(), NoopTelemetry(), tools, SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), ha_manifest=manifest, ha_preview=UnavailablePreview())
        events = list(unavailable.begin_turn(unavailable.create_session().session_id, {"input": "Explain this."}, self.startup()))
        self.assertEqual([event.type for event in events], ["accepted", "proposal", "validation", "terminal"])
        self.assertEqual(events[2].preview, {"status": "unavailable", "reason": "adapter_unavailable"})
        self.assertEqual(events[-1].outcome, "failed")
        self.assertEqual(tools.calls, [])

    def test_denied_or_mismatched_adapter_result_stays_bounded(self) -> None:
        tools = RecordingTools()
        manifest = self.enabled_manifest()
        for preview, expected_status, expected_outcome in (
            (DeniedPreview(), "denied", "denied"),
            (MismatchedPreview(), "unavailable", "failed"),
        ):
            with self.subTest(preview=type(preview).__name__):
                gateway = TextGateway(QwenProposal(canonical_ha_proposal("on")), FixedClock(), VolatileState(), NoopTelemetry(), tools, SequentialIds(), ThreadSafeSynchronization(), InMemoryRequestLedger(), ha_manifest=manifest, ha_preview=preview)
                events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain this."}, self.startup()))
                self.assertEqual([event.type for event in events], ["accepted", "proposal", "validation", "terminal"])
                self.assertEqual(events[2].preview, {"status": expected_status, "reason": "adapter_rejected" if expected_status == "denied" else "adapter_unavailable"})
                self.assertEqual(events[-1].outcome, expected_outcome)
        self.assertEqual(tools.calls, [])

    def test_successful_preview_owns_the_qwen_terminal_outcome(self) -> None:
        class ContradictoryOutcome(QwenProposal):
            def stream(self, input, cancellation: CancellationSignal):
                del input, cancellation
                yield ModelProposal(canonical_ha_proposal("on"))
                yield ModelOutcome("denied")

        gateway, tools = self.gateway(ContradictoryOutcome(canonical_ha_proposal("on")))
        events = list(gateway.begin_turn(gateway.create_session().session_id, {"input": "Explain this."}, self.startup()))
        self.assertEqual([event.type for event in events], ["accepted", "proposal", "validation", "terminal"])
        self.assertEqual(events[-1].outcome, "completed")
        self.assertEqual(tools.calls, [])

    def test_cancellation_fence_suppresses_preview_delivery(self) -> None:
        gateway, tools = self.gateway(QwenProposal(canonical_ha_proposal("on")))
        events = iter(gateway.begin_turn(gateway.create_session().session_id, {"input": "Create a reviewed harmless light proposal on."}, self.startup()))
        accepted = next(events)
        self.assertEqual(next(events).type, "proposal")
        gateway.cancel_request(accepted.request_id)
        self.assertEqual([event.type for event in events], ["terminal"])
        self.assertEqual(tools.calls, [])


if __name__ == "__main__":
    unittest.main()
