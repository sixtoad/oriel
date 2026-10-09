from __future__ import annotations

import unittest

from oriel.application.fast_router import CLARIFICATION_TEXT, RULE_REVISION, normalize_turn, route
from oriel.application.ports import ModelMessage
from oriel.domain.ha_manifest import canonical_ha_fact_request, validate_ha_proposal
from oriel.domain.proposals import validate_proposal


class FastRouterTests(unittest.TestCase):
    def test_equivalent_bounded_input_and_context_select_the_same_safe_content(self) -> None:
        first = normalize_turn("  ORIEL\tHELP! ", (ModelMessage("assistant", " Previous\nanswer "),))
        second = normalize_turn("oriel help", (ModelMessage("assistant", "previous answer"),))

        self.assertEqual(first, second)
        self.assertEqual(route("  ORIEL\tHELP! ", first.context), route("oriel help", second.context))
        decision = route("oriel help", ())
        self.assertEqual((decision.route, decision.rule_revision, decision.content), ("content", RULE_REVISION, "Oriel can provide limited deterministic responses."))

    def test_ambiguous_and_unsupported_live_requests_never_reach_a_provider(self) -> None:
        clarification = route("Turn on the desk lamp", ())
        self.assertEqual((clarification.route, clarification.content), ("clarification", CLARIFICATION_TEXT))

        self.assertEqual(route("What is the weather now?", ()).content, "Live weather lookup is unsupported.")
        self.assertEqual(route("What time is it now?", ()).content, "Live time lookup is unsupported.")
        self.assertEqual(route("What's today's date?", ()).content, "Live time lookup is unsupported.")
        self.assertEqual(route("Play music", ()).content, "Music playback is unsupported.")
        self.assertEqual(route("Are the lights on?", ()).content, "Live home-state lookup is unsupported.")
        self.assertEqual(route("The kitchen lights, are they on?", ()).content, "Live home-state lookup is unsupported.")
        for question in ("Explain weather patterns.", "How does music playback work?", "Why is a door open?"):
            self.assertEqual(route(question, ()).route, "qwen")

    def test_protected_and_multiple_action_language_is_a_typed_denial(self) -> None:
        for request in ("Unlock the front door", "Turn on the kitchen light and turn off the hallway light", "Pause music then play a song"):
            decision = route(request, ())
            self.assertEqual((decision.route, decision.error_code, decision.error_category), ("denial", "fast_route_denied", "policy_denial"))
        self.assertEqual(route("Explain how to turn on and turn off a light.", ()).route, "qwen")
        self.assertEqual(route("Turn on the desk lamp because it is dark.", ()).route, "clarification")

    def test_only_the_explicit_generic_rule_can_emit_a_valid_synthetic_proposal(self) -> None:
        decision = route("Create a synthetic proposal.", (), "2035-01-01T00:05:00Z")

        self.assertEqual(decision.route, "proposal")
        self.assertIsNotNone(decision.proposal)
        self.assertTrue(validate_proposal(decision.proposal))
        self.assertEqual(decision.proposal["deadline"], "2035-01-01T00:05:00Z")
        self.assertEqual(route("Write a proposal for my living room light", ()).route, "qwen")

    def test_reviewed_light_candidates_preserve_on_and_off_under_the_disabled_policy(self) -> None:
        for state in ("on", "off"):
            decision = route(f"Create a reviewed harmless light proposal {state}.", ())
            self.assertEqual(decision.route, "proposal")
            result = validate_ha_proposal(decision.proposal)
            self.assertTrue(result.permitted)
            self.assertEqual(result.material.argument_object(), {"desired_state": state})
            self.assertFalse(result.policy.enabled)

    def test_only_the_reviewed_status_question_selects_the_closed_fact_request(self) -> None:
        decision = route("What is the reviewed harmless light status?", ())
        self.assertEqual(decision.route, "fact")
        self.assertEqual(decision.fact_request, canonical_ha_fact_request())
        self.assertEqual(route("What is the kitchen light status?", ()).content, "Live home-state lookup is unsupported.")

    def test_complex_chat_is_explicitly_deferred_to_qwen(self) -> None:
        self.assertEqual(route("Explain why the sky looks blue.", ()).route, "qwen")


if __name__ == "__main__":
    unittest.main()
