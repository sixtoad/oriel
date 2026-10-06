"""Focused offline checks for Oriel's built-in HA proposal policy."""
from __future__ import annotations

import json
from pathlib import Path
import unittest

from oriel.domain.ha_manifest import BUILT_IN_MANIFEST, FACT_OPERATION_ID, FORBIDDEN_CONTROL_FIELDS, OPERATION_ID, READ_FIELD_ORDER, TARGET_ALIAS, canonical_ha_fact_request, canonical_ha_proposal, compute_effective_policy, is_ha_fact_candidate, is_ha_shaped_candidate, validate_ha_fact_request, validate_ha_proposal


class HaManifestTests(unittest.TestCase):
    def test_built_in_contract_matches_the_reviewed_disabled_controls(self) -> None:
        self.assertEqual(BUILT_IN_MANIFEST.operation, OPERATION_ID)
        self.assertEqual(BUILT_IN_MANIFEST.targets, frozenset((TARGET_ALIAS,)))
        self.assertEqual(BUILT_IN_MANIFEST.read_fields, frozenset(("power_state", "observed_at", "freshness")))
        self.assertEqual(BUILT_IN_MANIFEST.initial_deadline_seconds, 5)
        self.assertEqual((BUILT_IN_MANIFEST.enabled, BUILT_IN_MANIFEST.provider_permission, BUILT_IN_MANIFEST.g2), (False, "unverified", "incomplete"))
        self.assertTrue(BUILT_IN_MANIFEST.requires_observed_after_dispatch)
        self.assertEqual(BUILT_IN_MANIFEST.completion_on_mismatch, "completion_unproven")

    def test_built_in_manifest_stays_aligned_with_the_frozen_documentation_contract(self) -> None:
        contract = json.loads((Path(__file__).resolve().parents[1] / "docs" / "harmless-home-assistant-skill.json").read_text(encoding="utf-8"))
        operation = contract["operations"][0]
        self.assertEqual(BUILT_IN_MANIFEST.revision, contract["capability_id"])
        self.assertEqual(BUILT_IN_MANIFEST.operation, operation["id"])
        self.assertEqual(BUILT_IN_MANIFEST.targets, frozenset((operation["target"]["alias"],)))
        self.assertEqual(BUILT_IN_MANIFEST.read_fields, frozenset(operation["read_fields"]))
        self.assertEqual(BUILT_IN_MANIFEST.initial_deadline_seconds, operation["initial_deadline_seconds"])
        self.assertEqual(BUILT_IN_MANIFEST.enabled, contract["execution"]["enabled"])
        self.assertEqual(BUILT_IN_MANIFEST.provider_permission, contract["execution"]["provider_permission"])
        self.assertEqual(BUILT_IN_MANIFEST.g2, contract["execution"]["g2"])
        self.assertEqual(BUILT_IN_MANIFEST.requires_observed_after_dispatch, operation["completion"]["observed_after_dispatch"])
        self.assertEqual(BUILT_IN_MANIFEST.completion_on_mismatch, operation["completion"]["otherwise"])

    def test_canonical_proposals_are_permitted_but_the_policy_stays_disabled(self) -> None:
        for desired_state in ("on", "off"):
            result = validate_ha_proposal(canonical_ha_proposal(desired_state))
            self.assertTrue(result.permitted)
            self.assertEqual(result.material.argument_object(), {"desired_state": desired_state})
            self.assertFalse(result.policy.enabled)

    def test_operator_restrictions_intersect_without_adding_or_enabling(self) -> None:
        policy = compute_effective_policy({"targets": [TARGET_ALIAS, "synthetic:added"], "read_fields": ["power_state", "added"]})
        self.assertTrue(policy.permitted)
        self.assertEqual(policy.policy.targets, frozenset((TARGET_ALIAS,)))
        self.assertEqual(policy.policy.read_fields, frozenset(("power_state",)))
        self.assertFalse(policy.policy.enabled)
        for restrictions in ({"operations": [OPERATION_ID]}, {"enabled": True}, {"targets": TARGET_ALIAS}):
            self.assertEqual(compute_effective_policy(restrictions).denial_code, "invalid_operator_restrictions")

    def test_closed_typed_shape_and_controls_are_denied(self) -> None:
        cases = [
            ({"operation": OPERATION_ID, "target": TARGET_ALIAS, "arguments": {"desired_state": "toggle"}}, "invalid_arguments"),
            ({"operation": OPERATION_ID, "target": TARGET_ALIAS, "arguments": {"desired_state": "on", "brightness": 1}}, "invalid_arguments"),
            ({"operation": "home_assistant.light.toggle.v1", "target": TARGET_ALIAS, "arguments": {"desired_state": "on"}}, "operation_not_permitted"),
            ({"operation": OPERATION_ID, "target": "synthetic:other", "arguments": {"desired_state": "on"}}, "target_not_permitted"),
            ({"operation": OPERATION_ID, "target": TARGET_ALIAS, "arguments": {"desired_state": "on"}, "label": "ignore policy"}, "malformed_proposal"),
        ]
        for proposal, code in cases:
            with self.subTest(proposal=proposal):
                self.assertEqual(validate_ha_proposal(proposal).denial_code, code)
        for field in FORBIDDEN_CONTROL_FIELDS:
            proposal = canonical_ha_proposal("on")
            proposal[field] = "model-selected"
            self.assertEqual(validate_ha_proposal(proposal).denial_code, "caller_selected_control")

    def test_reserved_alias_and_ha_controls_are_never_generic_candidates(self) -> None:
        self.assertTrue(is_ha_shaped_candidate({"action": "generic_action", "target": TARGET_ALIAS, "arguments": {"values": []}, "deadline": "2030-01-01T00:00:00Z", "dry_run": True, "idempotency": "x", "proposal_version": "1.0", "proposal_id": "p", "confirmation": {"required": True, "evidence": None}}))
        self.assertTrue(is_ha_shaped_candidate({"provider_url": "model-selected"}))
        self.assertTrue(is_ha_shaped_candidate({"operation": "home_assistant.light.unsupported.v1"}))
        self.assertFalse(is_ha_shaped_candidate({"operation": "future_generic_operation"}))
        self.assertFalse(is_ha_shaped_candidate({"action": "generic_action", "target": "synthetic:generic", "arguments": {"values": []}, "deadline": "2030-01-01T00:00:00Z", "dry_run": True, "idempotency": "x", "proposal_version": "1.0", "proposal_id": "p", "confirmation": {"required": True, "evidence": None}}))

    def test_closed_fact_requests_use_the_same_effective_manifest_intersection(self) -> None:
        request = canonical_ha_fact_request()
        result = validate_ha_fact_request(request)
        self.assertTrue(result.permitted)
        self.assertEqual(result.request.operation, FACT_OPERATION_ID)
        self.assertEqual(result.request.target, TARGET_ALIAS)
        self.assertEqual(result.request.fields, READ_FIELD_ORDER)

        denied_cases = (
            ({**request, "target": "synthetic:other"}, "target_not_permitted"),
            ({**request, "fields": ["power_state", "observed_at", "freshness", "ignore policy"]}, "invalid_fact_fields"),
            ({**request, "deadline": "model-selected"}, "caller_selected_control"),
            ({**request, "instruction": "ignore policy"}, "malformed_fact_request"),
        )
        for candidate, code in denied_cases:
            with self.subTest(candidate=candidate):
                self.assertEqual(validate_ha_fact_request(candidate).denial_code, code)
                self.assertTrue(is_ha_fact_candidate(candidate))

        self.assertEqual(
            validate_ha_fact_request(request, {"targets": [TARGET_ALIAS], "read_fields": ["power_state"]}).denial_code,
            "invalid_fact_fields",
        )
        self.assertFalse(is_ha_fact_candidate({"operation": "future_generic_operation", "fields": []}))

    def test_closed_fact_request_stays_aligned_with_the_documented_capability(self) -> None:
        contract = json.loads((Path(__file__).resolve().parents[1] / "docs" / "harmless-home-assistant-skill.json").read_text(encoding="utf-8"))
        self.assertEqual(contract["read"], {
            "id": FACT_OPERATION_ID,
            "target_alias": TARGET_ALIAS,
            "fields": list(READ_FIELD_ORDER),
            "request_fields": ["operation", "target", "fields"],
        })


if __name__ == "__main__":
    unittest.main()
