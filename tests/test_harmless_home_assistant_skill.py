"""Offline checks for the frozen, disabled Home Assistant capability contract."""
import json
from pathlib import Path
import socket
import unittest


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "docs" / "harmless-home-assistant-skill.json"


class HarmlessHomeAssistantSkillTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    def test_single_disabled_explicit_power_operation_is_closed_and_bounded(self) -> None:
        self.assertEqual(self.contract["contract_version"], "1.0")
        self.assertEqual(self.contract["kind"], "built_in_documentation_contract")
        self.assertEqual(self.contract["execution"], {
            "enabled": False,
            "provider_permission": "unverified",
            "g2": "incomplete",
        })

        operations = self.contract["operations"]
        self.assertEqual(len(operations), 1)
        operation = operations[0]
        self.assertEqual(operation["id"], "home_assistant.light.set_power.v1")
        self.assertEqual(operation["target"], {"alias": "synthetic:reviewed-harmless-light"})
        self.assertEqual(operation["arguments"], {
            "type": "object",
            "additionalProperties": False,
            "required": ["desired_state"],
            "properties": {"desired_state": {"type": "string", "enum": ["on", "off"]}},
        })
        self.assertEqual(operation["read_fields"], ["power_state", "observed_at", "freshness"])
        self.assertEqual(operation["initial_deadline_seconds"], 5)
        self.assertEqual(operation["completion"], {
            "required": "fresh_post_operation_observation",
            "power_state_must_match": "desired_state",
            "otherwise": "completion_unproven",
            "observed_after_dispatch": True,
        })

    def test_synthetic_read_fixtures_and_completion_rule_never_claim_stale_or_unavailable_success(self) -> None:
        fixtures = self.contract["synthetic_read_fixtures"]
        self.assertEqual([fixture["freshness"] for fixture in fixtures], ["fresh", "stale", "unavailable"])
        for fixture in fixtures:
            self.assertEqual(set(fixture), {"target_alias", "power_state", "observed_at", "freshness"})
            self.assertEqual(fixture["target_alias"], "synthetic:reviewed-harmless-light")
        self.assertIn(fixtures[0]["power_state"], {"on", "off"})
        self.assertIsNone(fixtures[2]["power_state"])
        self.assertEqual(self.contract["operations"][0]["completion"]["otherwise"], "completion_unproven")
        self.assertTrue(self.contract["operations"][0]["completion"]["observed_after_dispatch"])

    def test_negative_unverified_permission_fixture_keeps_execution_disabled_with_zero_dispatch(self) -> None:
        negative = self.contract["negative_permission_fixture"]
        self.assertEqual(negative, {
            "restricted_call_path": "unverified",
            "enabled": False,
            "g2": "incomplete",
            "dispatch_expectation": 0,
            "result": "permission_prerequisite_unmet",
        })
        self.assertFalse(self.contract["execution"]["enabled"])


    def test_permission_gate_and_idempotency_require_evidence_without_broadening_access(self) -> None:
        self.assertEqual(self.contract["permission_prerequisite"], {
            "status": "unverified",
            "required_evidence": [
                "existing_restricted_connection",
                "effective_call_path_allows_only_contract_operation",
                "excluded_operation_is_denied",
                "sanitized_evidence_recorded",
            ],
            "prohibited_substitutes": ["account_label", "broad_privileged_connection"],
        })
        self.assertEqual(self.contract["idempotency"], {
            "owner": "oriel_action_ledger",
            "caller_selectable": False,
            "duplicate": "return_existing_action_status",
            "uncertain": "outcome_unknown_no_automatic_redispatch",
        })

    def test_exclusions_keep_unsupported_action_shapes_out_of_the_allowlist(self) -> None:
        exclusions = set(self.contract["exclusions"])
        self.assertTrue({
            "brightness", "toggle", "scenes", "scripts", "arbitrary_services",
            "arbitrary_attributes", "templates", "urls", "generic_executor_inputs",
            "authentication", "privileged_actions",
        }.issubset(exclusions))
        serialized = json.dumps(self.contract["operations"], sort_keys=True)
        for excluded in exclusions:
            self.assertNotIn(excluded, serialized)

    def test_contract_load_is_offline(self) -> None:
        original_socket = socket.socket
        original_connection = socket.create_connection
        try:
            socket.socket = lambda *args, **kwargs: self.fail("network attempted")
            socket.create_connection = lambda *args, **kwargs: self.fail("network attempted")
            contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        finally:
            socket.socket = original_socket
            socket.create_connection = original_connection
        self.assertEqual(contract["capability_id"], "home_assistant.harmless_light.v1")


if __name__ == "__main__":
    unittest.main()
