"""Immutable, offline policy for Oriel's one reviewed HA capability."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


MANIFEST_REVISION = "home_assistant.harmless_light.v1"
OPERATION_ID = "home_assistant.light.set_power.v1"
FACT_OPERATION_ID = "home_assistant.light.read_fact.v1"
TARGET_ALIAS = "synthetic:reviewed-harmless-light"
CAPABILITY_ID = "home_assistant.harmless_light"
READ_FIELD_ORDER = ("power_state", "observed_at", "freshness")
READ_FIELDS = frozenset(READ_FIELD_ORDER)
DESIRED_STATES = frozenset(("on", "off"))
INITIAL_DEADLINE_SECONDS = 5
FORBIDDEN_CONTROL_FIELDS = frozenset((
    "authority", "deadline", "dispatch", "dry_run", "enabled",
    "execution_authority", "idempotency", "idempotency_key",
    "manifest_revision", "plugin", "provider", "provider_domain",
    "provider_service", "provider_url", "service", "url",
))
_GENERIC_CONTROL_FIELDS = frozenset(("deadline", "dry_run", "idempotency"))


@dataclass(frozen=True)
class BuiltInManifest:
    revision: str = MANIFEST_REVISION
    operation: str = OPERATION_ID
    targets: frozenset[str] = frozenset((TARGET_ALIAS,))
    read_fields: frozenset[str] = READ_FIELDS
    enabled: bool = False
    initial_deadline_seconds: int = INITIAL_DEADLINE_SECONDS
    provider_permission: str = "unverified"
    g2: str = "incomplete"
    requires_observed_after_dispatch: bool = True
    completion_on_mismatch: str = "completion_unproven"


BUILT_IN_MANIFEST = BuiltInManifest()


@dataclass(frozen=True)
class EffectivePolicy:
    revision: str
    operation: str
    targets: frozenset[str]
    read_fields: frozenset[str]
    enabled: bool


@dataclass(frozen=True)
class PolicyResult:
    policy: EffectivePolicy | None = None
    denial_code: str | None = None

    @property
    def permitted(self) -> bool:
        return self.policy is not None


@dataclass(frozen=True)
class CanonicalProposal:
    operation: str
    target: str
    arguments: tuple[tuple[str, str], ...]
    manifest_revision: str

    def argument_object(self) -> dict[str, str]:
        return dict(self.arguments)


@dataclass(frozen=True)
class ValidationResult:
    material: CanonicalProposal | None = None
    policy: EffectivePolicy | None = None
    denial_code: str | None = None

    @property
    def permitted(self) -> bool:
        return self.material is not None


@dataclass(frozen=True)
class FactRequest:
    """A closed, read-only request for the reviewed synthetic observation."""

    operation: str
    target: str
    fields: tuple[str, ...]
    manifest_revision: str


@dataclass(frozen=True)
class FactValidationResult:
    request: FactRequest | None = None
    policy: EffectivePolicy | None = None
    denial_code: str | None = None

    @property
    def permitted(self) -> bool:
        return self.request is not None


def compute_effective_policy(restrictions: Mapping[str, object] | None = None, manifest: BuiltInManifest = BUILT_IN_MANIFEST) -> PolicyResult:
    """Intersect typed optional-skill restrictions with the built-in policy."""
    if restrictions is None:
        return PolicyResult(_effective(manifest.targets, manifest.read_fields, manifest))
    if not isinstance(restrictions, Mapping) or set(restrictions) - {"targets", "read_fields"}:
        return PolicyResult(denial_code="invalid_operator_restrictions")
    targets = _members(restrictions.get("targets", manifest.targets))
    fields = _members(restrictions.get("read_fields", manifest.read_fields))
    if targets is None or fields is None:
        return PolicyResult(denial_code="invalid_operator_restrictions")
    return PolicyResult(_effective(manifest.targets & targets, manifest.read_fields & fields, manifest))


def validate_ha_proposal(candidate: object, restrictions: Mapping[str, object] | None = None, manifest: BuiltInManifest = BUILT_IN_MANIFEST) -> ValidationResult:
    """Validate a closed HA request without granting dispatch authority."""
    policy_result = compute_effective_policy(restrictions, manifest)
    if policy_result.policy is None:
        return ValidationResult(denial_code=policy_result.denial_code)
    policy = policy_result.policy
    if not isinstance(candidate, Mapping):
        return ValidationResult(policy=policy, denial_code="malformed_proposal")
    if set(candidate) & FORBIDDEN_CONTROL_FIELDS:
        return ValidationResult(policy=policy, denial_code="caller_selected_control")
    if set(candidate) != {"operation", "target", "arguments"}:
        return ValidationResult(policy=policy, denial_code="malformed_proposal")
    if candidate.get("operation") != policy.operation:
        return ValidationResult(policy=policy, denial_code="operation_not_permitted")
    target = candidate.get("target")
    if not isinstance(target, str) or target not in policy.targets:
        return ValidationResult(policy=policy, denial_code="target_not_permitted")
    arguments = candidate.get("arguments")
    if not isinstance(arguments, Mapping) or set(arguments) != {"desired_state"}:
        return ValidationResult(policy=policy, denial_code="invalid_arguments")
    state = arguments.get("desired_state")
    if not isinstance(state, str) or state not in DESIRED_STATES:
        return ValidationResult(policy=policy, denial_code="invalid_arguments")
    if not policy.targets or not policy.read_fields:
        return ValidationResult(policy=policy, denial_code="no_permitted_capability")
    return ValidationResult(CanonicalProposal(policy.operation, target, (("desired_state", state),), policy.revision), policy)


def validate_ha_fact_request(candidate: object, restrictions: Mapping[str, object] | None = None) -> FactValidationResult:
    """Admit only the reviewed read request before an adapter lookup can occur."""
    policy_result = compute_effective_policy(restrictions)
    if policy_result.policy is None:
        return FactValidationResult(denial_code=policy_result.denial_code)
    policy = policy_result.policy
    if not isinstance(candidate, Mapping):
        return FactValidationResult(policy=policy, denial_code="malformed_fact_request")
    if set(candidate) & FORBIDDEN_CONTROL_FIELDS:
        return FactValidationResult(policy=policy, denial_code="caller_selected_control")
    if set(candidate) != {"operation", "target", "fields"}:
        return FactValidationResult(policy=policy, denial_code="malformed_fact_request")
    if candidate.get("operation") != FACT_OPERATION_ID:
        return FactValidationResult(policy=policy, denial_code="operation_not_permitted")
    target = candidate.get("target")
    if not isinstance(target, str) or target not in policy.targets:
        return FactValidationResult(policy=policy, denial_code="target_not_permitted")
    fields = candidate.get("fields")
    if not isinstance(fields, (list, tuple)) or not all(isinstance(field, str) for field in fields):
        return FactValidationResult(policy=policy, denial_code="invalid_fact_fields")
    if len(fields) != len(set(fields)) or frozenset(fields) != policy.read_fields or not policy.read_fields:
        return FactValidationResult(policy=policy, denial_code="invalid_fact_fields")
    return FactValidationResult(FactRequest(FACT_OPERATION_ID, target, READ_FIELD_ORDER, policy.revision), policy)


def is_ha_shaped_candidate(candidate: object) -> bool:
    """Reserve HA-specific material for typed validation before generic flow."""
    if not isinstance(candidate, Mapping):
        return False
    operation = candidate.get("operation")
    return (
        candidate.get("target") == TARGET_ALIAS
        or isinstance(operation, str) and operation.startswith("home_assistant.")
        or bool((set(candidate) & (FORBIDDEN_CONTROL_FIELDS - _GENERIC_CONTROL_FIELDS)))
    )


def is_ha_fact_candidate(candidate: object) -> bool:
    """Keep any attempted HA fact material out of generic proposal handling."""
    if not isinstance(candidate, Mapping):
        return False
    operation = candidate.get("operation")
    return (
        (candidate.get("target") == TARGET_ALIAS and "fields" in candidate)
        or (isinstance(operation, str) and operation.startswith("home_assistant.light.read"))
    )


def canonical_ha_proposal(desired_state: str) -> dict[str, object]:
    return {"operation": OPERATION_ID, "target": TARGET_ALIAS, "arguments": {"desired_state": desired_state}}


def canonical_ha_fact_request() -> dict[str, object]:
    return {"operation": FACT_OPERATION_ID, "target": TARGET_ALIAS, "fields": list(READ_FIELD_ORDER)}


def preview_eligibility(candidate: object, restrictions: Mapping[str, object] | None = None, manifest: BuiltInManifest = BUILT_IN_MANIFEST) -> ValidationResult:
    result = validate_ha_proposal(candidate, restrictions, manifest)
    if result.denial_code is not None or result.policy is None or not result.policy.enabled:
        return ValidationResult(policy=result.policy, denial_code=result.denial_code or "action_disabled")
    return result


def _effective(targets: frozenset[str], fields: frozenset[str], manifest: BuiltInManifest = BUILT_IN_MANIFEST) -> EffectivePolicy:
    return EffectivePolicy(manifest.revision, manifest.operation, targets, fields, manifest.enabled)


def _members(value: object) -> frozenset[str] | None:
    if not isinstance(value, (list, tuple, frozenset)) or not all(isinstance(item, str) for item in value):
        return None
    return frozenset(value)
