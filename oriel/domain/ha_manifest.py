"""Immutable, offline policy for Oriel's one reviewed HA capability."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


MANIFEST_REVISION = "home_assistant.harmless_light.v1"
OPERATION_ID = "home_assistant.light.set_power.v1"
TARGET_ALIAS = "synthetic:reviewed-harmless-light"
READ_FIELDS = frozenset(("power_state", "observed_at", "freshness"))
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


def compute_effective_policy(restrictions: Mapping[str, object] | None = None) -> PolicyResult:
    """Intersect typed optional-skill restrictions with the built-in policy."""
    if restrictions is None:
        return PolicyResult(_effective(BUILT_IN_MANIFEST.targets, BUILT_IN_MANIFEST.read_fields))
    if not isinstance(restrictions, Mapping) or set(restrictions) - {"targets", "read_fields"}:
        return PolicyResult(denial_code="invalid_operator_restrictions")
    targets = _members(restrictions.get("targets", BUILT_IN_MANIFEST.targets))
    fields = _members(restrictions.get("read_fields", BUILT_IN_MANIFEST.read_fields))
    if targets is None or fields is None:
        return PolicyResult(denial_code="invalid_operator_restrictions")
    return PolicyResult(_effective(BUILT_IN_MANIFEST.targets & targets, BUILT_IN_MANIFEST.read_fields & fields))


def validate_ha_proposal(candidate: object, restrictions: Mapping[str, object] | None = None) -> ValidationResult:
    """Validate a closed HA request without granting dispatch authority."""
    policy_result = compute_effective_policy(restrictions)
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


def canonical_ha_proposal(desired_state: str) -> dict[str, object]:
    return {"operation": OPERATION_ID, "target": TARGET_ALIAS, "arguments": {"desired_state": desired_state}}


def _effective(targets: frozenset[str], fields: frozenset[str]) -> EffectivePolicy:
    return EffectivePolicy(BUILT_IN_MANIFEST.revision, BUILT_IN_MANIFEST.operation, targets, fields, False)


def _members(value: object) -> frozenset[str] | None:
    if not isinstance(value, (list, tuple, frozenset)) or not all(isinstance(item, str) for item in value):
        return None
    return frozenset(value)
