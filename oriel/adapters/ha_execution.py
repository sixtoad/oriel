"""Fixed worker-local operation and fresh observation; no retries or read fixtures."""
from __future__ import annotations

from dataclasses import dataclass
import time
import math
from typing import Protocol

from ..application.ports import ActionExecutionRequest, ActionExecutionResult, BoundedHomeFact
from ..domain.ha_manifest import BUILT_IN_MANIFEST, BuiltInManifest, CanonicalProposal, execution_eligibility


@dataclass(frozen=True)
class WorkerObservation:
    fact: BoundedHomeFact
    obtained_at: float


class FixedLightProvider(Protocol):
    """Worker-owned provider binding with no caller-selectable target or service.

    Implementations must bound every I/O by the supplied absolute monotonic
    deadline. Observation must be a fresh fetch, never a cached fact. Shipping
    composition supplies no binding until the live prerequisites are reviewed.
    """
    def set_power(self, desired_state: str, deadline: float) -> str: ...
    def observe(self, deadline: float) -> WorkerObservation: ...


class HarmlessHaExecutor:
    def __init__(self, provider: FixedLightProvider | None = None,
                 manifest: BuiltInManifest = BUILT_IN_MANIFEST,
                 monotonic=time.monotonic, *, scope: str = "live") -> None:
        self._provider = provider
        self._manifest = manifest
        self._monotonic = monotonic
        self._scope = scope

    def execute(self, request: ActionExecutionRequest) -> ActionExecutionResult:
        if type(request) is not ActionExecutionRequest or type(request.proposal) is not CanonicalProposal:
            return ActionExecutionResult("denied", "adapter_rejected")
        proposal = request.proposal
        candidate = {"operation": proposal.operation, "target": proposal.target, "arguments": proposal.argument_object()}
        validation = execution_eligibility(candidate, manifest=self._manifest)
        evidence = self._manifest.execution_prerequisites
        if validation.material != proposal or evidence is None or evidence.scope != self._scope or self._provider is None:
            return ActionExecutionResult("denied", "prerequisite_unmet")
        # The same absolute deadline comes through IPC. Never reset the budget.
        now = self._monotonic()
        if type(request.deadline) not in (int, float) or not math.isfinite(request.deadline) or not now < request.deadline <= now + 5:
            return ActionExecutionResult("denied", "adapter_rejected")
        desired = proposal.argument_object()["desired_state"]
        dispatched_at = now
        try:
            acknowledgement = self._provider.set_power(desired, request.deadline)
        except Exception:
            return ActionExecutionResult("outcome_unknown", "deadline" if self._monotonic() >= request.deadline else "transport_unknown")
        if self._monotonic() >= request.deadline:
            return ActionExecutionResult("outcome_unknown", "deadline")
        if acknowledgement == "rejected":
            return ActionExecutionResult("denied", "service_rejected")
        if acknowledgement == "no_effect":
            return ActionExecutionResult("failed", "no_effect_failure")
        if acknowledgement != "accepted":
            return ActionExecutionResult("outcome_unknown", "transport_unknown")
        try:
            observation = self._provider.observe(request.deadline)
        except Exception:
            return ActionExecutionResult("outcome_unknown", "deadline" if self._monotonic() >= request.deadline else "observation_missing", "accepted")
        finished = self._monotonic()
        if finished >= request.deadline:
            return ActionExecutionResult("outcome_unknown", "deadline", "accepted")
        if type(observation) is not WorkerObservation or type(observation.fact) is not BoundedHomeFact:
            return ActionExecutionResult("outcome_unknown", "observation_missing", "accepted")
        if type(observation.obtained_at) not in (int, float) or not math.isfinite(observation.obtained_at):
            return ActionExecutionResult("outcome_unknown", "observation_missing", "accepted")
        fact = observation.fact
        if fact.freshness != "fresh" or not dispatched_at < observation.obtained_at <= finished:
            return ActionExecutionResult("outcome_unknown", "observation_stale", "accepted")
        if fact.power_state != desired:
            return ActionExecutionResult("outcome_unknown", "observation_mismatch", "observed", fact.power_state, fact.observed_at)
        return ActionExecutionResult("confirmed", "observation_confirmed", "observed", fact.power_state, fact.observed_at)
