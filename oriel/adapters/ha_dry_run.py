"""Synthetic, independently validating Home Assistant dry-run adapter."""
from __future__ import annotations

from ..application.ports import DryRunPreview
from ..domain.ha_manifest import BuiltInManifest, CanonicalProposal, DESIRED_STATES, MANIFEST_REVISION, OPERATION_ID, TARGET_ALIAS


class HarmlessHaDryRun:
    """Simulate only the reviewed canonical operation without provider I/O."""

    def __init__(self, manifest: BuiltInManifest) -> None:
        self._manifest = manifest

    def preview(self, proposal: CanonicalProposal) -> DryRunPreview:
        if type(proposal) is not CanonicalProposal:
            return DryRunPreview("denied", None, None, None, None, "adapter_rejected")
        arguments = proposal.argument_object()
        if (
            not self._manifest.enabled
            or self._manifest.operation != OPERATION_ID
            or self._manifest.targets != frozenset((TARGET_ALIAS,))
            or self._manifest.revision != MANIFEST_REVISION
            or proposal.operation != OPERATION_ID
            or proposal.target != TARGET_ALIAS
            or proposal.manifest_revision != MANIFEST_REVISION
            or proposal.arguments not in ((("desired_state", "on"),), (("desired_state", "off"),))
            or arguments.get("desired_state") not in DESIRED_STATES
        ):
            return DryRunPreview("denied", None, None, None, None, "adapter_rejected")
        return DryRunPreview(
            "simulated", proposal.operation, proposal.target,
            arguments["desired_state"], proposal.manifest_revision,
        )
