"""Application-owned immutable configuration activation and effective views."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..domain.configuration import API_VERSION, ConfigError, CoreConfig, parse_core_config
from .ports import ActionPolicyConfiguration, SynchronizationPort
from types import MappingProxyType


INVALID_CONFIGURATION_CODE = "invalid_configuration"
STALE_CONFIGURATION_CODE = "stale_configuration"
READY_PROFILE_LABEL = "configured"


@dataclass(frozen=True)
class EffectiveConfiguration:
    """The complete public view of an active configuration revision."""

    api_version: str
    revision: int
    provider_ready: bool
    provider_profile: str
    disabled_skills: tuple[str, ...]

    def payload(self) -> dict[str, object]:
        """Return only the frozen, provider-safe effective fields."""
        return {
            "api_version": self.api_version,
            "revision": self.revision,
            "provider_ready": self.provider_ready,
            "provider_profile": self.provider_profile,
            "disabled_skills": list(self.disabled_skills),
        }


@dataclass(frozen=True)
class ActiveConfiguration:
    """One immutable validated document installed by the application owner."""

    revision: int
    config: CoreConfig
    effective: EffectiveConfiguration


@dataclass(frozen=True)
class ActivationSucceeded:
    active: ActiveConfiguration
    kind: Literal["activated"] = "activated"


@dataclass(frozen=True)
class ActivationConflict:
    """A compare-and-swap loss; the active value was not changed."""

    active: ActiveConfiguration | None
    expected_revision: int | None
    code: Literal["stale_configuration"] = STALE_CONFIGURATION_CODE
    kind: Literal["stale"] = "stale"


@dataclass(frozen=True)
class ActivationRejected:
    """A sanitized invalid candidate result; the active value was not changed."""

    active: ActiveConfiguration | None
    code: Literal["invalid_configuration"] = INVALID_CONFIGURATION_CODE
    kind: Literal["rejected"] = "rejected"


ActivationResult = ActivationSucceeded | ActivationConflict | ActivationRejected


class ConfigurationService:
    """The only inward owner of active whole-document configuration revisions."""

    def __init__(self, synchronization: SynchronizationPort) -> None:
        self._synchronization = synchronization
        self._active: ActiveConfiguration | None = None

    def synchronized_by(self, synchronization: SynchronizationPort) -> bool:
        return self._synchronization is synchronization

    def action_policy(self) -> ActionPolicyConfiguration:
        with self._synchronization.locked():
            active = self._active
            if active is None:
                return ActionPolicyConfiguration(0, None, True)
            skill = active.config.skills.get("home_assistant", {})
            restrictions = MappingProxyType({key: tuple(skill[key]) for key in ("targets", "read_fields") if key in skill})
            return ActionPolicyConfiguration(active.revision, restrictions, "home_assistant" in active.config.disabled_skills)

    @property
    def active(self) -> ActiveConfiguration | None:
        with self._synchronization.locked():
            return self._active

    def activate(self, document: object, expected_revision: int | None, profile_label: str) -> ActivationResult:
        """Parse and atomically install one whole document when the revision matches."""
        try:
            candidate = parse_core_config(document)
        except ConfigError:
            return self.reject()
        return self.activate_config(candidate, expected_revision, profile_label)

    def activate_config(self, candidate: CoreConfig, expected_revision: int | None, profile_label: str) -> ActivationResult:
        """Install a previously parsed candidate without carrying provider material."""
        if not _is_profile_label(profile_label):
            return self.reject()
        try:
            candidate = _copy_validated_config(candidate)
        except ConfigError:
            return self.reject()
        with self._synchronization.locked():
            active = self._active
            actual_revision = active.revision if active is not None else None
            if expected_revision != actual_revision:
                return ActivationConflict(active, expected_revision)
            revision = 1 if active is None else active.revision + 1
            effective = EffectiveConfiguration(API_VERSION, revision, True, READY_PROFILE_LABEL, candidate.disabled_skills)
            self._active = ActiveConfiguration(revision, candidate, effective)
            return ActivationSucceeded(self._active)

    def stale_result(self, expected_revision: int | None) -> ActivationConflict | None:
        """Return a conflict before outer adapters perform candidate-only work."""
        with self._synchronization.locked():
            active = self._active
            actual_revision = active.revision if active is not None else None
            if expected_revision != actual_revision:
                return ActivationConflict(active, expected_revision)
            return None

    def reject(self) -> ActivationRejected:
        """Report an invalid or unavailable candidate without disturbing the active one."""
        with self._synchronization.locked():
            return ActivationRejected(self._active)


def _is_profile_label(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256


def _copy_validated_config(candidate: CoreConfig) -> CoreConfig:
    """Revalidate and detach a caller-supplied CoreConfig before retaining it."""
    if not isinstance(candidate, CoreConfig) or not isinstance(candidate.disabled_skills, tuple):
        raise ConfigError("invalid configuration")
    disabled = candidate.disabled_skills
    if any(not isinstance(name, str) for name in disabled):
        raise ConfigError("invalid configuration")
    if len(set(disabled)) != len(disabled):
        raise ConfigError("invalid configuration")
    try:
        skills = {name: dict(material) for name, material in candidate.skills.items()}
    except (AttributeError, TypeError, ValueError):
        raise ConfigError("invalid configuration") from None
    if any(not isinstance(name, str) for name in skills) or set(skills) & set(disabled):
        raise ConfigError("invalid configuration")
    document = {
        "api_version": API_VERSION,
        "provider": {"connection_ref": candidate.provider_connection_ref},
        "skills": {**skills, **{name: {"enabled": False} for name in disabled}},
    }
    copied = parse_core_config(document)
    if copied.disabled_skills != disabled:
        raise ConfigError("invalid configuration")
    return copied
