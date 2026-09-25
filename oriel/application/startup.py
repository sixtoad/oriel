"""Application readiness values independent of configuration transport."""
from __future__ import annotations

from dataclasses import dataclass

from .configuration import ActivationResult, EffectiveConfiguration
from ..domain.configuration import CoreConfig

UNREADY_CODE = "config_unavailable"


@dataclass(frozen=True)
class StartupState:
    config: CoreConfig | None
    code: str | None
    revision: int | None = None
    effective_config: EffectiveConfiguration | None = None
    activation_result: ActivationResult | None = None

    @property
    def ready(self) -> bool:
        return self.config is not None

    @property
    def effective_view(self) -> dict[str, object] | None:
        """Return the sanitized active-configuration view, if one was activated."""
        return None if self.effective_config is None else self.effective_config.payload()
