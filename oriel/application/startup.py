"""Application readiness values independent of configuration transport."""
from __future__ import annotations

from dataclasses import dataclass

from .configuration import ActivationResult, EffectiveConfiguration
from ..domain.configuration import CoreConfig

UNREADY_CODE = "config_unavailable"
MODEL_UNREADY_CODE = "model_unavailable"
OPTIONAL_HA_STATES = frozenset(("disabled", "degraded", "ready"))


@dataclass(frozen=True)
class StartupState:
    config: CoreConfig | None
    code: str | None
    revision: int | None = None
    effective_config: EffectiveConfiguration | None = None
    activation_result: ActivationResult | None = None
    model_ready: bool = True
    model_code: str | None = None
    optional_ha_state: str = "disabled"

    @property
    def ready(self) -> bool:
        return self.config is not None and self.model_ready

    @property
    def components(self) -> dict[str, dict[str, str]]:
        """Stable, payload-free component states for operational health."""
        core = {"state": "ready"} if self.config is not None else {"state": "unready", "code": self.code or UNREADY_CODE}
        model = {"state": "ready"} if self.model_ready else {"state": "unready", "code": self.model_code or MODEL_UNREADY_CODE}
        ha_state = self.optional_ha_state if self.optional_ha_state in OPTIONAL_HA_STATES else "degraded"
        return {"core": core, "model": model, "ha": {"state": ha_state}}

    @property
    def effective_view(self) -> dict[str, object] | None:
        """Return the sanitized active-configuration view, if one was activated."""
        return None if self.effective_config is None else self.effective_config.payload()
