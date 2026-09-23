"""Application readiness values independent of configuration transport."""
from __future__ import annotations

from dataclasses import dataclass

from ..domain.configuration import CoreConfig

UNREADY_CODE = "config_unavailable"


@dataclass(frozen=True)
class StartupState:
    config: CoreConfig | None
    code: str | None

    @property
    def ready(self) -> bool:
        return self.config is not None
