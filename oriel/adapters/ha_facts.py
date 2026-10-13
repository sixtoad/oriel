"""Synthetic, bounded Home Assistant facts used before provider reads exist."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ..application.ports import BoundedHomeFact, HomeFactReaderPort
from ..domain.ha_manifest import FACT_OPERATION_ID, FactRequest, MANIFEST_REVISION, READ_FIELD_ORDER, TARGET_ALIAS


_FRESH = BoundedHomeFact("off", "2026-10-05T00:00:00Z", "fresh")
_STALE = BoundedHomeFact("on", "2026-10-04T23:59:00Z", "stale")


@dataclass(frozen=True)
class SyntheticHaFactReader(HomeFactReaderPort):
    """Serves only the reviewed fixture and sanitizes every failed read."""

    ready: Callable[[], bool]
    fixture: str = "fresh"

    def read(self, request: FactRequest) -> BoundedHomeFact:
        try:
            if (request.operation != FACT_OPERATION_ID or request.target != TARGET_ALIAS
                    or request.fields != READ_FIELD_ORDER or request.manifest_revision != MANIFEST_REVISION
                    or not self.ready()):
                return BoundedHomeFact.unavailable()
            if self.fixture == "fresh":
                return _FRESH
            if self.fixture == "stale":
                return _STALE
            if self.fixture == "unavailable":
                return BoundedHomeFact.unavailable()
        except Exception:
            pass
        return BoundedHomeFact.unavailable()
