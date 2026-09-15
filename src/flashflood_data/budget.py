"""Disk-space guards for bounded static-data acquisition."""

import shutil
from dataclasses import dataclass
from pathlib import Path

from flashflood_data.core.config import StudyAreaConfig

_GIB = 2**30


@dataclass(frozen=True)
class BudgetDecision:
    """The projected storage state before an acquisition starts."""

    allowed: bool
    reason: str
    projected_new_raw_bytes: int
    projected_free_bytes: int


@dataclass(frozen=True)
class StorageBudget:
    """New-raw cap and retained-free-space requirements for a storage root."""

    root: Path
    soft_cap_bytes: int
    minimum_free_bytes: int
    existing_new_raw_bytes: int = 0

    def __post_init__(self) -> None:
        if min(self.soft_cap_bytes, self.minimum_free_bytes, self.existing_new_raw_bytes) < 0:
            raise ValueError("storage budget values must be non-negative")

    @classmethod
    def from_config(
        cls,
        root: Path,
        config: StudyAreaConfig,
        *,
        existing_new_raw_bytes: int = 0,
    ) -> "StorageBudget":
        """Create a budget from the validated study-area storage limits."""
        return cls(
            root=root,
            soft_cap_bytes=int(config.new_raw_soft_cap_gib * _GIB),
            minimum_free_bytes=int(config.minimum_free_gib * _GIB),
            existing_new_raw_bytes=existing_new_raw_bytes,
        )

    def preflight(self, new_bytes: int, temporary_bytes: int) -> BudgetDecision:
        """Decide whether an acquisition can preserve cap and free-space constraints."""
        if new_bytes < 0 or temporary_bytes < 0:
            raise ValueError("new_bytes and temporary_bytes must be non-negative")

        projected_new_raw_bytes = self.existing_new_raw_bytes + new_bytes
        projected_free_bytes = shutil.disk_usage(self.root).free - new_bytes - temporary_bytes
        if projected_new_raw_bytes > self.soft_cap_bytes:
            reason = "new_raw_soft_cap"
        elif projected_free_bytes < self.minimum_free_bytes:
            reason = "minimum_free_space"
        else:
            reason = "ok"
        return BudgetDecision(
            allowed=reason == "ok",
            reason=reason,
            projected_new_raw_bytes=projected_new_raw_bytes,
            projected_free_bytes=projected_free_bytes,
        )
