"""Project-relative locations for file-first pipeline assets."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    """Resolved project root and its fixed dataset directories."""

    root: Path
    dataset: Path
    raw: Path
    harmonized: Path
    derived: Path
    catalog: Path
    qa: Path

    @classmethod
    def discover(cls, root: Path | None = None) -> "ProjectPaths":
        """Construct paths from an explicit root, configured root, or working directory."""
        configured = os.environ.get("FLASHFLOOD_PROJECT_ROOT")
        selected = root if root is not None else Path(configured) if configured else Path.cwd()
        resolved = selected.resolve()
        dataset = resolved / "dataset"
        return cls(
            resolved,
            dataset,
            dataset / "raw",
            dataset / "harmonized",
            dataset / "derived",
            dataset / "catalog",
            dataset / "qa",
        )

    def ensure_output_dirs(self) -> None:
        """Create only the pipeline-owned output directories beneath the project root."""
        for path in (self.raw, self.harmonized, self.derived, self.catalog, self.qa):
            path.mkdir(parents=True, exist_ok=True)
