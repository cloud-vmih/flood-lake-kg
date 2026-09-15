"""Project-relative locations for file-first pipeline assets."""

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
        """Construct project-local paths from *root* or the working directory."""
        resolved = (root or Path.cwd()).resolve()
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
