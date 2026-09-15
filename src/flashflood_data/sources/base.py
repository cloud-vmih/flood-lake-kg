"""Common boundary implemented by every static-data source adapter."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from flashflood_data.catalog import AssetCatalog
from flashflood_data.catalog.models import (
    AssetRecord,
    RemoteAsset,
    SourceSpec,
    ValidationResult,
)
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths


@dataclass(frozen=True)
class SourceContext:
    """Shared, explicit dependencies available to a source adapter."""

    paths: ProjectPaths
    catalog: AssetCatalog
    study_area: StudyAreaConfig
    environment: EnvironmentSettings
    run_id: str


class SourceConfigurationError(ValueError):
    """Raised for invalid manifest settings before source work can begin."""


class SourceAdapter(ABC):
    """Resolve, validate, and harmonize assets for one configured source."""

    def __init__(self, spec: SourceSpec) -> None:
        self.spec = spec

    @abstractmethod
    def resolve(
        self, context: SourceContext, available: list[AssetRecord]
    ) -> list[RemoteAsset]:
        """Resolve immutable remote assets required for the current context."""
        raise NotImplementedError

    @abstractmethod
    def validate_raw(self, path: Path) -> ValidationResult:
        """Validate a downloaded raw payload before downstream use."""
        raise NotImplementedError

    @abstractmethod
    def harmonize(
        self, context: SourceContext, assets: list[AssetRecord]
    ) -> list[AssetRecord]:
        """Create source-specific harmonized assets from validated raw inputs."""
        raise NotImplementedError
