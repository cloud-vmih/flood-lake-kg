"""Validated static source landing policy."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, field_validator, model_validator

from flashflood_data.catalog.models import ImmutableModel, _reject_credential_mapping


class LandingSourcePolicy(ImmutableModel):
    """Selection and packaging policy for one configured source."""

    source_id: str
    mode: Literal["individual", "shapefile_bundle"]
    filename_contains: str | None = None
    output_name: str | None = None
    selection: dict[str, object] = Field(default_factory=dict)
    settings_override: dict[str, object] = Field(default_factory=dict)

    @field_validator("selection", "settings_override")
    @classmethod
    def metadata_has_no_credentials(cls, value: dict[str, object]) -> dict[str, object]:
        _reject_credential_mapping(value)
        return value

    @model_validator(mode="after")
    def bundle_has_selection_fields(self) -> "LandingSourcePolicy":
        if self.mode == "shapefile_bundle" and (
            not self.filename_contains or not self.output_name
        ):
            raise ValueError("shapefile bundles require filename_contains and output_name")
        return self


class StaticLandingConfig(ImmutableModel):
    """Complete policy for the first static source landing DAG."""

    basin_level: Literal[12]
    sources: tuple[LandingSourcePolicy, ...]

    @model_validator(mode="after")
    def source_ids_are_unique(self) -> "StaticLandingConfig":
        source_ids = [source.source_id for source in self.sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("duplicate landing source_id")
        return self

    def source(self, source_id: str) -> LandingSourcePolicy:
        """Return one policy by source ID."""
        for source in self.sources:
            if source.source_id == source_id:
                return source
        raise KeyError(source_id)


def load_static_landing_config(path: Path) -> StaticLandingConfig:
    """Load and validate a static landing YAML file."""
    return StaticLandingConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
