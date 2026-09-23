"""Validated configuration for the static data pipeline."""

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class StudyAreaConfig(BaseModel):
    """Approved spatial, quality, and storage constraints for the study area."""

    province_origin_code: str = "14"
    hydrobasins_level: int = Field(default=12, ge=1, le=12)
    upstream_hops: int = Field(default=1, ge=0, le=3)
    raster_buffer_km: float = Field(default=10, gt=0)
    exposure_buffer_km: float = Field(default=10, gt=0)
    processing_crs: str = "EPSG:32648"
    storage_crs: str = "EPSG:4326"
    new_raw_soft_cap_gib: float = 8
    minimum_free_gib: float = 10
    admin_expected_count: int = 75
    admin_expected_communes: int = 67
    admin_expected_wards: int = 8
    historical_event_expected_count: int = Field(default=30, ge=0)
    admin_gap_overlap_max_pct: float = 0.1
    legal_area_diff_max_pct: float = 2.0
    commune_basin_coverage_min_pct: float = 99.5
    commune_basin_coverage_max_pct: float = 100.5
    environmental_raster_coverage_min_pct: float = 99.0
    admin_area_exceptions: list[str] = Field(default_factory=list)


class EnvironmentSettings(BaseSettings):
    """Credentials loaded from ignored local environment configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="FLASHFLOOD_", extra="ignore")

    cdse_username: SecretStr | None = None
    cdse_password: SecretStr | None = None


def load_study_area(path: Path) -> StudyAreaConfig:
    """Load and validate the study-area configuration at *path*."""
    return StudyAreaConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
