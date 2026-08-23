"""Validated, repository-relative Task 15 feature semantics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class FeatureConfig:
    processing_crs: str
    terrain_resolution_m: int
    soil_properties: tuple[str, ...]
    soil_depths: tuple[str, ...]
    soil_statistics: tuple[str, ...]
    soil_divisors: dict[str, float]
    soil_depth_bands_cm: tuple[tuple[int, int], ...]
    worldcover_classes: dict[int, str]
    basinatlas_fields: tuple[str, ...]


def _feature_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "features.yaml"


def load_feature_config(path: Path | None = None) -> FeatureConfig:
    """Load exactly one explicit config file without depending on the current directory."""
    source = path or _feature_path()
    with source.open(encoding="utf-8") as stream:
        data = yaml.safe_load(stream)
    if not isinstance(data, dict):
        raise TypeError("features config must be a mapping")
    try:
        soil = data["soilgrids"]
        terrain = data["terrain"]
        config = FeatureConfig(
            processing_crs=str(data["processing_crs"]),
            terrain_resolution_m=int(terrain["target_resolution_m"]),
            soil_properties=tuple(str(value) for value in soil["properties"]),
            soil_depths=tuple(str(value) for value in soil["depths"]),
            soil_statistics=tuple(str(value) for value in soil["statistics"]),
            soil_divisors={str(key): float(value) for key, value in soil["raw_value_divisors"].items()},
            soil_depth_bands_cm=tuple(tuple(int(value) for value in band) for band in soil["depth_bands_cm"]),
            worldcover_classes={int(key): str(value) for key, value in data["worldcover_classes"].items()},
            basinatlas_fields=tuple(str(value) for value in data["basinatlas_fields"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("features config has invalid Task 15 semantics") from error
    if config.processing_crs != "EPSG:32648" or config.terrain_resolution_m != 30:
        raise ValueError("Task 15 requires EPSG:32648 at 30 m")
    if set(config.soil_properties) != set(config.soil_divisors) or len(config.soil_divisors) != 8:
        raise ValueError("features config must define one divisor for each SoilGrids property")
    if len(config.soil_properties) * len(config.soil_depths) * len(config.soil_statistics) != 96:
        raise ValueError("features config must define the 96 SoilGrids asset labels")
    if config.soil_depth_bands_cm != ((0, 30), (30, 100)):
        raise ValueError("Task 15 requires 0-30 and 30-100 cm SoilGrids bands")
    return config
