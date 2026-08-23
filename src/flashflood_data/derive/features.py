"""Validated, repository-relative Task 15 feature semantics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

_SOIL_DIVISORS = {
    "clay": 10.0,
    "sand": 10.0,
    "silt": 10.0,
    "bdod": 100.0,
    "cfvo": 10.0,
    "wv0010": 10.0,
    "wv0033": 10.0,
    "wv1500": 10.0,
}
_WORLDCOVER_CLASSES = {
    10: "tree_cover", 20: "shrubland", 30: "grassland", 40: "cropland",
    50: "built_up", 60: "bare_sparse", 70: "snow_ice", 80: "permanent_water",
    90: "herbaceous_wetland", 95: "mangroves", 100: "moss_lichen",
}
_BASINATLAS_FIELDS = (
    "dis_m3_pyr", "run_mm_syr", "inu_pc_smn", "inu_pc_smx", "lka_pc_sse",
    "dor_pc_pva", "ria_ha_ssu", "riv_tc_ssu", "gwt_cm_sav", "ele_mt_sav",
    "ele_mt_smn", "ele_mt_smx", "slp_dg_sav", "sgr_dk_sav", "pre_mm_syr",
)


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
    if config.soil_divisors != _SOIL_DIVISORS:
        raise ValueError("features config must define the exact SoilGrids divisor mapping")
    if len(config.soil_properties) * len(config.soil_depths) * len(config.soil_statistics) != 96:
        raise ValueError("features config must define the 96 SoilGrids asset labels")
    if config.soil_depth_bands_cm != ((0, 30), (30, 100)):
        raise ValueError("Task 15 requires 0-30 and 30-100 cm SoilGrids bands")
    if config.worldcover_classes != _WORLDCOVER_CLASSES or len(set(config.worldcover_classes.values())) != 11:
        raise ValueError("features config must define the exact WorldCover code-to-name mapping")
    if config.basinatlas_fields != _BASINATLAS_FIELDS:
        raise ValueError("features config must define the exact BasinATLAS field set")
    return config
