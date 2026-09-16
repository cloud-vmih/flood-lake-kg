"""Feature configuration is the single source of static-predictor semantics."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from flashflood_data.static.features.config import load_feature_config


def test_feature_config_declares_the_authoritative_task15_semantics() -> None:
    """Changing configured classes, divisors, or baseline names changes derivation inputs."""
    config = load_feature_config()

    assert config.processing_crs == "EPSG:32648"
    assert config.terrain_resolution_m == 30
    assert config.soil_divisors["bdod"] == 100.0
    assert config.worldcover_classes[10] == "tree_cover"
    assert config.basinatlas_fields[-1] == "pre_mm_syr"


def _mutated_config(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    source = Path(__file__).parents[2] / "config" / "features.yaml"
    data = yaml.safe_load(source.read_text(encoding="utf-8"))
    target = tmp_path / "features.yaml"
    return target, data


def test_feature_config_rejects_mutated_soil_divisor(tmp_path: Path) -> None:
    """Changing the bdod raw-value scale would silently corrupt every basin mean."""
    target, data = _mutated_config(tmp_path)
    data["soilgrids"]["raw_value_divisors"]["bdod"] = 10
    target.write_text(yaml.safe_dump(data), encoding="utf-8")

    with pytest.raises(ValueError, match="exact SoilGrids divisor"):
        load_feature_config(target)


def test_feature_config_rejects_mutated_worldcover_mapping(tmp_path: Path) -> None:
    """Reassigning a stable categorical code would relabel native-grid fractions."""
    target, data = _mutated_config(tmp_path)
    data["worldcover_classes"][10] = "cropland"
    target.write_text(yaml.safe_dump(data), encoding="utf-8")

    with pytest.raises(ValueError, match="exact WorldCover"):
        load_feature_config(target)


def test_feature_config_rejects_mutated_basinatlas_field_set(tmp_path: Path) -> None:
    """Replacing a configured baseline field makes the static join non-comparable."""
    target, data = _mutated_config(tmp_path)
    data["basinatlas_fields"][-1] = "other_field"
    target.write_text(yaml.safe_dump(data), encoding="utf-8")

    with pytest.raises(ValueError, match="exact BasinATLAS"):
        load_feature_config(target)
