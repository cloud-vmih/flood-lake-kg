"""Feature configuration is the single source of static-predictor semantics."""

from __future__ import annotations

from flashflood_data.derive.features import load_feature_config


def test_feature_config_declares_the_authoritative_task15_semantics() -> None:
    """Changing configured classes, divisors, or baseline names changes derivation inputs."""
    config = load_feature_config()

    assert config.processing_crs == "EPSG:32648"
    assert config.terrain_resolution_m == 30
    assert config.soil_divisors["bdod"] == 100.0
    assert config.worldcover_classes[10] == "tree_cover"
    assert config.basinatlas_fields[-1] == "pre_mm_syr"
