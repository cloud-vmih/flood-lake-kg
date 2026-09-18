import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from flashflood_data.orchestration.landing.config import (
    LandingSourcePolicy,
    StaticLandingConfig,
    load_static_landing_config,
)
from flashflood_data.orchestration.landing.models import LandingTaskEnvelope, PublishedBatch

ROOT = Path(__file__).resolve().parents[4]


def test_static_landing_config_is_l12_and_has_exact_soil_scope() -> None:
    config = load_static_landing_config(ROOT / "config" / "landing" / "static.yaml")

    assert config.basin_level == 12
    assert [source.source_id for source in config.sources] == [
        "sonla_admin_2025",
        "gadm_vnm_4_1",
        "hydrobasins_v1c",
        "basinatlas_v10",
        "hydrorivers_v10",
        "worldpop_vnm_2025",
        "historical_flood_evidence_2020_2026",
        "geofabrik_vietnam_snapshot",
        "cop_dem_glo30_2024_1",
        "soilgrids_2_0",
        "esa_worldcover_2021_v200",
    ]
    assert {
        source.source_id: source.mode for source in config.sources
    } == {
        "sonla_admin_2025": "individual",
        "gadm_vnm_4_1": "individual",
        "hydrobasins_v1c": "shapefile_bundle",
        "basinatlas_v10": "shapefile_bundle",
        "hydrorivers_v10": "shapefile_bundle",
        "worldpop_vnm_2025": "individual",
        "historical_flood_evidence_2020_2026": "individual",
        "geofabrik_vietnam_snapshot": "individual",
        "cop_dem_glo30_2024_1": "individual",
        "soilgrids_2_0": "individual",
        "esa_worldcover_2021_v200": "individual",
    }
    soil = config.source("soilgrids_2_0")
    assert list(soil.settings_override["properties"]) == [
        "clay",
        "sand",
        "silt",
        "bdod",
        "cfvo",
        "soc",
        "wv0033",
        "wv1500",
    ]
    assert list(soil.settings_override["depths"]) == ["0-5cm", "5-15cm", "15-30cm"]
    assert list(soil.settings_override["statistics"]) == ["mean", "Q0.05", "Q0.5", "Q0.95"]


def test_static_landing_config_rejects_duplicate_sources_and_non_l12() -> None:
    source = LandingSourcePolicy(source_id="fixture", mode="individual")
    with pytest.raises(ValidationError):
        StaticLandingConfig(basin_level=10, sources=(source,))
    with pytest.raises(ValidationError):
        StaticLandingConfig(basin_level=12, sources=(source, source))


def test_policy_rejects_credential_like_metadata() -> None:
    with pytest.raises(ValidationError):
        LandingSourcePolicy(
            source_id="fixture",
            mode="individual",
            settings_override={"api_token": "secret"},
        )


def test_task_envelope_round_trips_json_without_payload_bytes() -> None:
    batch = PublishedBatch(source_id="fixture", run_id="run-1")
    envelope = LandingTaskEnvelope.succeeded("published", batch)

    payload = json.loads(envelope.model_dump_json())
    restored = LandingTaskEnvelope.model_validate(payload)

    assert restored == envelope
    assert "bytes" not in envelope.model_dump_json()
