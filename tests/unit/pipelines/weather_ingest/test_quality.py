"""Weather Bronze quality is decided at object and raster-slice grain."""

from datetime import UTC, datetime
from pathlib import Path

from flashflood_data.pipelines.weather_ingest.quality import evaluate_weather_slices
from flashflood_data.shared.quality import load_quality_policy

POLICY = load_quality_policy(Path("config/quality/bronze_weather.yaml"))
TIME = datetime(2026, 9, 1, 1, tzinfo=UTC)


def _slice(variable: str, values: list[float | None], indices=None):
    return {
        "slice_id": f"slice-{variable}",
        "object_id": "object-1",
        "source_id": "era5_land",
        "source_product": "reanalysis-era5-land",
        "source_grid_version": "grid-v1",
        "spatial_scope_id": "sonla-v1",
        "variable": variable,
        "vertical_level": "surface",
        "source_cycle_id": "20260901",
        "model_run_time": None,
        "valid_time": TIME,
        "window_start": TIME,
        "window_end": TIME,
        "source_revision": 0,
        "cell_indices": indices or [1, 2, 3],
        "values": values,
        "unit": "m",
        "value_kind": "instantaneous",
        "available_at": TIME,
        "ingest_run_id": "run-1",
        "parser_version": "v1",
        "quality_status": "passed",
    }


def test_grid_mismatch_blocks_weather_object() -> None:
    evaluation = evaluate_weather_slices(
        [_slice("total_precipitation", [1.0, 2.0], indices=[1, 2])],
        {1, 2, 3},
        POLICY,
    )

    assert evaluation.disposition == "block_object"
    assert evaluation.accepted_rows == ()
    assert any(
        finding.rule_id == "source_grid_complete" and finding.status == "failed"
        for finding in evaluation.findings
    )


def test_negative_flux_rejects_only_affected_slice() -> None:
    evaluation = evaluate_weather_slices(
        [
            _slice("total_precipitation", [1.0, -0.5, 2.0]),
            _slice("volumetric_soil_water_layer_1", [0.2, 0.3, 0.4]),
        ],
        {1, 2, 3},
        POLICY,
    )

    assert evaluation.disposition == "reject_rows"
    assert [row["variable"] for row in evaluation.accepted_rows] == [
        "volumetric_soil_water_layer_1"
    ]
    assert evaluation.rejected_rows[0].source_record_key.endswith("total_precipitation")


def test_small_negative_flux_is_clamped_and_published_with_warning() -> None:
    evaluation = evaluate_weather_slices(
        [_slice("surface_runoff", [1.0, -0.0000005, 2.0])],
        {1, 2, 3},
        POLICY,
    )

    assert evaluation.disposition == "accept_with_warning"
    assert evaluation.accepted_rows[0]["values"] == [1.0, 0.0, 2.0]
    assert evaluation.accepted_rows[0]["quality_status"] == "warning"


def test_soft_null_ratio_keeps_slice_with_warning() -> None:
    evaluation = evaluate_weather_slices(
        [_slice("total_precipitation", [1.0, None, 2.0])],
        {1, 2, 3},
        POLICY,
    )

    assert evaluation.disposition == "accept_with_warning"
    assert evaluation.accepted_rows[0]["quality_status"] == "warning"
    assert any(
        finding.rule_id == "null_cell_ratio" and finding.status == "failed"
        for finding in evaluation.findings
    )
