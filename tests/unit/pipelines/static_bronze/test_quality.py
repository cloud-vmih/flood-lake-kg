"""Bronze-specific rules share one quality-result shape across pipelines."""

from pathlib import Path

from shapely.geometry import LineString

from flashflood_data.pipelines.static_bronze.quality import (
    check_parsed_rows,
    evaluate_static_object,
)
from flashflood_data.shared.quality import fatal_failures, load_quality_policy

POLICY = load_quality_policy(Path("config/quality/bronze_static.yaml"))


def _row(feature_id: str, bbox: list[float]) -> dict[str, object]:
    return {
        "object_id": "raw-1",
        "source_feature_id": feature_id,
        "bbox_wgs84": bbox,
        "geometry_wkb": b"valid",
    }


def test_bronze_quality_flags_duplicate_feature_and_invalid_bbox() -> None:
    results = check_parsed_rows(
        "basin_polygon_raw", [_row("42", [103, 21, 104, 22]), _row("42", [104, 22, 103, 21])]
    )
    assert {result.rule_id for result in fatal_failures(results)} == {
        "business_key_unique",
        "bbox_valid",
    }


def test_empty_parse_is_a_fatal_quality_failure() -> None:
    results = check_parsed_rows("basin_polygon_raw", [])
    assert {result.rule_id for result in fatal_failures(results)} == {"nonempty_parse"}


def _river_row(feature_id: str, *, bbox=None):
    line = LineString([(103.0, 21.0), (104.0, 22.0)])
    return {
        "object_id": "object-1",
        "source_feature_id": feature_id,
        "source_fields_json": "{}",
        "geometry_wkb": line.wkb,
        "crs": "EPSG:4326",
        "bbox_wgs84": bbox or [103.0, 21.0, 104.0, 22.0],
        "ingest_run_id": "run-1",
        "parser_version": "v2",
        "quality_status": "passed",
    }


def test_static_evaluation_partitions_invalid_feature_and_keeps_valid_feature() -> None:
    valid = _river_row("valid")
    invalid = _river_row("invalid", bbox=[104.0, 21.0, 103.0, 22.0])

    evaluation = evaluate_static_object(
        "river_reach_raw",
        {"object_id": "object-1", "checksum_valid": True, "readable": True, "crs_usable": True},
        [valid, invalid],
        POLICY,
    )

    assert evaluation.disposition == "reject_rows"
    assert [row["source_feature_id"] for row in evaluation.accepted_rows] == ["valid"]
    assert [row.source_record_key for row in evaluation.rejected_rows] == ["invalid"]
    assert evaluation.rejected_rows[0].findings[0].rule_id == "bbox_valid"


def test_static_evaluation_publishes_repaired_geometry_as_warning() -> None:
    repaired = {
        **_river_row("repaired"),
        "_dq_warning_codes": ("geometry_repaired",),
    }

    evaluation = evaluate_static_object(
        "river_reach_raw",
        {"object_id": "object-1", "checksum_valid": True, "readable": True, "crs_usable": True},
        [repaired],
        POLICY,
    )

    assert evaluation.disposition == "accept_with_warning"
    assert evaluation.accepted_rows[0]["quality_status"] == "warning"
    assert "_dq_warning_codes" not in evaluation.accepted_rows[0]
    assert any(item.rule_id == "geometry_repaired" for item in evaluation.findings)


def test_static_evaluation_blocks_object_when_checksum_is_invalid() -> None:
    evaluation = evaluate_static_object(
        "river_reach_raw",
        {
            "object_id": "object-1",
            "checksum_valid": False,
            "readable": True,
            "crs_usable": True,
        },
        [_river_row("valid")],
        POLICY,
    )

    assert evaluation.disposition == "block_object"
    assert evaluation.accepted_rows == ()
    assert {finding.rule_id for finding in fatal_failures(evaluation.findings)} == {
        "object_checksum"
    }
