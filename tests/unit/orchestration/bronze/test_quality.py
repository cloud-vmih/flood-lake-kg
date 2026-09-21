"""Bronze-specific rules share one quality-result shape across pipelines."""

from flashflood_data.orchestration.bronze.quality import check_parsed_rows
from flashflood_data.orchestration.quality import fatal_failures


def _row(feature_id: str, bbox: list[float]) -> dict[str, object]:
    return {
        "object_id": "raw-1", "source_feature_id": feature_id,
        "bbox_wgs84": bbox, "geometry_wkb": b"valid",
    }


def test_bronze_quality_flags_duplicate_feature_and_invalid_bbox() -> None:
    results = check_parsed_rows("basin_polygon_raw", [_row("42", [103, 21, 104, 22]), _row("42", [104, 22, 103, 21])])
    assert {result.rule_id for result in fatal_failures(results)} == {
        "business_key_unique", "bbox_valid",
    }


def test_empty_parse_is_a_fatal_quality_failure() -> None:
    results = check_parsed_rows("basin_polygon_raw", [])
    assert {result.rule_id for result in fatal_failures(results)} == {"nonempty_parse"}
