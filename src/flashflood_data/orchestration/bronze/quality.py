"""Bronze-specific batch rules recorded through the shared quality contract."""

import math
from collections.abc import Mapping, Sequence

from flashflood_data.orchestration.quality import QualityResult
from flashflood_data.storage.iceberg_schemas import BRONZE_KEYS


def _valid_bbox(value: object) -> bool:
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        return False
    try:
        west, south, east, north = (float(item) for item in value)
    except (TypeError, ValueError):
        return False
    return (
        all(math.isfinite(item) for item in (west, south, east, north))
        and -180 <= west <= east <= 180
        and -90 <= south <= north <= 90
    )


def check_parsed_rows(
    table_name: str, rows: Sequence[Mapping[str, object]]
) -> list[QualityResult]:
    """Check source-grain uniqueness and spatial envelope before Iceberg commit."""
    keys = BRONZE_KEYS[table_name]
    identities = [tuple(row.get(name) for name in keys) for row in rows]
    duplicate_count = len(identities) - len(set(identities))
    bad_bbox = sum(
        1 for row in rows
        if row.get("bbox_wgs84") is not None and not _valid_bbox(row["bbox_wgs84"])
    )
    return [
        QualityResult(
            "nonempty_parse", "passed" if rows else "failed", "fatal",
            {"row_count": len(rows)}, 0 if rows else 1,
        ),
        QualityResult(
            "business_key_unique", "passed" if duplicate_count == 0 else "failed", "fatal",
            {"duplicate_count": duplicate_count}, duplicate_count,
        ),
        QualityResult(
            "bbox_valid", "passed" if bad_bbox == 0 else "failed", "fatal",
            {"invalid_count": bad_bbox}, bad_bbox,
        ),
    ]
