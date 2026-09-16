"""Static quality checks split by subject."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa.models import CheckResult
from flashflood_data.static.qa.shared import (
    _check,
)
from flashflood_data.static.spatial.vector import validate_vector


def _admin_checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    path = paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
    if not path.is_file():
        missing = _check(
            "admin.current.count",
            False,
            "fatal",
            "75 total / 67 communes / 8 wards",
            "missing",
            "current administration layer is missing",
        )
        return [
            missing,
            _check(
                "admin.geometry",
                False,
                "fatal",
                "valid EPSG:4326 geometries",
                "missing",
                "current administration layer is missing",
            ),
            _check(
                "admin.legal_coverage",
                False,
                "fatal",
                "gap/overlap <= 0.1%; legal area difference <= 2%",
                "missing",
                "current administration layer is missing",
            ),
        ]
    try:
        admin = gpd.read_parquet(path)
    except Exception as error:  # noqa: BLE001
        text = type(error).__name__
        return [
            _check(
                "admin.current.count",
                False,
                "fatal",
                "75 total / 67 communes / 8 wards",
                text,
                "current administration layer is unreadable",
            ),
            _check(
                "admin.geometry",
                False,
                "fatal",
                "valid EPSG:4326 geometries",
                text,
                "current administration layer is unreadable",
            ),
            _check(
                "admin.legal_coverage",
                False,
                "fatal",
                "gap/overlap <= 0.1%; legal area difference <= 2%",
                text,
                "current administration layer is unreadable",
            ),
        ]
    codes = admin.get("current_commune_code", pd.Series(dtype="object"))
    unit_types = admin.get("unit_type", pd.Series(dtype="object")).astype(str)
    count_passed = (
        len(admin) == config.admin_expected_count
        and codes.is_unique
        and codes.notna().all()
        and int((unit_types == "commune").sum()) == config.admin_expected_communes
        and int((unit_types == "ward").sum()) == config.admin_expected_wards
    )
    count = _check(
        "admin.current.count",
        count_passed,
        "fatal",
        "75 total / 67 communes / 8 wards",
        f"{len(admin)} total / {(unit_types == 'commune').sum()} communes / {(unit_types == 'ward').sum()} wards",
        "current administrative legal-unit count and codes",
    )
    valid = validate_vector(admin, ("current_commune_code", "unit_type"), config.storage_crs).passed
    geometry = _check(
        "admin.geometry",
        valid,
        "fatal",
        "valid EPSG:4326 geometries",
        "valid" if valid else "invalid",
        "current administration geometry/schema validation",
    )
    repaired = int(
        admin.get("geometry_repaired", pd.Series(False, index=admin.index)).fillna(False).sum()
    )
    repair = _check(
        "admin.geometry.repairs",
        repaired == 0,
        "warning",
        "0 repaired geometries",
        repaired,
        "geometry repairs are retained as QA evidence",
    )
    reference_path = paths.harmonized / "admin" / "sonla_reference_boundary.geoparquet"
    try:
        reference = gpd.read_parquet(reference_path)
    except Exception:  # noqa: BLE001 - independent boundary absence is a fatal result.
        reference = None
    if (
        admin.empty
        or admin.crs is None
        or "legal_area_km2" not in admin
        or reference is None
        or reference.empty
    ):
        legal = _check(
            "admin.legal_coverage",
            False,
            "fatal",
            "gap/overlap <= 0.1%; legal area difference <= 2%",
            "unavailable",
            "legal-area evidence is missing",
        )
    else:
        metric = admin.to_crs(config.processing_crs)
        reference_metric = reference.to_crs(config.processing_crs).geometry.union_all()
        reference_area = float(reference_metric.area) / 1_000_000
        legal_area = pd.to_numeric(metric["legal_area_km2"], errors="coerce")
        computed = metric.geometry.area / 1_000_000
        union_area = float(metric.geometry.union_all().area) / 1_000_000
        total_area = float(metric.geometry.area.sum()) / 1_000_000
        overlap = (
            max(0.0, total_area - union_area) / reference_area * 100 if reference_area else 100.0
        )
        gap = (
            max(0.0, reference_area - union_area) / reference_area * 100
            if reference_area
            else 100.0
        )
        difference = ((computed - legal_area).abs() / legal_area * 100).fillna(float("inf"))
        exceptions = {str(value).zfill(5) for value in config.admin_area_exceptions}
        outside = difference[~metric["current_commune_code"].astype(str).isin(exceptions)]
        max_difference = float(outside.max()) if not outside.empty else 0.0
        legal = _check(
            "admin.legal_coverage",
            gap <= config.admin_gap_overlap_max_pct
            and overlap <= config.admin_gap_overlap_max_pct
            and max_difference <= config.legal_area_diff_max_pct,
            "fatal",
            f"gap/overlap <= {config.admin_gap_overlap_max_pct}%; legal area difference <= {config.legal_area_diff_max_pct}%",
            f"gap={gap:.6f}%, overlap={overlap:.6f}%, legal_difference={max_difference:.6f}%",
            "independent Son La reference-boundary-normalized gaps, overlaps, and per-unit tolerance",
        )
    exceptions_used = (
        sorted(
            set(
                metric.loc[
                    metric["current_commune_code"].astype(str).isin(exceptions),
                    "current_commune_code",
                ].astype(str)
            )
        )
        if "metric" in locals()
        else []
    )
    exception_warning = _check(
        "admin.legal_area.exceptions",
        not exceptions_used,
        "warning",
        "0 approved legal-area exceptions used",
        ",".join(exceptions_used) or "0",
        "approved legal-area exceptions are explicit QA evidence",
    )
    return [count, geometry, legal, repair, exception_warning]




def checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    return _admin_checks(paths, config)
