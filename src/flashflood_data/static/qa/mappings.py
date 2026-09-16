"""Static quality checks split by subject."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa.models import CheckResult
from flashflood_data.static.qa.shared import (
    _check,
    _first,
    _strict_integral_ids,
    _valid_entity_ids,
)


def _mapping_checks(paths: ProjectPaths) -> list[CheckResult]:
    basin_path = paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    admin_path = paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
    try:
        basin_ids = _strict_integral_ids(gpd.read_parquet(basin_path)["HYBAS_ID"])
        commune_ids = _valid_entity_ids(gpd.read_parquet(admin_path)["current_commune_code"])
        if basin_ids is None or commune_ids is None:
            raise ValueError("invalid source identifiers")
    except Exception:  # noqa: BLE001
        basin_ids, commune_ids = set(), set()
    products = {
        "commune": (
            ("current_commune_code",),
            (admin_path,),
            {
                "intersection_area_km2",
                "basin_fraction",
                "commune_fraction",
                "quality_flags_json",
                "processing_crs",
                "source_asset_ids_json",
            },
        ),
        "river": (
            ("HYRIV_ID",),
            (paths.harmonized / "hydro" / "river_reach.geoparquet",),
            {
                "intersected_length_km",
                "boundary_case",
                "quality_flags_json",
                "processing_crs",
                "source_asset_ids_json",
            },
        ),
        "road": (
            ("segment_id", "osm_id"),
            (
                paths.derived / "exposure" / "road_segment.geoparquet",
                paths.harmonized / "exposure" / "road_segment.geoparquet",
            ),
            {
                "intersected_length_km",
                "boundary_case",
                "quality_flags_json",
                "processing_crs",
                "source_asset_ids_json",
            },
        ),
        "bridge": (
            ("osm_id",),
            (
                paths.derived / "exposure" / "bridge.geoparquet",
                paths.harmonized / "exposure" / "bridge.geoparquet",
            ),
            {
                "intersected_length_km",
                "relationship_geometry_wkt",
                "boundary_case",
                "quality_flags_json",
                "processing_crs",
                "source_asset_ids_json",
            },
        ),
        "facility": (
            ("osm_id",),
            (
                paths.derived / "exposure" / "facility.geoparquet",
                paths.harmonized / "exposure" / "facility.geoparquet",
            ),
            {
                "relationship_type",
                "tags_json",
                "boundary_case",
                "quality_flags_json",
                "processing_crs",
                "source_asset_ids_json",
            },
        ),
        "settlement": (
            ("osm_id",),
            (
                paths.derived / "exposure" / "settlement.geoparquet",
                paths.harmonized / "exposure" / "settlement.geoparquet",
            ),
            {
                "relationship_type",
                "tags_json",
                "boundary_case",
                "quality_flags_json",
                "processing_crs",
                "source_asset_ids_json",
            },
        ),
        "population": (
            (),
            (),
            {
                "population_scope",
                "population_sum",
                "population_mean",
                "contributing_pixel_count",
                "nodata_pixel_count",
                "aoi_pixel_count",
                "coverage_ratio",
                "source_resolution_x",
                "source_resolution_y",
                "source_resolution_unit",
                "source_crs",
                "boundary_center_tie_pixel_count",
                "quality_flags_json",
                "source_asset_ids_json",
            },
        ),
    }
    foreign_ok, bad, coverage, boundary_cases = True, 0, pd.Series(dtype="float64"), 0
    for name, (entity_keys, entity_paths, extras) in products.items():
        path = _first(
            [
                paths.derived / "mappings" / f"map_subbasin_{name}.parquet",
                paths.derived / f"map_subbasin_{name}.parquet",
            ]
        )
        if path is None:
            foreign_ok, bad = False, bad + 1
            continue
        try:
            table = pd.read_parquet(path)
            required = {"HYBAS_ID", *extras, *entity_keys}
            if not required.issubset(table.columns):
                foreign_ok, bad = False, bad + 1
                continue
            mapped_basin_ids = _strict_integral_ids(table["HYBAS_ID"])
            invalid_basins = set() if mapped_basin_ids is None else mapped_basin_ids - basin_ids
            if mapped_basin_ids is None or invalid_basins:
                foreign_ok, bad = False, bad + max(1, len(invalid_basins))
            if entity_keys:
                entity_path = _first(entity_paths)
                if entity_path is None:
                    foreign_ok, bad = False, bad + 1
                    continue
                entity = gpd.read_parquet(entity_path)
                for entity_key in entity_keys:
                    if entity_key not in entity:
                        foreign_ok, bad = False, bad + 1
                        continue
                    mapping_ids = _valid_entity_ids(table[entity_key])
                    source_ids = _valid_entity_ids(entity[entity_key])
                    invalid_entities = (
                        set()
                        if mapping_ids is None or source_ids is None
                        else mapping_ids - source_ids
                    )
                    if mapping_ids is None or source_ids is None or invalid_entities:
                        foreign_ok, bad = False, bad + max(1, len(invalid_entities))
            if name == "commune":
                coverage = pd.to_numeric(
                    table.groupby("current_commune_code")["commune_fraction"].sum(), errors="coerce"
                )
            if "boundary_case" in table:
                boundary_cases += int(table["boundary_case"].fillna(False).astype(bool).sum())
        except Exception:  # noqa: BLE001
            foreign_ok, bad = False, bad + 1
    fk = _check(
        "mapping.foreign_keys",
        foreign_ok,
        "fatal",
        "every mapping foreign key exists",
        f"{bad} invalid/missing mapping products or keys",
        "all seven approved mappings have required schemas and two-sided foreign keys",
    )
    if coverage.empty:
        coverage_check = _check(
            "mapping.commune_coverage",
            False,
            "fatal",
            "99.5–100.5% commune coverage by L10",
            "missing",
            "commune-to-L10 area mapping is missing",
        )
    else:
        percentages = coverage.reindex(sorted(commune_ids)) * 100
        passed = percentages.notna().all() and percentages.between(99.5, 100.5).all()
        coverage_check = _check(
            "mapping.commune_coverage",
            bool(passed),
            "fatal",
            "99.5–100.5% commune coverage by L10",
            f"{percentages.min():.6f}–{percentages.max():.6f}%",
            "commune fractions summed by current commune",
        )
    boundary_warning = _check(
        "mapping.boundary_cases",
        boundary_cases == 0,
        "warning",
        "0 mapping boundary cases/ties",
        boundary_cases,
        "boundary-case relationships remain explicit QA evidence",
    )
    return [fk, coverage_check, boundary_warning]




def checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    del config
    return _mapping_checks(paths)
