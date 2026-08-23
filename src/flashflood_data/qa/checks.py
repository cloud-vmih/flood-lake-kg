"""Fatal quality gates over already-published static pipeline artifacts."""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.paths import ProjectPaths
from flashflood_data.raster import raster_coverage_ratio
from flashflood_data.vector import validate_vector

Severity = Literal["info", "warning", "fatal"]
_GIB = 2**30
_EVENT_STATUSES = frozenset({"matched", "ambiguous", "unresolved"})


@dataclass(frozen=True)
class CheckResult:
    """One independently-readable gate result."""

    check_id: str
    passed: bool
    severity: Severity
    expected: str
    actual: str
    message: str
    asset_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class QAReport:
    """Stable QA report, including failures instead of throwing them away."""

    run_id: str
    config_fingerprint: str
    checks: tuple[CheckResult, ...]

    def by_id(self, check_id: str) -> CheckResult:
        return next(check for check in self.checks if check.check_id == check_id)

    @property
    def fatal_failures(self) -> tuple[CheckResult, ...]:
        return tuple(
            check for check in self.checks if not check.passed and check.severity == "fatal"
        )


class QualityGateFailure(RuntimeError):
    """Raised only after fatal QA reports and map artifacts were published."""


def _check(
    check_id: str,
    passed: bool,
    severity: Severity,
    expected: str,
    actual: object,
    message: str,
    asset_ids: Iterable[str] = (),
) -> CheckResult:
    return CheckResult(
        check_id=check_id,
        passed=bool(passed),
        severity=severity,
        expected=expected,
        actual=str(actual),
        message=message,
        asset_ids=tuple(sorted(str(asset_id) for asset_id in asset_ids)),
    )


def _config_fingerprint(config: StudyAreaConfig) -> str:
    body = json.dumps(config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _first(paths: Iterable[Path]) -> Path | None:
    return next((path for path in paths if path.is_file()), None)


def _geometry(paths: ProjectPaths, name: str) -> tuple[gpd.GeoDataFrame | None, Path]:
    path = paths.harmonized / "aoi" / f"{name}_aoi.geoparquet"
    if not path.is_file():
        return None, path
    try:
        return gpd.read_parquet(path), path
    except Exception:  # noqa: BLE001 - gates must report a broken artifact.
        return None, path


def _asset_records(paths: ProjectPaths) -> list[AssetRecord]:
    catalog_path = paths.catalog / "assets.parquet"
    if not catalog_path.is_file():
        return []
    try:
        return AssetCatalog(paths)._read_assets()
    except Exception:  # noqa: BLE001
        return []


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
        clipped = metric.geometry.intersection(reference_metric)
        legal_area = pd.to_numeric(metric["legal_area_km2"], errors="coerce")
        computed = metric.geometry.area / 1_000_000
        union_area = float(clipped.union_all().area) / 1_000_000
        total_area = float(clipped.area.sum()) / 1_000_000
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


def _hydro_checks(paths: ProjectPaths) -> list[CheckResult]:
    l10_path = paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hierarchy_path = paths.harmonized / "hydro" / "subbasin_hierarchy.parquet"
    try:
        l10 = gpd.read_parquet(l10_path)
        hierarchy = pd.read_parquet(hierarchy_path)
        from flashflood_data.harmonize.hydro import default_hydro_inputs

        inputs = default_hydro_inputs(paths)
        l9 = gpd.read_file(inputs.l9)
        l8 = gpd.read_file(inputs.l8)
    except Exception:  # noqa: BLE001
        return [
            _check(
                "hydro.l10.hierarchy",
                False,
                "fatal",
                "unique L10 IDs with valid L9/L8 parents",
                "missing or unreadable",
                "hydrological hierarchy evidence is required",
            ),
            _check(
                "hydro.l10.topology",
                False,
                "fatal",
                "topology references internal L10 IDs or declared scope exits",
                "missing or unreadable",
                "hydrological topology evidence is required",
            ),
        ]
    ids = pd.to_numeric(l10.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
    parent_fields = {"HYBAS_ID", "parent_l9_hybas_id", "parent_l8_hybas_id", "scope_exit"}
    l9_ids = set(
        pd.to_numeric(l9.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
        .dropna()
        .astype("int64")
    )
    l8_ids = set(
        pd.to_numeric(l8.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
        .dropna()
        .astype("int64")
    )
    l10_pfaf = {
        int(row.HYBAS_ID): str(row.PFAF_ID).removesuffix(".0")
        for row in l10.itertuples(index=False)
        if hasattr(row, "HYBAS_ID") and hasattr(row, "PFAF_ID")
    }
    l9_pfaf = {
        int(row.HYBAS_ID): str(row.PFAF_ID).removesuffix(".0")
        for row in l9.itertuples(index=False)
        if hasattr(row, "HYBAS_ID") and hasattr(row, "PFAF_ID")
    }
    l8_pfaf = {
        int(row.HYBAS_ID): str(row.PFAF_ID).removesuffix(".0")
        for row in l8.itertuples(index=False)
        if hasattr(row, "HYBAS_ID") and hasattr(row, "PFAF_ID")
    }
    parents_ok = parent_fields.issubset(hierarchy.columns)
    if parents_ok:
        for row in hierarchy.itertuples(index=False):
            child = int(row.HYBAS_ID)
            parent9, parent8 = int(row.parent_l9_hybas_id), int(row.parent_l8_hybas_id)
            parents_ok = (
                parents_ok
                and parent9 in l9_ids
                and parent8 in l8_ids
                and child in l10_pfaf
                and l10_pfaf[child].startswith(l9_pfaf.get(parent9, "!"))
                and l10_pfaf[child].startswith(l8_pfaf.get(parent8, "!"))
            )
    hierarchy_ok = (
        ids.notna().all()
        and ids.is_unique
        and set(ids.astype("int64"))
        == set(
            pd.to_numeric(hierarchy.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
            .dropna()
            .astype("int64")
        )
        and parents_ok
    )
    hierarchy_check = _check(
        "hydro.l10.hierarchy",
        hierarchy_ok,
        "fatal",
        "unique L10 IDs with valid L9/L8 parents",
        f"{len(ids)} L10 IDs",
        "L10 uniqueness and parent references",
    )
    if "NEXT_DOWN" not in l10:
        topology_ok, scope_exits = False, 0
    else:
        selected = set(ids.dropna().astype("int64"))
        declared = (
            set(
                pd.to_numeric(
                    hierarchy.loc[hierarchy.get("scope_exit", False).astype(bool), "HYBAS_ID"],
                    errors="coerce",
                )
                .dropna()
                .astype("int64")
            )
            if "scope_exit" in hierarchy
            else set()
        )
        next_down = pd.to_numeric(l10["NEXT_DOWN"], errors="coerce").fillna(-1).astype("int64")
        bad = [
            int(identifier)
            for identifier, downstream in zip(ids, next_down, strict=False)
            if downstream != 0 and downstream not in selected and int(identifier) not in declared
        ]
        topology_ok, scope_exits = not bad, len(declared)
    topology = _check(
        "hydro.l10.topology",
        topology_ok,
        "fatal",
        "topology references internal L10 IDs or declared scope exits",
        f"{scope_exits} scope exits",
        "L10 downstream topology",
    )
    exit_warning = _check(
        "hydro.l10.scope_exits",
        scope_exits == 0,
        "warning",
        "0 external downstream scope exits",
        scope_exits,
        "external downstream scope exits are explicitly retained",
    )
    return [hierarchy_check, topology, exit_warning]


def _strict_integral_ids(values: pd.Series) -> set[int] | None:
    """Return IDs only when every value is finite and exactly integral."""
    numeric = pd.to_numeric(values, errors="coerce")
    array = numeric.to_numpy(dtype="float64", na_value=np.nan)
    valid = np.isfinite(array) & np.equal(array, np.floor(array))
    if not valid.all():
        return None
    try:
        return set(numeric.astype("int64"))
    except (TypeError, ValueError, OverflowError):
        return None


def _valid_entity_ids(values: pd.Series) -> set[str] | None:
    """Normalize finite, nonblank relationship keys without accepting null sentinels."""

    def normalized(value: object) -> str | None:
        if value is None or isinstance(value, (bool, np.bool_)):
            return None
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            return None
        if isinstance(value, (float, np.floating)):
            if not np.isfinite(value):
                return None
            return format(float(value), ".17g")
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        text = str(value).strip()
        if not text or text.casefold() in {"<na>", "nan", "none", "null"}:
            return None
        return text

    result = [normalized(value) for value in values]
    if any(value is None for value in result):
        return None
    return {value for value in result if value is not None}


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


def _raster_checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    hydro, _ = _geometry(paths, "hydrological")
    core, _ = _geometry(paths, "core")
    rasters: dict[str, tuple[Path | None, gpd.GeoDataFrame | None]] = {
        "dem": (_first([paths.harmonized / "rasters" / "dem_glo30.tif"]), hydro),
        "worldcover": (_first([paths.harmonized / "rasters" / "worldcover_2021.tif"]), hydro),
        "worldpop": (_first([paths.harmonized / "rasters" / "worldpop_2025.tif"]), core),
    }
    checks: list[CheckResult] = []
    for name, (path, aoi) in rasters.items():
        if path is None or aoi is None or aoi.empty:
            checks.append(
                _check(
                    f"raster.{name}.coverage",
                    False,
                    "fatal",
                    f">= {config.environmental_raster_coverage_min_pct}% valid coverage of {'Core' if name == 'worldpop' else 'Hydrological'} AOI",
                    "missing",
                    "required raster or AOI is missing",
                )
            )
            continue
        try:
            ratio = raster_coverage_ratio(path, aoi.geometry.union_all())
            checks.append(
                _check(
                    f"raster.{name}.coverage",
                    ratio * 100 >= config.environmental_raster_coverage_min_pct,
                    "fatal",
                    f">= {config.environmental_raster_coverage_min_pct}% valid coverage of {'Core' if name == 'worldpop' else 'Hydrological'} AOI",
                    f"{ratio * 100:.6f}%",
                    "native-grid valid-pixel AOI coverage",
                )
            )
            checks.append(
                _check(
                    f"raster.{name}.nodata",
                    ratio >= 1.0,
                    "warning",
                    "0 source nodata pixels in AOI",
                    f"{(1 - ratio) * 100:.6f}%",
                    "source nodata is retained as coverage evidence",
                )
            )
        except Exception as error:  # noqa: BLE001
            checks.append(
                _check(
                    f"raster.{name}.coverage",
                    False,
                    "fatal",
                    f">= {config.environmental_raster_coverage_min_pct}% valid coverage",
                    type(error).__name__,
                    "raster coverage could not be evaluated",
                )
            )
    from flashflood_data.derive.features import load_feature_config

    semantics = load_feature_config()
    for property_id in semantics.soil_properties:
        for depth in semantics.soil_depths:
            for statistic in semantics.soil_statistics:
                path = paths.harmonized / "soilgrids" / property_id / depth / f"{statistic}.tif"
                check_id = f"raster.soilgrids.{property_id}.{depth}.{statistic}.coverage"
                if hydro is None or hydro.empty or not path.is_file():
                    checks.append(
                        _check(
                            check_id,
                            False,
                            "fatal",
                            f">= {config.environmental_raster_coverage_min_pct}% valid coverage of Hydrological AOI",
                            "missing",
                            "required configured SoilGrids product or Hydrological AOI is missing",
                        )
                    )
                    checks.append(
                        _check(
                            check_id.removesuffix(".coverage") + ".nodata",
                            False,
                            "warning",
                            "0 source nodata pixels in AOI",
                            "unavailable",
                            "configured SoilGrids nodata evidence is unavailable",
                        )
                    )
                    continue
                try:
                    ratio = raster_coverage_ratio(path, hydro.geometry.union_all())
                    checks.append(
                        _check(
                            check_id,
                            ratio * 100 >= config.environmental_raster_coverage_min_pct,
                            "fatal",
                            f">= {config.environmental_raster_coverage_min_pct}% valid coverage of Hydrological AOI",
                            f"{ratio * 100:.6f}%",
                            "configured SoilGrids product native-grid valid-pixel coverage",
                        )
                    )
                    checks.append(
                        _check(
                            check_id.removesuffix(".coverage") + ".nodata",
                            ratio >= 1.0,
                            "warning",
                            "0 source nodata pixels in AOI",
                            f"{(1 - ratio) * 100:.6f}%",
                            "configured SoilGrids source nodata is retained as evidence",
                        )
                    )
                except Exception as error:  # noqa: BLE001
                    checks.append(
                        _check(
                            check_id,
                            False,
                            "fatal",
                            f">= {config.environmental_raster_coverage_min_pct}% valid coverage of Hydrological AOI",
                            type(error).__name__,
                            "configured SoilGrids product could not be evaluated",
                        )
                    )
                    checks.append(
                        _check(
                            check_id.removesuffix(".coverage") + ".nodata",
                            False,
                            "warning",
                            "0 source nodata pixels in AOI",
                            type(error).__name__,
                            "configured SoilGrids nodata evidence could not be evaluated",
                        )
                    )
    return checks


def _population_checks(paths: ProjectPaths) -> list[CheckResult]:
    path = _first(
        [
            paths.derived / "mappings" / "map_subbasin_population.parquet",
            paths.derived / "map_subbasin_population.parquet",
        ]
    )
    if path is None:
        return [
            _check(
                "population.scope",
                False,
                "fatal",
                "Core AOI only with pixel/nodata/coverage evidence",
                "missing",
                "population mapping is missing",
            ),
            _check(
                "population.no_double_count",
                False,
                "fatal",
                "each Core pixel assigned to at most one L10",
                "missing",
                "population mapping is missing",
            ),
        ]
    try:
        table = pd.read_parquet(path)
    except Exception:  # noqa: BLE001
        return [
            _check(
                "population.scope",
                False,
                "fatal",
                "Core AOI only with pixel/nodata/coverage evidence",
                "unreadable",
                "population mapping is unreadable",
            ),
            _check(
                "population.no_double_count",
                False,
                "fatal",
                "each Core pixel assigned to at most one L10",
                "unreadable",
                "population mapping is unreadable",
            ),
        ]
    required = {
        "population_scope",
        "contributing_pixel_count",
        "nodata_pixel_count",
        "aoi_pixel_count",
        "coverage_ratio",
    }
    scope = (
        required.issubset(table.columns)
        and not table.empty
        and table["population_scope"].eq("core_aoi_only").all()
    )
    mapped_ids = pd.to_numeric(table.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
    duplicate_ids = mapped_ids.duplicated().any() or mapped_ids.isna().any()
    values = {
        name: pd.to_numeric(table.get(name, pd.Series(dtype="float64")), errors="coerce")
        for name in (
            "contributing_pixel_count",
            "nodata_pixel_count",
            "aoi_pixel_count",
            "coverage_ratio",
        )
    }
    integer_counts = all(
        np.isfinite(values[name]).all()
        and (values[name] >= 0).all()
        and np.equal(values[name], np.floor(values[name])).all()
        for name in ("contributing_pixel_count", "nodata_pixel_count", "aoi_pixel_count")
    )
    counts_ok = (
        integer_counts
        and (values["contributing_pixel_count"] + values["nodata_pixel_count"])
        .eq(values["aoi_pixel_count"])
        .all()
    )
    positive = values["aoi_pixel_count"].gt(0)
    zero_rows = values["aoi_pixel_count"].eq(0)
    ratios_ok = (
        (
            positive
            & np.isclose(
                values["coverage_ratio"],
                values["contributing_pixel_count"] / values["aoi_pixel_count"],
                equal_nan=False,
            )
        )
        | (
            zero_rows
            & values["contributing_pixel_count"].eq(0)
            & values["nodata_pixel_count"].eq(0)
            & values["coverage_ratio"].isna()
        )
    ).all()
    l10_path = paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    try:
        selected_ids = set(
            pd.to_numeric(gpd.read_parquet(l10_path)["HYBAS_ID"], errors="raise").astype("int64")
        )
    except Exception:  # noqa: BLE001
        selected_ids = set()
    membership_ok = bool(selected_ids) and set(mapped_ids.astype("int64")) == selected_ids
    independent_ok = False
    core, _ = _geometry(paths, "core")
    worldpop = paths.harmonized / "rasters" / "worldpop_2025.tif"
    if core is not None and not core.empty and worldpop.is_file():
        try:
            from flashflood_data.derive._spatial import geometry_in_dataset_crs

            with rasterio.open(worldpop) as dataset:
                core_geometry = geometry_in_dataset_crs(
                    core.geometry.union_all(), core.crs.to_string(), dataset.crs
                )
                valid_total = nodata_total = 0
                for _, window in dataset.block_windows(1):
                    inside = geometry_mask(
                        [core_geometry.__geo_interface__],
                        out_shape=(int(window.height), int(window.width)),
                        transform=dataset.window_transform(window),
                        invert=True,
                    )
                    valid = dataset.read_masks(1, window=window) > 0
                    valid_total += int((inside & valid).sum())
                    nodata_total += int((inside & ~valid).sum())
            independent_ok = (
                int(values["contributing_pixel_count"].sum()) == valid_total
                and int(values["nodata_pixel_count"].sum()) == nodata_total
                and int(values["aoi_pixel_count"].sum()) == valid_total + nodata_total
            )
            from flashflood_data.derive.population import aggregate_population_by_basin

            expected = aggregate_population_by_basin(
                worldpop, gpd.read_parquet(l10_path), core.geometry.union_all()
            ).set_index("HYBAS_ID")
            observed = table.assign(HYBAS_ID=mapped_ids.astype("int64")).set_index("HYBAS_ID")
            for basin_id, row in expected.iterrows():
                observed_row = observed.loc[int(basin_id)]
                independent_ok = (
                    independent_ok
                    and int(observed_row["contributing_pixel_count"])
                    == int(row["contributing_pixel_count"])
                    and int(observed_row["nodata_pixel_count"]) == int(row["nodata_pixel_count"])
                    and int(observed_row["aoi_pixel_count"]) == int(row["aoi_pixel_count"])
                )
        except Exception:  # noqa: BLE001 - malformed evidence is a fatal outcome.
            independent_ok = False
    ties = int(
        pd.to_numeric(
            table.get("boundary_center_tie_pixel_count", pd.Series(0, index=table.index)),
            errors="coerce",
        )
        .fillna(0)
        .sum()
    )
    return [
        _check(
            "population.scope",
            bool(scope),
            "fatal",
            "Core AOI only with pixel/nodata/coverage evidence",
            "core_aoi_only" if scope else "invalid",
            "WorldPop aggregation scope and evidence",
        ),
        _check(
            "population.no_double_count",
            not duplicate_ids
            and bool(counts_ok)
            and bool(ratios_ok)
            and membership_ok
            and (not worldpop.is_file() or independent_ok),
            "fatal",
            "each Core pixel assigned to at most one L10",
            "complete selected-L10 accounting"
            if not duplicate_ids
            and counts_ok
            and ratios_ok
            and membership_ok
            and (not worldpop.is_file() or independent_ok)
            else "duplicate, incomplete, or inconsistent pixel accounting",
            "population pixel assignment is exclusive with exact accounting",
        ),
        _check(
            "population.boundary_ties",
            ties == 0,
            "warning",
            "0 boundary-center ties",
            ties,
            "ties are deterministically assigned to the lowest HYBAS_ID",
        ),
    ]


def _event_checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    expected_count = config.historical_event_expected_count
    path = paths.harmonized / "events" / "historical_flood_event_2020_2026.parquet"
    if not path.is_file():
        return [
            _check(
                "events.historical.matching",
                False,
                "fatal",
                f"{expected_count} rows with legal match status and confidence",
                "missing",
                "historical event evidence is missing",
            ),
            _check(
                "events.unresolved_names",
                False,
                "warning",
                "0 unresolved event names",
                "missing",
                "historical event evidence is missing",
            ),
        ]
    try:
        events = pd.read_parquet(path)
        statuses = events.get("match_status", pd.Series(dtype="object")).astype(str)
        confidence = pd.to_numeric(
            events.get("match_confidence", pd.Series(dtype="float64")), errors="coerce"
        )
        valid = (
            len(events) == expected_count
            and statuses.isin(_EVENT_STATUSES).all()
            and confidence.between(0, 1).all()
        )
        unresolved = int((statuses == "unresolved").sum())
        return [
            _check(
                "events.historical.matching",
                valid,
                "fatal",
                f"{expected_count} rows with legal match status and confidence",
                f"{len(events)} rows / {int(statuses.isin(_EVENT_STATUSES).sum())} legal statuses",
                "historical evidence legal-administration replay",
            ),
            _check(
                "events.unresolved_names",
                unresolved == 0,
                "warning",
                "0 unresolved event names",
                unresolved,
                "unresolved event names remain explicit evidence",
            ),
        ]
    except Exception as error:  # noqa: BLE001
        return [
            _check(
                "events.historical.matching",
                False,
                "fatal",
                f"{expected_count} rows with legal match status and confidence",
                type(error).__name__,
                "historical event evidence is unreadable",
            ),
            _check(
                "events.unresolved_names",
                False,
                "warning",
                "0 unresolved event names",
                "unreadable",
                "historical event evidence is unreadable",
            ),
        ]


def _provenance_checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    assets = _asset_records(paths)
    by_id = {asset.asset_id: asset for asset in assets}

    def dependency_ids(asset: AssetRecord) -> set[str] | None:
        try:
            metadata = json.loads(asset.metadata_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(metadata, dict):
            return None
        references: set[str] = set()
        for key in ("source_asset_ids", "dependency_asset_ids", "input_asset_ids"):
            value = metadata.get(key, [])
            if value is None:
                continue
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                return None
            references.update(value)
        return references

    roots = [
        asset
        for asset in assets
        if asset.kind is AssetKind.DERIVED
        and (asset.asset_id.startswith("task15-") or asset.asset_id.startswith("task16-"))
    ]
    unresolved: set[str] = set()
    used_ids: set[str] = set()

    def visit(asset_id: str, visiting: set[str]) -> None:
        if asset_id in visiting:
            unresolved.add(asset_id)
            return
        asset = by_id.get(asset_id)
        if asset is None:
            unresolved.add(asset_id)
            return
        if asset.kind is AssetKind.RAW:
            used_ids.add(asset_id)
            return
        references = dependency_ids(asset)
        if not references:
            unresolved.add(asset_id)
            return
        for reference in sorted(references):
            visit(reference, visiting | {asset_id})

    for root in sorted(roots, key=lambda asset: asset.asset_id):
        visit(root.asset_id, set())
    raw_root = paths.raw.resolve()
    pipeline_raw = [
        asset
        for asset in assets
        if asset.kind is AssetKind.RAW
        and Path(asset.storage_path).resolve().is_relative_to(raw_root)
        and asset.duplicate_of_asset_id is None
    ]
    used = [asset for asset in assets if asset.kind is AssetKind.RAW and asset.asset_id in used_ids]
    required = ("source_uri", "source_version", "license_id", "checksum", "retrieved_at")
    incomplete = [
        asset.asset_id for asset in used if any(not getattr(asset, field) for field in required)
    ]
    raw_bytes = sum(asset.size_bytes for asset in pipeline_raw)
    disk_free = shutil.disk_usage(paths.dataset if paths.dataset.exists() else paths.root).free
    return [
        _check(
            "raw.provenance",
            bool(used) and not incomplete and not unresolved,
            "fatal",
            "every used raw asset has URI/version/license/retrieval/checksum",
            f"{len(used) - len(incomplete)}/{len(used)} used raw assets complete; {len(unresolved)} unresolved dependencies",
            "provenance for raw assets actually used through source-asset references",
            [*incomplete, *sorted(unresolved)],
        ),
        _check(
            "storage.raw_cap",
            raw_bytes <= int(config.new_raw_soft_cap_gib * _GIB),
            "fatal",
            f"pipeline raw bytes <= {config.new_raw_soft_cap_gib:g} GiB",
            raw_bytes,
            "catalogued pipeline-acquired raw bytes",
        ),
        _check(
            "storage.reserve",
            disk_free >= int(config.minimum_free_gib * _GIB),
            "fatal",
            f"disk reserve >= {config.minimum_free_gib:g} GiB",
            disk_free,
            "available filesystem reserve",
        ),
    ]


def run_quality_gates(paths: ProjectPaths, config: StudyAreaConfig) -> QAReport:
    """Evaluate every gate without stopping at the first fatal outcome."""

    def contained(prefix: str, gate: Callable[[], list[CheckResult]]) -> list[CheckResult]:
        try:
            return gate()
        except Exception as error:  # noqa: BLE001 - report publication is fail-safe.
            return [
                _check(
                    f"{prefix}.gate_execution",
                    False,
                    "fatal",
                    "malformed artifacts produce a published fatal result",
                    type(error).__name__,
                    "quality gate could not evaluate its malformed input",
                )
            ]

    checks = [
        *contained("admin", lambda: _admin_checks(paths, config)),
        *contained("hydro", lambda: _hydro_checks(paths)),
        *contained("mapping", lambda: _mapping_checks(paths)),
        *contained("raster", lambda: _raster_checks(paths, config)),
        *contained("population", lambda: _population_checks(paths)),
        *contained("events", lambda: _event_checks(paths, config)),
        *contained("raw", lambda: _provenance_checks(paths, config)),
    ]
    ordered = tuple(sorted(checks, key=lambda check: check.check_id))
    config_fingerprint = _config_fingerprint(config)
    canonical = json.dumps(
        [asdict(check) for check in ordered], sort_keys=True, separators=(",", ":")
    )
    run_id = (
        "qa-" + hashlib.sha256((config_fingerprint + canonical).encode("utf-8")).hexdigest()[:16]
    )
    return QAReport(run_id=run_id, config_fingerprint=config_fingerprint, checks=ordered)


def task17_qa_handler(
    *, owner_source_id: str
) -> Callable[[Any, object, str, Any], list[AssetRecord]]:
    """Return a concrete ``Stage.QA`` handler; Task 19 owns its registration."""

    def handler(pipeline: Any, stage: object, source_id: str, context: Any) -> list[AssetRecord]:
        if str(stage) != "qa":
            raise ValueError("Task 17 handler can only run at the qa stage")
        if source_id != owner_source_id:
            return []
        from flashflood_data.qa.map import publish_qa_map
        from flashflood_data.qa.report import publish_report

        report = run_quality_gates(pipeline.paths, context.study_area)
        outputs = [
            *publish_report(report, pipeline.paths.qa),
            publish_qa_map(pipeline.paths, pipeline.paths.qa),
        ]
        source = pipeline.source_specs[source_id]

        def asset_suffix(path: Path) -> str:
            relative = path.relative_to(pipeline.paths.qa)
            return "-".join((*relative.parent.parts, relative.stem, relative.suffix[1:]))

        records = [
            AssetRecord(
                asset_id=f"task17-qa-{asset_suffix(path)}",
                source_id=source.source_id,
                source_version=source.version,
                kind=AssetKind.QA,
                source_uri="generated:task17-static-qa",
                storage_path=str(path),
                media_type="text/html"
                if path.suffix == ".html"
                else "application/vnd.apache.parquet"
                if path.suffix == ".parquet"
                else "application/json",
                size_bytes=path.stat().st_size,
                checksum=sha256_file(path),
                retrieved_at=datetime.now(UTC),
                license_id=source.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.DERIVED,
            )
            for path in outputs
        ]
        if report.fatal_failures:
            raise QualityGateFailure("fatal quality gates failed after publishing QA artifacts")
        return records

    return handler
