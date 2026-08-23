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
    core, _ = _geometry(paths, "core")
    if (
        admin.empty
        or admin.crs is None
        or "legal_area_km2" not in admin
        or core is None
        or core.empty
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
        core_metric = core.to_crs(config.processing_crs).geometry.union_all()
        core_area = float(core_metric.area)
        clipped = metric.geometry.intersection(core_metric)
        union_area = float(clipped.union_all().area)
        total_area = float(clipped.area.sum())
        overlap = max(0.0, total_area - union_area) / core_area * 100 if core_area else 100.0
        gap = max(0.0, core_area - union_area) / core_area * 100 if core_area else 100.0
        legal_area = pd.to_numeric(metric["legal_area_km2"], errors="coerce")
        computed = metric.geometry.area / 1_000_000
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
            "Core-area-normalized gaps, overlaps, and legal-area tolerance",
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


def _mapping_checks(paths: ProjectPaths) -> list[CheckResult]:
    basin_path = paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    admin_path = paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
    try:
        basin_ids = set(
            pd.to_numeric(gpd.read_parquet(basin_path)["HYBAS_ID"], errors="raise").astype("int64")
        )
        commune_ids = set(gpd.read_parquet(admin_path)["current_commune_code"].astype(str))
    except Exception:  # noqa: BLE001
        basin_ids, commune_ids = set(), set()
    products = {
        "commune": ("current_commune_code", (admin_path,), {"commune_fraction"}),
        "river": ("HYRIV_ID", (paths.harmonized / "hydro" / "river_reach.geoparquet",), set()),
        "road": (
            "segment_id",
            (
                paths.derived / "exposure" / "road_segment.geoparquet",
                paths.harmonized / "exposure" / "road_segment.geoparquet",
            ),
            set(),
        ),
        "bridge": (
            "bridge_id",
            (
                paths.derived / "exposure" / "bridge.geoparquet",
                paths.harmonized / "exposure" / "bridge.geoparquet",
            ),
            set(),
        ),
        "facility": (
            "facility_id",
            (
                paths.derived / "exposure" / "facility.geoparquet",
                paths.harmonized / "exposure" / "facility.geoparquet",
            ),
            set(),
        ),
        "settlement": (
            "settlement_id",
            (
                paths.derived / "exposure" / "settlement.geoparquet",
                paths.harmonized / "exposure" / "settlement.geoparquet",
            ),
            set(),
        ),
        "population": (
            None,
            (),
            {
                "population_scope",
                "contributing_pixel_count",
                "nodata_pixel_count",
                "aoi_pixel_count",
                "coverage_ratio",
            },
        ),
    }
    foreign_ok, bad, coverage, boundary_cases = True, 0, pd.Series(dtype="float64"), 0
    for name, (entity_key, entity_paths, extras) in products.items():
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
            required = {"HYBAS_ID", *extras} | ({entity_key} if entity_key else set())
            if not required.issubset(table.columns):
                foreign_ok, bad = False, bad + 1
                continue
            invalid = (
                set(pd.to_numeric(table["HYBAS_ID"], errors="coerce").dropna().astype("int64"))
                - basin_ids
            )
            if invalid:
                foreign_ok, bad = False, bad + len(invalid)
            if entity_key:
                entity_path = _first(entity_paths)
                if entity_path is None:
                    foreign_ok, bad = False, bad + 1
                    continue
                entity = gpd.read_parquet(entity_path)
                source_key = (
                    entity_key
                    if entity_key in entity.columns
                    else "osm_id"
                    if "osm_id" in entity.columns
                    else entity_key
                )
                if source_key not in entity:
                    foreign_ok, bad = False, bad + 1
                    continue
                invalid = set(table[entity_key].dropna().astype(str)) - set(
                    entity[source_key].dropna().astype(str)
                )
                if invalid:
                    foreign_ok, bad = False, bad + len(invalid)
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
        "all seven approved mappings have exact schemas and two-sided foreign keys",
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
    ratios_ok = (
        values["aoi_pixel_count"].gt(0)
        & np.isclose(
            values["coverage_ratio"],
            values["contributing_pixel_count"] / values["aoi_pixel_count"],
            equal_nan=False,
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


def _event_checks(paths: ProjectPaths) -> list[CheckResult]:
    path = paths.harmonized / "events" / "historical_flood_event_2020_2026.parquet"
    if not path.is_file():
        return [
            _check(
                "events.historical.matching",
                False,
                "fatal",
                "30 rows with legal match status and confidence",
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
            len(events) == 30
            and statuses.isin(_EVENT_STATUSES).all()
            and confidence.between(0, 1).all()
        )
        unresolved = int((statuses == "unresolved").sum())
        return [
            _check(
                "events.historical.matching",
                valid,
                "fatal",
                "30 rows with legal match status and confidence",
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
                "30 rows with legal match status and confidence",
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
    raw_root = paths.raw.resolve()
    raw = [
        asset
        for asset in assets
        if asset.kind is AssetKind.RAW
        and Path(asset.storage_path).resolve().is_relative_to(raw_root)
        and asset.duplicate_of_asset_id is None
    ]
    referenced: set[str] = set()
    evidence_malformed = False
    for path in sorted(paths.derived.glob("**/*.parquet")):
        try:
            table = pd.read_parquet(path)
            for column in ("source_asset_ids_json", "feature_group_source_asset_ids_json"):
                if column in table:
                    for value in table[column].dropna():
                        decoded = json.loads(str(value))
                        if isinstance(decoded, list):
                            referenced.update(str(item) for item in decoded)
                        elif isinstance(decoded, dict):
                            referenced.update(
                                str(item) for values in decoded.values() for item in values
                            )
        except Exception:  # noqa: BLE001 - malformed evidence is fatal provenance evidence.
            evidence_malformed = True
    used = [asset for asset in raw if not referenced or asset.asset_id in referenced]
    required = ("source_uri", "source_version", "license_id", "checksum", "retrieved_at")
    incomplete = [
        asset.asset_id for asset in used if any(not getattr(asset, field) for field in required)
    ]
    raw_bytes = sum(asset.size_bytes for asset in raw)
    disk_free = shutil.disk_usage(paths.dataset if paths.dataset.exists() else paths.root).free
    return [
        _check(
            "raw.provenance",
            bool(used) and not incomplete and not evidence_malformed,
            "fatal",
            "every used raw asset has URI/version/license/retrieval/checksum",
            f"{len(used) - len(incomplete)}/{len(used)} used pipeline raw assets complete",
            "provenance for raw assets actually used through source-asset references",
            [*incomplete],
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
        *contained("events", lambda: _event_checks(paths)),
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
        records = [
            AssetRecord(
                asset_id=f"task17-qa-{path.stem}",
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
