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
import pandas as pd

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
    if admin.empty or admin.crs is None or "legal_area_km2" not in admin:
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
        union_area = float(metric.geometry.union_all().area)
        total_area = float(metric.geometry.area.sum())
        overlap = max(0.0, total_area - union_area) / union_area * 100 if union_area else 100.0
        legal_area = pd.to_numeric(metric["legal_area_km2"], errors="coerce")
        computed = metric.geometry.area / 1_000_000
        difference = ((computed - legal_area).abs() / legal_area * 100).fillna(float("inf"))
        exceptions = {str(value).zfill(5) for value in config.admin_area_exceptions}
        outside = difference[~metric["current_commune_code"].astype(str).isin(exceptions)]
        max_difference = float(outside.max()) if not outside.empty else 0.0
        legal = _check(
            "admin.legal_coverage",
            overlap <= config.admin_gap_overlap_max_pct
            and max_difference <= config.legal_area_diff_max_pct,
            "fatal",
            f"gap/overlap <= {config.admin_gap_overlap_max_pct}%; legal area difference <= {config.legal_area_diff_max_pct}%",
            f"overlap={overlap:.6f}%, legal_difference={max_difference:.6f}%",
            "dissolved coverage and legal-area tolerance",
        )
    return [count, geometry, legal, repair]


def _hydro_checks(paths: ProjectPaths) -> list[CheckResult]:
    l10_path = paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hierarchy_path = paths.harmonized / "hydro" / "subbasin_hierarchy.parquet"
    try:
        l10 = gpd.read_parquet(l10_path)
        hierarchy = pd.read_parquet(hierarchy_path)
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
    parents_ok = (
        parent_fields.issubset(hierarchy.columns)
        and not hierarchy[["parent_l9_hybas_id", "parent_l8_hybas_id"]].isna().any().any()
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
    mapping_dir = paths.derived / "mappings"
    candidates = sorted(mapping_dir.glob("map_subbasin_*.parquet")) if mapping_dir.is_dir() else []
    if not candidates:
        candidates = (
            sorted((paths.derived / "mappings").glob("*.parquet"))
            if (paths.derived / "mappings").is_dir()
            else []
        )
    foreign_ok, bad, coverage = True, 0, pd.Series(dtype="float64")
    for path in candidates:
        try:
            table = pd.read_parquet(path)
        except Exception:  # noqa: BLE001
            foreign_ok, bad = False, bad + 1
            continue
        if "HYBAS_ID" in table:
            invalid = (
                set(pd.to_numeric(table["HYBAS_ID"], errors="coerce").dropna().astype("int64"))
                - basin_ids
            )
            bad += len(invalid)
            foreign_ok = foreign_ok and not invalid
        if "current_commune_code" in table:
            invalid = set(table["current_commune_code"].dropna().astype(str)) - commune_ids
            bad += len(invalid)
            foreign_ok = foreign_ok and not invalid
            if "commune_fraction" in table:
                coverage = pd.to_numeric(
                    table.groupby("current_commune_code")["commune_fraction"].sum(), errors="coerce"
                )
    fk = _check(
        "mapping.foreign_keys",
        bool(candidates) and foreign_ok,
        "fatal",
        "every mapping foreign key exists",
        f"{bad} invalid keys across {len(candidates)} tables",
        "mapping keys refer to selected L10 basins and current communes",
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
        percentages = coverage * 100
        passed = percentages.between(99.5, 100.5).all()
        coverage_check = _check(
            "mapping.commune_coverage",
            bool(passed),
            "fatal",
            "99.5–100.5% commune coverage by L10",
            f"{percentages.min():.6f}–{percentages.max():.6f}%",
            "commune fractions summed by current commune",
        )
    return [fk, coverage_check]


def _raster_checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    hydro, _ = _geometry(paths, "hydrological")
    core, _ = _geometry(paths, "core")
    rasters: dict[str, tuple[Path | None, gpd.GeoDataFrame | None]] = {
        "dem": (_first([paths.harmonized / "rasters" / "dem_glo30.tif"]), hydro),
        "soilgrids": (_first(sorted((paths.harmonized / "soilgrids").glob("**/*.tif"))), hydro),
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
    duplicate_ids = table.get("HYBAS_ID", pd.Series(dtype="object")).duplicated().any()
    counts_ok = (
        (
            pd.to_numeric(
                table.get("contributing_pixel_count", pd.Series(dtype="float64")), errors="coerce"
            )
            + pd.to_numeric(
                table.get("nodata_pixel_count", pd.Series(dtype="float64")), errors="coerce"
            )
            <= pd.to_numeric(
                table.get("aoi_pixel_count", pd.Series(dtype="float64")), errors="coerce"
            )
        )
        .fillna(False)
        .all()
    )
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
            not duplicate_ids and bool(counts_ok),
            "fatal",
            "each Core pixel assigned to at most one L10",
            "unique L10 assignments"
            if not duplicate_ids and counts_ok
            else "duplicate or inconsistent pixel counts",
            "population pixel assignment is exclusive",
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
    raw = [asset for asset in assets if asset.kind is AssetKind.RAW]
    required = ("source_uri", "source_version", "license_id", "checksum", "retrieved_at")
    incomplete = [
        asset.asset_id for asset in raw if any(not getattr(asset, field) for field in required)
    ]
    raw_bytes = sum(asset.size_bytes for asset in raw)
    disk_free = shutil.disk_usage(paths.dataset if paths.dataset.exists() else paths.root).free
    return [
        _check(
            "raw.provenance",
            bool(raw) and not incomplete,
            "fatal",
            "every used raw asset has URI/version/license/retrieval/checksum",
            f"{len(raw) - len(incomplete)}/{len(raw)} complete",
            "raw catalog provenance completeness",
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
    checks = [
        *_admin_checks(paths, config),
        *_hydro_checks(paths),
        *_mapping_checks(paths),
        *_raster_checks(paths, config),
        *_population_checks(paths),
        *_event_checks(paths),
        *_provenance_checks(paths, config),
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
