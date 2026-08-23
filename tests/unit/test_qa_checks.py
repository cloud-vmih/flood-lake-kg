"""Contracts for the static QA gate collection."""

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from flashflood_data.catalog import AssetCatalog
from flashflood_data.config import StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.qa import checks
from flashflood_data.qa.checks import run_quality_gates, task17_qa_handler


def _admin(path: Path, count: int) -> None:
    rows = []
    for index in range(count):
        rows.append(
            {
                "current_commune_code": f"{index:05d}",
                "current_commune_name": f"Unit {index}",
                "unit_type": "ward" if index >= 67 else "commune",
                "legal_area_km2": 1_000_000 / 1_000_000,
                "lookup_id": str(index),
                "valid_from": "2025-07-01",
                "raw_asset_id": "raw-admin",
                "geometry": box(index, 0, index + 1, 1),
            }
        )
    layer = gpd.GeoDataFrame(rows, crs="EPSG:3857")
    path.parent.mkdir(parents=True, exist_ok=True)
    layer.to_crs("EPSG:4326").to_parquet(path, index=False)


@pytest.fixture
def qa_paths(tmp_path: Path) -> ProjectPaths:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    _admin(paths.harmonized / "admin" / "admin_commune_2025.geoparquet", 75)
    return paths


def test_admin_count_gate_is_fatal_at_74(qa_paths: ProjectPaths) -> None:
    _admin(qa_paths.harmonized / "admin" / "admin_commune_2025.geoparquet", 74)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    check = report.by_id("admin.current.count")
    assert not check.passed
    assert check.severity == "fatal"
    assert check.expected == "75 total / 67 communes / 8 wards"


def test_gates_keep_running_after_a_fatal_check(qa_paths: ProjectPaths) -> None:
    _admin(qa_paths.harmonized / "admin" / "admin_commune_2025.geoparquet", 74)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert report.fatal_failures
    assert report.by_id("raw.provenance").check_id == "raw.provenance"
    assert tuple(check.check_id for check in report.checks) == tuple(
        sorted(check.check_id for check in report.checks)
    )


def test_population_gate_requires_core_scope_and_no_double_count(qa_paths: ProjectPaths) -> None:
    hydro = qa_paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hydro.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"HYBAS_ID": [1, 2]},
        geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1)],
        crs="EPSG:4326",
    ).to_parquet(hydro, index=False)
    mapping = qa_paths.derived / "mappings" / "map_subbasin_population.parquet"
    mapping.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "HYBAS_ID": [1, 2],
            "population_scope": ["core_aoi_only", "core_aoi_only"],
            "contributing_pixel_count": [3, 4],
            "nodata_pixel_count": [0, 0],
            "aoi_pixel_count": [3, 4],
            "coverage_ratio": [1.0, 1.0],
            "boundary_center_tie_pixel_count": [0, 0],
        }
    ).to_parquet(mapping, index=False)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert report.by_id("population.scope").passed
    assert report.by_id("population.no_double_count").passed


def test_malformed_gate_is_contained_as_a_fatal_result(
    qa_paths: ProjectPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    def malformed(*_args: object) -> list[checks.CheckResult]:
        raise ValueError("fixture malformed administration")

    monkeypatch.setattr(checks, "_admin_checks", malformed)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    contained = report.by_id("admin.gate_execution")
    assert not contained.passed
    assert contained.severity == "fatal"


def test_qa_handler_publishes_before_raising_for_fatal_gates(qa_paths: ProjectPaths) -> None:
    class Pipeline:
        def __init__(self) -> None:
            self.paths = qa_paths
            self.source_specs = {
                "owner": SourceSpec(
                    source_id="owner",
                    adapter="existing",
                    version="1",
                    license_id="CC-BY-4.0",
                )
            }

    class Context:
        study_area = StudyAreaConfig()
        run_id = "qa-fixture"

    with pytest.raises(checks.QualityGateFailure):
        task17_qa_handler(owner_source_id="owner")(Pipeline(), "qa", "owner", Context())

    assert (qa_paths.qa / "report.json").is_file()
    assert (qa_paths.qa / "report.parquet").is_file()
    assert (qa_paths.qa / "report.html").is_file()


def test_soilgrids_gate_enumerates_all_96_configured_products(qa_paths: ProjectPaths) -> None:
    report = run_quality_gates(qa_paths, StudyAreaConfig())

    soil_checks = [
        check
        for check in report.checks
        if check.check_id.startswith("raster.soilgrids.") and check.check_id.endswith(".coverage")
    ]
    assert len(soil_checks) == 96
    assert all(check.severity == "fatal" for check in soil_checks)
    nodata = [
        check
        for check in report.checks
        if check.check_id.startswith("raster.soilgrids.") and check.check_id.endswith(".nodata")
    ]
    assert len(nodata) == 96


def test_mapping_gate_requires_every_approved_product_and_all_communes(
    qa_paths: ProjectPaths,
) -> None:
    hydro = qa_paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hydro.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame({"HYBAS_ID": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326").to_parquet(
        hydro, index=False
    )
    mapping = qa_paths.derived / "mappings" / "map_subbasin_commune.parquet"
    mapping.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {"HYBAS_ID": [1], "current_commune_code": ["00000"], "commune_fraction": [1.0]}
    ).to_parquet(mapping, index=False)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert not report.by_id("mapping.foreign_keys").passed
    assert not report.by_id("mapping.commune_coverage").passed


def test_population_gate_requires_exact_pixel_accounting_and_all_selected_l10(
    qa_paths: ProjectPaths,
) -> None:
    hydro = qa_paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hydro.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"HYBAS_ID": [1, 2]}, geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1)], crs="EPSG:4326"
    ).to_parquet(hydro, index=False)
    mapping = qa_paths.derived / "mappings" / "map_subbasin_population.parquet"
    mapping.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "HYBAS_ID": [1],
            "population_scope": ["core_aoi_only"],
            "contributing_pixel_count": [1],
            "nodata_pixel_count": [1],
            "aoi_pixel_count": [3],
            "coverage_ratio": [1 / 3],
        }
    ).to_parquet(mapping, index=False)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert not report.by_id("population.no_double_count").passed


def test_population_gate_allows_selected_upstream_zero_core_row(qa_paths: ProjectPaths) -> None:
    hydro = qa_paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hydro.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame({"HYBAS_ID": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326").to_parquet(
        hydro, index=False
    )
    mapping = qa_paths.derived / "mappings" / "map_subbasin_population.parquet"
    mapping.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "HYBAS_ID": [1],
            "population_scope": ["core_aoi_only"],
            "contributing_pixel_count": [0],
            "nodata_pixel_count": [0],
            "aoi_pixel_count": [0],
            "coverage_ratio": [float("nan")],
        }
    ).to_parquet(mapping, index=False)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert report.by_id("population.no_double_count").passed


def test_admin_legal_area_evidence_detects_a_spatial_gap_without_core_aoi(
    qa_paths: ProjectPaths,
) -> None:
    admin = qa_paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
    rows = []
    for index in range(75):
        rows.append(
            {
                "current_commune_code": f"{index:05d}",
                "current_commune_name": f"Unit {index}",
                "unit_type": "ward" if index >= 67 else "commune",
                "legal_area_km2": 1.0,
                "geometry": box(index, 0, index + 0.5, 1),
            }
        )
    gpd.GeoDataFrame(rows, crs="EPSG:3857").to_crs("EPSG:4326").to_parquet(admin, index=False)
    reference = qa_paths.harmonized / "admin" / "sonla_reference_boundary.geoparquet"
    gpd.GeoDataFrame(
        {"reference": ["historical_gadm_sonla"]},
        geometry=[box(0, 0, 75, 1)],
        crs="EPSG:3857",
    ).to_crs("EPSG:4326").to_parquet(reference, index=False)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    check = report.by_id("admin.legal_coverage")
    assert not check.passed
    assert "gap=" in check.actual


def test_admin_spatial_coverage_requires_independent_son_la_reference(
    qa_paths: ProjectPaths,
) -> None:
    report = run_quality_gates(qa_paths, StudyAreaConfig())

    check = report.by_id("admin.legal_coverage")
    assert not check.passed
    assert check.actual == "unavailable"


def test_mapping_gate_rejects_null_fk_and_missing_task16_evidence(
    qa_paths: ProjectPaths,
) -> None:
    hydro = qa_paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hydro.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame({"HYBAS_ID": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326").to_parquet(
        hydro, index=False
    )
    mapping = qa_paths.derived / "mappings" / "map_subbasin_population.parquet"
    mapping.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"HYBAS_ID": [None], "population_scope": ["core_aoi_only"]}).to_parquet(
        mapping, index=False
    )

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert not report.by_id("mapping.foreign_keys").passed


def test_mapping_gate_rejects_nonintegral_hybas_ids(qa_paths: ProjectPaths) -> None:
    hydro = qa_paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hydro.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame({"HYBAS_ID": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326").to_parquet(
        hydro, index=False
    )
    mapping = qa_paths.derived / "mappings" / "map_subbasin_population.parquet"
    mapping.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"HYBAS_ID": [1.5], "population_scope": ["core_aoi_only"]}).to_parquet(
        mapping, index=False
    )

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert not report.by_id("mapping.foreign_keys").passed


def test_provenance_traverses_derived_to_harmonized_to_raw(qa_paths: ProjectPaths) -> None:
    catalog = AssetCatalog(qa_paths)

    def record(asset_id: str, kind: AssetKind, dependencies: dict[str, object]) -> AssetRecord:
        return AssetRecord(
            asset_id=asset_id,
            source_id="fixture",
            source_version="1",
            kind=kind,
            source_uri="https://example.test/source",
            storage_path=str(qa_paths.raw / f"{asset_id}.bin"),
            media_type="application/octet-stream",
            size_bytes=1,
            checksum="0" * 64,
            retrieved_at=datetime.now(UTC),
            license_id="CC-BY-4.0",
            pipeline_run_id="fixture",
            status=AssetStatus.DERIVED
            if kind is AssetKind.DERIVED
            else AssetStatus.HARMONIZED
            if kind is AssetKind.HARMONIZED
            else AssetStatus.VALIDATED,
            metadata_json=__import__("json").dumps(dependencies),
        )

    catalog.upsert(record("raw", AssetKind.RAW, {}))
    catalog.upsert(record("harm", AssetKind.HARMONIZED, {"source_asset_ids": ["raw"]}))
    catalog.upsert(record("task16-derived", AssetKind.DERIVED, {"dependency_asset_ids": ["harm"]}))

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert report.by_id("raw.provenance").passed


def test_provenance_marks_an_unresolved_nonraw_dependency_fatal(qa_paths: ProjectPaths) -> None:
    catalog = AssetCatalog(qa_paths)
    catalog.upsert(
        AssetRecord(
            asset_id="task16-derived",
            source_id="fixture",
            source_version="1",
            kind=AssetKind.DERIVED,
            source_uri="generated:fixture",
            storage_path=str(qa_paths.derived / "fixture.parquet"),
            media_type="application/vnd.apache.parquet",
            size_bytes=1,
            checksum="0" * 64,
            retrieved_at=datetime.now(UTC),
            license_id="CC-BY-4.0",
            pipeline_run_id="fixture",
            status=AssetStatus.DERIVED,
            metadata_json='{"dependency_asset_ids":["missing"]}',
        )
    )

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert not report.by_id("raw.provenance").passed
