"""Contracts for the static QA gate collection."""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from flashflood_data.config import StudyAreaConfig
from flashflood_data.models import SourceSpec
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
        check for check in report.checks if check.check_id.startswith("raster.soilgrids.")
    ]
    assert len(soil_checks) == 96
    assert all(check.severity == "fatal" for check in soil_checks)


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
