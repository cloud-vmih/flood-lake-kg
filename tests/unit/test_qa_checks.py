"""Contracts for the static QA gate collection."""

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import LineString, Point, box

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


def _write_valid_mapping_contract(paths: ProjectPaths) -> dict[str, Path]:
    """Publish the literal Task 16 schemas against the real producer identity columns."""
    hydro = paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hydro.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame({"HYBAS_ID": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326").to_parquet(
        hydro, index=False
    )
    exposure = paths.derived / "exposure"
    exposure.mkdir(parents=True, exist_ok=True)
    sources = {
        "river": (
            paths.harmonized / "hydro" / "river_reach.geoparquet",
            {"HYRIV_ID": ["river-1"]},
            [LineString([(0, 0.5), (1, 0.5)])],
        ),
        "road": (
            exposure / "road_segment.geoparquet",
            {"segment_id": ["way/1:000"], "osm_id": ["way/1"]},
            [LineString([(0, 0.25), (1, 0.25)])],
        ),
        "bridge": (
            exposure / "bridge.geoparquet",
            {"osm_id": ["way/2"]},
            [LineString([(0, 0.75), (1, 0.75)])],
        ),
        "facility": (
            exposure / "facility.geoparquet",
            {"osm_id": ["node/1"]},
            [Point(0.25, 0.25)],
        ),
        "settlement": (
            exposure / "settlement.geoparquet",
            {"osm_id": ["node/2"]},
            [Point(0.75, 0.75)],
        ),
    }
    for source, columns, geometry in sources.values():
        source.parent.mkdir(parents=True, exist_ok=True)
        gpd.GeoDataFrame(columns, geometry=geometry, crs="EPSG:4326").to_parquet(
            source, index=False
        )
    mapping_dir = paths.derived / "mappings"
    mapping_dir.mkdir(parents=True, exist_ok=True)
    provenance = {
        "quality_flags_json": ["{}"],
        "processing_crs": ["EPSG:32648"],
        "source_asset_ids_json": ['["raw-source"]'],
    }
    commune_codes = [f"{index:05d}" for index in range(75)]
    tables = {
        "commune": pd.DataFrame(
            {
                "HYBAS_ID": [1] * 75,
                "current_commune_code": commune_codes,
                "intersection_area_km2": [1.0] * 75,
                "basin_fraction": [1 / 75] * 75,
                "commune_fraction": [1.0] * 75,
                "quality_flags_json": ["{}"] * 75,
                "processing_crs": ["EPSG:32648"] * 75,
                "source_asset_ids_json": ['["raw-admin"]'] * 75,
            }
        ),
        "river": pd.DataFrame(
            {
                "HYBAS_ID": [1],
                "HYRIV_ID": ["river-1"],
                "intersected_length_km": [1.0],
                "boundary_case": [False],
                **provenance,
            }
        ),
        "road": pd.DataFrame(
            {
                "HYBAS_ID": [1],
                "segment_id": ["way/1:000"],
                "osm_id": ["way/1"],
                "intersected_length_km": [1.0],
                "boundary_case": [False],
                "producer_version": ["fixture-extra-column"],
                **provenance,
            }
        ),
        "bridge": pd.DataFrame(
            {
                "HYBAS_ID": [1],
                "osm_id": ["way/2"],
                "intersected_length_km": [1.0],
                "relationship_geometry_wkt": ["LINESTRING (0 0.75, 1 0.75)"],
                "boundary_case": [False],
                **provenance,
            }
        ),
        "facility": pd.DataFrame(
            {
                "HYBAS_ID": [1],
                "osm_id": ["node/1"],
                "relationship_type": ["within"],
                "tags_json": ["{}"],
                "boundary_case": [False],
                **provenance,
            }
        ),
        "settlement": pd.DataFrame(
            {
                "HYBAS_ID": [1],
                "osm_id": ["node/2"],
                "relationship_type": ["within"],
                "tags_json": ["{}"],
                "boundary_case": [False],
                **provenance,
            }
        ),
        "population": pd.DataFrame(
            {
                "HYBAS_ID": [1],
                "population_sum": [30.0],
                "population_mean": [15.0],
                "contributing_pixel_count": [2],
                "nodata_pixel_count": [0],
                "aoi_pixel_count": [2],
                "coverage_ratio": [1.0],
                "source_resolution_x": [0.01],
                "source_resolution_y": [0.01],
                "source_resolution_unit": ["degree"],
                "source_crs": ["EPSG:4326"],
                "population_scope": ["core_aoi_only"],
                "boundary_center_tie_pixel_count": [0],
                "quality_flags_json": ["{}"],
                "source_asset_ids_json": ['["raw-worldpop"]'],
            }
        ),
    }
    outputs: dict[str, Path] = {}
    for name, table in tables.items():
        output = mapping_dir / f"map_subbasin_{name}.parquet"
        table.to_parquet(output, index=False)
        outputs[name] = output
    return outputs


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
            self.catalog = AssetCatalog(qa_paths)
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
    assert len(
        [
            asset
            for asset in AssetCatalog(qa_paths)._read_assets()
            if asset.asset_id.startswith("task17-qa-")
        ]
    ) == 15


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


def _write_soilgrids_coverage_fixture(paths: ProjectPaths, values: list[list[float]]) -> None:
    aoi = paths.harmonized / "aoi" / "hydrological_aoi.geoparquet"
    aoi.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"scope": ["hydrological"]}, geometry=[box(0, 0, 2, 2)], crs="EPSG:32648"
    ).to_parquet(aoi, index=False)
    raster = paths.harmonized / "soilgrids" / "clay" / "0-5cm" / "mean.tif"
    raster.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        raster,
        "w",
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:32648",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    ) as destination:
        destination.write(np.asarray(values, dtype="float32"), 1)


def test_soilgrids_nodata_warning_passes_for_a_readable_fully_valid_product(
    qa_paths: ProjectPaths,
) -> None:
    """A real raster with four valid AOI cells must report 100%, not unavailable."""
    _write_soilgrids_coverage_fixture(qa_paths, [[1, 2], [3, 4]])

    results = {
        check.check_id: check for check in checks._raster_checks(qa_paths, StudyAreaConfig())
    }

    coverage = results["raster.soilgrids.clay.0-5cm.mean.coverage"]
    nodata = results["raster.soilgrids.clay.0-5cm.mean.nodata"]
    assert coverage.passed
    assert coverage.actual == "100.000000%"
    assert nodata.passed
    assert nodata.actual == "0.000000%"


def test_soilgrids_nodata_warning_reports_a_readable_partial_nodata_ratio(
    qa_paths: ProjectPaths,
) -> None:
    """One nodata cell in a four-cell AOI must remain explicit 25% warning evidence."""
    _write_soilgrids_coverage_fixture(qa_paths, [[1, -9999], [3, 4]])

    results = {
        check.check_id: check for check in checks._raster_checks(qa_paths, StudyAreaConfig())
    }

    coverage = results["raster.soilgrids.clay.0-5cm.mean.coverage"]
    nodata = results["raster.soilgrids.clay.0-5cm.mean.nodata"]
    assert not coverage.passed
    assert coverage.actual == "75.000000%"
    assert not nodata.passed
    assert nodata.severity == "warning"
    assert nodata.actual == "25.000000%"


def test_worldpop_coverage_uses_full_footprint_while_nodata_remains_warning(
    qa_paths: ProjectPaths,
) -> None:
    core = qa_paths.harmonized / "aoi" / "core_aoi.geoparquet"
    core.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"scope": ["core"]}, geometry=[box(0, 0, 2, 2)], crs="EPSG:32648"
    ).to_parquet(core, index=False)
    raster = qa_paths.harmonized / "rasters" / "worldpop_2025.tif"
    raster.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        raster,
        "w",
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:32648",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    ) as destination:
        destination.write(np.asarray([[1, -9999], [3, 4]], dtype="float32"), 1)

    results = {
        check.check_id: check for check in checks._raster_checks(qa_paths, StudyAreaConfig())
    }

    assert results["raster.worldpop.coverage"].passed
    assert results["raster.worldpop.coverage"].actual == "100.000000%"
    assert not results["raster.worldpop.nodata"].passed
    assert results["raster.worldpop.nodata"].actual == "25.000000%"


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


def test_mapping_contract_accepts_actual_osm_source_ids_and_extra_provenance_columns(
    qa_paths: ProjectPaths,
) -> None:
    """Requiring synthetic point IDs or exact column equality rejects Task 16 producer output."""
    _write_valid_mapping_contract(qa_paths)

    results = {check.check_id: check for check in checks._mapping_checks(qa_paths)}

    assert results["mapping.foreign_keys"].passed
    assert results["mapping.commune_coverage"].passed


@pytest.mark.parametrize(
    ("product", "required_column"),
    [("road", "osm_id"), ("road", "boundary_case"), ("bridge", "osm_id")],
)
def test_mapping_contract_rejects_missing_task16_producer_evidence(
    qa_paths: ProjectPaths, product: str, required_column: str
) -> None:
    """Dropping a required Task 16 field must fail even when every other product is valid."""
    outputs = _write_valid_mapping_contract(qa_paths)
    table = pd.read_parquet(outputs[product]).drop(columns=required_column)
    table.to_parquet(outputs[product], index=False)

    result = next(
        check
        for check in checks._mapping_checks(qa_paths)
        if check.check_id == "mapping.foreign_keys"
    )

    assert not result.passed


def test_mapping_contract_rejects_near_integral_hybas_id_before_cast(
    qa_paths: ProjectPaths,
) -> None:
    """Tolerance-based comparison must not turn 1.000000001 into selected basin 1."""
    outputs = _write_valid_mapping_contract(qa_paths)
    table = pd.read_parquet(outputs["population"])
    table["HYBAS_ID"] = table["HYBAS_ID"].astype("float64")
    table.loc[0, "HYBAS_ID"] = 1.000000001
    table.to_parquet(outputs["population"], index=False)

    result = next(
        check
        for check in checks._mapping_checks(qa_paths)
        if check.check_id == "mapping.foreign_keys"
    )

    assert not result.passed


def test_mapping_contract_rejects_nonfinite_required_entity_id(
    qa_paths: ProjectPaths,
) -> None:
    """A nonfinite OSM identity must not pass merely because the segment key is valid."""
    outputs = _write_valid_mapping_contract(qa_paths)
    mapping = pd.read_parquet(outputs["road"])
    mapping.loc[0, "osm_id"] = float("inf")
    mapping.to_parquet(outputs["road"], index=False)
    source_path = qa_paths.derived / "exposure" / "road_segment.geoparquet"
    source = gpd.read_parquet(source_path)
    source.loc[0, "osm_id"] = float("inf")
    source.to_parquet(source_path, index=False)

    result = next(
        check
        for check in checks._mapping_checks(qa_paths)
        if check.check_id == "mapping.foreign_keys"
    )

    assert not result.passed


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


def test_admin_coverage_normalizes_small_outer_boundary_source_offsets(
    qa_paths: ProjectPaths,
) -> None:
    admin = qa_paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
    rows = [
        {
            "current_commune_code": f"{index:05d}",
            "unit_type": "ward" if index >= 67 else "commune",
            "legal_area_km2": 1.0,
            "geometry": box(500_000 + index * 1_000, 2_200_000, 501_000 + index * 1_000, 2_201_000),
        }
        for index in range(75)
    ]
    gpd.GeoDataFrame(rows, crs="EPSG:32648").to_crs("EPSG:4326").to_parquet(admin, index=False)
    reference = qa_paths.harmonized / "admin" / "sonla_reference_boundary.geoparquet"
    gpd.GeoDataFrame(
        {"reference": ["independent_offset_boundary"]},
        geometry=[box(500_500, 2_200_000, 575_500, 2_201_000)],
        crs="EPSG:32648",
    ).to_crs("EPSG:4326").to_parquet(reference, index=False)

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert report.by_id("admin.legal_coverage").passed


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


def test_provenance_validates_direct_legacy_raw_without_counting_it_toward_pipeline_cap(
    qa_paths: ProjectPaths,
) -> None:
    """Storage location affects the 8 GiB budget, never the authority of a referenced raw ID."""
    catalog = AssetCatalog(qa_paths)
    catalog.upsert(
        AssetRecord(
            asset_id="legacy-raw",
            source_id="fixture",
            source_version="2026",
            kind=AssetKind.RAW,
            source_uri="https://example.test/legacy",
            storage_path=str(qa_paths.root / "legacy-inventory" / "source.bin"),
            media_type="application/octet-stream",
            size_bytes=9 * 2**30,
            checksum="1" * 64,
            retrieved_at=datetime.now(UTC),
            license_id="CC-BY-4.0",
            pipeline_run_id="fixture",
            status=AssetStatus.VALIDATED,
            metadata_json='{"dependency_asset_ids":["missing-must-not-be-followed"]}',
        )
    )
    catalog.upsert(
        AssetRecord(
            asset_id="task15-terrain-features",
            source_id="fixture",
            source_version="1",
            kind=AssetKind.DERIVED,
            source_uri="generated:fixture",
            storage_path=str(qa_paths.derived / "terrain.parquet"),
            media_type="application/vnd.apache.parquet",
            size_bytes=1,
            checksum="2" * 64,
            retrieved_at=datetime.now(UTC),
            license_id="CC-BY-4.0",
            pipeline_run_id="fixture",
            status=AssetStatus.DERIVED,
            metadata_json='{"dependency_asset_ids":["legacy-raw"]}',
        )
    )

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert report.by_id("raw.provenance").passed
    assert report.by_id("storage.raw_cap").passed
    catalog.upsert(catalog.get("legacy-raw").model_copy(update={"checksum": ""}))

    incomplete_report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert not incomplete_report.by_id("raw.provenance").passed
    assert incomplete_report.by_id("raw.provenance").asset_ids == ("legacy-raw",)


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


def test_provenance_marks_an_explicit_intermediate_cycle_fatal(qa_paths: ProjectPaths) -> None:
    """Recursive lineage must terminate with raw records, never accept a derived cycle."""
    catalog = AssetCatalog(qa_paths)

    def derived(asset_id: str, dependency: str) -> AssetRecord:
        return AssetRecord(
            asset_id=asset_id,
            source_id="fixture",
            source_version="1",
            kind=AssetKind.DERIVED,
            source_uri="generated:fixture",
            storage_path=str(qa_paths.derived / f"{asset_id}.parquet"),
            media_type="application/vnd.apache.parquet",
            size_bytes=1,
            checksum="0" * 64,
            retrieved_at=datetime.now(UTC),
            license_id="CC-BY-4.0",
            pipeline_run_id="fixture",
            status=AssetStatus.DERIVED,
            metadata_json=__import__("json").dumps({"dependency_asset_ids": [dependency]}),
        )

    catalog.upsert(derived("task16-cycle-root", "cycle-a"))
    catalog.upsert(derived("cycle-a", "cycle-b"))
    catalog.upsert(derived("cycle-b", "cycle-a"))

    report = run_quality_gates(qa_paths, StudyAreaConfig())

    assert not report.by_id("raw.provenance").passed
    assert "cycle-a" in report.by_id("raw.provenance").asset_ids
