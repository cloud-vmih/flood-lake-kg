"""The final static predictor profile preserves the selected L10 population."""

from __future__ import annotations

from datetime import UTC, datetime

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
from flashflood_data.qa.checks import run_quality_gates
from flashflood_data.static.features.profile import assemble_static_profile
from flashflood_data.static.mappings.builder import Task16MapInputs, task16_map_handler


def _basins() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"HYBAS_ID": [2, 1], "SUB_AREA": [20.0, 10.0], "UP_AREA": [30.0, 11.0]},
        geometry=[box(105, 20, 106, 21), box(104, 20, 105, 21)],
        crs="EPSG:4326",
    )


def _required_features() -> dict[str, pd.DataFrame]:
    return {
        name: pd.DataFrame({"HYBAS_ID": [1, 2], f"{name}_value": [1.0, 2.0]})
        for name in ("terrain", "soil", "landcover", "hydrology")
    }


def test_profile_has_exactly_one_row_per_selected_l10_and_keeps_optional_nulls() -> None:
    """An inner join to optional evidence would silently remove an otherwise selected L10."""
    features = _required_features()
    features["population"] = pd.DataFrame({"HYBAS_ID": [1], "population_sum": [40.0]})

    profile = assemble_static_profile(_basins(), features, "run-1")

    assert profile.HYBAS_ID.tolist() == [1, 2]
    assert profile.HYBAS_ID.is_unique
    assert len(profile) == 2
    assert profile.crs.to_epsg() == 4326
    assert profile.loc[profile.HYBAS_ID == 2, "population_sum"].isna().all()
    assert profile.pipeline_run_id.tolist() == ["run-1", "run-1"]
    assert set(profile) >= {
        "dependency_fingerprint",
        "feature_group_source_asset_ids_json",
        "quality_flags_json",
    }


def test_profile_rejects_duplicate_feature_keys_before_join() -> None:
    """Permitting duplicate Task 15 keys would violate the one-row-per-L10 output contract."""
    features = _required_features()
    features["terrain"] = pd.DataFrame({"HYBAS_ID": [1, 1], "terrain_value": [1.0, 2.0]})

    with pytest.raises(ValueError, match="duplicate HYBAS_ID"):
        assemble_static_profile(_basins(), features, "run-1")


def test_profile_rejects_near_integral_feature_key_before_cast() -> None:
    """A fractional predictor key must not be coerced onto an existing selected basin."""
    features = _required_features()
    features["terrain"]["HYBAS_ID"] = [1.000000001, 2.0]

    with pytest.raises(ValueError, match="exact integers"):
        assemble_static_profile(_basins(), features, "run-1")


def test_profile_rejects_a_missing_required_task15_basin_key() -> None:
    """A partial required predictor table must be a QA failure instead of a dropped basin."""
    features = _required_features()
    features["soil"] = pd.DataFrame({"HYBAS_ID": [1], "soil_value": [1.0]})

    with pytest.raises(ValueError, match="missing required basin keys"):
        assemble_static_profile(_basins(), features, "run-1")


def test_profile_rejects_event_evidence_group_to_prevent_static_label_leakage() -> None:
    """Joining historical event labels would turn a static predictor into target leakage."""
    features = _required_features()
    features["events"] = pd.DataFrame({"HYBAS_ID": [1, 2], "event_type": ["flood", "flood"]})

    with pytest.raises(ValueError, match="not an allowed static profile group"):
        assemble_static_profile(_basins(), features, "run-1")


def test_profile_fingerprint_tracks_content_and_geometry_but_not_row_order() -> None:
    """A schema-only fingerprint would incorrectly reuse a profile after data changed."""
    features = _required_features()
    baseline = assemble_static_profile(_basins(), features, "run-1").dependency_fingerprint.iloc[0]

    reordered = assemble_static_profile(
        _basins().iloc[::-1], {name: table.iloc[::-1] for name, table in features.items()}, "run-1"
    ).dependency_fingerprint.iloc[0]
    changed_values = _required_features()
    changed_values["terrain"].loc[0, "terrain_value"] = 99.0
    changed_value_fingerprint = assemble_static_profile(
        _basins(), changed_values, "run-1"
    ).dependency_fingerprint.iloc[0]
    changed_geometry = _basins()
    changed_geometry.loc[changed_geometry.HYBAS_ID == 1, "geometry"] = box(104, 20, 105.5, 21)
    changed_geometry_fingerprint = assemble_static_profile(
        changed_geometry, _required_features(), "run-1"
    ).dependency_fingerprint.iloc[0]

    assert reordered == baseline
    assert changed_value_fingerprint != baseline
    assert changed_geometry_fingerprint != baseline


def test_task16_map_handler_publishes_all_relationship_tables_and_profile(tmp_path) -> None:
    """Moving these products to a separate PROFILE stage would break the static stage order."""
    worldpop = tmp_path / "worldpop.tif"
    with rasterio.open(
        worldpop,
        "w",
        driver="GTiff",
        width=2,
        height=1,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(104, 21, 1, 1),
    ) as destination:
        destination.write(np.asarray([[10, 20]], dtype="float32"), 1)
    inputs = Task16MapInputs(
        basins=_basins(),
        core=box(104, 20, 106, 21),
        worldpop=worldpop,
        feature_tables=_required_features(),
        communes=gpd.GeoDataFrame(
            {"current_commune_code": ["14001"]},
            geometry=[box(104, 20, 106, 21)],
            crs="EPSG:4326",
        ),
        rivers=gpd.GeoDataFrame(
            {"HYRIV_ID": [7001]},
            geometry=[LineString([(104, 20.5), (106, 20.5)])],
            crs="EPSG:4326",
        ),
        roads=gpd.GeoDataFrame(
            {"segment_id": ["way/1:000"], "osm_id": ["way/1"]},
            geometry=[LineString([(104, 20.25), (106, 20.25)])],
            crs="EPSG:4326",
        ),
        bridges=gpd.GeoDataFrame(
            {"osm_id": ["way/2"]},
            geometry=[LineString([(104, 20.75), (106, 20.75)])],
            crs="EPSG:4326",
        ),
        facilities=gpd.GeoDataFrame(
            {"osm_id": ["node/1"]}, geometry=[Point(104.5, 20.5)], crs="EPSG:4326"
        ),
        settlements=gpd.GeoDataFrame(
            {"osm_id": ["node/2"]}, geometry=[Point(105.5, 20.5)], crs="EPSG:4326"
        ),
        source_asset_ids={
            "terrain": ("raw-basins", "raw-dem"),
            "soil": ("raw-basins", "raw-soilgrids"),
            "landcover": ("raw-basins", "raw-worldcover"),
            "hydrology": ("raw-basinatlas", "raw-basins", "raw-dem", "raw-rivers"),
            "commune": ("raw-admin", "raw-basins"),
            "river": ("raw-basins", "raw-rivers"),
            "road": ("raw-basins", "raw-osm"),
            "bridge": ("raw-basins", "raw-osm"),
            "facility": ("raw-basins", "raw-osm"),
            "settlement": ("raw-basins", "raw-osm"),
            "population": ("raw-basins", "raw-worldpop"),
        },
    )
    handler = task16_map_handler(inputs, tmp_path / "derived", owner_source_id="worldpop")
    pipeline = type(
        "Pipeline",
        (),
        {
            "source_specs": {
                "worldpop": SourceSpec(
                    source_id="worldpop", adapter="existing", version="1", license_id="x"
                )
            }
        },
    )()
    context = type("Context", (), {"run_id": "map-test"})()

    assert handler(pipeline, "map", "other", context) == []
    records = handler(pipeline, "map", "worldpop", context)

    assert {record.asset_id for record in records} == {
        "task16-map-subbasin-commune",
        "task16-map-subbasin-river",
        "task16-map-subbasin-road",
        "task16-map-subbasin-bridge",
        "task16-map-subbasin-facility",
        "task16-map-subbasin-settlement",
        "task16-map-subbasin-population",
        "task16-subbasin-static-feature",
    }
    assert (tmp_path / "derived" / "subbasin_static_feature.geoparquet").is_file()
    metadata = {
        record.asset_id: __import__("json").loads(record.metadata_json) for record in records
    }
    expected_mapping_dependencies = {
        "task16-map-subbasin-commune": ["raw-admin", "raw-basins"],
        "task16-map-subbasin-river": ["raw-basins", "raw-rivers"],
        "task16-map-subbasin-road": ["raw-basins", "raw-osm"],
        "task16-map-subbasin-bridge": ["raw-basins", "raw-osm"],
        "task16-map-subbasin-facility": ["raw-basins", "raw-osm"],
        "task16-map-subbasin-settlement": ["raw-basins", "raw-osm"],
        "task16-map-subbasin-population": ["raw-basins", "raw-worldpop"],
    }
    assert {
        asset_id: metadata[asset_id]["dependency_asset_ids"]
        for asset_id in expected_mapping_dependencies
    } == expected_mapping_dependencies
    assert metadata["task16-subbasin-static-feature"]["dependency_asset_ids"] == [
        "raw-admin",
        "raw-basinatlas",
        "raw-basins",
        "raw-dem",
        "raw-osm",
        "raw-rivers",
        "raw-soilgrids",
        "raw-worldcover",
        "raw-worldpop",
    ]
    profile = gpd.read_parquet(tmp_path / "derived" / "subbasin_static_feature.geoparquet")
    assert __import__("json").loads(profile.feature_group_source_asset_ids_json.iloc[0]) == {
        "hydrology": ["raw-basinatlas", "raw-basins", "raw-dem", "raw-rivers"],
        "landcover": ["raw-basins", "raw-worldcover"],
        "population": ["raw-basins", "raw-worldpop"],
        "soil": ["raw-basins", "raw-soilgrids"],
        "terrain": ["raw-basins", "raw-dem"],
    }
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    catalog = AssetCatalog(paths)
    raw_ids = {asset_id for asset_ids in inputs.source_asset_ids.values() for asset_id in asset_ids}
    for asset_id in raw_ids:
        catalog.upsert(
            AssetRecord(
                asset_id=asset_id,
                source_id="fixture",
                source_version="2026",
                kind=AssetKind.RAW,
                source_uri=f"https://example.test/{asset_id}",
                storage_path=str(paths.root / "legacy" / asset_id),
                media_type="application/octet-stream",
                size_bytes=1,
                checksum="3" * 64,
                retrieved_at=datetime.now(UTC),
                license_id="CC-BY-4.0",
                pipeline_run_id="map-test",
                status=AssetStatus.VALIDATED,
                metadata_json='{"dependency_asset_ids":["ignored-because-raw"]}',
            )
        )
    for record in records:
        catalog.upsert(record)

    lineage_report = run_quality_gates(paths, StudyAreaConfig())

    assert lineage_report.by_id("raw.provenance").passed
    assert (
        lineage_report.by_id("raw.provenance").actual
        == "9/9 used raw assets complete; 0 unresolved dependencies"
    )


def test_task16_empty_optional_relationships_keep_required_producer_schemas(
    tmp_path,
) -> None:
    """An empty relationship table remains readable with every mandatory Task 16 field."""
    worldpop = tmp_path / "worldpop-empty-contract.tif"
    with rasterio.open(
        worldpop,
        "w",
        driver="GTiff",
        width=2,
        height=1,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(104, 21, 1, 1),
    ) as destination:
        destination.write(np.asarray([[10, 20]], dtype="float32"), 1)
    handler = task16_map_handler(
        Task16MapInputs(
            basins=_basins(),
            core=box(104, 20, 106, 21),
            worldpop=worldpop,
            feature_tables=_required_features(),
        ),
        tmp_path / "empty-derived",
        owner_source_id="worldpop",
    )
    pipeline = type(
        "Pipeline",
        (),
        {
            "source_specs": {
                "worldpop": SourceSpec(
                    source_id="worldpop", adapter="existing", version="1", license_id="x"
                )
            }
        },
    )()

    handler(pipeline, "map", "worldpop", type("Context", (), {"run_id": "empty"})())

    road = pd.read_parquet(tmp_path / "empty-derived" / "map_subbasin_road.parquet")
    bridge = pd.read_parquet(tmp_path / "empty-derived" / "map_subbasin_bridge.parquet")
    assert {
        "HYBAS_ID",
        "segment_id",
        "osm_id",
        "intersected_length_km",
        "boundary_case",
        "quality_flags_json",
        "processing_crs",
        "source_asset_ids_json",
    } <= set(road)
    assert {
        "HYBAS_ID",
        "osm_id",
        "intersected_length_km",
        "relationship_geometry_wkt",
        "boundary_case",
        "quality_flags_json",
        "processing_crs",
        "source_asset_ids_json",
    } <= set(bridge)
    assert road.columns.is_unique
    assert bridge.columns.is_unique
