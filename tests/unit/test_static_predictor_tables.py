"""Persistence contract for the four Task 15 intermediate tables."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import LineString, box

from flashflood_data.derive.static import (
    StaticPredictorInputs,
    derive_static_predictor_tables,
    task15_derive_handler,
)
from flashflood_data.models import SourceSpec


def _raster(path: Path, value: float, dtype: str) -> Path:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype=dtype,
        crs="EPSG:32648",
        transform=from_origin(500_000, 60, 30, 30),
        nodata=-9999 if dtype == "float32" else -32768,
    ) as destination:
        destination.write(np.full((2, 2), value, dtype=dtype), 1)
    return path


def _full_soil_matrix(paths: dict[str, Path]) -> dict[tuple[str, str, str], Path]:
    return {
        (property_name, depth, statistic): paths[depth]
        for property_name in ("clay", "sand", "silt", "bdod", "cfvo", "wv0010", "wv0033", "wv1500")
        for depth in ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm")
        for statistic in ("mean", "uncertainty")
    }


def test_static_predictor_writer_persists_four_id_keyed_tables(tmp_path: Path) -> None:
    """Replacing any table with duplicate keys must remain impossible at the output boundary."""
    dem = _raster(tmp_path / "dem.tif", 10.0, "float32")
    worldcover = _raster(tmp_path / "worldcover.tif", 10, "int16")
    soil_paths = _full_soil_matrix(
        {
            depth: _raster(tmp_path / f"soil-{depth}.tif", 100, "int16")
            for depth in ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm")
        }
    )
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [99]}, geometry=[box(500_000, 0, 500_060, 60)], crs="EPSG:32648"
    )
    rivers = gpd.GeoDataFrame(
        geometry=[LineString([(500_000, 30), (500_060, 30)])], crs="EPSG:32648"
    )

    outputs = derive_static_predictor_tables(
        StaticPredictorInputs(
            dem_path=dem,
            soil_raster_paths=soil_paths,
            worldcover_path=worldcover,
            rivers=rivers,
            basins=basins,
        ),
        tmp_path / "derived",
    )

    assert set(outputs) == {"terrain", "soil", "landcover", "hydrology"}
    for path in outputs.values():
        table = __import__("pandas").read_parquet(path)
        assert table.HYBAS_ID.tolist() == [99]


def test_task15_handler_runs_once_for_its_composed_owner_source(tmp_path: Path) -> None:
    """Running a derive stage for unrelated source IDs must not duplicate tables."""
    dem = _raster(tmp_path / "dem.tif", 10.0, "float32")
    worldcover = _raster(tmp_path / "worldcover.tif", 10, "int16")
    soil_paths = _full_soil_matrix(
        {
            depth: _raster(tmp_path / f"soil-{depth}.tif", 100, "int16")
            for depth in ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm")
        }
    )
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [99]}, geometry=[box(500_000, 0, 500_060, 60)], crs="EPSG:32648"
    )
    inputs = StaticPredictorInputs(
        dem_path=dem,
        soil_raster_paths=soil_paths,
        worldcover_path=worldcover,
        rivers=gpd.GeoDataFrame(
            geometry=[LineString([(500_000, 30), (500_060, 30)])], crs="EPSG:32648"
        ),
        basins=basins,
        source_asset_ids={
            "terrain": ("raw-basins", "raw-dem"),
            "soil": ("raw-basins", "raw-soilgrids"),
            "landcover": ("raw-basins", "raw-worldcover"),
            "hydrology": ("raw-basins", "raw-basinatlas", "raw-dem", "raw-rivers"),
        },
    )
    handler = task15_derive_handler(inputs, tmp_path / "derived", owner_source_id="terrain")
    pipeline = SimpleNamespace(
        source_specs={
            "terrain": SourceSpec(
                source_id="terrain", adapter="existing", version="1", license_id="x"
            )
        }
    )
    context = SimpleNamespace(run_id="task15-test")

    assert handler(pipeline, "derive", "other", context) == []
    records = handler(pipeline, "derive", "terrain", context)

    assert len(records) == 4
    assert {Path(record.storage_path).name for record in records} == {
        "terrain_features.parquet",
        "soil_features.parquet",
        "landcover_features.parquet",
        "hydrology_features.parquet",
    }
    expected_dependencies = {
        "task15-terrain-features": ["raw-basins", "raw-dem"],
        "task15-soil-features": ["raw-basins", "raw-soilgrids"],
        "task15-landcover-features": ["raw-basins", "raw-worldcover"],
        "task15-hydrology-features": [
            "raw-basinatlas",
            "raw-basins",
            "raw-dem",
            "raw-rivers",
        ],
    }
    assert {
        record.asset_id: __import__("json").loads(record.metadata_json)["dependency_asset_ids"]
        for record in records
    } == expected_dependencies
