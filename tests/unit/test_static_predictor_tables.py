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


def test_static_predictor_writer_persists_four_id_keyed_tables(tmp_path: Path) -> None:
    """Replacing any table with duplicate keys must remain impossible at the output boundary."""
    dem = _raster(tmp_path / "dem.tif", 10.0, "float32")
    worldcover = _raster(tmp_path / "worldcover.tif", 10, "int16")
    soil_paths = {
        ("clay", "0-5cm", "mean"): _raster(tmp_path / "soil0.tif", 100, "int16"),
        ("clay", "5-15cm", "mean"): _raster(tmp_path / "soil1.tif", 100, "int16"),
        ("clay", "15-30cm", "mean"): _raster(tmp_path / "soil2.tif", 100, "int16"),
    }
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
    soil_paths = {
        ("clay", "0-5cm", "mean"): _raster(tmp_path / "soil0.tif", 100, "int16"),
        ("clay", "5-15cm", "mean"): _raster(tmp_path / "soil1.tif", 100, "int16"),
        ("clay", "15-30cm", "mean"): _raster(tmp_path / "soil2.tif", 100, "int16"),
    }
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
    )
    handler = task15_derive_handler(inputs, tmp_path / "derived", owner_source_id="terrain")
    pipeline = SimpleNamespace(
        source_specs={"terrain": SourceSpec(source_id="terrain", adapter="existing", version="1", license_id="x")}
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
