"""Bronze parsing keeps source values and records object provenance."""

import json
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import LineString, box

from flashflood_data.orchestration.bronze.parsers import (
    iter_vector_batches,
    parse_events,
    parse_raster,
    parse_vector,
)


def _zip_shapefile(tmp_path: Path, geometry: object, *, field: str, value: int) -> Path:
    frame = gpd.GeoDataFrame({field: [value]}, geometry=[geometry], crs="EPSG:4326")
    source = tmp_path / "shape.shp"
    frame.to_file(source)
    archive = tmp_path / "source.zip"
    with ZipFile(archive, "w") as output:
        for member in tmp_path.glob("shape.*"):
            output.write(member, member.name)
    return archive


def test_l12_basin_zip_preserves_raw_field_and_geometry(tmp_path: Path) -> None:
    archive = _zip_shapefile(tmp_path, box(103, 21, 104, 22), field="HYBAS_ID", value=42)
    table, rows = parse_vector(
        "hydrobasins_v1c", archive, object_id="raw-1", run_id="parse-1", parser_version="v1"
    )
    assert table == "basin_polygon_raw"
    assert len(rows) == 1
    assert rows[0]["source_feature_id"] == "42"
    assert json.loads(rows[0]["source_fields_json"])["HYBAS_ID"] == 42
    assert rows[0]["bbox_wgs84"] == [103.0, 21.0, 104.0, 22.0]
    assert rows[0]["geometry_wkb"]


def test_basinatlas_reads_only_curated_source_fields(tmp_path: Path) -> None:
    frame = gpd.GeoDataFrame(
        {"HYBAS_ID": [42], "NEXT_DOWN": [7], "UNUSED": [999]},
        geometry=[box(103, 21, 104, 22)], crs="EPSG:4326",
    )
    frame.to_file(tmp_path / "atlas.shp")
    archive = tmp_path / "atlas.zip"
    with ZipFile(archive, "w") as output:
        for member in tmp_path.glob("atlas.*"):
            if member != archive:
                output.write(member, member.name)

    _, rows = parse_vector(
        "basinatlas_v10", archive, object_id="atlas-1", run_id="parse-1", parser_version="v1"
    )

    fields = json.loads(rows[0]["source_fields_json"])
    assert fields["HYBAS_ID"] == 42
    assert fields["NEXT_DOWN"] == 7
    assert "UNUSED" not in fields


def test_vector_parser_yields_bounded_batches_without_losing_records(
    tmp_path: Path, monkeypatch
) -> None:
    frame = gpd.GeoDataFrame(
        {"HYBAS_ID": [1, 2, 3]},
        geometry=[box(103 + i, 21, 104 + i, 22) for i in range(3)],
        crs="EPSG:4326",
    )
    frame.to_file(tmp_path / "basins.shp")
    archive = tmp_path / "basins.zip"
    with ZipFile(archive, "w") as output:
        for member in tmp_path.glob("basins.*"):
            if member != archive:
                output.write(member, member.name)
    monkeypatch.setattr(
        pyogrio, "read_dataframe",
        lambda *args, **kwargs: pytest.fail("batch parser reopened the vector source"),
    )

    batches = list(iter_vector_batches(
        "hydrobasins_v1c", archive, object_id="raw-1", run_id="parse-1",
        parser_version="v1", batch_size=2,
    ))

    assert [len(batch) for batch in batches] == [2, 1]
    assert [row["source_feature_id"] for batch in batches for row in batch] == ["1", "2", "3"]


def test_vector_parser_clamps_and_flags_small_antimeridian_bbox_artifact(tmp_path: Path) -> None:
    archive = _zip_shapefile(
        tmp_path, box(179.5, 21, 180.0007, 22), field="HYBAS_ID", value=42
    )

    _, rows = parse_vector(
        "hydrobasins_v1c", archive, object_id="raw-1", run_id="parse-1", parser_version="v1"
    )

    assert rows[0]["bbox_wgs84"] == [179.5, 21.0, 180.0, 22.0]
    assert rows[0]["quality_status"] == "flagged"


def test_vector_parser_does_not_hide_large_out_of_range_bbox(tmp_path: Path) -> None:
    archive = _zip_shapefile(
        tmp_path, box(179.5, 21, 180.01, 22), field="HYBAS_ID", value=42
    )

    _, rows = parse_vector(
        "hydrobasins_v1c", archive, object_id="raw-1", run_id="parse-1", parser_version="v1"
    )

    assert rows[0]["bbox_wgs84"][2] == 180.01


def test_river_zip_rejects_duplicate_source_ids(tmp_path: Path) -> None:
    frame = gpd.GeoDataFrame(
        {"HYRIV_ID": [7, 7]},
        geometry=[LineString([(103, 21), (104, 22)]), LineString([(103, 22), (104, 21)])],
        crs="EPSG:4326",
    )
    frame.to_file(tmp_path / "river.shp")
    archive = tmp_path / "river.zip"
    with ZipFile(archive, "w") as output:
        for member in tmp_path.glob("river.*"):
            output.write(member, member.name)
    with pytest.raises(ValueError, match="duplicate"):
        parse_vector("hydrorivers_v10", archive, object_id="raw-2", run_id="parse-1", parser_version="v1")


def test_raster_parser_reads_header_without_reading_pixels(tmp_path: Path, monkeypatch) -> None:
    raster = tmp_path / "sand_0-5cm_Q0.50.tif"
    with rasterio.open(
        raster, "w", driver="GTiff", width=2, height=2, count=1, dtype="uint16",
        crs="EPSG:4326", transform=from_origin(103, 22, 0.5, 0.5), nodata=0,
    ) as output:
        output.write(np.ones((2, 2), dtype="uint16"), 1)
    original = rasterio.open

    class HeaderOnly:
        def __init__(self, dataset):
            self.dataset = dataset

        def __enter__(self):
            self.dataset.__enter__()
            return self

        def __exit__(self, *args):
            return self.dataset.__exit__(*args)

        def __getattr__(self, name):
            if name == "read":
                pytest.fail("read pixels")
            return getattr(self.dataset, name)

    def forbid_pixel_read(*args, **kwargs):
        return HeaderOnly(original(*args, **kwargs))

    monkeypatch.setattr(rasterio, "open", forbid_pixel_read)
    rows = parse_raster(
        raster, object_id="raw-raster", object_uri="s3://raw/sand.tif", checksum="abc",
        run_id="parse-1", parser_version="v1", source_id="soilgrids_2_0",
    )
    assert len(rows) == 1
    assert rows[0]["property"] == "sand"
    assert rows[0]["depth_interval"] == "0-5cm"
    assert rows[0]["statistic"] == "Q0.50"
    assert rows[0]["bbox_wgs84"] == [103.0, 21.0, 104.0, 22.0]
    assert rows[0]["dtype"] == "uint16"


def test_event_parser_preserves_original_columns_and_time_text(tmp_path: Path) -> None:
    path = tmp_path / "events.xlsx"
    pd.DataFrame([
        {"STT": 3, "Năm": 2024, "Ngày xảy ra": "10/8/2024", "Mô tả ngắn": "Lũ quét"}
    ]).to_excel(path, index=False)
    rows = parse_events(path, object_id="raw-event", run_id="parse-1", parser_version="v1")
    assert rows[0]["source_record_id"] == "3"
    assert rows[0]["source_valid_time"] == "10/8/2024"
    assert json.loads(rows[0]["original_fields_json"])["Mô tả ngắn"] == "Lũ quét"
