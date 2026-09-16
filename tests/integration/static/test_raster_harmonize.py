"""Small-file integration contracts for raster harmonization."""

from __future__ import annotations

import runpy
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.enums import Resampling
from shapely.geometry import box

from flashflood_data.static.spatial.raster import mosaic_clip_to_cog, raster_coverage_ratio


@pytest.fixture
def raster_fixtures(tmp_path: Path) -> dict[str, Path]:
    factory = runpy.run_path(str(Path(__file__).parents[2] / "fixtures" / "raster" / "make_fixtures.py"))
    return factory["write_raster_fixtures"](tmp_path / "rasters")


@pytest.fixture
def aoi():
    return box(0, 0, 4, 4)


def test_categorical_clip_uses_nearest_and_retains_codes(
    raster_fixtures: dict[str, Path], aoi, tmp_path: Path
) -> None:
    output = mosaic_clip_to_cog(
        [raster_fixtures["class_left"], raster_fixtures["class_right"]],
        aoi,
        tmp_path / "classes.tif",
        Resampling.nearest,
    )

    with rasterio.open(output) as source:
        assert set(np.unique(source.read(1))) <= {0, 10, 20, 30}
        assert source.profile["driver"] == "GTiff"
        assert source.overviews(1)


def test_continuous_clip_preserves_value_range_with_bilinear(
    raster_fixtures: dict[str, Path], aoi, tmp_path: Path
) -> None:
    output = mosaic_clip_to_cog(
        [raster_fixtures["continuous"]],
        aoi,
        tmp_path / "continuous.tif",
        Resampling.bilinear,
        dst_crs="EPSG:3857",
    )

    with rasterio.open(output) as source:
        values = source.read(1, masked=True).compressed()
        assert source.crs.to_string() == "EPSG:3857"
        assert values.min() >= -0.01
        assert values.max() <= 150.01


def test_coverage_counts_nodata_inside_aoi(raster_fixtures: dict[str, Path], aoi) -> None:
    assert raster_coverage_ratio(raster_fixtures["partial"], aoi) == pytest.approx(0.75)
