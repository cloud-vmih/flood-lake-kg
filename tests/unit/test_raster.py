"""Unit contracts for raster inspection and validation."""

from __future__ import annotations

import runpy
from pathlib import Path

import pytest
from shapely.geometry import box

from flashflood_data.static.spatial.raster import (
    RasterExpectation,
    inspect_raster,
    validate_raster,
)


@pytest.fixture
def raster_fixtures(tmp_path: Path) -> dict[str, Path]:
    factory = runpy.run_path(str(Path(__file__).parents[1] / "fixtures" / "raster" / "make_fixtures.py"))
    return factory["write_raster_fixtures"](tmp_path / "rasters")


def test_inspect_raster_reports_structural_metadata(raster_fixtures: dict[str, Path]) -> None:
    metadata = inspect_raster(raster_fixtures["partial"])

    assert metadata.driver == "GTiff"
    assert (metadata.width, metadata.height, metadata.count) == (4, 4, 1)
    assert metadata.dtype == "uint8"
    assert metadata.nodata == 0
    assert metadata.crs == "EPSG:4326"
    assert metadata.bounds == (0.0, 0.0, 4.0, 4.0)
    assert metadata.pixel_size == (1.0, 1.0)
    assert metadata.size_bytes > 0


def test_validate_raster_accepts_matching_structure(raster_fixtures: dict[str, Path]) -> None:
    expectation = RasterExpectation(("uint8",), "EPSG:4326", (0.5, 1.5), box(0, 0, 4, 4))

    result = validate_raster(raster_fixtures["partial"], expectation)

    assert result.passed is True
    assert all(result.checks.values())


def test_validate_raster_returns_failed_readability_for_truncated_tiff(tmp_path: Path) -> None:
    truncated = tmp_path / "truncated.tif"
    truncated.write_bytes(b"II*\x00")
    expectation = RasterExpectation(("uint8",), "EPSG:4326", (0.5, 1.5), box(0, 0, 4, 4))

    result = validate_raster(truncated, expectation)

    assert result.passed is False
    assert result.checks["readable"] is False
    assert result.messages
