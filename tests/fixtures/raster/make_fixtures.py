"""Create deterministic, tiny GeoTIFF fixtures for raster tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin


def _write(path: Path, values: np.ndarray, transform) -> Path:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=values.shape[1],
        height=values.shape[0],
        count=1,
        dtype=values.dtype,
        crs="EPSG:4326",
        transform=transform,
        nodata=0,
    ) as destination:
        destination.write(values, 1)
    return path


def write_raster_fixtures(directory: Path) -> dict[str, Path]:
    """Write tiles with hand-checked classes, values, and nodata coverage."""
    directory.mkdir(parents=True, exist_ok=True)
    return {
        "class_left": _write(
            directory / "class_left.tif",
            np.array([[10, 10], [10, 10], [10, 10], [10, 10]], dtype="uint8"),
            from_origin(0, 4, 1, 1),
        ),
        "class_right": _write(
            directory / "class_right.tif",
            np.array([[20, 20], [20, 20], [30, 30], [30, 30]], dtype="uint8"),
            from_origin(2, 4, 1, 1),
        ),
        "continuous": _write(
            directory / "continuous.tif",
            np.array([
                [0.0, 10.0, 20.0, 30.0],
                [40.0, 50.0, 60.0, 70.0],
                [80.0, 90.0, 100.0, 110.0],
                [120.0, 130.0, 140.0, 150.0],
            ], dtype="float32"),
            from_origin(0, 4, 1, 1),
        ),
        "partial": _write(
            directory / "partial.tif",
            np.array([[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 0, 0], [1, 1, 0, 0]], dtype="uint8"),
            from_origin(0, 4, 1, 1),
        ),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    write_raster_fixtures(arguments.output)
