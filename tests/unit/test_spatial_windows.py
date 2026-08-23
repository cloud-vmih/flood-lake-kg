"""Fixed raster windows must cover non-square raster residuals exactly once."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from flashflood_data.derive._spatial import raster_windows


def test_raster_windows_cover_non_square_final_residuals(tmp_path: Path) -> None:
    """Swapping width/height residuals skips or overruns the final production blocks."""
    path = tmp_path / "non_square.tif"
    with rasterio.open(path, "w", driver="GTiff", width=5, height=3, count=1, dtype="uint8", crs="EPSG:32648", transform=from_origin(0, 3, 1, 1)) as destination:
        destination.write(np.ones((3, 5), dtype="uint8"), 1)
    with rasterio.open(path) as dataset:
        windows = list(raster_windows(dataset, block_size=2))

    assert [(window.width, window.height) for window in windows] == [(2, 2), (2, 2), (1, 2), (2, 1), (2, 1), (1, 1)]
    assert sum(window.width * window.height for window in windows) == 15
