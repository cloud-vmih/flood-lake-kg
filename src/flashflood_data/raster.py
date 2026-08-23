"""Bounded-memory raster inspection, validation, and AOI harmonization."""

from __future__ import annotations

import tempfile
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError, WindowError
from rasterio.features import geometry_mask, geometry_window
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_geom
from rasterio.windows import Window
from shapely.geometry import box, mapping, shape
from shapely.geometry.base import BaseGeometry

from flashflood_data.io_atomic import atomic_target
from flashflood_data.models import ValidationResult

COG_PROFILE = {
    "driver": "GTiff",
    "tiled": True,
    "blockxsize": 512,
    "blockysize": 512,
    "compress": "DEFLATE",
    "predictor": 2,
    "BIGTIFF": "IF_SAFER",
}


@dataclass(frozen=True)
class RasterMetadata:
    """Structural metadata read directly from a raster dataset."""

    driver: str
    width: int
    height: int
    count: int
    dtype: str
    nodata: float | int | None
    crs: str
    bounds: tuple[float, float, float, float]
    transform: tuple[float, float, float, float, float, float]
    pixel_size: tuple[float, float]
    tiled: bool
    overviews: tuple[int, ...]
    size_bytes: int


@dataclass(frozen=True)
class RasterExpectation:
    """Structural requirements checked without mutating the source raster."""

    dtypes: tuple[str, ...]
    crs: str | None
    resolution_range: tuple[float, float] | None
    aoi: BaseGeometry


def _crs_text(dataset: rasterio.io.DatasetReader) -> str:
    return dataset.crs.to_string() if dataset.crs else ""


def _metadata(dataset: rasterio.io.DatasetReader, size_bytes: int) -> RasterMetadata:
    transform = dataset.transform
    bounds = dataset.bounds
    return RasterMetadata(
        driver=dataset.driver,
        width=dataset.width,
        height=dataset.height,
        count=dataset.count,
        dtype=dataset.dtypes[0] if dataset.dtypes else "",
        nodata=dataset.nodata,
        crs=_crs_text(dataset),
        bounds=(bounds.left, bounds.bottom, bounds.right, bounds.top),
        transform=(transform.a, transform.b, transform.c, transform.d, transform.e, transform.f),
        pixel_size=(abs(transform.a), abs(transform.e)),
        tiled=bool(dataset.profile.get("tiled", False)),
        overviews=tuple(dataset.overviews(1)) if dataset.count else (),
        size_bytes=size_bytes,
    )


def inspect_raster(path: Path) -> RasterMetadata:
    """Return immutable metadata for a readable raster."""
    with rasterio.open(path) as dataset:
        return _metadata(dataset, path.stat().st_size)


def _failed_validation(message: str, *, exists: bool, non_empty: bool) -> ValidationResult:
    return ValidationResult(
        passed=False,
        checks={
            "exists": exists,
            "non_empty": non_empty,
            "readable": False,
            "dtypes": False,
            "crs": False,
            "resolution": False,
            "nodata_semantics": False,
            "aoi_intersection": False,
        },
        messages=(message,),
    )


def _nodata_semantics(dataset: rasterio.io.DatasetReader) -> bool:
    """Ensure every band can expose a valid-data mask consistent with its values."""
    for band in range(1, dataset.count + 1):
        values = dataset.read(band)
        valid = dataset.read_masks(band) > 0
        if dataset.nodata is None:
            continue
        if np.isnan(dataset.nodata):
            if np.any(valid & np.isnan(values)):
                return False
        elif np.any(valid & (values == dataset.nodata)):
            return False
    return True


def validate_raster(path: Path, expected: RasterExpectation) -> ValidationResult:
    """Check raster readability and declared structure without changing the source file."""
    if not path.exists():
        return _failed_validation(f"raster does not exist: {path}", exists=False, non_empty=False)
    if path.stat().st_size == 0:
        return _failed_validation(f"raster is empty: {path}", exists=True, non_empty=False)

    try:
        with rasterio.open(path) as dataset:
            for band in range(1, dataset.count + 1):
                dataset.read(band)
            pixel_sizes = (abs(dataset.transform.a), abs(dataset.transform.e))
            checks = {
                "exists": True,
                "non_empty": True,
                "readable": True,
                "dtypes": bool(dataset.dtypes)
                and all(dtype in expected.dtypes for dtype in dataset.dtypes),
                "crs": expected.crs is None or _crs_text(dataset) == expected.crs,
                "resolution": expected.resolution_range is None
                or all(expected.resolution_range[0] <= size <= expected.resolution_range[1] for size in pixel_sizes),
                "nodata_semantics": _nodata_semantics(dataset),
                "aoi_intersection": box(*dataset.bounds).intersects(expected.aoi),
            }
            metadata = _metadata(dataset, path.stat().st_size)
    except (RasterioIOError, OSError, ValueError) as error:
        return _failed_validation(str(error), exists=True, non_empty=True)

    messages = tuple(name for name, passed in checks.items() if not passed)
    return ValidationResult(
        passed=all(checks.values()),
        checks=checks,
        metrics={
            "width": metadata.width,
            "height": metadata.height,
            "count": metadata.count,
            "size_bytes": metadata.size_bytes,
        },
        messages=messages,
    )


def _target_aoi(aoi: BaseGeometry, source_crs: str, dst_crs: str | None) -> BaseGeometry:
    if dst_crs is None or source_crs == dst_crs:
        return aoi
    return shape(transform_geom(source_crs, dst_crs, mapping(aoi)))


def _overlap(left: Window, right: Window) -> Window | None:
    col_start = max(left.col_off, right.col_off)
    row_start = max(left.row_off, right.row_off)
    col_end = min(left.col_off + left.width, right.col_off + right.width)
    row_end = min(left.row_off + left.height, right.row_off + right.height)
    if col_start >= col_end or row_start >= row_end:
        return None
    return Window(col_start, row_start, col_end - col_start, row_end - row_start)


def _windows(width: int, height: int, block_size: int = 512):
    for row_off in range(0, height, block_size):
        for col_off in range(0, width, block_size):
            yield Window(
                col_off,
                row_off,
                min(block_size, width - col_off),
                min(block_size, height - row_off),
            )


def _crop_window(dataset: rasterio.io.DatasetReader, aoi: BaseGeometry) -> Window:
    try:
        requested = geometry_window(dataset, [mapping(aoi)]).round_offsets().round_lengths()
    except WindowError as error:
        raise ValueError("AOI does not intersect raster inputs") from error
    full = Window(0, 0, dataset.width, dataset.height)
    cropped = _overlap(requested, full)
    if cropped is None:
        raise ValueError("AOI does not intersect raster inputs")
    return cropped


def _destination_profile(dataset: rasterio.io.DatasetReader, crop: Window) -> dict[str, object]:
    profile = dataset.profile.copy()
    profile.update(COG_PROFILE)
    profile.update(
        width=int(crop.width),
        height=int(crop.height),
        transform=dataset.window_transform(crop),
        predictor=3 if np.issubdtype(np.dtype(dataset.dtypes[0]), np.floating) else 2,
    )
    return profile


def _build_overviews(dataset: rasterio.io.DatasetWriter, resampling: Resampling) -> None:
    factors = [factor for factor in (2, 4, 8, 16) if min(dataset.width, dataset.height) >= factor]
    if not factors:
        return
    method = Resampling.nearest if resampling == Resampling.nearest else Resampling.average
    dataset.build_overviews(factors, method)
    dataset.update_tags(ns="rio_overview", resampling=method.name)


def _clip_to_dataset(
    source: rasterio.io.DatasetReader,
    aoi: BaseGeometry,
    destination: rasterio.io.DatasetWriter,
    crop: Window,
) -> None:
    """Clip one output block at a time to bound peak memory by the source block size."""
    fill_value = source.nodata if source.nodata is not None else 0
    for source_window in _windows(source.width, source.height):
        window = _overlap(source_window, crop)
        if window is None:
            continue
        destination_window = Window(
            window.col_off - crop.col_off,
            window.row_off - crop.row_off,
            window.width,
            window.height,
        )
        inside_aoi = geometry_mask(
            [mapping(aoi)],
            out_shape=(int(window.height), int(window.width)),
            transform=source.window_transform(window),
            invert=True,
        )
        data = source.read(window=window)
        source_valid = np.all(source.read_masks(window=window) > 0, axis=0)
        valid = inside_aoi & source_valid
        if not valid.all():
            data[:, ~valid] = fill_value
        destination.write(data, window=destination_window)
        destination.write_mask(np.where(valid, 255, 0).astype("uint8"), window=destination_window)


def mosaic_clip_to_cog(
    inputs: Sequence[Path],
    aoi: BaseGeometry,
    output: Path,
    resampling: Resampling,
    dst_crs: str | None = None,
) -> Path:
    """Merge source tiles, clip to an AOI, and atomically publish tiled overviewed GeoTIFF."""
    if not inputs:
        raise ValueError("at least one raster input is required")
    output.parent.mkdir(parents=True, exist_ok=True)

    with ExitStack() as stack:
        sources = [stack.enter_context(rasterio.open(path)) for path in inputs]
        source_crs = _crs_text(sources[0])
        if not source_crs:
            raise ValueError("raster inputs must have a CRS")
        if dst_crs is None and any(_crs_text(source) != source_crs for source in sources):
            raise ValueError("raster inputs need a common CRS unless dst_crs is explicit")
        target_aoi = _target_aoi(aoi, source_crs, dst_crs)
        merge_sources: list[rasterio.io.DatasetReader] = sources
        if dst_crs is not None:
            merge_sources = [
                stack.enter_context(WarpedVRT(source, crs=dst_crs, resampling=resampling))
                for source in sources
            ]

        with tempfile.TemporaryDirectory(prefix=f".{output.name}.", dir=output.parent) as temporary_dir:
            merged_path = Path(temporary_dir) / "merged.tif"
            merge_profile = COG_PROFILE | {
                "dtype": merge_sources[0].dtypes[0],
                "nodata": merge_sources[0].nodata,
            }
            merge(
                merge_sources,
                bounds=target_aoi.bounds,
                resampling=resampling,
                mem_limit=64,
                dst_path=merged_path,
                dst_kwds=merge_profile,
            )
            with rasterio.open(merged_path) as merged:
                crop = _crop_window(merged, target_aoi)
                profile = _destination_profile(merged, crop)
                with atomic_target(output) as partial:
                    with rasterio.open(partial, "w", **profile) as destination:
                        _clip_to_dataset(merged, target_aoi, destination, crop)
                        _build_overviews(destination, resampling)
                    expectation = RasterExpectation(
                        dtypes=tuple(dict.fromkeys(merged.dtypes)),
                        crs=_crs_text(merged),
                        resolution_range=None,
                        aoi=target_aoi,
                    )
                    validation = validate_raster(partial, expectation)
                    if not validation.passed:
                        raise ValueError(f"generated raster failed validation: {validation.messages}")
    return output


def raster_coverage_ratio(path: Path, aoi: BaseGeometry) -> float:
    """Return the fraction of native-grid AOI pixels with valid first-band data."""
    with rasterio.open(path) as dataset:
        crop = _crop_window(dataset, aoi)
        aoi_pixels = 0
        covered_pixels = 0
        for block_window in _windows(dataset.width, dataset.height):
            window = _overlap(block_window, crop)
            if window is None:
                continue
            inside_aoi = geometry_mask(
                [mapping(aoi)],
                out_shape=(int(window.height), int(window.width)),
                transform=dataset.window_transform(window),
                invert=True,
            )
            valid = dataset.read_masks(1, window=window) > 0
            aoi_pixels += int(inside_aoi.sum())
            covered_pixels += int((inside_aoi & valid).sum())
    return covered_pixels / aoi_pixels if aoi_pixels else 0.0
