"""Immutable ESA WorldCover 2021 v200 grid selection and categorical mosaicking."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from shapely.geometry import box, mapping
from shapely.geometry.base import BaseGeometry

from flashflood_data.catalog import sha256_bundle, sha256_file
from flashflood_data.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    ValidationResult,
)
from flashflood_data.raster import (
    RasterExpectation,
    mosaic_clip_to_cog,
    raster_coverage_ratio,
    validate_raster,
)
from flashflood_data.sources.base import SourceAdapter, SourceConfigurationError, SourceContext

_DEFAULT_VALID_CLASSES = (10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100)
_WATER_CLASS = 80


@dataclass(frozen=True)
class WorldCoverTile:
    """One official 3-by-3-degree WorldCover map tile."""

    tile_id: str
    bounds: tuple[float, float, float, float]
    uri: str = ""
    content_length: int = 0


def select_worldcover_tiles(grid: Path | gpd.GeoDataFrame, aoi: BaseGeometry) -> list[WorldCoverTile]:
    """Select sorted official `ll_tile` records whose geometries intersect an AOI."""
    if aoi.is_empty:
        raise ValueError("WorldCover tile selection requires a non-empty AOI")
    layer = gpd.read_file(grid) if isinstance(grid, Path) else grid
    if "ll_tile" not in layer.columns:
        raise ValueError("WorldCover grid is missing ll_tile")
    if layer.crs is None:
        raise ValueError("WorldCover grid is missing CRS")
    geographic = layer.to_crs("EPSG:4326") if layer.crs.to_string() != "EPSG:4326" else layer
    selected: list[WorldCoverTile] = []
    seen: set[str] = set()
    for _, feature in geographic.iterrows():
        tile_id = feature["ll_tile"]
        geometry = feature.geometry
        if not isinstance(tile_id, str) or not tile_id:
            raise ValueError("WorldCover grid contains an invalid ll_tile")
        if geometry is None or geometry.is_empty:
            raise ValueError(f"WorldCover grid tile {tile_id} has no geometry")
        if not geometry.intersects(aoi):
            continue
        if tile_id in seen:
            raise ValueError(f"WorldCover grid contains duplicate ll_tile {tile_id}")
        seen.add(tile_id)
        bounds = geometry.bounds
        selected.append(
            WorldCoverTile(tile_id, (bounds[0], bounds[1], bounds[2], bounds[3]))
        )
    return sorted(selected, key=lambda tile: tile.tile_id)


def validate_worldcover_classes(
    path: Path,
    valid_classes: Iterable[int] = _DEFAULT_VALID_CLASSES,
    nodata: int = 0,
) -> None:
    """Reject class codes outside the configured categorical vocabulary and nodata."""
    permitted = np.array(sorted({*valid_classes, nodata}), dtype="uint8")
    with rasterio.open(path) as dataset:
        for _, window in dataset.block_windows(1):
            values = dataset.read(1, window=window)
            unknown = np.unique(values[~np.isin(values, permitted)])
            if unknown.size:
                raise ValueError(f"unknown WorldCover class {int(unknown[0])}")


class WorldCoverAdapter(SourceAdapter):
    """Resolve public v200 tiles from a retained v100 grid and publish a categorical COG."""

    def __init__(self, spec, *, client: httpx.Client | None = None) -> None:
        super().__init__(spec)
        self.client = client or httpx.Client()

    def _setting(self, key: str) -> str:
        value = self.spec.settings.get(key)
        if not isinstance(value, str) or not value:
            raise SourceConfigurationError(f"WorldCover setting {key!r} must be a non-empty string")
        return value

    def _classes(self) -> tuple[int, ...]:
        value = self.spec.settings.get("valid_classes")
        if not isinstance(value, tuple) or not value or any(type(item) is not int for item in value):
            raise SourceConfigurationError("WorldCover valid_classes must be a non-empty integer list")
        return value

    def _nodata(self) -> int:
        value = self.spec.settings.get("nodata")
        if type(value) is not int or not 0 <= value <= 255:
            raise SourceConfigurationError("WorldCover nodata must be a byte value")
        return value

    def _aoi(self, context: SourceContext, name: str) -> BaseGeometry:
        path = context.paths.harmonized / "aoi" / f"{name}_aoi.geoparquet"
        if not path.is_file():
            raise ValueError(f"WorldCover requires {name}_aoi.geoparquet")
        layer = gpd.read_parquet(path)
        if layer.empty or layer.crs is None:
            raise ValueError(f"{name} AOI is missing geometry or CRS")
        geographic = layer.to_crs("EPSG:4326") if layer.crs.to_string() != "EPSG:4326" else layer
        geometry = geographic.geometry.union_all()
        if geometry.is_empty:
            raise ValueError(f"{name} AOI is empty")
        return geometry

    def _head_size(self, uri: str) -> int:
        response = self.client.head(uri, follow_redirects=True)
        response.raise_for_status()
        raw_length = response.headers.get("Content-Length")
        try:
            size = int(raw_length) if raw_length is not None else 0
        except ValueError as exc:
            raise ValueError(f"WorldCover HEAD has invalid Content-Length for {uri}") from exc
        if size <= 0:
            raise ValueError(f"WorldCover HEAD has no safe Content-Length for {uri}")
        return size

    def _grid_asset(self, available: list[AssetRecord]) -> AssetRecord | None:
        candidates = [
            asset
            for asset in available
            if asset.source_id == self.spec.source_id
            and asset.kind is AssetKind.RAW
            and Path(asset.storage_path).name == "grid.geojson"
        ]
        if len(candidates) > 1:
            raise ValueError("WorldCover has multiple retained raw grids")
        return candidates[0] if candidates else None

    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        """Return the grid first, then only preflighted tiles intersecting Environmental AOI."""
        grid = self._grid_asset(available)
        if grid is None:
            uri = self._setting("grid_url")
            return [
                RemoteAsset(
                    asset_id=f"worldcover-{self.spec.version}-grid",
                    source_id=self.spec.source_id,
                    source_version=self.spec.version,
                    uri=uri,
                    target_relative_path=Path("raw") / "worldcover" / self.spec.version / "grid.geojson",
                    media_type="application/geo+json",
                    license_id=self.spec.license_id,
                    expected_size=self._head_size(uri),
                    source_valid_time=self.spec.version,
                )
            ]
        grid_path = Path(grid.storage_path)
        if not grid_path.is_file():
            raise ValueError(f"WorldCover retained grid is missing: {grid_path}")
        template = self._setting("tile_template")
        remotes: list[RemoteAsset] = []
        for tile in select_worldcover_tiles(grid_path, self._aoi(context, "environmental")):
            uri = template.format(tile=tile.tile_id)
            tile = replace(tile, uri=uri, content_length=self._head_size(uri))
            remotes.append(
                RemoteAsset(
                    asset_id=f"worldcover-{self.spec.version}-{tile.tile_id}",
                    source_id=self.spec.source_id,
                    source_version=self.spec.version,
                    uri=tile.uri,
                    target_relative_path=(
                        Path("raw") / "worldcover" / self.spec.version / "map" / f"{tile.tile_id}_Map.tif"
                    ),
                    media_type="image/tiff",
                    license_id=self.spec.license_id,
                    expected_size=tile.content_length,
                    source_valid_time=self.spec.version,
                )
            )
        return remotes

    def validate_raw(self, path: Path) -> ValidationResult:
        """Validate either the retained selection grid or one categorical COG tile."""
        if path.suffix.lower() in {".geojson", ".json"}:
            try:
                layer = gpd.read_file(path)
                tiles = select_worldcover_tiles(layer, box(-180, -90, 180, 90))
                checks = {
                    "readable": True,
                    "grid_schema": bool(tiles),
                    "unique_tiles": len(tiles) == len({tile.tile_id for tile in tiles}),
                }
                return ValidationResult(
                    passed=all(checks.values()),
                    checks=checks,
                    metrics={"tile_count": len(tiles)},
                )
            except (OSError, ValueError) as exc:
                return ValidationResult(
                    passed=False,
                    checks={"readable": False, "grid_schema": False},
                    messages=(str(exc),),
                )
        try:
            expectation = RasterExpectation(
                dtypes=("uint8",), crs="EPSG:4326", resolution_range=None, aoi=self._tile_bounds(path)
            )
            structural = validate_raster(path, expectation)
            with rasterio.open(path) as dataset:
                cog_checks = {
                    "single_band": dataset.count == 1,
                    "tiled": bool(dataset.profile.get("tiled", False)),
                    "overviews": bool(dataset.overviews(1)),
                    "nodata": dataset.nodata == self._nodata(),
                }
            validate_worldcover_classes(path, self._classes(), self._nodata())
            checks = {**structural.checks, **cog_checks, "classes": True}
            return ValidationResult(
                passed=all(checks.values()), checks=checks, metrics=structural.metrics,
                messages=structural.messages + tuple(name for name, passed in cog_checks.items() if not passed),
            )
        except (OSError, ValueError, rasterio.errors.RasterioError) as exc:
            return ValidationResult(
                passed=False,
                checks={"readable": False, "classes": False},
                messages=(str(exc),),
            )

    @staticmethod
    def _tile_bounds(path: Path) -> BaseGeometry:
        with rasterio.open(path) as dataset:
            return box(*dataset.bounds)

    def _pixel_counts(self, path: Path, aoi: BaseGeometry) -> dict[str, int]:
        nodata, water = 0, 0
        with rasterio.open(path) as dataset:
            for _, window in dataset.block_windows(1):
                inside = geometry_mask(
                    [mapping(aoi)],
                    out_shape=(int(window.height), int(window.width)),
                    transform=dataset.window_transform(window),
                    invert=True,
                )
                values = dataset.read(1, window=window)
                nodata += int((inside & (values == self._nodata())).sum())
                water += int((inside & (values == _WATER_CLASS)).sum())
        return {"nodata_pixels_hydrological_aoi": nodata, "water_pixels_hydrological_aoi": water}

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        """Mosaic raw COG tiles and exactly clip a nearest-neighbour categorical output."""
        environmental = self._aoi(context, "environmental")
        hydrological = self._aoi(context, "hydrological")
        raw_assets = sorted(
            (
                asset
                for asset in assets
                if asset.source_id == self.spec.source_id
                and asset.kind is AssetKind.RAW
                and Path(asset.storage_path).suffix.lower() in {".tif", ".tiff"}
            ),
            key=lambda asset: asset.asset_id,
        )
        if not raw_assets:
            raise ValueError("WorldCover harmonization is missing raw map tiles")
        raw_paths = [Path(asset.storage_path) for asset in raw_assets]
        for asset, path in zip(raw_assets, raw_paths, strict=True):
            validation = self.validate_raw(path)
            if not validation.passed:
                raise ValueError(f"WorldCover raw validation failed for {asset.asset_id}: {validation.messages}")
        output_path = context.paths.harmonized / "rasters" / "worldcover_2021.tif"
        mosaic_clip_to_cog(raw_paths, environmental, output_path, Resampling.nearest)
        validate_worldcover_classes(output_path, self._classes(), self._nodata())
        coverage = raster_coverage_ratio(output_path, hydrological)
        minimum = context.study_area.environmental_raster_coverage_min_pct / 100
        if coverage < minimum:
            raise ValueError(f"WorldCover Hydrological AOI coverage {coverage:.2%} is below {minimum:.2%}")
        output_validation = validate_raster(
            output_path,
            RasterExpectation(dtypes=("uint8",), crs="EPSG:4326", resolution_range=None, aoi=environmental),
        )
        if not output_validation.passed:
            raise ValueError(f"WorldCover output validation failed: {output_validation.messages}")
        now = datetime.now(UTC)
        metadata = {
            "coverage_ratio_hydrological_aoi": coverage,
            "grid_policy": "native WorldCover grid; categorical nearest-neighbour only",
            "resampling": "nearest",
            "validation": dict(output_validation.metrics),
            **self._pixel_counts(output_path, hydrological),
        }
        output = AssetRecord(
            asset_id=f"worldcover-{self.spec.version}-harmonized",
            source_id=self.spec.source_id,
            source_version=self.spec.version,
            kind=AssetKind.HARMONIZED,
            source_uri="generated:worldcover-environmental-aoi-mosaic",
            storage_path=str(output_path),
            media_type="image/tiff",
            size_bytes=output_path.stat().st_size,
            checksum=sha256_file(output_path),
            retrieved_at=now,
            source_valid_time=self.spec.version,
            license_id=self.spec.license_id,
            pipeline_run_id=context.run_id,
            status=AssetStatus.HARMONIZED,
            dependency_fingerprint=sha256_bundle(raw_paths),
            metadata_json=json.dumps(metadata, sort_keys=True, separators=(",", ":")),
        )
        context.catalog.upsert(output)
        return [output]
