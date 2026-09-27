"""Stable global grid definitions and Iceberg registration for weather providers."""

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Protocol

import geopandas as gpd
import shapely
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry


@dataclass(frozen=True)
class GridDefinition:
    """Globally anchored regular grid whose identity is independent of an AOI."""

    source_id: str
    source_grid_version: str
    resolution_x: float
    resolution_y: float
    north: float
    west: float
    width: int
    height: int
    crs: str

    @classmethod
    def create(
        cls,
        *,
        source_id: str,
        resolution_x: float,
        resolution_y: float,
        north: float,
        west: float,
        width: int,
        height: int,
        crs: str = "EPSG:4326",
    ) -> "GridDefinition":
        values = {
            "source_id": source_id,
            "resolution_x": resolution_x,
            "resolution_y": resolution_y,
            "north": north,
            "west": west,
            "width": width,
            "height": height,
            "crs": crs,
        }
        canonical = json.dumps(values, sort_keys=True, separators=(",", ":"))
        version = f"regular-grid-{sha256(canonical.encode()).hexdigest()[:16]}"
        return cls(source_grid_version=version, **values)


@dataclass(frozen=True)
class GridRegistration:
    """Registered provider grid and current AOI's stable source-cell mapping."""

    source_grid_version: str
    scope_id: str
    cell_index_by_grid_id: Mapping[str, int]


class _GridWriter(Protocol):
    def get_keyed_rows(self, identifier, key): ...

    def upsert_keyed_rows(self, identifier, key_fields, rows): ...


def grid_definition_for_provider(provider: str, resolution: float) -> GridDefinition:
    """Return the approved global alignment for one weather provider."""
    if not math.isclose(resolution, 0.1, abs_tol=1e-12):
        raise ValueError("weather source grids currently require 0.1-degree resolution")
    if provider == "gsmap":
        return GridDefinition.create(
            source_id="gsmap",
            resolution_x=resolution,
            resolution_y=resolution,
            north=60.0,
            west=0.0,
            width=3600,
            height=1200,
        )
    if provider == "era5_land":
        # ERA5-Land coordinate points lie on 0.1-degree multiples. Cell edges
        # therefore start half a cell west/north of those coordinate centers.
        return GridDefinition.create(
            source_id="era5_land",
            resolution_x=resolution,
            resolution_y=resolution,
            north=90.05,
            west=-180.05,
            width=3600,
            height=1801,
        )
    if provider == "ifs_openmeteo":
        return GridDefinition.create(
            source_id="ifs_openmeteo",
            resolution_x=resolution,
            resolution_y=resolution,
            north=90.0,
            west=-180.0,
            width=3600,
            height=1800,
        )
    raise ValueError(f"unsupported weather grid provider: {provider}")


def _candidate_range(
    low: float, high: float, origin: float, step: float, count: int
) -> range:
    start = max(0, math.floor((low - origin) / step))
    stop = min(count - 1, math.floor((high - origin) / step))
    return range(start, stop + 1)


def build_grid_rows(
    definition: GridDefinition,
    geometry: BaseGeometry,
    scope_id: str,
) -> list[dict[str, object]]:
    """Build intersecting cells while retaining their global row/column index."""
    if geometry.is_empty:
        raise ValueError("weather grid scope geometry cannot be empty")
    west, south, east, north = geometry.bounds
    columns = _candidate_range(
        west, east, definition.west, definition.resolution_x, definition.width
    )
    first_row = max(
        0, math.floor((definition.north - north) / definition.resolution_y)
    )
    last_row = min(
        definition.height - 1,
        math.floor((definition.north - south) / definition.resolution_y),
    )
    rows: list[dict[str, object]] = []
    for row_index in range(first_row, last_row + 1):
        top = definition.north - row_index * definition.resolution_y
        bottom = top - definition.resolution_y
        for column_index in columns:
            left = definition.west + column_index * definition.resolution_x
            right = left + definition.resolution_x
            cell = box(left, bottom, right, top)
            if not cell.intersects(geometry):
                continue
            cell_index = row_index * definition.width + column_index
            rows.append(
                {
                    "source_id": definition.source_id,
                    "source_grid_version": definition.source_grid_version,
                    "source_grid_id": f"row={row_index},col={column_index}",
                    "cell_index": cell_index,
                    "row_index": row_index,
                    "column_index": column_index,
                    "geometry_wkb": shapely.to_wkb(
                        cell, byte_order=1, output_dimension=2
                    ),
                    "bbox_wgs84": [left, bottom, right, top],
                    "centroid_lon": round((left + right) / 2, 10),
                    "centroid_lat": round((bottom + top) / 2, 10),
                    "resolution_x": definition.resolution_x,
                    "resolution_y": definition.resolution_y,
                    "crs": definition.crs,
                    "scope_ids": [scope_id],
                }
            )
    return sorted(rows, key=lambda item: int(item["cell_index"]))


def _scope_id(name: str, geometry: BaseGeometry) -> str:
    normalized = shapely.normalize(geometry)
    digest = sha256(shapely.to_wkb(normalized, byte_order=1)).hexdigest()[:16]
    return f"{name}-{digest}"


def _load_geometry(root: Path, configured: Path) -> BaseGeometry:
    path = configured if configured.is_absolute() else root / configured
    if not path.is_file():
        raise FileNotFoundError(f"weather grid scope is missing: {path}")
    frame = gpd.read_parquet(path, columns=["geometry"])
    if frame.empty or frame.crs is None:
        raise ValueError(f"weather grid scope must be georeferenced: {path}")
    geographic = frame if frame.crs.to_epsg() == 4326 else frame.to_crs("EPSG:4326")
    geometry = geographic.geometry.union_all()
    if geometry.is_empty:
        raise ValueError(f"weather grid scope is empty: {path}")
    return geometry


class WeatherGridRegistry:
    """Register the national provider grid and mark cells used by the current AOI."""

    IDENTIFIER = ("silver", "source_grid")
    KEY_FIELDS = ("source_id", "source_grid_version", "source_grid_id")
    _IMMUTABLE_FIELDS = (
        "source_id",
        "source_grid_version",
        "source_grid_id",
        "cell_index",
        "row_index",
        "column_index",
        "geometry_wkb",
        "bbox_wgs84",
        "centroid_lon",
        "centroid_lat",
        "resolution_x",
        "resolution_y",
        "crs",
    )

    def __init__(self, writer: _GridWriter, root: Path) -> None:
        self.writer = writer
        self.root = root

    def ensure_grid(
        self,
        *,
        definition: GridDefinition,
        grid_scope_path: Path,
        aoi_path: Path,
        spatial_scope_name: str,
    ) -> GridRegistration:
        national = _load_geometry(self.root, grid_scope_path)
        local = _load_geometry(self.root, aoi_path)
        national_scope_id = _scope_id("vietnam-hydrological", national)
        local_scope_id = _scope_id(spatial_scope_name, local)
        rows = build_grid_rows(definition, national, national_scope_id)
        local_ids = {
            str(row["source_grid_id"])
            for row in build_grid_rows(definition, local, local_scope_id)
        }
        for row in rows:
            if row["source_grid_id"] in local_ids:
                row["scope_ids"] = sorted({national_scope_id, local_scope_id})

        existing = self.writer.get_keyed_rows(
            self.IDENTIFIER,
            {
                "source_id": definition.source_id,
                "source_grid_version": definition.source_grid_version,
            },
        )
        existing_by_id = {str(row["source_grid_id"]): row for row in existing}
        incoming_ids = {str(row["source_grid_id"]) for row in rows}
        if set(existing_by_id) - incoming_ids:
            raise ValueError("existing source grid extends outside its registered definition")
        changed: list[dict[str, object]] = []
        for row in rows:
            stored = existing_by_id.get(str(row["source_grid_id"]))
            if stored is not None and any(
                stored[field] != row[field] for field in self._IMMUTABLE_FIELDS
            ):
                raise ValueError(
                    f"source grid cell has changed definition: {row['source_grid_id']}"
                )
            if stored is not None:
                row["scope_ids"] = sorted(
                    {*stored.get("scope_ids", []), *row["scope_ids"]}
                )
            if stored != row:
                changed.append(row)

        for offset in range(0, len(changed), 500):
            self.writer.upsert_keyed_rows(
                self.IDENTIFIER, self.KEY_FIELDS, changed[offset : offset + 500]
            )
        current = {
            str(row["source_grid_id"]): int(row["cell_index"])
            for row in rows
            if local_scope_id in row["scope_ids"]
        }
        if not current:
            raise ValueError("current weather AOI does not intersect the provider grid")
        return GridRegistration(
            source_grid_version=definition.source_grid_version,
            scope_id=local_scope_id,
            cell_index_by_grid_id=current,
        )
