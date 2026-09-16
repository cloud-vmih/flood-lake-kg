"""Task 16 mapping and static-profile publication orchestration."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from flashflood_data.catalog import sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.static.features.population import map_subbasin_population
from flashflood_data.static.features.profile import assemble_static_profile, write_static_profile
from flashflood_data.static.mappings.spatial import (
    map_subbasin_commune,
    map_subbasin_lines,
    map_subbasin_points,
)
from flashflood_data.storage.atomic import atomic_target


@dataclass(frozen=True)
class Task16MapInputs:
    """Resolved Task 16 dependencies supplied by later source-discovery composition."""

    basins: gpd.GeoDataFrame
    core: Any
    worldpop: Path
    feature_tables: Mapping[str, pd.DataFrame]
    communes: gpd.GeoDataFrame | None = None
    rivers: gpd.GeoDataFrame | None = None
    roads: gpd.GeoDataFrame | None = None
    bridges: gpd.GeoDataFrame | None = None
    facilities: gpd.GeoDataFrame | None = None
    settlements: gpd.GeoDataFrame | None = None
    bridge_id: str = "osm_id"
    facility_id: str = "osm_id"
    settlement_id: str = "osm_id"
    source_asset_ids: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


def _empty_map(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(dict.fromkeys(columns)))


def _set_provenance(table: pd.DataFrame, source_asset_ids: tuple[str, ...]) -> pd.DataFrame:
    result = table.copy()
    asset_json = json.dumps(list(source_asset_ids), sort_keys=True)
    if "source_asset_ids_json" in result:
        result["source_asset_ids_json"] = asset_json
    result.attrs["source_asset_ids"] = list(source_asset_ids)
    return result


def _write_map_table(table: pd.DataFrame, output: Path) -> Path:
    with atomic_target(output) as partial:
        table.to_parquet(partial, index=False)
    return output


def _task16_tables(inputs: Task16MapInputs) -> dict[str, pd.DataFrame]:
    """Build every Task 16 relationship table without inferring any live inputs."""
    tables: dict[str, pd.DataFrame] = {
        "commune": (
            map_subbasin_commune(inputs.basins, inputs.communes)
            if inputs.communes is not None
            else _empty_map(
                [
                    "HYBAS_ID",
                    "current_commune_code",
                    "intersection_area_km2",
                    "basin_fraction",
                    "commune_fraction",
                    "quality_flags_json",
                    "processing_crs",
                    "source_asset_ids_json",
                ]
            )
        ),
        "river": (
            map_subbasin_lines(inputs.basins, inputs.rivers, "HYRIV_ID")
            if inputs.rivers is not None
            else _empty_map(
                [
                    "HYBAS_ID",
                    "HYRIV_ID",
                    "intersected_length_km",
                    "boundary_case",
                    "quality_flags_json",
                    "processing_crs",
                    "source_asset_ids_json",
                ]
            )
        ),
        "road": (
            map_subbasin_lines(inputs.basins, inputs.roads, "segment_id")
            if inputs.roads is not None
            else _empty_map(
                [
                    "HYBAS_ID",
                    "segment_id",
                    "osm_id",
                    "intersected_length_km",
                    "boundary_case",
                    "quality_flags_json",
                    "processing_crs",
                    "source_asset_ids_json",
                ]
            )
        ),
        "bridge": (
            map_subbasin_lines(
                inputs.basins,
                inputs.bridges,
                inputs.bridge_id,
                include_relationship_geometry=True,
            )
            if inputs.bridges is not None
            else _empty_map(
                [
                    "HYBAS_ID",
                    inputs.bridge_id,
                    "osm_id",
                    "intersected_length_km",
                    "relationship_geometry_wkt",
                    "boundary_case",
                    "quality_flags_json",
                    "processing_crs",
                    "source_asset_ids_json",
                ]
            )
        ),
        "facility": (
            map_subbasin_points(inputs.basins, inputs.facilities, inputs.facility_id)
            if inputs.facilities is not None
            else _empty_map(
                [
                    "HYBAS_ID",
                    inputs.facility_id,
                    "relationship_type",
                    "tags_json",
                    "boundary_case",
                    "quality_flags_json",
                    "processing_crs",
                    "source_asset_ids_json",
                ]
            )
        ),
        "settlement": (
            map_subbasin_points(inputs.basins, inputs.settlements, inputs.settlement_id)
            if inputs.settlements is not None
            else _empty_map(
                [
                    "HYBAS_ID",
                    inputs.settlement_id,
                    "relationship_type",
                    "tags_json",
                    "boundary_case",
                    "quality_flags_json",
                    "processing_crs",
                    "source_asset_ids_json",
                ]
            )
        ),
        "population": map_subbasin_population(inputs.worldpop, inputs.basins, inputs.core),
    }
    return {
        name: _set_provenance(table, tuple(inputs.source_asset_ids.get(name, ())))
        for name, table in tables.items()
    }


def task16_map_handler(inputs: Task16MapInputs, output_dir: Path, *, owner_source_id: str):
    """Build a MAP-stage seam after Task 19 has resolved Task 16's dependencies.

    It deliberately has no input-discovery behavior.  This keeps mappings, WorldPop
    evidence, and the final profile in the distinct ``Stage.MAP`` handler required
    by R-033 while preserving Task 19's responsibility for default wiring.
    """

    def handler(pipeline: Any, stage: object, source_id: str, context: Any) -> list[AssetRecord]:
        if stage != "map":
            raise ValueError("Task 16 handler can only run at the map stage")
        if source_id != owner_source_id:
            return []
        mappings = _task16_tables(inputs)
        profile_inputs = {
            name: _set_provenance(table, tuple(inputs.source_asset_ids.get(name, ())))
            for name, table in inputs.feature_tables.items()
        }
        profile_inputs["population"] = mappings["population"]
        profile = assemble_static_profile(inputs.basins, profile_inputs, context.run_id)
        outputs = {
            **{
                f"map_subbasin_{name}": _write_map_table(
                    table, output_dir / f"map_subbasin_{name}.parquet"
                )
                for name, table in mappings.items()
            },
            "subbasin_static_feature": write_static_profile(
                profile, output_dir / "subbasin_static_feature.geoparquet"
            ),
        }
        source = pipeline.source_specs[source_id]
        all_dependency_ids = tuple(
            sorted(
                {
                    asset_id
                    for asset_ids in inputs.source_asset_ids.values()
                    for asset_id in asset_ids
                }
            )
        )
        return [
            AssetRecord(
                asset_id=f"task16-{name.replace('_', '-')}",
                source_id=source.source_id,
                source_version=source.version,
                kind=AssetKind.DERIVED,
                source_uri="generated:task16-basin-mappings-and-static-profile",
                storage_path=str(path),
                media_type=(
                    "application/vnd.apache.parquet"
                    if path.suffix == ".parquet"
                    else "application/vnd.apache.parquet; profile=geoparquet"
                ),
                size_bytes=path.stat().st_size,
                checksum=sha256_file(path),
                retrieved_at=datetime.now(UTC),
                license_id=source.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.DERIVED,
                metadata_json=json.dumps(
                    {
                        "dependency_asset_ids": (
                            all_dependency_ids
                            if name == "subbasin_static_feature"
                            else tuple(
                                inputs.source_asset_ids.get(name.removeprefix("map_subbasin_"), ())
                            )
                        )
                    },
                    sort_keys=True,
                ),
            )
            for name, path in outputs.items()
        ]

    return handler
