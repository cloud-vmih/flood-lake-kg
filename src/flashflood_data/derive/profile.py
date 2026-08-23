"""Strict composition and publication of the selected-L10 static profile."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from flashflood_data.catalog import sha256_file
from flashflood_data.derive._spatial import checked_basins
from flashflood_data.derive.mappings import (
    map_subbasin_commune,
    map_subbasin_lines,
    map_subbasin_points,
)
from flashflood_data.derive.population import map_subbasin_population
from flashflood_data.io_atomic import atomic_target
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus

_REQUIRED_TASK15_GROUPS = frozenset({"terrain", "soil", "landcover", "hydrology"})
_ALLOWED_PROFILE_GROUPS = _REQUIRED_TASK15_GROUPS | {"population"}
_FORBIDDEN_STATIC_FIELD_TOKENS = frozenset({"event", "label", "outcome", "status"})


def _checked_feature_table(name: str, table: pd.DataFrame, basin_ids: set[int]) -> pd.DataFrame:
    if "HYBAS_ID" not in table.columns:
        raise ValueError(f"feature table {name} is missing required HYBAS_ID column")
    result = table.copy()
    ids = pd.to_numeric(result["HYBAS_ID"], errors="raise")
    if ids.isna().any() or ids.duplicated().any():
        raise ValueError(f"feature table {name} has duplicate HYBAS_ID values")
    result["HYBAS_ID"] = ids.astype("int64")
    unexpected = set(result.HYBAS_ID) - basin_ids
    if unexpected:
        raise ValueError(f"feature table {name} has keys outside selected basins")
    if name in _REQUIRED_TASK15_GROUPS:
        missing = basin_ids - set(result.HYBAS_ID)
        if missing:
            raise ValueError(f"feature table {name} is missing required basin keys")
    return result.sort_values("HYBAS_ID", kind="stable").reset_index(drop=True)


def _field_tokens(field: object) -> set[str]:
    return set(str(field).casefold().replace("-", "_").split("_"))


def _canonical_table(table: pd.DataFrame) -> dict[str, object]:
    ordered = table.sort_values("HYBAS_ID", kind="stable").reset_index(drop=True)
    return {
        "columns": list(ordered.columns),
        "records_json": ordered.to_json(orient="records", date_format="iso", force_ascii=False),
        "provenance": dict(sorted(ordered.attrs.items())),
    }


def _canonical_basins(selected: gpd.GeoDataFrame) -> dict[str, object]:
    values = pd.DataFrame(selected.drop(columns="geometry")).copy()
    values["geometry_wkb_hex"] = selected.geometry.map(lambda geometry: geometry.wkb_hex)
    values = values.sort_values("HYBAS_ID", kind="stable").reset_index(drop=True)
    return {
        "crs": selected.crs.to_string(),
        "columns": list(values.columns),
        "records_json": values.to_json(orient="records", date_format="iso", force_ascii=False),
    }


def _fingerprint(selected: gpd.GeoDataFrame, tables: Mapping[str, pd.DataFrame]) -> str:
    payload = {
        "basins": _canonical_basins(selected),
        "feature_tables": {name: _canonical_table(table) for name, table in sorted(tables.items())},
        "profile_config": {
            "allowed_groups": sorted(_ALLOWED_PROFILE_GROUPS),
            "output_crs": "EPSG:4326",
            "required_task15_groups": sorted(_REQUIRED_TASK15_GROUPS),
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def assemble_static_profile(
    basins: gpd.GeoDataFrame, feature_tables: Mapping[str, pd.DataFrame], run_id: str
) -> gpd.GeoDataFrame:
    """Left-join validated static evidence without ever changing selected L10 cardinality."""
    selected = checked_basins(basins)
    unknown_groups = sorted(set(feature_tables) - _ALLOWED_PROFILE_GROUPS)
    if unknown_groups:
        raise ValueError(
            "feature table group is not an allowed static profile group: "
            + ", ".join(unknown_groups)
        )
    missing_groups = sorted(_REQUIRED_TASK15_GROUPS - set(feature_tables))
    if missing_groups:
        raise ValueError(f"missing required Task 15 feature tables: {', '.join(missing_groups)}")
    basin_ids = {int(identifier) for identifier in selected.HYBAS_ID}
    checked = {
        name: _checked_feature_table(name, table, basin_ids)
        for name, table in feature_tables.items()
    }
    forbidden_fields = sorted(
        {
            str(column)
            for table in checked.values()
            for column in table.columns
            if _field_tokens(column) & _FORBIDDEN_STATIC_FIELD_TOKENS
        }
        - {"HYBAS_ID"}
    )
    if forbidden_fields:
        raise ValueError(
            "feature tables contain forbidden event/label/outcome fields: "
            + ", ".join(forbidden_fields)
        )
    profile = selected.copy()
    quality: dict[int, dict[str, bool]] = {int(identifier): {} for identifier in profile.HYBAS_ID}
    for name, table in checked.items():
        conflicting = (set(profile.columns) & set(table.columns)) - {"HYBAS_ID"}
        if conflicting:
            raise ValueError(
                f"feature table {name} conflicts with profile columns: {', '.join(sorted(conflicting))}"
            )
        profile = profile.merge(table, on="HYBAS_ID", how="left", validate="one_to_one")
        if name not in _REQUIRED_TASK15_GROUPS:
            observed = {int(identifier) for identifier in table.HYBAS_ID}
            for identifier in basin_ids - observed:
                quality[identifier][f"missing_{name}_observation"] = True
    asset_ids = {
        name: list(table.attrs.get("source_asset_ids", []))
        for name, table in sorted(checked.items())
    }
    profile["pipeline_run_id"] = run_id
    profile["dependency_fingerprint"] = _fingerprint(selected, checked)
    profile["feature_group_source_asset_ids_json"] = json.dumps(
        asset_ids, sort_keys=True, default=str
    )
    profile["quality_flags_json"] = profile.HYBAS_ID.map(
        lambda identifier: json.dumps(quality[int(identifier)], sort_keys=True)
    )
    return gpd.GeoDataFrame(profile, geometry="geometry", crs=selected.crs).to_crs("EPSG:4326")


def write_static_profile(profile: gpd.GeoDataFrame, output: Path) -> Path:
    """Atomically publish the EPSG:4326 static feature GeoParquet product."""
    if profile.crs is None or profile.crs.to_epsg() != 4326:
        raise ValueError("static profile must be EPSG:4326 before publication")
    with atomic_target(output) as partial:
        profile.to_parquet(partial, index=False)
    return output


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
    bridge_id: str = "bridge_id"
    facility_id: str = "facility_id"
    settlement_id: str = "settlement_id"
    source_asset_ids: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


def _empty_map(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


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
