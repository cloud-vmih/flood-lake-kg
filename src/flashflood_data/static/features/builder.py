"""Explicit composition point for Task 15's four static predictor tables."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd

from flashflood_data.catalog import sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.static.features.config import load_feature_config
from flashflood_data.static.features.hydrology import derive_hydrology_features
from flashflood_data.static.features.landcover import derive_landcover_fractions
from flashflood_data.static.features.soil import depth_weighted_soil
from flashflood_data.static.features.terrain import derive_terrain_features
from flashflood_data.storage.atomic import atomic_target


@dataclass(frozen=True)
class StaticPredictorInputs:
    """Fully resolved Task 15 dependencies supplied by later pipeline composition."""

    dem_path: Path
    soil_raster_paths: Mapping[tuple[str, str, str], Path]
    worldcover_path: Path
    rivers: gpd.GeoDataFrame
    basins: gpd.GeoDataFrame
    basinatlas: gpd.GeoDataFrame | None = None
    processing_crs: str = "EPSG:32648"
    source_asset_ids: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


def _validate_output(table: pd.DataFrame) -> None:
    if "HYBAS_ID" not in table.columns:
        raise ValueError("static predictor output is missing HYBAS_ID")
    ids = pd.to_numeric(table["HYBAS_ID"], errors="raise").to_numpy(dtype="float64")
    if not np.isfinite(ids).all():
        raise ValueError("static predictor output has nonfinite HYBAS_ID")
    if not np.equal(ids, np.floor(ids)).all():
        raise ValueError("static predictor output HYBAS_ID values must be exact integers")
    if pd.Series(ids).duplicated().any():
        raise ValueError("static predictor output has duplicate HYBAS_ID")
    numeric = table.select_dtypes(include=[np.number])
    non_id_columns = [column for column in numeric.columns if column != "HYBAS_ID"]
    if non_id_columns and np.isinf(numeric[non_id_columns].to_numpy(dtype="float64")).any():
        raise ValueError("static predictor output contains infinity")


def _write_table(table: pd.DataFrame, path: Path) -> Path:
    _validate_output(table)
    with atomic_target(path) as partial:
        table.to_parquet(partial, index=False)
    return path


def derive_static_predictor_tables(
    inputs: StaticPredictorInputs, output_dir: Path
) -> dict[str, Path]:
    """Derive and atomically publish four independently joinable Task 15 tables.

    This is intentionally parameterized rather than default-registered with the
    pipeline: resolving the coordinated source assets is Task 19 composition.
    """
    config = load_feature_config()
    tables = {
        "terrain": derive_terrain_features(inputs.dem_path, inputs.basins, inputs.processing_crs),
        "soil": depth_weighted_soil(
            inputs.soil_raster_paths, inputs.basins, bands_cm=config.soil_depth_bands_cm
        ),
        "landcover": derive_landcover_fractions(inputs.worldcover_path, inputs.basins),
        "hydrology": derive_hydrology_features(
            inputs.rivers,
            inputs.basins,
            dem_path=inputs.dem_path,
            basinatlas=inputs.basinatlas,
        ),
    }
    return {
        name: _write_table(table, output_dir / f"{name}_features.parquet")
        for name, table in tables.items()
    }


def task15_derive_handler(inputs: StaticPredictorInputs, output_dir: Path, *, owner_source_id: str):
    """Build a DERIVE seam handler after Task 19 has resolved all five dependencies.

    The owner source selects exactly one call in the pipeline's per-source loop;
    this avoids silently rewriting the same four tables for unrelated sources.
    """

    def handler(pipeline: Any, stage: object, source_id: str, context: Any) -> list[AssetRecord]:
        if stage != "derive":
            raise ValueError("Task 15 handler can only run at the derive stage")
        if source_id != owner_source_id:
            return []
        outputs = derive_static_predictor_tables(inputs, output_dir)
        source = pipeline.source_specs[source_id]
        return [
            AssetRecord(
                asset_id=f"task15-{name}-features",
                source_id=source.source_id,
                source_version=source.version,
                kind=AssetKind.DERIVED,
                source_uri="generated:task15-static-basin-predictors",
                storage_path=str(path),
                media_type="application/vnd.apache.parquet",
                size_bytes=path.stat().st_size,
                checksum=sha256_file(path),
                retrieved_at=datetime.now(UTC),
                license_id=source.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.DERIVED,
                metadata_json=json.dumps(
                    {"dependency_asset_ids": sorted(set(inputs.source_asset_ids.get(name, ())))},
                    sort_keys=True,
                ),
            )
            for name, path in outputs.items()
        ]

    return handler
