"""Strict composition and publication of the selected-L10 static profile."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from flashflood_data.static.features.spatial import checked_basins
from flashflood_data.storage.atomic import atomic_target

_REQUIRED_TASK15_GROUPS = frozenset({"terrain", "soil", "landcover", "hydrology"})
_ALLOWED_PROFILE_GROUPS = _REQUIRED_TASK15_GROUPS | {"population"}
_FORBIDDEN_STATIC_FIELD_TOKENS = frozenset({"event", "label", "outcome", "status"})


def _checked_feature_table(name: str, table: pd.DataFrame, basin_ids: set[int]) -> pd.DataFrame:
    if "HYBAS_ID" not in table.columns:
        raise ValueError(f"feature table {name} is missing required HYBAS_ID column")
    result = table.copy()
    ids = pd.to_numeric(result["HYBAS_ID"], errors="raise")
    numeric = ids.to_numpy(dtype="float64")
    if not np.isfinite(numeric).all():
        raise ValueError(f"feature table {name} has nonfinite HYBAS_ID values")
    if not np.equal(numeric, np.floor(numeric)).all():
        raise ValueError(f"feature table {name} HYBAS_ID values must be exact integers")
    if ids.duplicated().any():
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
