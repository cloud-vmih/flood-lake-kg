"""Static quality checks split by subject."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa.models import CheckResult
from flashflood_data.static.qa.shared import (
    _check,
)


def _hydro_checks(paths: ProjectPaths) -> list[CheckResult]:
    l10_path = paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    hierarchy_path = paths.harmonized / "hydro" / "subbasin_hierarchy.parquet"
    try:
        l10 = gpd.read_parquet(l10_path)
        hierarchy = pd.read_parquet(hierarchy_path)
        from flashflood_data.static.harmonize.hydro import default_hydro_inputs

        inputs = default_hydro_inputs(paths)
        l9 = gpd.read_file(inputs.l9)
        l8 = gpd.read_file(inputs.l8)
    except Exception:  # noqa: BLE001
        return [
            _check(
                "hydro.l10.hierarchy",
                False,
                "fatal",
                "unique L10 IDs with valid L9/L8 parents",
                "missing or unreadable",
                "hydrological hierarchy evidence is required",
            ),
            _check(
                "hydro.l10.topology",
                False,
                "fatal",
                "topology references internal L10 IDs or declared scope exits",
                "missing or unreadable",
                "hydrological topology evidence is required",
            ),
        ]
    ids = pd.to_numeric(l10.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
    parent_fields = {"HYBAS_ID", "parent_l9_hybas_id", "parent_l8_hybas_id", "scope_exit"}
    l9_ids = set(
        pd.to_numeric(l9.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
        .dropna()
        .astype("int64")
    )
    l8_ids = set(
        pd.to_numeric(l8.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
        .dropna()
        .astype("int64")
    )
    l10_pfaf = {
        int(row.HYBAS_ID): str(row.PFAF_ID).removesuffix(".0")
        for row in l10.itertuples(index=False)
        if hasattr(row, "HYBAS_ID") and hasattr(row, "PFAF_ID")
    }
    l9_pfaf = {
        int(row.HYBAS_ID): str(row.PFAF_ID).removesuffix(".0")
        for row in l9.itertuples(index=False)
        if hasattr(row, "HYBAS_ID") and hasattr(row, "PFAF_ID")
    }
    l8_pfaf = {
        int(row.HYBAS_ID): str(row.PFAF_ID).removesuffix(".0")
        for row in l8.itertuples(index=False)
        if hasattr(row, "HYBAS_ID") and hasattr(row, "PFAF_ID")
    }
    parents_ok = parent_fields.issubset(hierarchy.columns)
    if parents_ok:
        for row in hierarchy.itertuples(index=False):
            child = int(row.HYBAS_ID)
            parent9, parent8 = int(row.parent_l9_hybas_id), int(row.parent_l8_hybas_id)
            parents_ok = (
                parents_ok
                and parent9 in l9_ids
                and parent8 in l8_ids
                and child in l10_pfaf
                and l10_pfaf[child].startswith(l9_pfaf.get(parent9, "!"))
                and l10_pfaf[child].startswith(l8_pfaf.get(parent8, "!"))
            )
    hierarchy_ok = (
        ids.notna().all()
        and ids.is_unique
        and set(ids.astype("int64"))
        == set(
            pd.to_numeric(hierarchy.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
            .dropna()
            .astype("int64")
        )
        and parents_ok
    )
    hierarchy_check = _check(
        "hydro.l10.hierarchy",
        hierarchy_ok,
        "fatal",
        "unique L10 IDs with valid L9/L8 parents",
        f"{len(ids)} L10 IDs",
        "L10 uniqueness and parent references",
    )
    if "NEXT_DOWN" not in l10:
        topology_ok, scope_exits = False, 0
    else:
        selected = set(ids.dropna().astype("int64"))
        declared = (
            set(
                pd.to_numeric(
                    hierarchy.loc[hierarchy.get("scope_exit", False).astype(bool), "HYBAS_ID"],
                    errors="coerce",
                )
                .dropna()
                .astype("int64")
            )
            if "scope_exit" in hierarchy
            else set()
        )
        next_down = pd.to_numeric(l10["NEXT_DOWN"], errors="coerce").fillna(-1).astype("int64")
        bad = [
            int(identifier)
            for identifier, downstream in zip(ids, next_down, strict=False)
            if downstream != 0 and downstream not in selected and int(identifier) not in declared
        ]
        topology_ok, scope_exits = not bad, len(declared)
    topology = _check(
        "hydro.l10.topology",
        topology_ok,
        "fatal",
        "topology references internal L10 IDs or declared scope exits",
        f"{scope_exits} scope exits",
        "L10 downstream topology",
    )
    exit_warning = _check(
        "hydro.l10.scope_exits",
        scope_exits == 0,
        "warning",
        "0 external downstream scope exits",
        scope_exits,
        "external downstream scope exits are explicitly retained",
    )
    return [hierarchy_check, topology, exit_warning]




def checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    del config
    return _hydro_checks(paths)
