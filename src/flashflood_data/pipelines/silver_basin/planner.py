"""Pure revision selection and incremental planning for Silver basin topology."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Literal

from flashflood_data.shared.spark.models import PinnedSnapshot
from flashflood_data.shared.spark.snapshots import BuildRecord, plan_incremental_build

OUTPUT_TABLES = ("silver.dim_basin", "silver.basin_edge")


def _text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"basin revision record requires non-empty {field}")
    return value.strip()


@dataclass(frozen=True)
class SelectedBasinObjects:
    hydro_object_id: str
    atlas_object_id: str

    def __post_init__(self) -> None:
        if not self.hydro_object_id or not self.atlas_object_id:
            raise ValueError("selected basin object IDs must not be empty")
        if self.hydro_object_id == self.atlas_object_id:
            raise ValueError("HydroBASINS and BasinATLAS must use different objects")


def select_current_objects(
    *,
    bronze_objects: Sequence[Mapping[str, object]],
    source_objects: Sequence[Mapping[str, object]],
    hydro_source_id: str,
    atlas_source_id: str,
) -> SelectedBasinObjects:
    """Choose the newest available source revision that is present in pinned Bronze."""
    expected = {hydro_source_id, atlas_source_id}
    if len(expected) != 2 or any(not source_id.strip() for source_id in expected):
        raise ValueError("basin revision sources must be non-empty and distinct")
    parsed = {
        (_text(row, "source_id"), _text(row, "object_id")) for row in bronze_objects
    }
    candidates: dict[str, list[tuple[datetime, str]]] = {source_id: [] for source_id in expected}
    for row in source_objects:
        source_id = _text(row, "source_id")
        object_id = _text(row, "object_id")
        if source_id not in expected or (source_id, object_id) not in parsed:
            continue
        if row.get("status") != "available":
            continue
        retrieved_at = row.get("retrieved_at")
        if not isinstance(retrieved_at, datetime) or retrieved_at.tzinfo is None:
            raise ValueError("source object retrieved_at must be timezone-aware")
        candidates[source_id].append((retrieved_at, object_id))
    missing = tuple(sorted(source_id for source_id, rows in candidates.items() if not rows))
    if missing:
        raise ValueError(f"no parsed Bronze revision is available for sources: {missing}")
    selected = {
        source_id: max(rows, key=lambda item: (item[0], item[1]))[1]
        for source_id, rows in candidates.items()
    }
    return SelectedBasinObjects(
        hydro_object_id=selected[hydro_source_id],
        atlas_object_id=selected[atlas_source_id],
    )


@dataclass(frozen=True)
class BasinExecutionPlan:
    action: Literal["build", "repair", "skip"]
    build_signature: str
    pipeline_run_id: str
    snapshot: PinnedSnapshot
    hydro_object_id: str
    atlas_object_id: str
    basin_ids: tuple[str, ...]
    completed_outputs: tuple[str, ...]
    missing_outputs: tuple[str, ...]
    prior_run_id: str | None = None

    def to_document(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> BasinExecutionPlan:
        raw = dict(document)
        snapshot = raw.get("snapshot")
        if not isinstance(snapshot, Mapping):
            raise TypeError("basin execution plan snapshot must be a mapping")
        raw["snapshot"] = PinnedSnapshot(**snapshot)
        raw["completed_outputs"] = tuple(raw.get("completed_outputs", ()))
        raw["missing_outputs"] = tuple(raw.get("missing_outputs", ()))
        raw["basin_ids"] = tuple(raw.get("basin_ids", ()))
        return cls(**raw)  # type: ignore[arg-type]


def _selected_config_hash(
    base_config_hash: str,
    hydro_object_id: str,
    atlas_object_id: str,
    basin_ids: tuple[str, ...],
) -> str:
    payload = {
        "atlas_object_id": atlas_object_id,
        "base_config_hash": base_config_hash,
        "hydro_object_id": hydro_object_id,
        "basin_ids": basin_ids,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_basin_execution_plan(
    *,
    snapshot: PinnedSnapshot,
    base_config_hash: str,
    policy_id: str,
    algorithm_version: str,
    contract_version: str,
    hydro_object_id: str,
    atlas_object_id: str,
    basin_ids: tuple[str, ...],
    history: Sequence[BuildRecord],
) -> BasinExecutionPlan:
    """Build or repair only outputs missing for the same selected revisions."""
    selected_hash = _selected_config_hash(
        base_config_hash,
        hydro_object_id,
        atlas_object_id,
        basin_ids,
    )
    if not basin_ids or any(not basin_id.isdecimal() for basin_id in basin_ids):
        raise ValueError("Silver basin scope requires numeric basin IDs")
    incremental = plan_incremental_build(
        snapshots=(snapshot,),
        config_hash=selected_hash,
        policy_id=policy_id,
        algorithm_version=algorithm_version,
        contract_version=contract_version,
        required_outputs=OUTPUT_TABLES,
        history=history,
    )
    pipeline_run_id = f"silver-basin-{incremental.build_signature[:20]}"
    return BasinExecutionPlan(
        action=incremental.action,
        build_signature=incremental.build_signature,
        pipeline_run_id=pipeline_run_id,
        snapshot=snapshot,
        hydro_object_id=hydro_object_id,
        atlas_object_id=atlas_object_id,
        basin_ids=basin_ids,
        completed_outputs=incremental.completed_outputs,
        missing_outputs=incremental.missing_outputs,
        prior_run_id=incremental.prior_run_id,
    )
