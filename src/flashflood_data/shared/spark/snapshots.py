"""Pinned Iceberg snapshot reads and deterministic incremental build decisions."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from flashflood_data.shared.spark.models import BuildSignature, PinnedSnapshot

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _segments(identifier: str, expected: tuple[int, ...]) -> tuple[str, ...]:
    parts = tuple(identifier.split("."))
    if len(parts) not in expected or any(_IDENTIFIER.fullmatch(part) is None for part in parts):
        raise ValueError(f"unsafe Iceberg table identifier: {identifier}")
    return parts


def _row_value(row: object, name: str) -> object:
    if isinstance(row, Mapping):
        return row[name]
    try:
        return row[name]  # type: ignore[index]
    except (KeyError, TypeError):
        return getattr(row, name)


@dataclass(frozen=True)
class BuildRecord:
    pipeline_run_id: str
    status: str
    build_signature: str
    completed_outputs: tuple[str, ...]


@dataclass(frozen=True)
class IncrementalBuildPlan:
    action: Literal["build", "repair", "skip"]
    build_signature: str
    input_snapshots: tuple[PinnedSnapshot, ...]
    completed_outputs: tuple[str, ...]
    missing_outputs: tuple[str, ...]
    prior_run_id: str | None = None


def plan_incremental_build(
    *,
    snapshots: Sequence[PinnedSnapshot],
    config_hash: str,
    policy_id: str,
    algorithm_version: str,
    contract_version: str,
    required_outputs: Sequence[str],
    history: Sequence[BuildRecord],
) -> IncrementalBuildPlan:
    required = tuple(dict.fromkeys(required_outputs))
    if not required or len(required) != len(tuple(required_outputs)):
        raise ValueError("required output tables must be non-empty and unique")
    for output in required:
        _segments(output, (2, 3))
    signature = BuildSignature.create(
        snapshots=snapshots,
        config_hash=config_hash,
        policy_id=policy_id,
        algorithm_version=algorithm_version,
        contract_version=contract_version,
    ).value
    matching = [row for row in history if row.build_signature == signature]
    completed = tuple(
        output
        for output in required
        if any(output in row.completed_outputs for row in matching)
    )
    missing = tuple(output for output in required if output not in completed)
    prior = matching[-1].pipeline_run_id if matching else None
    if not missing:
        action: Literal["build", "repair", "skip"] = "skip"
    elif completed:
        action = "repair"
    else:
        action = "build"
    return IncrementalBuildPlan(
        action=action,
        build_signature=signature,
        input_snapshots=tuple(sorted(snapshots)),
        completed_outputs=completed,
        missing_outputs=missing,
        prior_run_id=prior,
    )


class SparkSnapshotManager:
    """Resolve current Iceberg snapshot IDs and read only those immutable versions."""

    def __init__(self, catalog_name: str) -> None:
        if _IDENTIFIER.fullmatch(catalog_name) is None:
            raise ValueError("unsafe Spark catalog identifier")
        self.catalog_name = catalog_name

    def qualify(self, table: str) -> str:
        parts = _segments(table, (2, 3))
        return ".".join(parts if len(parts) == 3 else (self.catalog_name, *parts))

    def pin(self, spark: Any, tables: Sequence[str]) -> tuple[PinnedSnapshot, ...]:
        unique = tuple(dict.fromkeys(tables))
        if not unique or len(unique) != len(tuple(tables)):
            raise ValueError("input tables must be non-empty and unique")
        for table in unique:
            _segments(table, (2,))
        pinned: list[PinnedSnapshot] = []
        for table in unique:
            qualified = self.qualify(table)
            rows = spark.sql(
                f"SELECT snapshot_id FROM {qualified}.snapshots "
                "ORDER BY committed_at DESC LIMIT 1"
            ).collect()
            if not rows:
                raise ValueError(f"Iceberg input table has no snapshot: {table}")
            pinned.append(
                PinnedSnapshot(
                    ".".join(_segments(table, (2,))[-2:]),
                    int(_row_value(rows[0], "snapshot_id")),
                )
            )
        return tuple(sorted(pinned))

    def read(self, spark: Any, snapshot: PinnedSnapshot) -> Any:
        qualified = self.qualify(snapshot.table)
        return spark.read.option("snapshot-id", str(snapshot.snapshot_id)).table(qualified)

    def history(self, spark: Any, job_name: str) -> tuple[BuildRecord, ...]:
        """Load prior build signatures and durable output refs from Meta."""
        if _IDENTIFIER.fullmatch(job_name) is None:
            raise ValueError("unsafe Spark job name")
        runs = self.qualify("meta.pipeline_runs")
        refs = self.qualify("meta.table_snapshot_ref")
        rows = spark.sql(
            "SELECT r.pipeline_run_id, r.status, r.metrics_json, s.table_name "
            f"FROM {runs} AS r LEFT JOIN {refs} AS s "
            "ON r.pipeline_run_id = s.pipeline_run_id AND s.role = 'output' "
            f"WHERE r.job_name = '{job_name}' "
            "ORDER BY r.started_at, r.pipeline_run_id, s.table_name"
        ).collect()

        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            run_id = str(_row_value(row, "pipeline_run_id"))
            metrics_value = _row_value(row, "metrics_json")
            if not isinstance(metrics_value, str):
                continue
            try:
                metrics = json.loads(metrics_value)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid metrics_json for pipeline run {run_id}") from error
            signature = metrics.get("build_signature") if isinstance(metrics, dict) else None
            if not isinstance(signature, str) or not signature:
                continue
            item = grouped.setdefault(
                run_id,
                {
                    "status": str(_row_value(row, "status")),
                    "build_signature": signature,
                    "outputs": [],
                },
            )
            table = _row_value(row, "table_name")
            if table is not None:
                output = ".".join(_segments(str(table), (2,))[-2:])
                if output not in item["outputs"]:
                    item["outputs"].append(output)
        return tuple(
            BuildRecord(
                pipeline_run_id=run_id,
                status=str(item["status"]),
                build_signature=str(item["build_signature"]),
                completed_outputs=tuple(item["outputs"]),
            )
            for run_id, item in grouped.items()
        )

    def plan(
        self,
        spark: Any,
        *,
        job_name: str,
        input_tables: Sequence[str],
        config_hash: str,
        policy_id: str,
        algorithm_version: str,
        contract_version: str,
        required_outputs: Sequence[str],
    ) -> IncrementalBuildPlan:
        """Pin current inputs and reconcile one deterministic build against Meta."""
        snapshots = self.pin(spark, input_tables)
        history = self.history(spark, job_name)
        return plan_incremental_build(
            snapshots=snapshots,
            config_hash=config_hash,
            policy_id=policy_id,
            algorithm_version=algorithm_version,
            contract_version=contract_version,
            required_outputs=required_outputs,
            history=history,
        )
