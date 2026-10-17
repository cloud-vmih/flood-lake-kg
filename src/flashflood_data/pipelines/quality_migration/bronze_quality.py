"""Provider-free planner and executor for existing Bronze quality migration."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Literal, Protocol

from flashflood_data.storage.iceberg_schemas import table_schema

MigrationAction = Literal[
    "audit_only",
    "rewrite_bronze",
    "reprocess_raw",
    "mark_raw_unavailable",
    "skip",
]


class MigrationBackend(Protocol):
    def bronze_rows(self, dataset_id: str, snapshot_id: int) -> Sequence[Mapping[str, object]]: ...

    def raw_object(self, object_id: str): ...

    def raw_exists(self, object_uri: str) -> bool: ...

    def checkpoint_exists(self, checkpoint_id: str) -> bool: ...

    def record_decisions(
        self, decisions: Sequence[MigrationDecision], run_id: str, mode: str
    ) -> None: ...

    def rewrite_object(
        self,
        dataset_id: str,
        object_id: str,
        rows: Sequence[Mapping[str, object]],
    ) -> None: ...

    def reprocess_object(self, dataset_id: str, object_id: str, run_id: str) -> None: ...


@dataclass(frozen=True)
class MigrationDecision:
    status: str
    dataset_id: str
    input_snapshot_id: int
    quality_policy_id: str
    object_id: str
    action: MigrationAction
    reason: str
    checkpoint_id: str
    raw_uri: str | None = None
    rows: tuple[Mapping[str, object], ...] = field(default_factory=tuple, repr=False)


@dataclass(frozen=True)
class MigrationSummary:
    run_id: str
    mode: str
    audited: int
    rewritten: int
    reprocessed: int
    unavailable: int
    skipped: int


def _identifier(dataset_id: str) -> tuple[str, str]:
    parts = dataset_id.split(".")
    if len(parts) == 2:
        namespace, name = parts
    elif len(parts) == 3:
        _catalog, namespace, name = parts
    else:
        raise ValueError("dataset_id must be namespace.table or catalog.namespace.table")
    if namespace != "bronze":
        raise ValueError("quality migration currently supports Bronze datasets only")
    table_schema((namespace, name))
    return namespace, name


def _checkpoint_id(
    dataset_id: str, snapshot_id: int, policy_id: str, object_id: str
) -> str:
    identity = f"{dataset_id}|{snapshot_id}|{policy_id}|{object_id}"
    return sha256(identity.encode()).hexdigest()


def _raw_uri(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        value = raw.get("object_uri")
    else:
        value = getattr(raw, "object_uri", None)
    return None if value is None else str(value)


class BronzeQualityMigration:
    """Plan from Iceberg/Raw evidence only; never contact an upstream provider."""

    def __init__(self, backend: MigrationBackend) -> None:
        self.backend = backend

    def plan(
        self,
        dataset_id: str,
        snapshot_id: int,
        policy_id: str,
        *,
        object_limit: int | None = None,
        force: bool = False,
    ) -> tuple[MigrationDecision, ...]:
        identifier = _identifier(dataset_id)
        if snapshot_id < 0 or not policy_id:
            raise ValueError("migration requires snapshot and policy identities")
        if object_limit is not None and object_limit < 1:
            raise ValueError("object_limit must be positive")
        rows_by_object: dict[str, list[Mapping[str, object]]] = defaultdict(list)
        for row in self.backend.bronze_rows(dataset_id, snapshot_id):
            object_id = str(row.get("object_id") or "")
            if not object_id:
                continue
            rows_by_object[object_id].append(dict(row))

        required = {
            field.name for field in table_schema(identifier) if not field.nullable
        }
        decisions: list[MigrationDecision] = []
        for object_id in sorted(rows_by_object):
            if object_limit is not None and len(decisions) >= object_limit:
                break
            rows = rows_by_object[object_id]
            checkpoint_id = _checkpoint_id(dataset_id, snapshot_id, policy_id, object_id)
            if not force and self.backend.checkpoint_exists(checkpoint_id):
                action: MigrationAction = "skip"
                reason = "checkpoint_already_completed"
                raw_uri = None
            else:
                missing_required = sorted(
                    name
                    for name in required
                    if any(row.get(name) is None for row in rows)
                )
                statuses = {str(row.get("quality_status") or "") for row in rows}
                needs_raw = identifier[1] == "raster_coverage" or bool(missing_required)
                raw = self.backend.raw_object(object_id) if needs_raw else None
                raw_uri = _raw_uri(raw)
                if needs_raw:
                    if raw_uri is not None and self.backend.raw_exists(raw_uri):
                        action = "reprocess_raw"
                        reason = (
                            "pixel_level_rules_require_raw"
                            if identifier[1] == "raster_coverage"
                            else "required_bronze_fields_missing"
                        )
                    else:
                        action = "mark_raw_unavailable"
                        reason = "raw_object_unavailable"
                elif statuses <= {"passed", "warning"}:
                    action = "audit_only"
                    reason = "existing_columns_support_quality_audit"
                elif statuses == {"flagged"} or statuses <= {"passed", "warning", "flagged"}:
                    action = "rewrite_bronze"
                    reason = "normalize_legacy_quality_status"
                else:
                    raw = self.backend.raw_object(object_id)
                    raw_uri = _raw_uri(raw)
                    if raw_uri is not None and self.backend.raw_exists(raw_uri):
                        action = "reprocess_raw"
                        reason = "unsupported_legacy_quality_state"
                    else:
                        action = "mark_raw_unavailable"
                        reason = "unsupported_quality_state_and_raw_unavailable"
            decisions.append(
                MigrationDecision(
                    status="planned" if action != "skip" else "skipped",
                    dataset_id=dataset_id,
                    input_snapshot_id=snapshot_id,
                    quality_policy_id=policy_id,
                    object_id=object_id,
                    action=action,
                    reason=reason,
                    checkpoint_id=checkpoint_id,
                    raw_uri=raw_uri,
                    rows=tuple(rows),
                )
            )
        return tuple(decisions)

    def execute(
        self,
        decisions: Sequence[MigrationDecision],
        run_id: str,
        *,
        mode: Literal["audit", "apply"] = "audit",
    ) -> MigrationSummary:
        if mode not in {"audit", "apply"}:
            raise ValueError("migration mode must be audit or apply")
        requested = tuple(decisions)
        rewritten = 0
        reprocessed = 0
        if mode == "apply":
            for decision in requested:
                if decision.action == "rewrite_bronze":
                    normalized = [
                        {
                            **dict(row),
                            "quality_status": (
                                "warning"
                                if row.get("quality_status") == "flagged"
                                else row.get("quality_status")
                            ),
                        }
                        for row in decision.rows
                    ]
                    self.backend.rewrite_object(
                        decision.dataset_id, decision.object_id, normalized
                    )
                    rewritten += 1
                elif decision.action == "reprocess_raw":
                    self.backend.reprocess_object(
                        decision.dataset_id, decision.object_id, run_id
                    )
                    reprocessed += 1
        self.backend.record_decisions(requested, run_id, mode)
        return MigrationSummary(
            run_id=run_id,
            mode=mode,
            audited=len(requested),
            rewritten=rewritten,
            reprocessed=reprocessed,
            unavailable=sum(item.action == "mark_raw_unavailable" for item in requested),
            skipped=sum(item.action == "skip" for item in requested),
        )
