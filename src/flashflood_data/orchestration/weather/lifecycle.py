"""Guarded lifecycle transitions for transient dynamic Raw objects."""

from datetime import UTC, datetime, timedelta
from typing import Protocol

from flashflood_data.orchestration.weather.models import (
    ObjectLifecycleRow,
    RetentionClass,
)


class _LifecycleBackend(Protocol):
    def get_meta_row(self, identifier, key): ...

    def get_keyed_rows(self, identifier, key): ...

    def upsert_meta_row(self, identifier, key_fields, row): ...


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("lifecycle timestamps must be timezone-aware")
    return value.astimezone(UTC)


class ObjectLifecycleStore:
    """Persist current storage state while keeping source-object provenance immutable."""

    IDENTIFIER = ("meta", "object_lifecycle")
    KEY_FIELDS = ("object_id",)

    def __init__(self, store: _LifecycleBackend) -> None:
        self.store = store

    def _load(self, object_id: str) -> ObjectLifecycleRow:
        row = self.store.get_meta_row(self.IDENTIFIER, {"object_id": object_id})
        if row is None:
            raise KeyError(f"object lifecycle does not exist: {object_id}")
        return ObjectLifecycleRow.model_validate(row)

    def _save(self, row: ObjectLifecycleRow) -> int:
        return self.store.upsert_meta_row(
            self.IDENTIFIER, self.KEY_FIELDS, row.model_dump(mode="python")
        )

    def register(
        self,
        object_id: str,
        retention_class: RetentionClass,
        published_at: datetime,
        run_id: str,
    ) -> int:
        """Create the initial state, or reuse the exact retention policy on retry."""
        if not object_id or not run_id:
            raise ValueError("lifecycle registration requires object and run IDs")
        published_at = _utc(published_at)
        expires_at = (
            published_at + timedelta(days=7)
            if retention_class == "transient_7d"
            else None
        )
        existing = self.store.get_meta_row(
            self.IDENTIFIER, {"object_id": object_id}
        )
        if existing is not None:
            current = ObjectLifecycleRow.model_validate(existing)
            if (
                current.retention_class != retention_class
                or current.expires_at != expires_at
            ):
                raise ValueError(f"conflicting lifecycle policy for object: {object_id}")
            return self._save(current)
        return self._save(
            ObjectLifecycleRow(
                object_id=object_id,
                retention_class=retention_class,
                storage_status="available",
                expires_at=expires_at,
                bronze_snapshot_id=None,
                quality_status="pending",
                lineage_edge_id=None,
                deleted_at=None,
                last_checked_at=published_at,
                reason=None,
            )
        )

    def mark_bronze_evidence(
        self,
        object_id: str,
        snapshot_id: int,
        quality_status: str,
        lineage_edge_id: str,
        checked_at: datetime,
    ) -> int:
        """Attach the three proofs required before a transient payload may expire."""
        current = self._load(object_id)
        if current.storage_status == "expired":
            raise ValueError("cannot attach Bronze evidence to an expired object")
        if snapshot_id < 0 or not lineage_edge_id:
            raise ValueError("Bronze lifecycle evidence is incomplete")
        if quality_status not in {"passed", "warning", "failed"}:
            raise ValueError("unsupported lifecycle quality status")
        return self._save(
            current.model_copy(
                update={
                    "bronze_snapshot_id": snapshot_id,
                    "quality_status": quality_status,
                    "lineage_edge_id": lineage_edge_id,
                    "last_checked_at": _utc(checked_at),
                    "reason": None,
                }
            )
        )

    def eligible(self, now: datetime) -> tuple[ObjectLifecycleRow, ...]:
        """Return and mark transient rows whose time and Bronze evidence gates pass."""
        now = _utc(now)
        candidates = self.store.get_keyed_rows(
            self.IDENTIFIER, {"retention_class": "transient_7d"}
        )
        eligible: list[ObjectLifecycleRow] = []
        for document in candidates:
            row = ObjectLifecycleRow.model_validate(document)
            if (
                row.storage_status == "expired"
                or row.expires_at is None
                or row.expires_at > now
                or row.bronze_snapshot_id is None
                or row.quality_status != "passed"
                or not row.lineage_edge_id
            ):
                continue
            marked = row.model_copy(
                update={
                    "storage_status": "eligible_for_expiry",
                    "last_checked_at": now,
                    "reason": None,
                }
            )
            self._save(marked)
            eligible.append(marked)
        return tuple(sorted(eligible, key=lambda item: item.object_id))

    def mark_expired(self, object_id: str, deleted_at: datetime) -> int:
        """Confirm the Raw payload is absent after a successful object-store delete."""
        current = self._load(object_id)
        if current.storage_status not in {"eligible_for_expiry", "delete_failed"}:
            raise ValueError("object is not eligible for expiry")
        deleted_at = _utc(deleted_at)
        return self._save(
            current.model_copy(
                update={
                    "storage_status": "expired",
                    "deleted_at": deleted_at,
                    "last_checked_at": deleted_at,
                    "reason": None,
                }
            )
        )

    def mark_delete_failed(
        self, object_id: str, checked_at: datetime, reason: str
    ) -> int:
        """Record a retryable deletion failure without dropping Bronze evidence."""
        current = self._load(object_id)
        if current.storage_status not in {"eligible_for_expiry", "delete_failed"}:
            raise ValueError("object is not eligible for deletion")
        if not reason:
            raise ValueError("delete failure requires a reason")
        return self._save(
            current.model_copy(
                update={
                    "storage_status": "delete_failed",
                    "last_checked_at": _utc(checked_at),
                    "reason": reason,
                }
            )
        )
