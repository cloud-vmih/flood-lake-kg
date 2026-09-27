from datetime import UTC, datetime, timedelta

import pytest

from flashflood_data.orchestration.weather.lifecycle import ObjectLifecycleStore


class MemoryStore:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}

    def get_meta_row(self, identifier, key):
        assert identifier == ("meta", "object_lifecycle")
        return self.rows.get(key["object_id"])

    def get_keyed_rows(self, identifier, key):
        assert identifier == ("meta", "object_lifecycle")
        return [
            row for row in self.rows.values() if all(row[name] == value for name, value in key.items())
        ]

    def upsert_meta_row(self, identifier, key_fields, row):
        assert identifier == ("meta", "object_lifecycle")
        assert key_fields == ("object_id",)
        self.rows[row["object_id"]] = dict(row)
        return len(self.rows)


def test_transient_object_becomes_eligible_only_with_complete_bronze_evidence() -> None:
    backend = MemoryStore()
    lifecycle = ObjectLifecycleStore(backend)
    published = datetime(2026, 9, 1, tzinfo=UTC)
    lifecycle.register("object-1", "transient_7d", published, "run-1")

    assert lifecycle.eligible(published + timedelta(days=8)) == ()
    lifecycle.mark_bronze_evidence(
        "object-1",
        snapshot_id=42,
        quality_status="passed",
        lineage_edge_id="edge-1",
        checked_at=published + timedelta(days=1),
    )

    eligible = lifecycle.eligible(published + timedelta(days=8))
    assert [row.object_id for row in eligible] == ["object-1"]
    assert backend.rows["object-1"]["storage_status"] == "eligible_for_expiry"


def test_lifecycle_expiry_and_delete_failure_preserve_evidence() -> None:
    backend = MemoryStore()
    lifecycle = ObjectLifecycleStore(backend)
    published = datetime(2026, 9, 1, tzinfo=UTC)
    lifecycle.register("object-1", "transient_7d", published, "run-1")
    lifecycle.mark_bronze_evidence(
        "object-1", 42, "passed", "edge-1", published + timedelta(days=1)
    )
    lifecycle.eligible(published + timedelta(days=8))
    failed_at = published + timedelta(days=8, minutes=1)

    lifecycle.mark_delete_failed("object-1", failed_at, "MinIO timeout")
    assert backend.rows["object-1"]["bronze_snapshot_id"] == 42
    assert backend.rows["object-1"]["storage_status"] == "delete_failed"
    lifecycle.mark_expired("object-1", failed_at + timedelta(minutes=1))
    assert backend.rows["object-1"]["storage_status"] == "expired"
    assert backend.rows["object-1"]["deleted_at"] == failed_at + timedelta(minutes=1)


def test_durable_object_never_becomes_expiry_candidate() -> None:
    backend = MemoryStore()
    lifecycle = ObjectLifecycleStore(backend)
    published = datetime(2026, 9, 1, tzinfo=UTC)
    lifecycle.register("object-1", "durable", published, "run-1")
    lifecycle.mark_bronze_evidence("object-1", 42, "passed", "edge-1", published)

    assert lifecycle.eligible(published + timedelta(days=365)) == ()
    with pytest.raises(ValueError, match="eligible"):
        lifecycle.mark_expired("object-1", published + timedelta(days=365))
