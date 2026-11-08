from datetime import UTC, datetime, timedelta

import pytest

from flashflood_data.pipelines.weather_ingest.lifecycle import ObjectLifecycleStore


class MemoryStore:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}
        self.batch_upserts = 0
        self.single_reads = 0
        self.batch_reads = 0

    def get_meta_row(self, identifier, key):
        assert identifier == ("meta", "object_lifecycle")
        self.single_reads += 1
        return self.rows.get(key["object_id"])

    def get_meta_rows(self, identifier, key_field, values):
        assert identifier == ("meta", "object_lifecycle")
        assert key_field == "object_id"
        self.batch_reads += 1
        return [self.rows[value] for value in values if value in self.rows]

    def get_keyed_rows(self, identifier, key):
        assert identifier == ("meta", "object_lifecycle")
        return [
            row
            for row in self.rows.values()
            if all(row[name] == value for name, value in key.items())
        ]

    def upsert_meta_row(self, identifier, key_fields, row):
        assert identifier == ("meta", "object_lifecycle")
        assert key_fields == ("object_id",)
        self.rows[row["object_id"]] = dict(row)
        return len(self.rows)

    def upsert_meta_rows(self, identifier, key_fields, rows):
        assert identifier == ("meta", "object_lifecycle")
        assert key_fields == ("object_id",)
        self.batch_upserts += 1
        for row in rows:
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


def test_warning_only_object_can_expire_but_rejected_slice_keeps_raw() -> None:
    backend = MemoryStore()
    lifecycle = ObjectLifecycleStore(backend)
    published = datetime(2026, 9, 1, tzinfo=UTC)
    lifecycle.register("warning-object", "transient_7d", published, "run-1")
    lifecycle.register("rejected-slice-object", "transient_7d", published, "run-1")
    lifecycle.mark_bronze_evidence(
        "warning-object", 42, "warning", "edge-warning", published + timedelta(days=1)
    )
    lifecycle.mark_bronze_evidence(
        "rejected-slice-object", 42, "failed", "edge-rejected", published + timedelta(days=1)
    )

    eligible = lifecycle.eligible(published + timedelta(days=8))

    assert [row.object_id for row in eligible] == ["warning-object"]
    assert backend.rows["rejected-slice-object"]["storage_status"] == "available"


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


def test_bronze_evidence_for_an_object_batch_uses_one_lifecycle_upsert() -> None:
    backend = MemoryStore()
    lifecycle = ObjectLifecycleStore(backend)
    published = datetime(2026, 9, 1, tzinfo=UTC)
    lifecycle.register("object-1", "transient_7d", published, "run-1")
    lifecycle.register("object-2", "transient_7d", published, "run-1")
    backend.single_reads = 0

    lifecycle.mark_bronze_evidence_many(
        [
            ("object-1", 42, "passed", "edge-1", published),
            ("object-2", 42, "passed", "edge-2", published),
        ]
    )

    assert backend.batch_upserts == 1
    assert backend.batch_reads == 1
    assert backend.single_reads == 0
    assert backend.rows["object-1"]["lineage_edge_id"] == "edge-1"
    assert backend.rows["object-2"]["lineage_edge_id"] == "edge-2"


def test_bronze_evidence_batch_rejects_missing_or_expired_without_writing() -> None:
    backend = MemoryStore()
    lifecycle = ObjectLifecycleStore(backend)
    published = datetime(2026, 9, 1, tzinfo=UTC)
    lifecycle.register("existing", "transient_7d", published, "run-1")

    with pytest.raises(KeyError, match="missing"):
        lifecycle.mark_bronze_evidence_many(
            [
                ("existing", 42, "passed", "edge-1", published),
                ("missing", 42, "passed", "edge-2", published),
            ]
        )
    assert backend.batch_upserts == 0
    assert backend.rows["existing"]["bronze_snapshot_id"] is None

    backend.rows["existing"]["storage_status"] = "expired"
    with pytest.raises(ValueError, match="expired"):
        lifecycle.mark_bronze_evidence_many(
            [("existing", 42, "passed", "edge-1", published)]
        )
    assert backend.batch_upserts == 0


def test_registration_batch_reads_once_and_preserves_existing_bronze_evidence() -> None:
    backend = MemoryStore()
    lifecycle = ObjectLifecycleStore(backend)
    published = datetime(2026, 10, 27, tzinfo=UTC)
    lifecycle.register("existing", "durable", published, "old-run")
    lifecycle.mark_bronze_evidence("existing", 42, "passed", "edge-1", published)
    backend.single_reads = 0

    lifecycle.register_many(
        [
            ("existing", "durable", published, "new-run"),
            ("new", "durable", published, "new-run"),
        ]
    )

    assert backend.batch_reads == 1
    assert backend.single_reads == 0
    assert backend.rows["existing"]["bronze_snapshot_id"] == 42
    assert backend.rows["existing"]["lineage_edge_id"] == "edge-1"
    assert backend.rows["new"]["quality_status"] == "pending"

    with pytest.raises(ValueError, match="conflicting lifecycle policy"):
        lifecycle.register_many([("existing", "transient_7d", published, "new-run")])
    assert backend.rows["existing"]["retention_class"] == "durable"
