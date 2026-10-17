from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from flashflood_data.shared.quality import (
    DqPublishCoordinator,
    DqPublishRequest,
    QualityEvaluation,
    QualityFinding,
    QuarantineRepository,
    RejectedRecord,
)

NOW = datetime(2026, 10, 11, tzinfo=UTC)


class FakeTargetWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, str], str, tuple[dict[str, object], ...]]] = []
        self.snapshot = 31

    def replace_object_rows(self, identifier, object_id, rows):
        call = (identifier, object_id, tuple(dict(row) for row in rows))
        if call not in self.calls:
            self.calls.append(call)
        return self.snapshot


class FakeQuarantineStore:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, dict[str, object]]] = {}
        self.snapshot = 100

    def upsert_quarantine_rows(self, identifier, key_fields, rows):
        target = self.rows.setdefault(identifier[1], {})
        for row in rows:
            target[str(row["rejection_id"])] = dict(row)
        self.snapshot += 1
        return self.snapshot


class FakeMeta:
    def __init__(self, fail_once_after_output: bool = False) -> None:
        self.runs: list[dict[str, object]] = []
        self.qualities: list[dict[str, object]] = []
        self.snapshots: list[dict[str, object]] = []
        self.lineages: list[dict[str, object]] = []
        self.fail_once_after_output = fail_once_after_output

    def record_qualities(self, rows):
        self.qualities = [dict(row) for row in rows]
        return 1

    def record_snapshot_ref(self, **row):
        if self.fail_once_after_output:
            self.fail_once_after_output = False
            raise RuntimeError("meta unavailable")
        self.snapshots.append(row)
        return 1

    def record_lineages(self, rows):
        self.lineages.extend(dict(row) for row in rows)
        return tuple(str(index) for index, _ in enumerate(rows))

    def record_run(self, row):
        self.runs.append(dict(row))
        return len(self.runs)


def _request() -> DqPublishRequest:
    return DqPublishRequest(
        pipeline_run_id="run-1",
        layer="bronze",
        target_dataset_id="bronze.river_reach_raw",
        object_id="object-1",
        policy_id="bronze-static-v1",
        parser_version="v2",
        lineage_inputs=({"input_object_id": "object-1", "transform_role": "source"},),
        started_at=NOW,
    )


def _coordinator(meta=None):
    writer = FakeTargetWriter()
    quarantine_store = FakeQuarantineStore()
    coordinator = DqPublishCoordinator(
        target_writer=writer,
        quarantine=QuarantineRepository(quarantine_store),
        meta=meta or FakeMeta(),
        now=lambda: NOW,
    )
    return coordinator, writer, quarantine_store


def test_fatal_records_quality_and_object_rejection_without_target_write() -> None:
    coordinator, writer, quarantine = _coordinator()
    evaluation = QualityEvaluation.from_parts(
        accepted_rows=[{"id": "feature-1"}],
        rejected_rows=[],
        findings=[
            QualityFinding(
                "object_readable", "failed", "fatal", {"readable": False}, scope_key="object-1"
            )
        ],
    )

    result = coordinator.publish_object(_request(), evaluation)

    assert result.status == "failed_data_quality"
    assert writer.calls == []
    assert len(quarantine.rows["rejected_objects"]) == 1


def test_mixed_result_publishes_valid_rows_and_rejections() -> None:
    coordinator, writer, quarantine = _coordinator()
    reject = QualityFinding(
        "geometry_valid", "failed", "reject", {"valid": False}, scope_key="bad"
    )
    warning = QualityFinding(
        "geometry_repaired", "failed", "warning", {"repaired": True}, scope_key="warn"
    )
    evaluation = QualityEvaluation.from_parts(
        accepted_rows=[
            {"id": "ok", "quality_status": "passed"},
            {"id": "warn", "quality_status": "warning"},
        ],
        rejected_rows=[RejectedRecord("bad", {"id": "bad"}, None, (reject,))],
        findings=[reject, warning],
    )

    result = coordinator.publish_object(_request(), evaluation)

    assert result.status == "succeeded_with_warning"
    assert result.accepted_count == 2
    assert result.rejected_count == 1
    assert result.warning_count == 1
    assert len(writer.calls) == 1
    assert len(quarantine.rows["rejected_rows"]) == 1


def test_retry_repairs_meta_after_output_commit_without_new_target_commit() -> None:
    meta = FakeMeta(fail_once_after_output=True)
    coordinator, writer, _ = _coordinator(meta)
    evaluation = QualityEvaluation.from_parts(
        accepted_rows=[{"id": "ok", "quality_status": "passed"}],
        rejected_rows=[],
        findings=[],
    )

    with pytest.raises(RuntimeError, match="meta unavailable"):
        coordinator.publish_object(_request(), evaluation)
    result = coordinator.publish_object(_request(), evaluation)

    assert result.status == "succeeded"
    assert len(writer.calls) == 1


def test_catalog_qualified_dataset_id_targets_the_layer_and_table() -> None:
    coordinator, writer, _ = _coordinator()
    evaluation = QualityEvaluation.from_parts(
        accepted_rows=[{"id": "ok", "quality_status": "passed"}],
        rejected_rows=[],
        findings=[],
    )

    coordinator.publish_object(
        replace(_request(), target_dataset_id="flood_lakehouse.bronze.river_reach_raw"),
        evaluation,
    )

    assert writer.calls[0][0] == ("bronze", "river_reach_raw")
