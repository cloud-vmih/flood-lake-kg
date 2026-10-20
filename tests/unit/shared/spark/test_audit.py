from __future__ import annotations

from datetime import UTC, datetime

import pytest

from flashflood_data.shared.spark.models import PinnedSnapshot, PublishedSnapshot


class _Meta:
    def __init__(self):
        self.events = []

    def record_run(self, row):
        self.events.append(("run", dict(row)))

    def record_snapshot_refs(self, rows):
        self.events.append(("snapshots", tuple(rows)))

    def record_qualities(self, rows):
        self.events.append(("quality", tuple(rows)))

    def record_lineages(self, rows):
        self.events.append(("lineage", tuple(rows)))


def _request():
    from flashflood_data.shared.spark.audit import SparkAuditRequest

    return SparkAuditRequest(
        pipeline_run_id="run-1",
        orchestrator_run_id="airflow-1",
        job_name="silver_basin_topology",
        build_signature="sig-1",
        config_hash="cfg-1",
        policy_id="policy-1",
        algorithm_version="topology-v1",
        input_snapshots=(PinnedSnapshot("bronze.basin_polygon_raw", 17),),
        input_object_ids=("hydro-object", "atlas-object"),
        required_outputs=("silver.dim_basin", "silver.basin_edge"),
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_audit_finalizes_success_only_after_refs_quality_and_lineage() -> None:
    from flashflood_data.shared.spark.audit import SparkAudit

    meta = _Meta()
    audit = SparkAudit(meta, now=lambda: datetime(2026, 1, 2, tzinfo=UTC))
    outputs = (
        PublishedSnapshot("silver.dim_basin", 91, 2, 1),
        PublishedSnapshot("silver.basin_edge", 92, 2, 1),
    )

    audit.start(_request())
    audit.finalize_success(_request(), outputs, quality_rows=({"rule_id": "ok"},))

    assert [event[0] for event in meta.events][-1] == "run"
    assert meta.events[-1][1]["status"] == "succeeded"
    snapshot_events = [event for event in meta.events if event[0] == "snapshots"]
    assert len(snapshot_events) == 1
    assert len(snapshot_events[0][1]) == 3
    assert sum(event[0] == "lineage" for event in meta.events) == 1
    lineage_rows = next(event[1] for event in meta.events if event[0] == "lineage")
    assert {row["input_object_id"] for row in lineage_rows} >= {
        "hydro-object",
        "atlas-object",
    }


def test_audit_refuses_success_when_a_required_output_is_missing() -> None:
    from flashflood_data.shared.spark.audit import SparkAudit

    meta = _Meta()
    audit = SparkAudit(meta)
    with pytest.raises(ValueError, match="missing required outputs"):
        audit.finalize_success(
            _request(),
            (PublishedSnapshot("silver.dim_basin", 91, 2, 1),),
        )
    assert not any(event[0] == "run" and event[1]["status"] == "succeeded" for event in meta.events)


def test_audit_refuses_success_without_durable_quality_results() -> None:
    from flashflood_data.shared.spark.audit import SparkAudit

    meta = _Meta()
    outputs = (
        PublishedSnapshot("silver.dim_basin", 91, 2, 1),
        PublishedSnapshot("silver.basin_edge", 92, 2, 1),
    )

    with pytest.raises(ValueError, match="quality"):
        SparkAudit(meta).finalize_success(_request(), outputs)

    assert meta.events == []


def test_audit_records_quarantine_snapshot_as_auxiliary_lineage() -> None:
    from flashflood_data.shared.spark.audit import SparkAudit

    meta = _Meta()
    outputs = (
        PublishedSnapshot("silver.dim_basin", 91, 2, 1),
        PublishedSnapshot("silver.basin_edge", 92, 2, 1),
    )
    quarantine = PublishedSnapshot("quarantine.rejected_rows", 93, 1, 1)

    SparkAudit(meta, now=lambda: datetime(2026, 1, 2, tzinfo=UTC)).finalize_success(
        _request(),
        outputs,
        auxiliary_outputs=(quarantine,),
        quality_rows=({"rule_id": "atlas_join", "severity": "reject"},),
    )

    snapshot_rows = next(rows for kind, rows in meta.events if kind == "snapshots")
    assert {row["table_name"] for row in snapshot_rows} == {
        "bronze.basin_polygon_raw",
        "silver.dim_basin",
        "silver.basin_edge",
        "quarantine.rejected_rows",
    }
    final_run = [row for kind, row in meta.events if kind == "run"][-1]
    assert final_run["output_row_count"] == 4
