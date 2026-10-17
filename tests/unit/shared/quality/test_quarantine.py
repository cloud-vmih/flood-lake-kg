from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from flashflood_data.shared.quality import QualityFinding
from flashflood_data.shared.quality.quarantine import (
    ObjectRejection,
    QuarantineRepository,
    RowRejection,
    make_rejection_id,
)


class FakeStore:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, dict[str, object]]] = {}
        self.snapshot = 0

    def upsert_quarantine_rows(self, identifier, key_fields, rows):
        assert key_fields == ("rejection_id",)
        target = self.rows.setdefault(identifier, {})
        for row in rows:
            target[str(row["rejection_id"])] = dict(row)
        self.snapshot += 1
        return self.snapshot


def test_rejection_id_ignores_pipeline_retry_run_id() -> None:
    identity = {
        "layer": "bronze",
        "target_dataset_id": "bronze.river_reach_raw",
        "object_id": "object-1",
        "source_record_key": "feature-1",
        "quality_policy_id": "policy-v1",
        "rule_ids": ("geometry_valid",),
    }

    assert make_rejection_id(**identity, pipeline_run_id="run-1") == make_rejection_id(
        **identity, pipeline_run_id="run-2"
    )


def test_repository_upserts_retry_to_one_logical_row() -> None:
    store = FakeStore()
    repository = QuarantineRepository(store)
    finding = QualityFinding(
        rule_id="geometry_valid",
        rule_version="v1",
        status="failed",
        severity="reject",
        scope_key="feature-1",
        observed_value={"is_valid": False},
    )
    rejection = RowRejection(
        pipeline_run_id="run-1",
        layer="bronze",
        target_dataset_id="bronze.river_reach_raw",
        object_id="object-1",
        source_record_key="feature-1",
        quality_policy_id="policy-v1",
        findings=(finding,),
        reason_codes=("invalid_geometry",),
        record={"id": "feature-1"},
        parser_version="v2",
        rejected_at=datetime(2026, 10, 11, tzinfo=UTC),
    )

    ids1, _ = repository.reject_rows([rejection])
    ids2, _ = repository.reject_rows([replace(rejection, pipeline_run_id="run-2")])

    assert ids1 == ids2
    assert len(store.rows[("quarantine", "rejected_rows")]) == 1
    stored = next(iter(store.rows[("quarantine", "rejected_rows")].values()))
    assert stored["pipeline_run_id"] == "run-2"


def test_object_rejection_requires_one_complete_input_identity() -> None:
    base = {
        "pipeline_run_id": "run-1",
        "layer": "silver",
        "target_dataset_id": "silver.basin_static_features",
        "scope_key": "build-1",
        "quality_policy_id": "silver-policy",
        "rule_id": "coverage_complete",
        "rule_version": "v1",
        "reason_code": "missing_input",
        "detail": {"count": 0},
    }

    with pytest.raises(ValueError, match="exactly one input identity"):
        ObjectRejection(**base)
    with pytest.raises(ValueError, match="input_table and input_snapshot_id"):
        ObjectRejection(**base, input_table="bronze.basin_polygon_raw")


def test_row_rejection_requires_record_evidence_and_rejects_secret_fields() -> None:
    repository = QuarantineRepository(FakeStore())
    base = {
        "pipeline_run_id": "run-1",
        "layer": "bronze",
        "target_dataset_id": "bronze.river_reach_raw",
        "object_id": "object-1",
        "source_record_key": "feature-1",
        "quality_policy_id": "policy-v1",
        "findings": (),
        "reason_codes": ("invalid",),
    }

    with pytest.raises(ValueError, match="record or record_uri"):
        RowRejection(**base)
    with pytest.raises(ValueError, match="credential"):
        repository.reject_rows([RowRejection(**base, record={"password": "secret"})])
