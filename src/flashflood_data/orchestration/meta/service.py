"""Write source, run, quality, snapshot and lineage records to Meta Iceberg tables."""

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from hashlib import sha256
from typing import Any

from flashflood_data.storage.iceberg_tables import IcebergTableStore


class MetaRecorder:
    """Small shared recording API used by landing and Bronze pipelines."""

    def __init__(self, store: IcebergTableStore, meta_namespace: str = "meta") -> None:
        self.store = store
        self.meta_namespace = meta_namespace

    def _register_immutable(
        self, identifier: tuple[str, str], key_fields: tuple[str, ...], row: Mapping[str, Any]
    ) -> int | None:
        key = {name: row[name] for name in key_fields}
        existing = self.store.get_meta_row(identifier, key)
        if existing is not None:
            if existing != dict(row):
                raise ValueError(f"conflicting immutable Meta registry entry: {identifier[1]}")
            return None
        return self.store.upsert_meta_row(identifier, key_fields, row)

    def register_source(self, row: Mapping[str, Any]) -> int | None:
        return self._register_immutable(
            (self.meta_namespace, "source_registry"), ("source_id", "source_version"), row
        )

    def register_dataset(self, row: Mapping[str, Any]) -> int | None:
        return self._register_immutable(
            (self.meta_namespace, "dataset_registry"), ("dataset_id", "contract_version"), row
        )

    def record_attempt(
        self,
        *,
        ingest_run_id: str,
        source_id: str,
        asset_id: str,
        attempt_no: int,
        request_fingerprint: str,
        status: str,
        started_at: datetime,
        ended_at: datetime | None = None,
        http_status: int | None = None,
        error_code: str | None = None,
    ) -> int:
        row = {
            "ingest_run_id": ingest_run_id,
            "source_id": source_id,
            "asset_id": asset_id,
            "attempt_no": attempt_no,
            "request_fingerprint": request_fingerprint,
            "http_status": http_status,
            "error_code": error_code,
            "started_at": started_at,
            "ended_at": ended_at,
            "status": status,
        }
        return self.record_attempts((row,))

    def record_attempts(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Persist one source's object attempts in one Iceberg commit."""
        requested = [dict(row) for row in rows]
        if not requested or any(
            row["attempt_no"] < 1
            or row["status"] not in {"running", "succeeded", "failed", "skipped"}
            for row in requested
        ):
            raise ValueError("invalid ingest attempt batch")
        return self.store.upsert_meta_rows(
            (self.meta_namespace, "ingest_attempts"),
            ("ingest_run_id", "source_id", "asset_id", "attempt_no"),
            requested,
        )

    def record_run(self, row: Mapping[str, Any]) -> int:
        """Persist the full run state, including start/finish and publish decision."""
        if row["published_at"] is not None and row["status"] != "succeeded":
            raise ValueError("only a succeeded run may be published")
        return self.store.upsert_meta_row(
            (self.meta_namespace, "pipeline_runs"), ("pipeline_run_id",), row
        )

    def record_quality(
        self,
        *,
        pipeline_run_id: str,
        dataset_id: str,
        rule_id: str,
        status: str,
        checked_at: datetime,
        check_phase: str = "post_commit",
        rule_version: str = "v1",
        scope_key: str = "all",
        severity: str = "fatal",
        observed_value_json: str | None = None,
        expected_value_json: str | None = None,
        failed_row_count: int | None = None,
        sample_uri: str | None = None,
        snapshot_table: str | None = None,
        snapshot_id: int | None = None,
    ) -> int:
        row = {
            "pipeline_run_id": pipeline_run_id,
            "check_phase": check_phase,
            "dataset_id": dataset_id,
            "rule_id": rule_id,
            "rule_version": rule_version,
            "scope_key": scope_key,
            "severity": severity,
            "status": status,
            "observed_value_json": observed_value_json,
            "expected_value_json": expected_value_json,
            "failed_row_count": failed_row_count,
            "sample_uri": sample_uri,
            "snapshot_table": snapshot_table,
            "snapshot_id": snapshot_id,
            "checked_at": checked_at,
        }
        return self.record_qualities((row,))

    def record_qualities(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Persist one dataset's object quality results in one Iceberg commit."""
        requested = [dict(row) for row in rows]
        if not requested:
            raise ValueError("quality result batch cannot be empty")
        for row in requested:
            if (row["snapshot_table"] is None) != (row["snapshot_id"] is None):
                raise ValueError("snapshot table and snapshot ID must be set together")
            if row["check_phase"] not in {"pre_commit", "post_commit", "pre_publish"}:
                raise ValueError("unsupported quality check phase")
            if row["status"] not in {"passed", "failed", "error", "skipped"}:
                raise ValueError("unsupported quality status")
        return self.store.upsert_meta_rows(
            (self.meta_namespace, "quality_results"),
            (
                "pipeline_run_id",
                "check_phase",
                "dataset_id",
                "rule_id",
                "rule_version",
                "scope_key",
            ),
            requested,
        )

    def record_snapshot_ref(
        self,
        *,
        pipeline_run_id: str,
        table_name: str,
        iceberg_snapshot_id: int,
        role: str,
        quality_status: str,
        created_at: datetime,
    ) -> int:
        if role not in {"input", "output"}:
            raise ValueError("snapshot role must be input or output")
        row = {
            "pipeline_run_id": pipeline_run_id,
            "table_name": table_name,
            "iceberg_snapshot_id": iceberg_snapshot_id,
            "role": role,
            "created_at": created_at,
            "quality_status": quality_status,
        }
        return self.store.upsert_meta_row(
            (self.meta_namespace, "table_snapshot_ref"),
            ("pipeline_run_id", "table_name", "iceberg_snapshot_id", "role"), row,
        )

    def record_lineage(
        self,
        *,
        pipeline_run_id: str,
        output_table: str,
        output_snapshot_id: int,
        transform_role: str,
        mapping_version: str | None,
        created_at: datetime,
        input_object_id: str | None = None,
        input_table: str | None = None,
        input_snapshot_id: int | None = None,
    ) -> str:
        raw = input_object_id is not None
        table = input_table is not None and input_snapshot_id is not None
        if raw == table or (input_table is None) != (input_snapshot_id is None):
            raise ValueError("lineage requires exactly one raw object or input snapshot")
        identity = {
            "pipeline_run_id": pipeline_run_id,
            "input_object_id": input_object_id,
            "input_table": input_table,
            "input_snapshot_id": input_snapshot_id,
            "output_table": output_table,
            "output_snapshot_id": output_snapshot_id,
            "transform_role": transform_role,
            "mapping_version": mapping_version,
        }
        edge_id = sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()
        self.store.upsert_meta_row(
            (self.meta_namespace, "lineage_edges"), ("lineage_edge_id",),
            {
                "lineage_edge_id": edge_id,
                "pipeline_run_id": pipeline_run_id,
                "input_kind": "raw_object" if raw else "iceberg_snapshot",
                "input_object_id": input_object_id,
                "input_table": input_table,
                "input_snapshot_id": input_snapshot_id,
                "output_table": output_table,
                "output_snapshot_id": output_snapshot_id,
                "transform_role": transform_role,
                "mapping_version": mapping_version,
                "created_at": created_at,
            },
        )
        return edge_id
