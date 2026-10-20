"""Airflow-side planning, Spark submission, and durable audit for basin topology."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pyiceberg.expressions import And, EqualTo, In, Or

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.pipelines.silver_basin.config import (
    BasinTopologyConfig,
    load_basin_topology_config,
)
from flashflood_data.pipelines.silver_basin.planner import (
    BasinExecutionPlan,
    build_basin_execution_plan,
    select_current_objects,
)
from flashflood_data.pipelines.silver_basin.scope import BasinScope, read_basin_scope
from flashflood_data.shared.metadata.registry import load_dataset_registry
from flashflood_data.shared.metadata.service import MetaRecorder
from flashflood_data.shared.quality.policy import QualityPolicy, load_quality_policy
from flashflood_data.shared.spark.audit import SparkAudit, SparkAuditRequest
from flashflood_data.shared.spark.models import (
    PinnedSnapshot,
    SparkApplicationRequest,
    SparkApplicationResult,
)
from flashflood_data.shared.spark.snapshots import BuildRecord
from flashflood_data.shared.spark.submit import SparkGatewaySubmitter
from flashflood_data.storage.iceberg import load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import ObjectStore, PyArrowS3ObjectStore

_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10
_JOB_NAME = "silver_basin_topology"


def _distinct_bronze_objects(batches: Iterable[Any]) -> tuple[dict[str, str], ...]:
    """Keep only source/object pairs while streaming Bronze Arrow batches."""
    pairs: set[tuple[str, str]] = set()
    for batch in batches:
        sources = batch["source_id"].to_pylist()
        objects = batch["object_id"].to_pylist()
        for source_id, object_id in zip(sources, objects, strict=True):
            if not isinstance(source_id, str) or not isinstance(object_id, str):
                raise TypeError("Bronze basin source and object IDs must be strings")
            pairs.add((source_id, object_id))
    return tuple(
        {"source_id": source_id, "object_id": object_id} for source_id, object_id in sorted(pairs)
    )


def _manifest_key(uri: str) -> str:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError("basin output manifest must use s3://bucket/key")
    return f"{parsed.netloc}/{parsed.path.strip('/')}"


def _application_id(orchestrator_run_id: str, build_signature: str) -> str:
    digest = hashlib.sha256(f"{orchestrator_run_id}\u001f{build_signature}".encode()).hexdigest()[
        :20
    ]
    return f"silver-basin-{digest}"


def basin_application_request(
    plan: BasinExecutionPlan,
    *,
    orchestrator_run_id: str,
) -> SparkApplicationRequest:
    """Create the complete, secret-free request passed across the Spark boundary."""
    if plan.action == "skip" or not plan.missing_outputs:
        raise ValueError("a skipped basin plan has no Spark application")
    run_id = plan.pipeline_run_id
    return SparkApplicationRequest(
        application_id=_application_id(orchestrator_run_id, plan.build_signature),
        pipeline_run_id=run_id,
        pipeline=_JOB_NAME,
        entrypoint="flashflood_data.pipelines.silver_basin.spark_job",
        snapshots=(plan.snapshot,),
        build_signature=plan.build_signature,
        output_manifest_uri=(f"s3://staging/silver/{_JOB_NAME}/{run_id}/result.json"),
        arguments=(
            f"hydro_object_id={plan.hydro_object_id}",
            f"atlas_object_id={plan.atlas_object_id}",
            f"basin_ids={','.join(plan.basin_ids)}",
            f"outputs={','.join(plan.missing_outputs)}",
            f"orchestrator_run_id={orchestrator_run_id}",
        ),
    )


def validate_manifest(
    manifest: object,
    *,
    plan: BasinExecutionPlan,
    result: SparkApplicationResult,
) -> Mapping[str, object]:
    """Bind the untrusted object-store manifest to the submitted application."""
    if not isinstance(manifest, dict):
        raise TypeError("basin Spark output manifest must be a JSON object")
    identity = (
        manifest.get("application_id"),
        manifest.get("pipeline_run_id"),
        manifest.get("build_signature"),
    )
    expected = (result.application_id, plan.pipeline_run_id, plan.build_signature)
    if identity != expected:
        raise ValueError("basin Spark output manifest identity is invalid")
    outputs = manifest.get("outputs")
    if not isinstance(outputs, list):
        raise TypeError("basin Spark output manifest requires outputs")
    manifest_outputs = {
        (row.get("table"), row.get("snapshot_id"), row.get("row_count"), row.get("file_count"))
        for row in outputs
        if isinstance(row, dict)
    }
    result_outputs = {
        (item.table, item.snapshot_id, item.row_count, item.file_count)
        for item in result.published_snapshots
    }
    if len(manifest_outputs) != len(outputs) or manifest_outputs != result_outputs:
        raise ValueError("basin Spark output manifest snapshots do not match the result")
    return manifest


class BasinSilverRuntime:
    """Keep catalog discovery in Airflow and all row transforms inside Spark."""

    def __init__(
        self,
        *,
        catalog: Any,
        object_store: ObjectStore,
        submitter: Any,
        config: BasinTopologyConfig,
        policy: QualityPolicy,
        config_hash: str,
        scope: BasinScope | None = None,
        dataset_rows: Sequence[Mapping[str, object]] = (),
        now: Any | None = None,
    ) -> None:
        self.catalog = catalog
        self.object_store = object_store
        self.submitter = submitter
        self.config = config
        self.policy = policy
        self.config_hash = config_hash
        self.scope = scope
        self.dataset_rows = tuple(dict(row) for row in dataset_rows)
        self.now = now or (lambda: datetime.now(_UTC))
        self.table_store = IcebergTableStore(catalog) if hasattr(catalog, "load_table") else None

    @property
    def algorithm_version(self) -> str:
        return (
            f"{self.config.transform_version}:{self.config.topology_version}:"
            f"{self.config.revision_policy_version}"
        )

    def _history(self) -> tuple[BuildRecord, ...]:
        assert self.table_store is not None
        runs = self.table_store.ensure_table(("meta", "pipeline_runs"))
        refs = self.table_store.ensure_table(("meta", "table_snapshot_ref"))
        run_rows = (
            runs.scan(
                row_filter=EqualTo("job_name", _JOB_NAME),
                selected_fields=("pipeline_run_id", "status", "metrics_json"),
            )
            .to_arrow()
            .to_pylist()
        )
        run_ids = tuple(str(row["pipeline_run_id"]) for row in run_rows)
        ref_rows: Sequence[Mapping[str, object]] = ()
        if run_ids:
            run_filter = (
                EqualTo("pipeline_run_id", run_ids[0])
                if len(run_ids) == 1
                else In("pipeline_run_id", run_ids)
            )
            ref_rows = (
                refs.scan(
                    row_filter=And(run_filter, EqualTo("role", "output")),
                    selected_fields=("pipeline_run_id", "table_name"),
                )
                .to_arrow()
                .to_pylist()
            )
        outputs: dict[str, list[str]] = {run_id: [] for run_id in run_ids}
        for row in ref_rows:
            outputs[str(row["pipeline_run_id"])].append(str(row["table_name"]))
        history: list[BuildRecord] = []
        for row in run_rows:
            metrics_json = row.get("metrics_json")
            if not isinstance(metrics_json, str):
                continue
            try:
                metrics = json.loads(metrics_json)
            except json.JSONDecodeError as error:
                raise ValueError("invalid basin pipeline metrics_json") from error
            signature = metrics.get("build_signature") if isinstance(metrics, dict) else None
            if not isinstance(signature, str) or not signature:
                continue
            run_id = str(row["pipeline_run_id"])
            history.append(
                BuildRecord(
                    pipeline_run_id=run_id,
                    status=str(row["status"]),
                    build_signature=signature,
                    completed_outputs=tuple(dict.fromkeys(outputs[run_id])),
                )
            )
        return tuple(history)

    def plan(self) -> BasinExecutionPlan:
        """Pin Bronze and select the newest source objects already parsed into it."""
        if self.scope is None:
            raise ValueError("Silver basin scope was not loaded")
        bronze = self.catalog.load_table(("bronze", "basin_polygon_raw"))
        bronze.refresh()
        snapshot = bronze.current_snapshot()
        if snapshot is None:
            raise ValueError("bronze.basin_polygon_raw has no snapshot")
        pinned = PinnedSnapshot("bronze.basin_polygon_raw", int(snapshot.snapshot_id))
        source_filter = Or(
            EqualTo("source_id", self.config.hydro_source_id),
            EqualTo("source_id", self.config.atlas_source_id),
        )
        bronze_objects = _distinct_bronze_objects(
            bronze.scan(
                snapshot_id=pinned.snapshot_id,
                row_filter=source_filter,
                selected_fields=("object_id", "source_id"),
            ).to_arrow_batch_reader()
        )
        source_objects = self.catalog.load_table(("meta", "source_objects"))
        source_objects.refresh()
        source_rows = (
            source_objects.scan(
                row_filter=source_filter,
                selected_fields=("object_id", "source_id", "status", "retrieved_at"),
            )
            .to_arrow()
            .to_pylist()
        )
        selected = select_current_objects(
            bronze_objects=bronze_objects,
            source_objects=source_rows,
            hydro_source_id=self.config.hydro_source_id,
            atlas_source_id=self.config.atlas_source_id,
        )
        return build_basin_execution_plan(
            snapshot=pinned,
            base_config_hash=self.config_hash,
            policy_id=self.policy.policy_id,
            algorithm_version=self.algorithm_version,
            contract_version=self.config.contract_version,
            hydro_object_id=selected.hydro_object_id,
            atlas_object_id=selected.atlas_object_id,
            basin_ids=self.scope.basin_ids,
            history=self._history(),
        )

    def _ensure_output_tables(self) -> None:
        if self.table_store is None:
            raise TypeError("basin runtime catalog does not support Iceberg table creation")
        for identifier in (
            ("silver", "dim_basin"),
            ("silver", "basin_edge"),
            ("quarantine", "rejected_rows"),
        ):
            self.table_store.ensure_table(identifier)
        meta = MetaRecorder(self.table_store)
        required = {
            f"{self.catalog.name}.silver.dim_basin",
            f"{self.catalog.name}.silver.basin_edge",
            f"{self.catalog.name}.quarantine.rejected_rows",
        }
        selected = {str(row["dataset_id"]): row for row in self.dataset_rows}
        if self.dataset_rows and set(selected) != required:
            raise ValueError("basin runtime dataset registry rows are incomplete")
        for row in selected.values():
            meta.register_dataset(row)

    def _quality_rows(self, manifest: Mapping[str, object]) -> tuple[dict[str, object], ...]:
        raw_rows = manifest.get("quality_rows")
        if not isinstance(raw_rows, list) or not raw_rows:
            raise ValueError("basin Spark manifest requires quality_rows")
        rows: list[dict[str, object]] = []
        for raw in raw_rows:
            if not isinstance(raw, dict):
                raise TypeError("basin manifest quality row must be a mapping")
            row = dict(raw)
            checked_at = row.get("checked_at")
            if not isinstance(checked_at, str):
                raise TypeError("basin manifest checked_at must be ISO text")
            row["checked_at"] = datetime.fromisoformat(checked_at)
            if row["checked_at"].tzinfo is None:
                raise ValueError("basin manifest checked_at must be timezone-aware")
            rows.append(row)
        return tuple(rows)

    def execute(
        self,
        plan: BasinExecutionPlan,
        *,
        orchestrator_run_id: str,
    ) -> dict[str, object]:
        """Submit one application, then make Meta success the final durable event."""
        if plan.action == "skip":
            return {
                "status": "skipped",
                "pipeline_run_id": plan.pipeline_run_id,
                "build_signature": plan.build_signature,
                "prior_run_id": plan.prior_run_id,
            }
        self._ensure_output_tables()
        request = basin_application_request(
            plan,
            orchestrator_run_id=orchestrator_run_id,
        )
        assert self.table_store is not None
        audit = SparkAudit(MetaRecorder(self.table_store), now=self.now)
        audit_request = SparkAuditRequest(
            pipeline_run_id=plan.pipeline_run_id,
            orchestrator_run_id=orchestrator_run_id,
            job_name=_JOB_NAME,
            build_signature=plan.build_signature,
            config_hash=self.config_hash,
            policy_id=self.policy.policy_id,
            algorithm_version=self.algorithm_version,
            input_snapshots=(plan.snapshot,),
            input_object_ids=(plan.hydro_object_id, plan.atlas_object_id),
            required_outputs=plan.missing_outputs,
            started_at=self.now(),
        )
        audit.start(audit_request)
        failure_recorded = False
        try:
            result: SparkApplicationResult = self.submitter.submit(request)
            if result.status != "succeeded":
                audit.finalize_failure(
                    audit_request,
                    result.error_code or f"spark_application_{result.status}",
                )
                failure_recorded = True
                return json.loads(result.to_json())
            if result.output_manifest_uri != request.output_manifest_uri:
                raise ValueError("Spark result returned a different output manifest")
            raw_manifest = json.loads(
                self.object_store.read(_manifest_key(request.output_manifest_uri)).decode("utf-8")
            )
            manifest = validate_manifest(raw_manifest, plan=plan, result=result)
            outputs = tuple(
                output
                for output in result.published_snapshots
                if output.table in plan.missing_outputs
            )
            auxiliary = tuple(
                output
                for output in result.published_snapshots
                if output.table == "quarantine.rejected_rows"
            )
            unexpected = (
                {output.table for output in result.published_snapshots}
                - set(plan.missing_outputs)
                - {"quarantine.rejected_rows"}
            )
            if unexpected:
                raise ValueError(f"Spark result contains unexpected outputs: {sorted(unexpected)}")
            audit.finalize_success(
                audit_request,
                outputs,
                auxiliary_outputs=auxiliary,
                quality_rows=self._quality_rows(manifest),
            )
            return json.loads(result.to_json())
        except Exception as error:
            if not failure_recorded:
                error_code = (
                    "output_manifest_validation_failed"
                    if isinstance(error, (TypeError, ValueError, json.JSONDecodeError))
                    else f"basin_runtime_{type(error).__name__.lower()}"
                )
                try:
                    audit.finalize_failure(audit_request, error_code)
                except Exception as audit_error:  # noqa: BLE001 - preserve primary failure
                    error.add_note(f"Meta failure audit also failed: {audit_error}")
            raise


def build_basin_silver_runtime(root: Path | None = None) -> BasinSilverRuntime:
    paths = ProjectPaths.discover(root)
    settings = LakehouseSettings(_env_file=paths.root / ".env", project_root=paths.root)
    config_path = paths.root / "config" / "pipelines" / "silver" / "basin_topology.yaml"
    config = load_basin_topology_config(config_path)
    scope = read_basin_scope(
        paths.root,
        Path(config.aoi_path),
        level=config.hydrobasins_level,
    )
    policy = load_quality_policy(paths.root / config.quality_policy_path)
    config_hash = hashlib.sha256(
        config_path.read_bytes() + bytes.fromhex(scope.aoi_checksum)
    ).hexdigest()
    catalog = load_polaris_catalog(settings)
    dataset_ids = {
        f"{catalog.name}.silver.dim_basin",
        f"{catalog.name}.silver.basin_edge",
        f"{catalog.name}.quarantine.rejected_rows",
    }
    dataset_rows = tuple(
        row
        for row in load_dataset_registry(
            paths.root / "config" / "shared" / "metadata.yaml",
            catalog_name=catalog.name,
        )
        if row["dataset_id"] in dataset_ids
    )
    endpoint = os.environ.get("FLASHFLOOD_SPARK_GATEWAY_URL", "http://spark-submit-gateway:8090")
    return BasinSilverRuntime(
        catalog=catalog,
        object_store=PyArrowS3ObjectStore.from_settings(settings),
        submitter=SparkGatewaySubmitter(endpoint),
        config=config,
        policy=policy,
        config_hash=config_hash,
        scope=scope,
        dataset_rows=dataset_rows,
    )
