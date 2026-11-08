"""Airflow-side snapshot planning, Spark submission and Silver weather audit."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any

from pyiceberg.expressions import EqualTo, In

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.pipelines.silver_weather.pipeline import (
    ALGORITHM_VERSION,
    JOB_NAME,
    WEATHER_OUTPUTS,
    WeatherBuildPlan,
    plan_weather_build,
)
from flashflood_data.shared.metadata.registry import load_dataset_registry
from flashflood_data.shared.metadata.service import MetaRecorder
from flashflood_data.shared.spark.audit import SparkAudit, SparkAuditRequest
from flashflood_data.shared.spark.manifest import manifest_key
from flashflood_data.shared.spark.models import (
    PinnedSnapshot,
    PublishedSnapshot,
    SparkApplicationRequest,
    SparkApplicationResult,
)
from flashflood_data.shared.spark.snapshots import BuildRecord
from flashflood_data.shared.spark.submit import SparkGatewaySubmitter
from flashflood_data.storage.iceberg import load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import PyArrowS3ObjectStore

_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10.
_INPUTS = ("bronze.weather_raster_slice", "silver.source_grid", "silver.dim_basin")
_LOG = logging.getLogger(__name__)


def weather_application_request(
    plan: WeatherBuildPlan, *, orchestrator_run_id: str,
    outputs: tuple[str, ...] | None = None,
    batch_month: str | None = None,
) -> SparkApplicationRequest:
    if plan.action == "skip" or not plan.missing_outputs:
        raise ValueError("skipped weather plan has no Spark application")
    selected = outputs or plan.missing_outputs
    if not set(selected) <= set(plan.missing_outputs) or not selected:
        raise ValueError("weather application outputs are not in the build plan")
    if ("silver.basin_weather_value" in selected) != (batch_month is not None):
        raise ValueError("weather value application requires batch_month")
    if batch_month is not None and (
        len(batch_month) != 7 or batch_month[4] != "-"
        or not batch_month[:4].isdigit() or not batch_month[5:].isdigit()
        or not 1 <= int(batch_month[5:]) <= 12
    ):
        raise ValueError("invalid weather batch_month")
    batch_label = f"batch={batch_month}" if batch_month else "weights"
    digest = hashlib.sha256(
        f"{plan.build_signature}\u001f{batch_label}\u001f{','.join(selected)}".encode()
    ).hexdigest()[:20]
    arguments = (f"outputs={','.join(selected)}",)
    if batch_month is not None:
        arguments += (f"batch_month={batch_month}",)
    return SparkApplicationRequest(
        application_id=f"silver-weather-{digest}",
        pipeline_run_id=plan.pipeline_run_id,
        pipeline=JOB_NAME,
        entrypoint="flashflood_data.pipelines.silver_weather.spark_job",
        snapshots=plan.snapshots,
        build_signature=plan.build_signature,
        output_manifest_uri=(
            f"s3://staging/silver/{JOB_NAME}/{plan.pipeline_run_id}/"
            f"{batch_label}/result.json"
        ),
        arguments=arguments,
    )


class WeatherSilverRuntime:
    """Keep catalog IO in Airflow and all weather transformations in Spark."""

    def __init__(
        self,
        *,
        catalog: Any,
        object_store: Any,
        submitter: Any,
        config_hash: str,
        dataset_rows: tuple[dict[str, object], ...] = (),
    ) -> None:
        self.catalog = catalog
        self.object_store = object_store
        self.submitter = submitter
        self.config_hash = config_hash
        self.dataset_rows = dataset_rows
        self.tables = IcebergTableStore(catalog)

    def _snapshot(self, name: str) -> PinnedSnapshot:
        table = self.catalog.load_table(tuple(name.split(".")))
        table.refresh()
        snapshot = table.current_snapshot()
        if snapshot is None:
            raise ValueError(f"Silver weather input has no snapshot: {name}")
        return PinnedSnapshot(name, int(snapshot.snapshot_id))

    def _history(self) -> tuple[BuildRecord, ...]:
        runs = self.tables.ensure_table(("meta", "pipeline_runs"))
        refs = self.tables.ensure_table(("meta", "table_snapshot_ref"))
        run_rows = runs.scan(
            row_filter=EqualTo("job_name", JOB_NAME),
            selected_fields=("pipeline_run_id", "status", "metrics_json"),
        ).to_arrow().to_pylist()
        ids = tuple(str(row["pipeline_run_id"]) for row in run_rows)
        ref_rows = (
            refs.scan(
                row_filter=In("pipeline_run_id", ids),
                selected_fields=("pipeline_run_id", "table_name", "role"),
            ).to_arrow().to_pylist()
            if ids else []
        )
        outputs: dict[str, list[str]] = {run_id: [] for run_id in ids}
        for row in ref_rows:
            if row["role"] == "output" and row["pipeline_run_id"] in outputs:
                outputs[row["pipeline_run_id"]].append(row["table_name"])
        records = []
        for row in run_rows:
            try:
                metrics = json.loads(row["metrics_json"] or "{}")
            except json.JSONDecodeError as error:
                raise ValueError("invalid weather pipeline metrics_json") from error
            signature = metrics.get("build_signature") if isinstance(metrics, dict) else None
            if isinstance(signature, str) and signature:
                records.append(BuildRecord(
                    pipeline_run_id=row["pipeline_run_id"],
                    status=row["status"],
                    build_signature=signature,
                    completed_outputs=tuple(dict.fromkeys(outputs[row["pipeline_run_id"]])),
                ))
        return tuple(records)

    def plan(self) -> WeatherBuildPlan:
        snapshots = tuple(self._snapshot(name) for name in _INPUTS)
        return plan_weather_build(snapshots, config_hash=self.config_hash, history=self._history())

    def _prepare_outputs(self) -> None:
        for name in (*WEATHER_OUTPUTS, "quarantine.rejected_rows"):
            self.tables.ensure_table(tuple(name.split(".")))
        meta = MetaRecorder(self.tables)
        for row in self.dataset_rows:
            meta.register_dataset(row)

    def _weather_months(self, snapshot: PinnedSnapshot) -> tuple[str, ...]:
        table = self.catalog.load_table(tuple(snapshot.table.split(".")))
        rows = table.scan(
            snapshot_id=snapshot.snapshot_id,
            selected_fields=("valid_time",),
        ).to_arrow().column("valid_time").to_pylist()
        if not rows or any(value is None for value in rows):
            raise ValueError("Bronze weather snapshot has no valid monthly timestamps")
        return tuple(sorted({value.strftime("%Y-%m") for value in rows}))

    @staticmethod
    def _quality_rows(manifest: dict[str, object]) -> tuple[dict[str, object], ...]:
        rows = manifest.get("quality_rows")
        if not isinstance(rows, list) or not rows:
            raise ValueError("weather Spark manifest requires quality rows")
        decoded = []
        for source in rows:
            if not isinstance(source, dict) or not isinstance(source.get("checked_at"), str):
                raise TypeError("invalid weather quality manifest row")
            row = dict(source)
            row["checked_at"] = datetime.fromisoformat(row["checked_at"])
            if row["checked_at"].tzinfo is None:
                raise ValueError("weather quality timestamp must be timezone-aware")
            decoded.append(row)
        return tuple(decoded)

    def _run_application(
        self, request: SparkApplicationRequest, expected_outputs: tuple[str, ...],
    ) -> tuple[tuple[PublishedSnapshot, ...], tuple[PublishedSnapshot, ...],
               tuple[dict[str, object], ...]]:
        started = monotonic()
        batch_month = next(
            (arg.partition("=")[2] for arg in request.arguments if arg.startswith("batch_month=")),
            "weights",
        )
        key = manifest_key(request.output_manifest_uri)
        cached = self.object_store.exists(key)
        _LOG.info(
            "weather_silver_batch_started application_id=%s batch_month=%s reused=%s",
            request.application_id, batch_month, cached,
        )
        result: SparkApplicationResult | None = None
        if not cached:
            result = self.submitter.submit(request)
            if result.status != "succeeded":
                raise RuntimeError(result.error_code or f"spark_{result.status}")
            if result.output_manifest_uri != request.output_manifest_uri:
                raise ValueError("weather Spark result manifest URI differs from request")
        manifest = json.loads(self.object_store.read(key).decode("utf-8"))
        if not isinstance(manifest, dict) or (
            manifest.get("application_id"), manifest.get("pipeline_run_id"),
            manifest.get("build_signature"),
        ) != (request.application_id, request.pipeline_run_id, request.build_signature):
            raise ValueError("weather Spark manifest identity mismatch")
        rows = manifest.get("outputs")
        if not isinstance(rows, list):
            raise TypeError("weather Spark manifest lacks output evidence")
        declared = tuple(PublishedSnapshot(**row) for row in rows)
        if result is not None and declared != tuple(result.published_snapshots):
            raise ValueError("weather Spark manifest output evidence mismatch")
        outputs = tuple(row for row in declared if row.table in expected_outputs)
        auxiliary = tuple(row for row in declared if row.table == "quarantine.rejected_rows")
        if (
            tuple(row.table for row in outputs) != expected_outputs
            or len(declared) != len(outputs) + len(auxiliary)
            or len(auxiliary) > 1
        ):
            raise ValueError("weather Spark returned unexpected or missing outputs")
        quality = self._quality_rows(manifest)
        _LOG.info(
            "weather_silver_batch_finished application_id=%s batch_month=%s reused=%s "
            "total_seconds=%.3f output_snapshots=%s",
            request.application_id, batch_month, cached, monotonic() - started,
            {row.table: row.snapshot_id for row in outputs},
        )
        return outputs, auxiliary, quality

    def execute(self, plan: WeatherBuildPlan, *, orchestrator_run_id: str) -> dict[str, object]:
        if plan.action == "skip":
            return {"status": "skipped", "build_signature": plan.build_signature}
        self._prepare_outputs()
        audit = SparkAudit(MetaRecorder(self.tables))
        audit_request = SparkAuditRequest(
            pipeline_run_id=plan.pipeline_run_id,
            orchestrator_run_id=orchestrator_run_id,
            job_name=JOB_NAME,
            build_signature=plan.build_signature,
            config_hash=plan.config_hash,
            policy_id="silver-weather-v1",
            algorithm_version=ALGORITHM_VERSION,
            input_snapshots=plan.snapshots,
            required_outputs=plan.missing_outputs,
            started_at=datetime.now(_UTC),
        )
        audit.start(audit_request)
        try:
            latest: dict[str, PublishedSnapshot] = {}
            auxiliary_latest: dict[str, PublishedSnapshot] = {}
            quality: list[dict[str, object]] = []
            if "silver.grid_basin_weight" in plan.missing_outputs:
                request = weather_application_request(
                    plan, orchestrator_run_id=orchestrator_run_id,
                    outputs=("silver.grid_basin_weight",),
                )
                outputs, auxiliary, findings = self._run_application(
                    request, ("silver.grid_basin_weight",)
                )
                latest.update((row.table, row) for row in outputs)
                auxiliary_latest.update((row.table, row) for row in auxiliary)
                quality.extend(findings)
            if "silver.basin_weather_value" in plan.missing_outputs:
                bronze = next(
                    row for row in plan.snapshots
                    if row.table == "bronze.weather_raster_slice"
                )
                for batch_month in self._weather_months(bronze):
                    request = weather_application_request(
                        plan, orchestrator_run_id=orchestrator_run_id,
                        outputs=("silver.basin_weather_value",),
                        batch_month=batch_month,
                    )
                    outputs, auxiliary, findings = self._run_application(
                        request, ("silver.basin_weather_value",)
                    )
                    latest.update((row.table, row) for row in outputs)
                    auxiliary_latest.update((row.table, row) for row in auxiliary)
                    quality.extend(findings)
            outputs = tuple(latest[name] for name in plan.missing_outputs)
            audit.finalize_success(
                audit_request, outputs,
                auxiliary_outputs=tuple(auxiliary_latest.values()),
                quality_rows=tuple(quality),
            )
            return {"status": "succeeded", "build_signature": plan.build_signature,
                    "outputs": [row.table for row in outputs]}
        except Exception as error:
            try:
                audit.finalize_failure(audit_request, f"weather_{type(error).__name__.lower()}")
            except Exception as audit_error:  # noqa: BLE001 - retain primary failure
                error.add_note(f"weather failure audit also failed: {audit_error}")
            raise


def build_weather_silver_runtime(root: Path | None = None) -> WeatherSilverRuntime:
    paths = ProjectPaths.discover(root)
    settings = LakehouseSettings(_env_file=paths.root / ".env", project_root=paths.root)
    config_path = paths.root / "config/pipelines/silver/weather_aggregation.yaml"
    catalog = load_polaris_catalog(settings)
    dataset_ids = {f"{catalog.name}.{name}" for name in WEATHER_OUTPUTS}
    rows = tuple(
        row for row in load_dataset_registry(
            paths.root / "config/shared/metadata.yaml", catalog_name=catalog.name,
        ) if row["dataset_id"] in dataset_ids
    )
    if {row["dataset_id"] for row in rows} != dataset_ids:
        raise ValueError("weather Silver dataset registry rows are incomplete")
    endpoint = os.environ.get("FLASHFLOOD_SPARK_GATEWAY_URL", "http://spark-submit-gateway:8090")
    return WeatherSilverRuntime(
        catalog=catalog,
        object_store=PyArrowS3ObjectStore.from_settings(settings),
        submitter=SparkGatewaySubmitter(endpoint),
        config_hash=hashlib.sha256(config_path.read_bytes()).hexdigest(),
        dataset_rows=rows,
    )
