"""One Spark application for Silver weather weights and basin values."""

from __future__ import annotations

import json
import logging
import os
import re
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Any

from flashflood_data.pipelines.silver_weather.aggregate import aggregate_weather_slices
from flashflood_data.pipelines.silver_weather.config import load_weather_aggregation_config
from flashflood_data.pipelines.silver_weather.normalize import normalize_weather_slices
from flashflood_data.pipelines.silver_weather.pipeline import (
    JOB_NAME,
    WEATHER_KEY,
    WEATHER_OUTPUTS,
    WEIGHT_KEY,
    missing_weight_rows,
)
from flashflood_data.pipelines.silver_weather.preference import (
    prefer_gsmap,
    remove_superseded_era5,
)
from flashflood_data.pipelines.silver_weather.quality import (
    weather_quality_plan,
    weight_quality_plan,
)
from flashflood_data.pipelines.silver_weather.weights import build_grid_basin_weights
from flashflood_data.shared.quality.models import QualityFinding
from flashflood_data.shared.spark.manifest import put_json_manifest
from flashflood_data.shared.spark.models import (
    PublishedSnapshot,
    SparkApplicationRequest,
    SparkApplicationResult,
    load_spark_runtime_config,
)
from flashflood_data.shared.spark.publish import SparkTablePublisher, TimestampMergeScope
from flashflood_data.shared.spark.quality import (
    apply_quality_plan,
    build_quarantine_frame,
    quality_rows,
)
from flashflood_data.shared.spark.session import build_spark_session
from flashflood_data.shared.spark.snapshots import SparkSnapshotManager

_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10.
_ROOT = Path("/opt/flashflood")
_CHECKPOINT_ROOT = Path("/opt/flashflood/spark-checkpoints")
_LOG = logging.getLogger(__name__)


class WeatherQualityBlocked(RuntimeError):
    """Fatal domain DQ prevents weather business table publication."""


def parse_job_arguments(request: SparkApplicationRequest) -> tuple[tuple[str, ...], str | None]:
    if request.pipeline != JOB_NAME or len(request.arguments) not in {1, 2}:
        raise ValueError("weather Spark outputs arguments are invalid")
    name, separator, value = request.arguments[0].partition("=")
    outputs = tuple(value.split(",")) if separator and name == "outputs" else ()
    if not outputs or len(set(outputs)) != len(outputs) or not set(outputs) <= set(WEATHER_OUTPUTS):
        raise ValueError("weather Spark outputs are invalid")
    if tuple(name for name in WEATHER_OUTPUTS if name in outputs) != outputs:
        raise ValueError("weather Spark outputs must be ordered")
    batch_month = None
    if len(request.arguments) == 2:
        name, separator, value = request.arguments[1].partition("=")
        if separator != "=" or name != "batch_month" or not re.fullmatch(
            r"[0-9]{4}-(0[1-9]|1[0-2])", value
        ):
            raise ValueError("weather batch_month is invalid")
        batch_month = value
    if ("silver.basin_weather_value" in outputs) != (batch_month is not None):
        raise ValueError("weather value output requires batch_month")
    return outputs, batch_month


def _target_frame(spark: Any, table: str, frame: Any) -> Any:
    """Match Iceberg field order and primitive types before the strict publisher."""
    from pyspark.sql import functions

    target = spark.table(table)
    if not set(target.columns).issubset(frame.columns):
        raise ValueError(f"Silver weather frame lacks target columns: {table}")
    return frame.select(
        *[functions.col(field.name).cast(field.dataType).alias(field.name) for field in target.schema]
    )


def _existing_evidence(spark: Any, table: str) -> PublishedSnapshot:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        raise ValueError(f"Silver weather table has no rows or snapshot: {table}")
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()[0]["n"]
    files = spark.sql(f"SELECT COUNT(*) AS n FROM {table}.files").collect()[0]["n"]
    return PublishedSnapshot(
        table=".".join(table.split(".")[-2:]),
        snapshot_id=int(rows[0]["snapshot_id"]),
        row_count=int(count), file_count=int(files),
    )


def _publish_or_reuse(
    spark: Any, table: str, frame: Any, keys: tuple[str, ...],
    *, detach_source: bool = False, merge_scope: TimestampMergeScope | None = None,
) -> PublishedSnapshot:
    if frame.limit(1).count() == 0:
        return _existing_evidence(spark, table)
    target_frame = _target_frame(spark, table, frame)
    if detach_source:
        # The missing-weight anti-join reads the same table that MERGE writes.
        # Spark 4/Iceberg cannot plan that self-referential MERGE. An eager
        # checkpoint materializes the bounded weight set and cuts its lineage.
        target_frame = target_frame.localCheckpoint(eager=True)
    options = {"merge_scope": merge_scope} if merge_scope is not None else {}
    return SparkTablePublisher(spark, table).publish(
        target_frame, key_fields=keys, mode="merge", **options,
    )


def _assert_unique(frame: Any, keys: tuple[str, ...]) -> None:
    if frame.groupBy(*keys).count().filter("count > 1").limit(1).count():
        raise WeatherQualityBlocked(f"duplicate Silver weather key: {keys}")


def _publish_weather_values(
    spark: Any, table: str, accepted: Any, config: Any, batch_month: str,
) -> PublishedSnapshot:
    started = monotonic()
    # Preference reads the basin frame twice (all candidates and GSMaP keys).
    # Cut its lazy aggregation/DQ plan first: otherwise Catalyst can push the
    # source-specific unit predicates back into the aggregation assertions.
    accepted = accepted.checkpoint(eager=True)
    selected = prefer_gsmap(
        spark, accepted, spark.table(table), config, batch_month=batch_month,
    ).checkpoint(eager=True)
    selected_at = monotonic()
    year, month = map(int, batch_month.split("-"))
    scope = TimestampMergeScope(
        "valid_time", datetime(year, month, 1, tzinfo=_UTC),
        datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=_UTC),
    )
    output = _publish_or_reuse(
        spark, table, selected, (*WEATHER_KEY, "valid_time"), merge_scope=scope,
    )
    published_at = monotonic()
    deleted = remove_superseded_era5(spark, table, config, batch_month=batch_month)
    if deleted:
        output = _existing_evidence(spark, table)
    _LOG.info(
        "weather_silver_preference_timings batch_month=%s selection_seconds=%.3f "
        "publish_seconds=%.3f cleanup_seconds=%.3f deleted_era5_rows=%d",
        batch_month, selected_at - started, published_at - selected_at,
        monotonic() - published_at, deleted,
    )
    return output


def _build_weather_values(
    spark: Any, weather: Any, grid: Any, weights: Any, config: Any,
    checkpoint_dir: str, batch_month: str,
) -> Any:
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    normalized = normalize_weather_slices(spark, weather, config, batch_month=batch_month)
    # The ERA5 predecessor self-join creates a large logical plan. MERGE can
    # exhaust the driver heap while expanding it. Use a shared, durable
    # checkpoint; localCheckpoint loses partitions when executor blocks evict.
    normalized = normalized.checkpoint(eager=True)
    return aggregate_weather_slices(spark, normalized, grid, weights, config)


def _empty_quality_row(
    request: SparkApplicationRequest, dataset_id: str, batch_month: str | None = None,
) -> dict[str, object]:
    finding = QualityFinding(
        rule_id="no_new_rows", rule_version="v1", status="passed", severity="info",
        scope_key=(
            f"{dataset_id}|month={batch_month}" if batch_month is not None else dataset_id
        ),
        observed_value={"new_rows": 0},
        expected_value={"new_rows": 0}, failed_row_count=0,
    )
    return quality_rows(
        (finding,), pipeline_run_id=request.pipeline_run_id,
        dataset_id=dataset_id, checked_at=datetime.now(_UTC),
    )[0]


def _run(request: SparkApplicationRequest) -> SparkApplicationResult:
    outputs_requested, batch_month = parse_job_arguments(request)
    snapshots = {item.table: item for item in request.snapshots}
    required = {"bronze.weather_raster_slice", "silver.source_grid", "silver.dim_basin"}
    if set(snapshots) != required or len(request.snapshots) != len(required):
        raise ValueError("weather Spark job requires three pinned input snapshots")
    runtime = load_spark_runtime_config(_ROOT / "config/spark/local.yaml")
    config = load_weather_aggregation_config(
        _ROOT / "config/pipelines/silver/weather_aggregation.yaml"
    )
    spark = build_spark_session(
        request.application_id, config=runtime, pipeline=request.pipeline,
        pipeline_run_id=request.pipeline_run_id, build_signature=request.build_signature,
    )
    checkpoint_workspace = None
    try:
        manager = SparkSnapshotManager(runtime.catalog_name)
        weather = manager.read(spark, snapshots["bronze.weather_raster_slice"])
        grid = manager.read(spark, snapshots["silver.source_grid"])
        basins = manager.read(spark, snapshots["silver.dim_basin"]).filter(
            "hydrobasins_level = 12 AND valid_to IS NULL"
        )
        weight_table = manager.qualify(config.weight_table)
        value_table = manager.qualify(config.output_table)
        published: list[PublishedSnapshot] = []
        findings: list[dict[str, object]] = []

        if config.weight_table in outputs_requested:
            candidates = build_grid_basin_weights(spark, grid, basins, config)
            missing = missing_weight_rows(candidates, spark.table(weight_table))
            if missing.limit(1).count() == 0:
                findings.append(_empty_quality_row(request, config.weight_table))
                published.append(_existing_evidence(spark, weight_table))
            else:
                _assert_unique(missing, WEIGHT_KEY)
                checked = apply_quality_plan(
                    missing, weight_quality_plan(), pipeline_run_id=request.pipeline_run_id,
                    dataset_id=config.weight_table, key_fields=WEIGHT_KEY,
                )
                if checked.blocked:
                    raise WeatherQualityBlocked("fatal grid_basin_weight quality finding")
                findings.extend(quality_rows(
                    checked.findings, pipeline_run_id=request.pipeline_run_id,
                    dataset_id=config.weight_table, checked_at=datetime.now(_UTC),
                ))
                published.append(_publish_or_reuse(
                    spark, weight_table, checked.accepted, WEIGHT_KEY,
                    detach_source=True,
                ))

        if config.output_table in outputs_requested:
            checkpoint_workspace = TemporaryDirectory(
                prefix="silver-weather-", dir=_CHECKPOINT_ROOT,
            )
            values = _build_weather_values(
                spark, weather, grid, spark.table(weight_table), config,
                checkpoint_workspace.name, batch_month,
            )
            if values.limit(1).count() == 0:
                findings.append(_empty_quality_row(
                    request, config.output_table, batch_month,
                ))
                published.append(_existing_evidence(spark, value_table))
            else:
                _assert_unique(values, WEATHER_KEY)
                checked = apply_quality_plan(
                    values, weather_quality_plan(config), pipeline_run_id=request.pipeline_run_id,
                    dataset_id=config.output_table, key_fields=WEATHER_KEY,
                )
                if checked.blocked:
                    failed = ", ".join(
                        f"{item.rule_id}={item.failed_row_count}"
                        for item in checked.findings
                        if item.severity == "fatal" and item.failed_row_count
                    )
                    raise WeatherQualityBlocked(
                        f"fatal basin_weather_value quality finding: {failed}"
                    )
                batch_findings = quality_rows(
                    checked.findings, pipeline_run_id=request.pipeline_run_id,
                    dataset_id=config.output_table, checked_at=datetime.now(_UTC),
                )
                findings.extend(
                    {**row, "scope_key": f"{row['scope_key']}|month={batch_month}"}
                    for row in batch_findings
                )
                published.append(_publish_weather_values(
                    spark, value_table, checked.accepted, config, batch_month,
                ))
                if checked.rejected.limit(1).count():
                    quarantine = build_quarantine_frame(
                        checked.rejected,
                        pipeline_run_id=request.pipeline_run_id,
                        target_dataset_id=config.output_table,
                        quality_policy_id=config.quality_policy_id,
                        input_table="bronze.weather_raster_slice",
                        input_snapshot_id=snapshots["bronze.weather_raster_slice"].snapshot_id,
                        parser_version=config.contract_version,
                        rejected_at=datetime.now(_UTC),
                    )
                    published.append(_publish_or_reuse(
                        spark, manager.qualify("quarantine.rejected_rows"),
                        quarantine, ("rejection_id",),
                    ))

        manifest = json.dumps(
            {
                "application_id": request.application_id,
                "pipeline_run_id": request.pipeline_run_id,
                "build_signature": request.build_signature,
                "outputs": [asdict(item) for item in published],
                "quality_rows": [
                    {**item, "checked_at": item["checked_at"].isoformat()}
                    for item in findings
                ],
            },
            sort_keys=True, separators=(",", ":"),
        )
        put_json_manifest(
            request.output_manifest_uri, manifest, now=datetime.now(_UTC),
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ.get("MINIO_ROOT_USER") or os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ.get("MINIO_ROOT_PASSWORD") or os.environ["AWS_SECRET_ACCESS_KEY"],
            region=os.environ.get("AWS_REGION", "us-east-1"),
        )
        return SparkApplicationResult(
            application_id=request.application_id, status="succeeded",
            output_manifest_uri=request.output_manifest_uri,
            published_snapshots=tuple(published),
        )
    finally:
        try:
            spark.stop()
        finally:
            if checkpoint_workspace is not None:
                checkpoint_workspace.cleanup()


def run(request: SparkApplicationRequest) -> SparkApplicationResult:
    try:
        return _run(request)
    except Exception as error:  # noqa: BLE001 - Spark process returns structured failure
        traceback.print_exc()
        return SparkApplicationResult(
            application_id=request.application_id,
            status="failed", output_manifest_uri=None,
            error_code=(
                "weather_quality_blocked" if isinstance(error, WeatherQualityBlocked)
                else f"weather_{type(error).__name__.lower()}"
            ),
        )
