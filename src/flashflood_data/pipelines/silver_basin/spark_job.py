"""One-shot Spark/Sedona application for Bronze-to-Silver basin topology."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import traceback
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from flashflood_data.pipelines.silver_basin.config import load_basin_topology_config
from flashflood_data.pipelines.silver_basin.models import TopologyReport
from flashflood_data.pipelines.silver_basin.normalize import normalize_basins
from flashflood_data.pipelines.silver_basin.quality import (
    BASIN_EDGE_COLUMNS,
    DIM_BASIN_COLUMNS,
    compile_basin_quality_plans,
    evaluate_basin_quality,
    evaluate_edge_quality,
    publishable_frame,
)
from flashflood_data.pipelines.silver_basin.topology import build_basin_topology
from flashflood_data.shared.quality.policy import load_quality_policy
from flashflood_data.shared.spark.models import (
    PublishedSnapshot,
    SparkApplicationRequest,
    SparkApplicationResult,
    load_spark_runtime_config,
)
from flashflood_data.shared.spark.publish import SparkTablePublisher
from flashflood_data.shared.spark.quality import (
    build_quarantine_frame,
    quality_rows,
)
from flashflood_data.shared.spark.session import build_spark_session
from flashflood_data.shared.spark.snapshots import SparkSnapshotManager

_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10
_ROOT = Path("/opt/flashflood")
_OUTPUTS = {"silver.dim_basin", "silver.basin_edge"}


class BasinQualityBlocked(RuntimeError):
    """A fatal domain finding prevents both business outputs from publishing."""


@dataclass(frozen=True)
class BasinJobArguments:
    hydro_object_id: str
    atlas_object_id: str
    basin_ids: tuple[str, ...]
    outputs: tuple[str, ...]
    orchestrator_run_id: str


def parse_job_arguments(request: SparkApplicationRequest) -> BasinJobArguments:
    parsed: dict[str, str] = {}
    for argument in request.arguments:
        name, separator, value = argument.partition("=")
        if not separator or not name or not value or name in parsed:
            raise ValueError("invalid basin Spark arguments")
        parsed[name] = value
    expected = {
        "hydro_object_id",
        "atlas_object_id",
        "basin_ids",
        "outputs",
        "orchestrator_run_id",
    }
    if set(parsed) != expected:
        raise ValueError("basin Spark arguments must match the domain contract")
    outputs = tuple(parsed["outputs"].split(","))
    if not outputs or len(outputs) != len(set(outputs)) or not set(outputs) <= _OUTPUTS:
        raise ValueError("basin Spark outputs are invalid")
    basin_ids = tuple(parsed["basin_ids"].split(","))
    if (
        not basin_ids
        or any(not value.isdecimal() for value in basin_ids)
        or tuple(sorted(set(basin_ids), key=int)) != basin_ids
    ):
        raise ValueError("basin Spark scope IDs are invalid")
    return BasinJobArguments(
        hydro_object_id=parsed["hydro_object_id"],
        atlas_object_id=parsed["atlas_object_id"],
        basin_ids=basin_ids,
        outputs=outputs,
        orchestrator_run_id=parsed["orchestrator_run_id"],
    )


def manifest_payload(
    request: SparkApplicationRequest,
    *,
    outputs: tuple[PublishedSnapshot, ...],
    quality_rows: tuple[dict[str, object], ...],
) -> str:
    serializable_quality = []
    for source in quality_rows:
        row = dict(source)
        checked_at = row.get("checked_at")
        if isinstance(checked_at, datetime):
            row["checked_at"] = checked_at.isoformat()
        serializable_quality.append(row)
    return json.dumps(
        {
            "application_id": request.application_id,
            "pipeline_run_id": request.pipeline_run_id,
            "build_signature": request.build_signature,
            "outputs": [asdict(output) for output in outputs],
            "quality_rows": serializable_quality,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _signing_key(secret: str, date: str, region: str) -> bytes:
    def digest(key: bytes, value: str) -> bytes:
        return hmac.new(key, value.encode(), hashlib.sha256).digest()

    service = digest(digest(digest(("AWS4" + secret).encode(), date), region), "s3")
    return digest(service, "aws4_request")


def _put_manifest(uri: str, payload: str, now: datetime) -> None:
    """Write one small result object to MinIO with a standard-library SigV4 PUT."""
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError("Spark output manifest must use s3://bucket/key")
    endpoint = urlsplit(os.environ["MINIO_ENDPOINT"])
    if endpoint.scheme not in {"http", "https"} or not endpoint.netloc:
        raise ValueError("MINIO_ENDPOINT must be absolute HTTP(S)")
    access_key = os.environ.get("MINIO_ROOT_USER") or os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD") or os.environ[
        "AWS_SECRET_ACCESS_KEY"
    ]
    region = os.environ.get("AWS_REGION", "us-east-1")
    body = payload.encode()
    payload_hash = hashlib.sha256(body).hexdigest()
    utc = now.astimezone(_UTC)
    date = utc.strftime("%Y%m%d")
    amz_date = utc.strftime("%Y%m%dT%H%M%SZ")
    canonical_uri = "/" + "/".join(
        quote(segment, safe="-_.~")
        for segment in (parsed.netloc, *parsed.path.strip("/").split("/"))
    )
    canonical_headers = (
        f"host:{endpoint.netloc}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = (
        f"PUT\n{canonical_uri}\n\n{canonical_headers}\n"
        f"{signed_headers}\n{payload_hash}"
    )
    scope = f"{date}/{region}/s3/aws4_request"
    canonical_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_hash}"
    )
    signature = hmac.new(
        _signing_key(secret_key, date, region),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    target = f"{endpoint.scheme}://{endpoint.netloc}{canonical_uri}"
    request = urllib.request.Request(
        target,
        data=body,
        method="PUT",
        headers={
            "Authorization": authorization,
            "Content-Type": "application/json",
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in {200, 201}:
            raise RuntimeError(f"MinIO manifest PUT failed: HTTP {response.status}")


def _snapshot_time(spark: Any, table: str, snapshot_id: int) -> datetime:
    rows = spark.sql(
        f"SELECT committed_at FROM {table}.snapshots WHERE snapshot_id = {snapshot_id}"
    ).collect()
    if len(rows) != 1:
        raise ValueError("pinned Bronze snapshot metadata is missing")
    value = rows[0]["committed_at"]
    if not isinstance(value, datetime):
        raise TypeError("Iceberg committed_at must be a timestamp")
    return value.replace(tzinfo=_UTC) if value.tzinfo is None else value.astimezone(_UTC)


def _empty_report() -> TopologyReport:
    return TopologyReport(
        basin_count=0,
        edge_count=0,
        internal_edge_count=0,
        exit_edge_count=0,
        terminal_edge_count=0,
    )


def _qualified(catalog: str, table: str) -> str:
    return table if table.count(".") == 2 else f"{catalog}.{table}"


def _publish(
    spark: Any,
    catalog: str,
    table: str,
    frame: Any,
    keys: tuple[str, ...],
) -> PublishedSnapshot:
    return SparkTablePublisher(spark, _qualified(catalog, table)).publish(
        frame,
        key_fields=keys,
        mode="merge",
    )


def _run(request: SparkApplicationRequest) -> SparkApplicationResult:
    if request.pipeline != "silver_basin_topology" or len(request.snapshots) != 1:
        raise ValueError("invalid basin Spark application request")
    snapshot = request.snapshots[0]
    if snapshot.table != "bronze.basin_polygon_raw":
        raise ValueError("basin Spark job requires bronze.basin_polygon_raw")
    arguments = parse_job_arguments(request)
    runtime_config = load_spark_runtime_config(_ROOT / "config" / "spark" / "local.yaml")
    domain_config = load_basin_topology_config(
        _ROOT / "config" / "pipelines" / "silver" / "basin_topology.yaml"
    )
    policy = load_quality_policy(_ROOT / domain_config.quality_policy_path)
    spark = build_spark_session(
        request.application_id,
        config=runtime_config,
        pipeline=request.pipeline,
        pipeline_run_id=request.pipeline_run_id,
        build_signature=request.build_signature,
    )
    try:
        snapshot_manager = SparkSnapshotManager(runtime_config.catalog_name)
        table = snapshot_manager.qualify(snapshot.table)
        bronze = snapshot_manager.read(spark, snapshot)
        selected = bronze.filter(
            (
                (bronze["source_id"] == domain_config.hydro_source_id)
                & (bronze["object_id"] == arguments.hydro_object_id)
            )
            | (
                (bronze["source_id"] == domain_config.atlas_source_id)
                & (bronze["object_id"] == arguments.atlas_object_id)
            )
        )
        basin_version = f"basin-{request.build_signature[:16]}"
        normalized = normalize_basins(
            selected,
            domain_config,
            basin_version=basin_version,
            valid_from=_snapshot_time(spark, table, snapshot.snapshot_id),
            basin_ids=arguments.basin_ids,
        )
        actual_ids = {row[0] for row in normalized.select("basin_id").toLocalIterator()}
        if actual_ids != set(arguments.basin_ids):
            raise ValueError("pinned Bronze revision does not contain every scoped basin")
        basin_plan = compile_basin_quality_plans(policy, _empty_report()).basin
        basin_quality = evaluate_basin_quality(
            normalized,
            basin_plan,
            pipeline_run_id=request.pipeline_run_id,
        )
        if basin_quality.blocked:
            raise BasinQualityBlocked("fatal dim_basin quality finding")
        if basin_quality.accepted.limit(1).count() == 0:
            raise BasinQualityBlocked("no basin rows remain after row quarantine")

        edges, report = build_basin_topology(
            basin_quality.accepted,
            topology_version=domain_config.topology_version,
        )
        edge_plan = compile_basin_quality_plans(policy, report).edge
        edge_quality = evaluate_edge_quality(
            edges,
            edge_plan,
            pipeline_run_id=request.pipeline_run_id,
        )
        if edge_quality.blocked:
            raise BasinQualityBlocked("fatal basin_edge quality finding")

        overall_status = (
            "warning"
            if "warning" in {basin_quality.quality_status, edge_quality.quality_status}
            else "passed"
        )
        basin_frame = publishable_frame(
            basin_quality.accepted,
            DIM_BASIN_COLUMNS,
            overall_status,
        )
        edge_frame = publishable_frame(
            edge_quality.accepted,
            BASIN_EDGE_COLUMNS,
            overall_status,
        )
        outputs: list[PublishedSnapshot] = []
        for table_name in arguments.outputs:
            if table_name == "silver.dim_basin":
                outputs.append(
                    _publish(
                        spark,
                        runtime_config.catalog_name,
                        table_name,
                        basin_frame,
                        ("basin_id", "basin_version"),
                    )
                )
            else:
                outputs.append(
                    _publish(
                        spark,
                        runtime_config.catalog_name,
                        table_name,
                        edge_frame,
                        ("topology_version", "basin_version", "upstream_basin_id"),
                    )
                )

        rejected_at = datetime.now(_UTC)
        if basin_quality.rejected.limit(1).count():
            quarantine = build_quarantine_frame(
                basin_quality.rejected,
                pipeline_run_id=request.pipeline_run_id,
                target_dataset_id="silver.dim_basin",
                quality_policy_id=policy.policy_id,
                input_table=snapshot.table,
                input_snapshot_id=snapshot.snapshot_id,
                parser_version=domain_config.transform_version,
                rejected_at=rejected_at,
            )
            outputs.append(
                _publish(
                    spark,
                    runtime_config.catalog_name,
                    "quarantine.rejected_rows",
                    quarantine,
                    ("rejection_id",),
                )
            )

        checked_at = datetime.now(_UTC)
        audit_rows = quality_rows(
            basin_quality.findings,
            pipeline_run_id=request.pipeline_run_id,
            dataset_id="silver.dim_basin",
            checked_at=checked_at,
        ) + quality_rows(
            edge_quality.findings,
            pipeline_run_id=request.pipeline_run_id,
            dataset_id="silver.basin_edge",
            checked_at=checked_at,
        )
        manifest = manifest_payload(
            request,
            outputs=tuple(outputs),
            quality_rows=tuple(dict(row) for row in audit_rows),
        )
        _put_manifest(request.output_manifest_uri, manifest, datetime.now(_UTC))
        return SparkApplicationResult(
            application_id=request.application_id,
            status="succeeded",
            output_manifest_uri=request.output_manifest_uri,
            published_snapshots=tuple(outputs),
        )
    finally:
        spark.stop()


def run(request: SparkApplicationRequest) -> SparkApplicationResult:
    """Return a structured failure so Airflow retains the domain error code."""
    try:
        return _run(request)
    except Exception as error:  # noqa: BLE001 - process boundary must return a result DTO
        traceback.print_exc()
        error_code = (
            "basin_quality_blocked"
            if isinstance(error, BasinQualityBlocked)
            else f"basin_{type(error).__name__.lower()}"
        )
        return SparkApplicationResult(
            application_id=request.application_id,
            status="failed",
            output_manifest_uri=None,
            error_code=error_code,
        )
