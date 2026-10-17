from __future__ import annotations

import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from runtime import build_spark_session

from flashflood_data.pipelines.silver_basin.config import load_basin_topology_config
from flashflood_data.pipelines.silver_basin.normalize import normalize_basins
from flashflood_data.shared.spark.publish import SparkTablePublisher
from flashflood_data.shared.spark.quality import (
    SparkQualityRule,
    apply_quality_plan,
    compile_quality_plan,
)

CATALOG = "flood_lakehouse"
NAMESPACE_PATTERN = re.compile(r"smoke_[0-9a-f]{32}")
EXPECTED_ROWS = [(1, "basin"), (2, "rain"), (3, "risk")]
UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10


def make_smoke_namespace() -> str:
    return f"smoke_{uuid.uuid4().hex}"


def run_spatial_smoke(spark) -> None:
    """Fail early when the Sedona SQL extension or JARs are unavailable."""
    rows = [
        tuple(row)
        for row in spark.sql(
            "SELECT ST_AsText(ST_Point(102.0, 21.0)) AS wkt"
        ).collect()
    ]
    if rows != [("POINT (102 21)",)]:
        raise RuntimeError(f"Sedona spatial smoke returned unexpected rows: {rows!r}")


def run_basin_normalize_smoke(spark) -> None:
    """Execute the production basin expressions against one joined fixture."""
    plan = compile_quality_plan(
        (SparkQualityRule("basin_id_present", "v1", "fatal", "basin_id IS NOT NULL"),)
    )
    if len(plan.fatal_rules) != 1:
        raise RuntimeError("Spark DQ smoke failed to compile its quality plan")
    hydro = spark.sql(
        """
        SELECT
          'hydrobasins_v1c' AS source_id,
          'hydro-object-1' AS object_id,
          'EPSG:4326' AS crs,
          ST_AsBinary(ST_GeomFromText(
            'POLYGON ((102 21, 103 21, 103 22, 102 22, 102 21))'
          )) AS geometry_wkb,
          '{"HYBAS_ID":42,"NEXT_DOWN":0,"MAIN_BAS":42,"PFAF_ID":123,"SORT":1,"NEXT_SINK":0}'
            AS source_fields_json
        """
    )
    atlas = spark.sql(
        """
        SELECT
          'basinatlas_v10' AS source_id,
          'atlas-object-1' AS object_id,
          'EPSG:4326' AS crs,
          ST_AsBinary(ST_GeomFromText(
            'POLYGON ((102 21, 103 21, 103 22, 102 22, 102 21))'
          )) AS geometry_wkb,
          '{"HYBAS_ID":42,"SUB_AREA":12.5,"UP_AREA":20.0,"DIST_MAIN":3.0,"DIST_SINK":4.0}'
            AS source_fields_json
        """
    )
    config = load_basin_topology_config(
        Path("/opt/flashflood/config/pipelines/silver/basin_topology.yaml")
    )
    normalized = normalize_basins(
        hydro.unionByName(atlas),
        config,
        basin_version="smoke-v1",
        valid_from=datetime(2026, 1, 1, tzinfo=UTC),
    )
    quality = apply_quality_plan(
        normalized,
        plan,
        pipeline_run_id="smoke-run",
        dataset_id="silver.dim_basin",
        key_fields=("basin_id", "basin_version"),
    )
    if quality.blocked or quality.quality_status != "passed":
        raise RuntimeError("Spark DQ smoke unexpectedly blocked the normalized basin")
    normalized = quality.accepted
    rows = normalized.select(
        "basin_id",
        "next_down_id",
        "basin_area_km2",
        "geometry_hash",
    ).collect()
    values = [tuple(row) for row in rows]
    if len(values) != 1 or values[0][:3] != ("42", None, 12.5):
        raise RuntimeError(f"basin normalize smoke returned unexpected rows: {values!r}")
    if len(values[0][3]) != 64:
        raise RuntimeError("basin normalize smoke returned an invalid geometry hash")
    geometry_valid = normalized.selectExpr(
        "ST_IsValid(ST_GeomFromWKB(geometry_wkb)) AS valid"
    ).first()["valid"]
    if geometry_valid is not True:
        raise RuntimeError("basin normalize smoke returned invalid canonical geometry")


def run_roundtrip(spark, namespace: str) -> None:
    if NAMESPACE_PATTERN.fullmatch(namespace) is None:
        raise ValueError(f"unsafe smoke namespace: {namespace!r}")

    qualified_namespace = f"{CATALOG}.{namespace}"
    qualified_table = f"{qualified_namespace}.roundtrip"
    primary_error: Exception | None = None

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {qualified_namespace}")
        spark.sql(f"CREATE TABLE {qualified_table} (id BIGINT, name STRING) USING iceberg")
        frame = spark.createDataFrame(EXPECTED_ROWS, ("id", "name"))
        first = SparkTablePublisher(spark, qualified_table).publish(
            frame,
            key_fields=("id",),
            mode="merge",
        )
        repeated = SparkTablePublisher(spark, qualified_table).publish(
            frame,
            key_fields=("id",),
            mode="merge",
        )
        if repeated.snapshot_id != first.snapshot_id:
            raise RuntimeError("idempotent Spark MERGE created an unnecessary snapshot")
        rows = [
            tuple(row)
            for row in spark.sql(
                f"SELECT id, name FROM {qualified_table} ORDER BY id"
            ).collect()
        ]
        if rows != EXPECTED_ROWS:
            raise RuntimeError(f"Iceberg round trip returned unexpected rows: {rows!r}")
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors: list[Exception] = []
        for statement in (
            f"DROP TABLE IF EXISTS {qualified_table}",
            f"DROP NAMESPACE IF EXISTS {qualified_namespace}",
        ):
            try:
                spark.sql(statement)
            except Exception as exc:  # noqa: BLE001 - both cleanup statements must be attempted
                cleanup_errors.append(exc)

        if cleanup_errors:
            if primary_error is not None:
                for error in cleanup_errors:
                    print(f"Spark smoke cleanup also failed: {error}", file=sys.stderr)
            else:
                raise cleanup_errors[0]


def main() -> None:
    namespace = make_smoke_namespace()
    print(f"Spark Iceberg smoke namespace: {namespace}")
    spark = build_spark_session("flood-lakehouse-iceberg-smoke")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_spatial_smoke(spark)
        run_basin_normalize_smoke(spark)
        run_roundtrip(spark, namespace)
        print(
            "Spark Sedona, basin normalization, and Iceberg smoke passed; "
            "temporary table and namespace removed."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
