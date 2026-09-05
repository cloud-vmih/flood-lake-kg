from __future__ import annotations

import re
import sys
import uuid

from runtime import build_spark_session

CATALOG = "flood_lakehouse"
NAMESPACE_PATTERN = re.compile(r"smoke_[0-9a-f]{32}")
EXPECTED_ROWS = [(1, "basin"), (2, "rain"), (3, "risk")]


def make_smoke_namespace() -> str:
    return f"smoke_{uuid.uuid4().hex}"


def run_roundtrip(spark, namespace: str) -> None:
    if NAMESPACE_PATTERN.fullmatch(namespace) is None:
        raise ValueError(f"unsafe smoke namespace: {namespace!r}")

    qualified_namespace = f"{CATALOG}.{namespace}"
    qualified_table = f"{qualified_namespace}.roundtrip"
    primary_error: Exception | None = None

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {qualified_namespace}")
        spark.sql(
            f"CREATE TABLE {qualified_table} (id INT, name STRING) USING iceberg"
        )
        spark.sql(
            f"INSERT INTO {qualified_table} VALUES "
            "(1, 'basin'), (2, 'rain'), (3, 'risk')"
        )
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
    try:
        run_roundtrip(spark, namespace)
        print("Spark Iceberg round trip passed; temporary table and namespace removed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
