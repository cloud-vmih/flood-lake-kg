"""Single-owner Spark Iceberg publication with guarded write modes."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from flashflood_data.shared.spark.models import PublishedSnapshot

PublishMode = Literal["append", "merge", "overwrite"]
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UNSAFE_SQL = (";", "--", "/*", "*/")


def _parts(identifier: str, lengths: tuple[int, ...] = (2, 3)) -> tuple[str, ...]:
    values = tuple(identifier.split("."))
    if len(values) not in lengths or any(_IDENTIFIER.fullmatch(item) is None for item in values):
        raise ValueError(f"unsafe Spark identifier: {identifier}")
    return values


def _quoted(identifier: str) -> str:
    return ".".join(f"`{part}`" for part in _parts(identifier))


def _column(name: str, alias: str | None = None) -> str:
    if _IDENTIFIER.fullmatch(name) is None:
        raise ValueError(f"unsafe Spark column identifier: {name}")
    quoted = f"`{name}`"
    return f"{alias}.{quoted}" if alias else quoted


def _value(row: object, name: str) -> object:
    if isinstance(row, Mapping):
        return row[name]
    try:
        return row[name]  # type: ignore[index]
    except (KeyError, TypeError):
        return getattr(row, name)


def merge_sql(
    target_table: str,
    source_view: str,
    columns: Sequence[str],
    key_fields: Sequence[str],
    *,
    update_matches: bool,
) -> str:
    target = _quoted(target_table)
    source = _column(source_view)
    keys = tuple(key_fields)
    if not keys or any(key not in columns for key in keys):
        raise ValueError("merge key fields must be present and non-empty")
    join = " AND ".join(
        f"{_column(key, 't')} <=> {_column(key, 's')}" for key in keys
    )
    names = ", ".join(_column(name) for name in columns)
    values = ", ".join(_column(name, "s") for name in columns)
    matched = ""
    if update_matches:
        equal = " AND ".join(
            f"{_column(name, 't')} <=> {_column(name, 's')}" for name in columns
        )
        assignments = ", ".join(
            f"{_column(name, 't')} = {_column(name, 's')}" for name in columns
        )
        matched = f" WHEN MATCHED AND NOT ({equal}) THEN UPDATE SET {assignments}"
    return (
        f"MERGE INTO {target} AS t USING {source} AS s ON {join}"
        f"{matched} WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
    )


class SparkTablePublisher:
    """Own exactly one commit attempt for one physical Silver table."""

    def __init__(self, spark: Any, table: str) -> None:
        _parts(table)
        self.spark = spark
        self.table = table
        self._committed = False

    def _validate_frame(self, frame: Any, key_fields: tuple[str, ...], view: str) -> None:
        columns = tuple(frame.columns)
        target_frame = self.spark.table(self.table)
        target_columns = tuple(target_frame.columns)
        if columns != target_columns or frame.schema != target_frame.schema:
            raise ValueError(
                f"publish schema mismatch for {self.table}: "
                f"{frame.schema!r} != {target_frame.schema!r}"
            )
        if not key_fields or any(key not in columns for key in key_fields):
            raise ValueError("publish key fields must be present and non-empty")
        keys = ", ".join(_column(name) for name in key_fields)
        nulls = " OR ".join(f"{_column(name)} IS NULL" for name in key_fields)
        rows = self.spark.sql(
            "SELECT COUNT(*) AS total_count, "
            f"COUNT(DISTINCT struct({keys})) AS distinct_count, "
            f"SUM(CASE WHEN {nulls} THEN 1 ELSE 0 END) AS null_count FROM {_column(view)}"
        ).collect()
        if not rows:
            raise RuntimeError("Spark publish validation returned no aggregate row")
        total = int(_value(rows[0], "total_count"))
        distinct = int(_value(rows[0], "distinct_count"))
        null_count = int(_value(rows[0], "null_count") or 0)
        if total < 1:
            raise ValueError("cannot publish an empty Spark DataFrame")
        if distinct != total or null_count:
            raise ValueError("publish business keys must be non-null and unique")

    def _conflict_count(
        self,
        source_view: str,
        columns: tuple[str, ...],
        key_fields: tuple[str, ...],
    ) -> int:
        join = " AND ".join(
            f"{_column(key, 't')} <=> {_column(key, 's')}" for key in key_fields
        )
        equal = " AND ".join(
            f"{_column(name, 't')} <=> {_column(name, 's')}" for name in columns
        )
        rows = self.spark.sql(
            "SELECT COUNT(*) AS conflict_count "
            f"FROM {_quoted(self.table)} AS t INNER JOIN {_column(source_view)} AS s ON {join} "
            f"WHERE NOT ({equal})"
        ).collect()
        return 0 if not rows else int(_value(rows[0], "conflict_count"))

    def _change_count(
        self,
        source_view: str,
        columns: tuple[str, ...],
        key_fields: tuple[str, ...],
    ) -> int:
        join = " AND ".join(
            f"{_column(key, 't')} <=> {_column(key, 's')}" for key in key_fields
        )
        equal = " AND ".join(
            f"{_column(name, 't')} <=> {_column(name, 's')}" for name in columns
        )
        rows = self.spark.sql(
            "SELECT COUNT(*) AS change_count "
            f"FROM {_column(source_view)} AS s LEFT JOIN {_quoted(self.table)} AS t ON {join} "
            f"WHERE {_column(key_fields[0], 't')} IS NULL OR NOT ({equal})"
        ).collect()
        return 0 if not rows else int(_value(rows[0], "change_count"))

    def _evidence(self) -> PublishedSnapshot:
        target = _quoted(self.table)
        snapshots = self.spark.sql(
            f"SELECT snapshot_id FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        if not snapshots:
            raise RuntimeError(f"published table has no Iceberg snapshot: {self.table}")
        rows = self.spark.sql(f"SELECT COUNT(*) AS row_count FROM {target}").collect()
        files = self.spark.sql(f"SELECT COUNT(*) AS file_count FROM {target}.files").collect()
        return PublishedSnapshot(
            table=".".join(_parts(self.table)[-2:]),
            snapshot_id=int(_value(snapshots[0], "snapshot_id")),
            row_count=int(_value(rows[0], "row_count")),
            file_count=int(_value(files[0], "file_count")),
        )

    def publish(
        self,
        frame: Any,
        *,
        key_fields: Sequence[str],
        mode: PublishMode,
        overwrite_filter: str | None = None,
    ) -> PublishedSnapshot:
        if self._committed:
            raise RuntimeError(f"publisher already committed table: {self.table}")
        if mode not in {"append", "merge", "overwrite"}:
            raise ValueError(f"unsupported Spark publish mode: {mode}")
        if mode == "overwrite" and (
            not overwrite_filter
            or overwrite_filter.strip().lower() in {"true", "1=1"}
            or any(marker in overwrite_filter for marker in _UNSAFE_SQL)
        ):
            raise ValueError("overwrite mode requires a bounded safe filter")

        keys = tuple(key_fields)
        view = "_flashflood_publish_" + hashlib.sha256(
            self.table.encode("utf-8")
        ).hexdigest()[:16]
        frame.createOrReplaceTempView(view)
        try:
            self._validate_frame(frame, keys, view)
            columns = tuple(frame.columns)
            if mode in {"append", "merge"}:
                if mode == "append":
                    conflicts = self._conflict_count(view, columns, keys)
                    if conflicts:
                        raise ValueError("immutable append key conflicts with existing target row")
                if mode != "merge" or self._change_count(view, columns, keys):
                    self.spark.sql(
                        merge_sql(
                            self.table,
                            view,
                            columns,
                            keys,
                            update_matches=mode == "merge",
                        )
                    )
            else:
                from pyspark.sql import functions as spark_functions

                frame.writeTo(self.table).overwrite(spark_functions.expr(str(overwrite_filter)))
            output = self._evidence()
            self._committed = True
            return output
        finally:
            catalog = getattr(self.spark, "catalog", None)
            if catalog is not None and hasattr(catalog, "dropTempView"):
                catalog.dropTempView(view)
