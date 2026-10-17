from __future__ import annotations

import pytest


class _Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def collect(self):
        return self.rows


class _Frame:
    columns = ("basin_id", "basin_version", "value")
    schema = ("string", "string", "double")

    def __init__(self):
        self.views = []

    def createOrReplaceTempView(self, name):
        self.views.append(name)


class _Target:
    columns = _Frame.columns
    schema = _Frame.schema


class _Spark:
    def __init__(self):
        self.statements = []

    def table(self, name):
        return _Target()

    def sql(self, statement):
        self.statements.append(statement)
        if "AS total_count" in statement:
            return _Result([{"total_count": 2, "distinct_count": 2, "null_count": 0}])
        if "AS conflict_count" in statement:
            return _Result([{"conflict_count": 0}])
        if "AS change_count" in statement:
            return _Result([{"change_count": 1}])
        if ".snapshots" in statement:
            return _Result([{"snapshot_id": 91}])
        if ".files" in statement:
            return _Result([{"file_count": 1}])
        if statement.startswith("SELECT COUNT(*) AS row_count"):
            return _Result([{"row_count": 2}])
        return _Result()


def test_merge_publisher_commits_once_and_returns_snapshot_evidence() -> None:
    from flashflood_data.shared.spark.publish import SparkTablePublisher

    spark = _Spark()
    publisher = SparkTablePublisher(spark, "flood_lakehouse.silver.dim_basin")

    output = publisher.publish(
        _Frame(),
        key_fields=("basin_id", "basin_version"),
        mode="merge",
    )

    assert output.snapshot_id == 91
    assert output.row_count == 2
    assert sum(statement.startswith("MERGE INTO") for statement in spark.statements) == 1
    merge = next(statement for statement in spark.statements if statement.startswith("MERGE INTO"))
    assert "WHEN MATCHED AND NOT" in merge
    assert sum("AS conflict_count" in statement for statement in spark.statements) == 0
    with pytest.raises(RuntimeError, match="already committed"):
        publisher.publish(_Frame(), key_fields=("basin_id", "basin_version"), mode="merge")


def test_publisher_rejects_unsafe_identifier_and_unfiltered_overwrite() -> None:
    from flashflood_data.shared.spark.publish import SparkTablePublisher

    with pytest.raises(ValueError, match="identifier"):
        SparkTablePublisher(_Spark(), "silver.dim_basin; DROP TABLE x")
    publisher = SparkTablePublisher(_Spark(), "flood_lakehouse.silver.dim_basin")
    with pytest.raises(ValueError, match="filter"):
        publisher.publish(_Frame(), key_fields=("basin_id",), mode="overwrite")


def test_publisher_rejects_same_column_names_with_different_types() -> None:
    from flashflood_data.shared.spark.publish import SparkTablePublisher

    class WrongTypes(_Frame):
        schema = ("string", "string", "string")

    with pytest.raises(ValueError, match="schema mismatch"):
        SparkTablePublisher(
            _Spark(), "flood_lakehouse.silver.dim_basin"
        ).publish(
            WrongTypes(),
            key_fields=("basin_id", "basin_version"),
            mode="merge",
        )


def test_merge_publisher_skips_commit_when_source_matches_target() -> None:
    from flashflood_data.shared.spark.publish import SparkTablePublisher

    class NoChangesSpark(_Spark):
        def sql(self, statement):
            if "AS change_count" in statement:
                self.statements.append(statement)
                return _Result([{"change_count": 0}])
            return super().sql(statement)

    spark = NoChangesSpark()

    output = SparkTablePublisher(
        spark, "flood_lakehouse.silver.dim_basin"
    ).publish(
        _Frame(),
        key_fields=("basin_id", "basin_version"),
        mode="merge",
    )

    assert output.snapshot_id == 91
    assert not any(statement.startswith("MERGE INTO") for statement in spark.statements)
