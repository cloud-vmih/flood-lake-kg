"""A bounded timestamp merge must prune the target and reject out-of-scope rows."""

from datetime import UTC, datetime

import pytest

from flashflood_data.shared.spark.publish import SparkTablePublisher, TimestampMergeScope, merge_sql


def _scope():
    return TimestampMergeScope("valid_time", datetime(2021, 7, 1, tzinfo=UTC),
                               datetime(2021, 8, 1, tzinfo=UTC))


class Frame:
    columns = ("business_key_hash", "valid_time", "value")
    schema = ("string", "timestamp", "double")

    def createOrReplaceTempView(self, name):
        pass


class Result:
    def __init__(self, row):
        self.row = row

    def collect(self):
        return [self.row]


class Spark:
    def __init__(self, outside=0):
        self.statements = []
        self.outside = outside

    def table(self, name):
        return Frame()

    def sql(self, query):
        self.statements.append(query)
        if "AS total_count" in query:
            return Result({"total_count": 1, "distinct_count": 1, "null_count": 0,
                           "out_of_scope_count": self.outside})
        if "AS change_count" in query:
            return Result({"change_count": 1})
        if ".snapshots" in query:
            return Result({"snapshot_id": 5})
        if ".files" in query:
            return Result({"file_count": 1})
        return Result({"row_count": 1})


def test_merge_bounds_apply_to_change_detection_and_commit() -> None:
    spark = Spark()
    output = SparkTablePublisher(spark, "silver.basin_weather_value").publish(
        Frame(), key_fields=("business_key_hash", "valid_time"), mode="merge", merge_scope=_scope(),
    )
    assert output.snapshot_id == 5
    for statement in spark.statements:
        if "AS change_count" in statement or statement.startswith("MERGE INTO"):
            assert "t.`valid_time` >= TIMESTAMP '2021-07-01 00:00:00.000000'" in statement
            assert "t.`valid_time` < TIMESTAMP '2021-08-01 00:00:00.000000'" in statement
    validation = next(query for query in spark.statements if "AS total_count" in query)
    assert "out_of_scope_count" in validation


def test_out_of_scope_source_cannot_write_or_create_duplicates() -> None:
    spark = Spark(outside=1)
    with pytest.raises(ValueError, match="scope"):
        SparkTablePublisher(spark, "silver.basin_weather_value").publish(
            Frame(), key_fields=("business_key_hash", "valid_time"), mode="merge",
            merge_scope=_scope(),
        )
    assert not any(query.startswith("MERGE INTO") for query in spark.statements)


def test_scope_column_must_be_part_of_merge_identity() -> None:
    with pytest.raises(ValueError, match="scope"):
        merge_sql("silver.basin_weather_value", "source", Frame.columns,
                  ("business_key_hash",), update_matches=True, merge_scope=_scope())


def test_scope_rejects_unsafe_columns_invalid_bounds_and_naive_times() -> None:
    with pytest.raises(ValueError, match="column"):
        TimestampMergeScope("valid_time; DROP", _scope().start, _scope().end)
    with pytest.raises(ValueError, match="bounds"):
        TimestampMergeScope("valid_time", _scope().end, _scope().start)
    with pytest.raises(ValueError, match="UTC"):
        TimestampMergeScope(
            "valid_time", datetime(2021, 7, 1), _scope().end,  # noqa: DTZ001 - reject naive input
        )
