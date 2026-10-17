from __future__ import annotations

from flashflood_data.shared.spark.models import PinnedSnapshot


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _Reader:
    def __init__(self):
        self.options = {}
        self.table_name = None

    def option(self, name, value):
        self.options[name] = value
        return self

    def table(self, name):
        self.table_name = name
        return (name, dict(self.options))


class _Spark:
    def __init__(self):
        self.read = _Reader()
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        return _Result([{"snapshot_id": 17}])


class _HistorySpark(_Spark):
    def sql(self, statement):
        self.statements.append(statement)
        if ".snapshots" in statement:
            return _Result([{"snapshot_id": 17}])
        return _Result(
            [
                {
                    "pipeline_run_id": "run-1",
                    "status": "failed",
                    "metrics_json": '{"build_signature":"sig-1"}',
                    "table_name": "silver.dim_basin",
                },
                {
                    "pipeline_run_id": "run-1",
                    "status": "failed",
                    "metrics_json": '{"build_signature":"sig-1"}',
                    "table_name": "silver.dim_basin",
                },
                {
                    "pipeline_run_id": "run-2",
                    "status": "succeeded",
                    "metrics_json": '{"build_signature":"sig-2"}',
                    "table_name": None,
                },
            ]
        )


def test_snapshot_manager_pins_and_reads_the_exact_snapshot() -> None:
    from flashflood_data.shared.spark.snapshots import SparkSnapshotManager

    spark = _Spark()
    manager = SparkSnapshotManager("flood_lakehouse")

    pinned = manager.pin(spark, ("bronze.basin_polygon_raw",))
    frame = manager.read(spark, pinned[0])

    assert pinned == (PinnedSnapshot("bronze.basin_polygon_raw", 17),)
    assert spark.read.options == {"snapshot-id": "17"}
    assert frame[0] == "flood_lakehouse.bronze.basin_polygon_raw"


def test_incremental_plan_returns_build_repair_and_skip() -> None:
    from flashflood_data.shared.spark.snapshots import BuildRecord, plan_incremental_build

    snapshots = (PinnedSnapshot("bronze.basin_polygon_raw", 17),)
    kwargs = {
        "snapshots": snapshots,
        "config_hash": "cfg",
        "policy_id": "policy",
        "algorithm_version": "v1",
        "contract_version": "1",
        "required_outputs": ("silver.dim_basin", "silver.basin_edge"),
    }

    build = plan_incremental_build(history=(), **kwargs)
    repair = plan_incremental_build(
        history=(BuildRecord("old", "failed", build.build_signature, ("silver.dim_basin",)),),
        **kwargs,
    )
    skip = plan_incremental_build(
        history=(
            BuildRecord(
                "done",
                "succeeded",
                build.build_signature,
                ("silver.dim_basin", "silver.basin_edge"),
            ),
        ),
        **kwargs,
    )

    assert build.action == "build"
    assert repair.action == "repair" and repair.missing_outputs == ("silver.basin_edge",)
    assert skip.action == "skip" and skip.missing_outputs == ()


def test_snapshot_manager_reads_deduplicated_build_history_from_meta() -> None:
    from flashflood_data.shared.spark.snapshots import BuildRecord, SparkSnapshotManager

    spark = _HistorySpark()
    manager = SparkSnapshotManager("flood_lakehouse")

    history = manager.history(spark, "silver_basin_topology")

    assert history == (
        BuildRecord("run-1", "failed", "sig-1", ("silver.dim_basin",)),
        BuildRecord("run-2", "succeeded", "sig-2", ()),
    )
    assert "flood_lakehouse.meta.pipeline_runs" in spark.statements[-1]
    assert "role = 'output'" in spark.statements[-1]


def test_snapshot_manager_rejects_unsafe_job_name() -> None:
    import pytest

    from flashflood_data.shared.spark.snapshots import SparkSnapshotManager

    with pytest.raises(ValueError, match="job name"):
        SparkSnapshotManager("flood_lakehouse").history(_Spark(), "job'; DROP TABLE x")


def test_snapshot_manager_discovers_inputs_and_returns_one_build_plan() -> None:
    from flashflood_data.shared.spark.snapshots import SparkSnapshotManager

    plan = SparkSnapshotManager("flood_lakehouse").plan(
        _HistorySpark(),
        job_name="silver_basin_topology",
        input_tables=("bronze.basin_polygon_raw",),
        config_hash="cfg",
        policy_id="policy",
        algorithm_version="v1",
        contract_version="1",
        required_outputs=("silver.dim_basin", "silver.basin_edge"),
    )

    assert plan.action == "build"
    assert plan.input_snapshots == (PinnedSnapshot("bronze.basin_polygon_raw", 17),)
