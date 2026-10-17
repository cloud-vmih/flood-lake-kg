from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from flashflood_data.shared.spark.models import PublishedSnapshot, StagedDataset


class _Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def collect(self):
        return self.rows


class _Reader:
    def option(self, _name, _value):
        return self

    def table(self, name):
        return name


class _Catalog:
    def dropTempView(self, _name):
        return True


class _Frame:
    columns = ("basin_id", "basin_version", "value")
    schema = ("string", "string", "double")

    def createOrReplaceTempView(self, _name):
        return None


class _Target:
    columns = _Frame.columns
    schema = _Frame.schema


class _Spark:
    def __init__(self):
        self.read = _Reader()
        self.catalog = _Catalog()
        self.history_rows = []

    def table(self, _name):
        return _Target()

    def sql(self, statement):
        if "meta.pipeline_runs" in statement:
            return _Result(self.history_rows)
        if "bronze.basin_polygon_raw.snapshots" in statement:
            return _Result(({"snapshot_id": 17},))
        if "AS total_count" in statement:
            return _Result(({"total_count": 1, "distinct_count": 1, "null_count": 0},))
        if "AS conflict_count" in statement:
            return _Result(({"conflict_count": 0},))
        if ".snapshots" in statement:
            return _Result(({"snapshot_id": 91},))
        if ".files" in statement:
            return _Result(({"file_count": 1},))
        if "AS row_count" in statement:
            return _Result(({"row_count": 1},))
        return _Result()


class _Store:
    def __init__(self):
        self.objects = {}

    def exists(self, key):
        return key in self.objects

    def sha256(self, key):
        return sha256(self.objects[key]).hexdigest()

    def read(self, key):
        return self.objects[key]

    def upload(self, local_path: Path, key, metadata):
        del metadata
        self.objects[key] = local_path.read_bytes()

    def delete(self, key):
        self.objects.pop(key, None)


class _Meta:
    def __init__(self):
        self.runs = []

    def record_run(self, row):
        self.runs.append(dict(row))
        return 1

    def record_snapshot_refs(self, rows):
        return len(tuple(rows))

    def record_qualities(self, rows):
        return len(tuple(rows))

    def record_lineages(self, rows):
        return tuple(str(index) for index, _ in enumerate(rows))


def test_fake_domain_build_stages_publishes_audits_then_skips() -> None:
    from flashflood_data.shared.spark.audit import SparkAudit, SparkAuditRequest
    from flashflood_data.shared.spark.publish import SparkTablePublisher
    from flashflood_data.shared.spark.snapshots import SparkSnapshotManager
    from flashflood_data.shared.spark.staging import SparkStaging, StagingManifest

    spark = _Spark()
    snapshots = SparkSnapshotManager("flood_lakehouse")
    plan_arguments = {
        "job_name": "fake_domain",
        "input_tables": ("bronze.basin_polygon_raw",),
        "config_hash": "cfg",
        "policy_id": "policy",
        "algorithm_version": "v1",
        "contract_version": "1",
        "required_outputs": ("silver.dim_basin",),
    }
    plan = snapshots.plan(spark, **plan_arguments)
    assert plan.action == "build"

    store = _Store()
    data_key = "staging/silver/fake_domain/run-1/batch-1/data.parquet"
    store.objects[data_key] = b"parquet"
    manifest = StagingManifest(
        pipeline="fake_domain",
        pipeline_run_id="run-1",
        batch_id="batch-1",
        build_signature=plan.build_signature,
        input_snapshots=plan.input_snapshots,
        datasets=(
            StagedDataset(
                uri=f"s3://{data_key}",
                schema_hash="schema-v1",
                row_count=1,
                checksum=sha256(b"parquet").hexdigest(),
            ),
        ),
    )
    assert SparkStaging(store).commit(manifest) is False

    output = SparkTablePublisher(
        spark, "flood_lakehouse.silver.dim_basin"
    ).publish(
        _Frame(),
        key_fields=("basin_id", "basin_version"),
        mode="append",
    )
    assert output == PublishedSnapshot("silver.dim_basin", 91, 1, 1)

    meta = _Meta()
    request = SparkAuditRequest(
        pipeline_run_id="run-1",
        orchestrator_run_id="airflow-1",
        job_name="fake_domain",
        build_signature=plan.build_signature,
        config_hash="cfg",
        policy_id="policy",
        algorithm_version="v1",
        input_snapshots=plan.input_snapshots,
        required_outputs=("silver.dim_basin",),
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    SparkAudit(meta, now=lambda: datetime(2026, 1, 2, tzinfo=UTC)).finalize_success(
        request,
        (output,),
        quality_rows=(
            {
                "rule_id": "row_count_positive",
                "status": "passed",
                "severity": "fatal",
            },
        ),
    )
    final_run = meta.runs[-1]
    spark.history_rows = [
        {
            "pipeline_run_id": "run-1",
            "status": final_run["status"],
            "metrics_json": final_run["metrics_json"],
            "table_name": "silver.dim_basin",
        }
    ]

    rerun = snapshots.plan(spark, **plan_arguments)
    assert rerun.action == "skip"
    assert rerun.prior_run_id == "run-1"
