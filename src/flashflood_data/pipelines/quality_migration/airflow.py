"""Manual Airflow DAG for resumable Bronze quality migration."""

from datetime import UTC, datetime

from airflow.sdk import dag, task

from flashflood_data.pipelines.quality_migration.runtime import (
    _boolean,
    build_quality_migration_runtime,
)


@task(retries=0, pool="bronze_writer")
def migrate_bronze_quality(
    dataset_id: str,
    mode: str,
    object_limit: str,
    force: object,
    snapshot_id: str,
    migration_run_id: str,
) -> dict[str, object]:
    if not dataset_id:
        raise ValueError("bronze_quality_backfill requires dag_run.conf.dataset_id")
    return build_quality_migration_runtime().run(
        dataset_id=dataset_id,
        mode=mode or "audit",
        object_limit=int(object_limit or "100"),
        force=_boolean(force),
        snapshot_id=int(snapshot_id or "0"),
        run_id=migration_run_id,
    )


@dag(
    dag_id="bronze_quality_backfill",
    description="Audit or selectively repair existing Bronze objects under a DQ policy",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 0},
    tags=["bronze", "quality", "quarantine", "migration"],
)
def bronze_quality_backfill_dag():
    migrate_bronze_quality(
        dataset_id="{{ dag_run.conf.get('dataset_id', '') }}",
        mode="{{ dag_run.conf.get('mode', 'audit') }}",
        object_limit="{{ dag_run.conf.get('object_limit', '100') }}",
        force="{{ dag_run.conf.get('force', false) }}",
        snapshot_id="{{ dag_run.conf.get('snapshot_id', '0') }}",
        migration_run_id="{{ run_id }}",
    )


bronze_quality_backfill = bronze_quality_backfill_dag()
