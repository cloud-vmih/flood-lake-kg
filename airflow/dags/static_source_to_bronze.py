"""Airflow DAG for parsing registered static raw objects into Bronze Iceberg."""

from datetime import UTC, datetime

from airflow.sdk import dag, task

from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.bronze.batching import batch_object_refs
from flashflood_data.orchestration.bronze.config import load_bronze_config
from flashflood_data.orchestration.bronze.factory import build_bronze_service

CONFIG_PATH = ProjectPaths.discover().root / "config" / "bronze" / "static.yaml"
SOURCE_IDS = load_bronze_config(CONFIG_PATH).ready_source_ids()


@task(retries=2)
def discover_batches(
    source_id: str, requested_source_id: str = "", force_reprocess: object = False
) -> list[dict[str, object]]:
    """Pass only registered raw object identities to mapped parse tasks."""
    config = load_bronze_config(CONFIG_PATH)
    if not config.should_process(source_id, requested_source_id):
        return []
    service = build_bronze_service()
    object_ids = service.discover(
        source_id,
        parser_version=config.parser_version(source_id),
        force_reprocess=config.force_reprocess(force_reprocess),
    )
    return batch_object_refs(source_id, object_ids, batch_size=config.batch_size(source_id))


@task(retries=2)
def process_batch(batch_ref: dict[str, object], bronze_run_id: str) -> dict[str, object]:
    """Parse and audit one configured raw-object batch; no raw bytes use XCom."""
    source_id = str(batch_ref["source_id"])
    object_ids = tuple(str(value) for value in batch_ref["object_ids"])
    config = load_bronze_config(CONFIG_PATH)
    service = build_bronze_service()
    if config.batch_size(source_id) == 1:
        if len(object_ids) != 1:
            raise ValueError("single-object Bronze work must contain exactly one object")
        return service.process_object(
            source_id, object_ids[0], run_id=bronze_run_id,
            parser_version=config.parser_version(source_id),
        )
    return service.process_batch(
        source_id, object_ids, run_id=bronze_run_id,
        parser_version=config.parser_version(source_id),
    )


@dag(
    dag_id="static_source_to_bronze",
    description="Parse registered static raw objects into Bronze Iceberg tables",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2},
    tags=["source", "static", "bronze", "iceberg"],
)
def static_source_to_bronze_dag():
    """Keep source groups independent; serialize all Iceberg writers via one pool."""
    bronze_run_id = "{{ run_id }}"
    requested_source_id = "{{ dag_run.conf.get('source_id', '') if dag_run else '' }}"
    force_reprocess = "{{ dag_run.conf.get('force_reprocess', false) if dag_run else false }}"
    for source_id in SOURCE_IDS:
        batches = discover_batches.override(task_id=f"discover_{source_id}")(
            source_id, requested_source_id, force_reprocess
        )
        process_batch.override(task_id=f"parse_{source_id}", pool="bronze_writer").partial(
            bronze_run_id=bronze_run_id
        ).expand(batch_ref=batches)


static_source_to_bronze = static_source_to_bronze_dag()
