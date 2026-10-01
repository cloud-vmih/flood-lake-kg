"""Airflow TaskFlow factory shared by the dynamic weather DAGs."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, dag, get_current_context, task, task_group

from flashflood_data.orchestration.bronze.batching import batch_object_refs
from flashflood_data.orchestration.weather.config import load_weather_config
from flashflood_data.orchestration.weather.factory import build_weather_runtime
from flashflood_data.orchestration.weather.grids import GridRegistration
from flashflood_data.orchestration.weather.models import (
    FetchAttemptRecord,
    FetchedWeatherObject,
    PlannedWeatherObject,
    ScopedWeatherObject,
)
from flashflood_data.orchestration.weather.planner import (
    batch_documents,
    verify_weather_outcomes,
)

WEATHER_BRONZE_ASSET = Asset(
    "iceberg://flood_lakehouse/bronze/weather_bronze_updated"
)


def _boolean(value: object) -> bool:
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


def build_weather_dag(dag_id: str, config_path: Path):
    """Build one source DAG while keeping all provider logic outside the DAG file."""
    config_path = Path(config_path)
    config = load_weather_config(config_path)

    @task(retries=2)
    def register_dynamic_registry() -> dict[str, int]:
        return build_weather_runtime(config_path).register_meta()

    @task(retries=2)
    def ensure_source_grid() -> dict[str, object]:
        return build_weather_runtime(config_path).ensure_source_grid().to_document()

    @task(retries=2)
    def load_cursor(grid_document: dict[str, object]) -> dict[str, object]:
        grid = GridRegistration.from_document(grid_document)
        return build_weather_runtime(config_path).cursor_document(grid.scope_id)

    @task(retries=2)
    def determine_available_end() -> dict[str, str]:
        return build_weather_runtime(config_path).safe_end_document(datetime.now(UTC))

    @task(retries=0)
    def plan_expected_windows(
        cursors: dict[str, object],
        safe_ends: dict[str, str],
        mode: str,
        requested_start: str,
        requested_end: str,
        requested_limit: str,
        grid_document: dict[str, object],
    ) -> dict[str, object]:
        grid = GridRegistration.from_document(grid_document)
        return build_weather_runtime(config_path).plan_document(
            cursors,
            safe_ends,
            mode=mode,
            requested_start=requested_start,
            requested_end=requested_end,
            requested_limit=requested_limit,
            spatial_scope_id=grid.scope_id,
        )

    @task(retries=0)
    def extract_missing_batches(plan: dict[str, object]) -> list[list[dict[str, object]]]:
        """Bound mapped task count while retaining every planned provider object."""
        return batch_documents(plan["missing"], config.task_batch_size)

    @task(
        retries=3,
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        pool="weather_fetch",
    )
    def fetch_missing_or_revised(
        planned_documents: list[dict[str, object]],
        weather_run_id: str,
        grid_document: dict[str, object],
    ) -> list[dict[str, object]]:
        context = get_current_context()
        task_instance = context["ti"]
        attempt_no = int(task_instance.try_number)
        batch_index = int(task_instance.map_index)
        runtime = build_weather_runtime(config_path)
        grid = GridRegistration.from_document(grid_document)
        plans = [PlannedWeatherObject.model_validate(item) for item in planned_documents]
        fetched_documents: list[dict[str, object]] = []
        attempts: list[FetchAttemptRecord] = []
        failure: Exception | None = None
        fetched_batch = runtime.fetch_many(tuple(plans), weather_run_id, grid=grid)
        for planned in plans:
            started_at = datetime.now(UTC)
            try:
                fetched = next(fetched_batch)
                fetched_documents.append(
                    {
                        "status": "available",
                        "attempt_no": attempt_no,
                        "fetched": fetched.model_dump(mode="json"),
                    }
                )
                attempts.append(
                    FetchAttemptRecord(
                        ingest_run_id=weather_run_id,
                        source_id=planned.source_id,
                        asset_id=planned.asset_id,
                        attempt_no=attempt_no,
                        request_fingerprint=planned.request_fingerprint,
                        status="running",
                        started_at=started_at,
                    )
                )
            except Exception as error:  # noqa: BLE001 - Airflow owns batch retries
                response = getattr(error, "response", None)
                attempts.append(
                    FetchAttemptRecord(
                        ingest_run_id=weather_run_id,
                        source_id=planned.source_id,
                        asset_id=planned.asset_id,
                        attempt_no=attempt_no,
                        request_fingerprint=planned.request_fingerprint,
                        status="failed",
                        started_at=started_at,
                        ended_at=datetime.now(UTC),
                        http_status=getattr(response, "status_code", None),
                        error_code=type(error).__name__,
                    )
                )
                failure = error
                break
        if failure is not None:
            ended_at = datetime.now(UTC)
            attempts = [
                row.model_copy(
                    update={
                        "status": "skipped",
                        "ended_at": ended_at,
                        "error_code": "BatchAborted",
                    }
                )
                if row.status == "running"
                else row
                for row in attempts
            ]
            attempted_ids = {row.asset_id for row in attempts}
            attempts.extend(
                FetchAttemptRecord(
                    ingest_run_id=weather_run_id,
                    source_id=planned.source_id,
                    asset_id=planned.asset_id,
                    attempt_no=attempt_no,
                    request_fingerprint=planned.request_fingerprint,
                    status="skipped",
                    started_at=ended_at,
                    ended_at=ended_at,
                    error_code="BatchNotAttempted",
                )
                for planned in plans
                if planned.asset_id not in attempted_ids
            )
        runtime.stage_fetch_attempts(
            weather_run_id,
            batch_index=batch_index,
            attempt_no=attempt_no,
            rows=attempts,
        )
        if failure is not None:
            raise failure
        return fetched_documents

    @task(retries=2, trigger_rule="all_done", pool="weather_raw_writer")
    def commit_fetch_attempts(weather_run_id: str) -> dict[str, int]:
        return build_weather_runtime(config_path).commit_fetch_attempts(weather_run_id)

    @task(retries=2)
    def scope_fetched_payload(
        fetched_documents: list[dict[str, object]], grid_document: dict[str, object]
    ) -> list[dict[str, object]]:
        runtime = build_weather_runtime(config_path)
        grid = GridRegistration.from_document(grid_document)
        results = []
        for fetched_document in fetched_documents:
            if fetched_document.get("status") == "no_data":
                results.append(fetched_document)
                continue
            fetched = FetchedWeatherObject.model_validate(fetched_document["fetched"])
            scoped = runtime.scope_fetched(fetched, grid)
            results.append(
                {
                    "status": "available",
                    "attempt_no": fetched_document.get("attempt_no", 1),
                    "scoped": scoped.model_dump(mode="json"),
                }
            )
        return results

    @task(retries=3, pool="weather_raw_writer")
    def register_raw_and_meta(
        scoped_documents: list[dict[str, object]], weather_run_id: str
    ) -> list[dict[str, object]]:
        runtime = build_weather_runtime(config_path)
        service = runtime.landing_service()
        results: list[dict[str, object]] = []
        scoped_objects: list[ScopedWeatherObject] = []
        attempt_numbers: set[int] = set()
        for scoped_document in scoped_documents:
            if scoped_document.get("status") == "no_data":
                results.append(scoped_document)
                continue
            scoped_objects.append(
                ScopedWeatherObject.model_validate(scoped_document["scoped"])
            )
            attempt_numbers.add(int(scoped_document.get("attempt_no", 1)))
        if len(attempt_numbers) > 1:
            raise ValueError("one weather batch cannot mix Airflow attempt numbers")
        published_batch = service.publish_many(
            scoped_objects,
            run_id=weather_run_id,
            attempt_no=next(iter(attempt_numbers), 1),
        )
        for published in published_batch:
            results.append(
                {
                    "status": published.status,
                    "asset_id": published.asset_id,
                    "object_id": published.object_id,
                    "stream_id": published.stream_id,
                    "product": published.product,
                    "reused": published.reused,
                }
            )
        return results

    @task(retries=0, trigger_rule="none_failed")
    def verify_contiguous_coverage(
        plan: dict[str, object], outcomes: list[list[dict[str, object]]]
    ) -> list[dict[str, object]]:
        return verify_weather_outcomes(plan, outcomes)

    @task(retries=2, pool="weather_raw_writer")
    def advance_cursor(
        plan: dict[str, object], outcomes: list[dict[str, object]], weather_run_id: str
    ) -> dict[str, object]:
        return build_weather_runtime(config_path).advance(
            plan, outcomes, run_id=weather_run_id
        )

    @task(retries=2)
    def discover_unparsed_batches(
        _landing_result: dict[str, object], force_reprocess: object
    ) -> list[dict[str, object]]:
        runtime = build_weather_runtime(config_path)
        object_ids = runtime.bronze_service().discover(
            config.source_id,
            parser_version=config.parser_version,
            force_reprocess=_boolean(force_reprocess),
        )
        return batch_object_refs(
            config.source_id,
            object_ids,
            batch_size=config.bronze_task_batch_size,
        )

    @task(retries=2, pool="weather_bronze_writer")
    def parse_bronze(
        batch_ref: dict[str, object], weather_run_id: str
    ) -> dict[str, object]:
        return build_weather_runtime(config_path).bronze_service().process_batch(
            str(batch_ref["source_id"]),
            tuple(map(str, batch_ref["object_ids"])),
            run_id=weather_run_id,
            parser_version=config.parser_version,
        )

    @task(retries=0, outlets=[WEATHER_BRONZE_ASSET])
    def publish_bronze_update(results: list[dict[str, object]]) -> dict[str, object]:
        return {
            "source_id": config.source_id,
            "published_objects": sum(
                len(result.get("object_ids", ())) for result in results
            ),
            "asset": "weather_bronze_updated",
        }

    @task(retries=0, trigger_rule="all_done", pool="weather_raw_writer")
    def expire_transient_raw() -> dict[str, object]:
        return build_weather_runtime(config_path).expire_transient_raw(datetime.now(UTC))

    @task(retries=0, trigger_rule="all_done")
    def cleanup_weather_staging(weather_run_id: str) -> dict[str, object]:
        return build_weather_runtime(config_path).cleanup_run_staging(weather_run_id)

    @task(retries=0)
    def finalize_weather_run(
        bronze_result: dict[str, object],
        raw_cleanup: dict[str, object],
        staging_cleanup: dict[str, object],
    ) -> dict[str, object]:
        """Keep a failed upstream stage visible in the final DAG-run state."""
        return {
            "bronze": bronze_result,
            "raw_cleanup": raw_cleanup,
            "staging_cleanup": staging_cleanup,
        }

    @task_group(group_id="landing_raw")
    def landing_raw_group(
        weather_run_id: str,
        mode: str,
        requested_start: str,
        requested_end: str,
        requested_limit: str,
    ):
        registry = register_dynamic_registry()
        grid = ensure_source_grid()
        registry >> grid
        cursors = load_cursor(grid)
        safe_ends = determine_available_end()
        plan = plan_expected_windows(
            cursors,
            safe_ends,
            mode,
            requested_start,
            requested_end,
            requested_limit,
            grid,
        )
        missing = extract_missing_batches(plan)
        fetched = fetch_missing_or_revised.partial(
            weather_run_id=weather_run_id, grid_document=grid
        ).expand(planned_documents=missing)
        attempts_committed = commit_fetch_attempts(weather_run_id)
        fetched >> attempts_committed
        scoped = scope_fetched_payload.partial(grid_document=grid).expand(
            fetched_documents=fetched
        )
        attempts_committed >> scoped
        registered = register_raw_and_meta.partial(
            weather_run_id=weather_run_id
        ).expand(scoped_documents=scoped)
        verified = verify_contiguous_coverage(plan, registered)
        return advance_cursor(plan, verified, weather_run_id)

    @task_group(group_id="bronze")
    def bronze_group(
        landing_result: dict[str, object], weather_run_id: str, force_reprocess: object
    ):
        batches = discover_unparsed_batches(landing_result, force_reprocess)
        parsed = parse_bronze.partial(weather_run_id=weather_run_id).expand(
            batch_ref=batches
        )
        return publish_bronze_update(parsed)

    @dag(
        dag_id=dag_id,
        description=f"Catch up {config.source_id} Raw objects and parse them into Bronze Iceberg",
        schedule=config.schedule,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        default_args={"retries": 2},
        tags=["source", "dynamic", config.source_id, "minio", "iceberg"],
    )
    def weather_dag():
        weather_run_id = "{{ run_id }}"
        mode = "{{ dag_run.conf.get('mode', 'catchup') if dag_run else 'catchup' }}"
        requested_start = "{{ dag_run.conf.get('start', '') if dag_run else '' }}"
        requested_end = "{{ dag_run.conf.get('end', '') if dag_run else '' }}"
        requested_limit = "{{ dag_run.conf.get('max_objects', '') if dag_run else '' }}"
        force_reprocess = (
            "{{ dag_run.conf.get('force_reprocess', false) if dag_run else false }}"
        )
        landed = landing_raw_group(
            weather_run_id, mode, requested_start, requested_end, requested_limit
        )
        bronze_result = bronze_group(landed, weather_run_id, force_reprocess)
        raw_cleanup = expire_transient_raw()
        staging_cleanup = cleanup_weather_staging(weather_run_id)
        bronze_result >> [raw_cleanup, staging_cleanup]
        finalize_weather_run(bronze_result, raw_cleanup, staging_cleanup)

    return weather_dag()
