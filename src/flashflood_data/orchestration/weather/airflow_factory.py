"""Airflow TaskFlow factory shared by the dynamic weather DAGs."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, dag, get_current_context, task, task_group

from flashflood_data.orchestration.weather.config import load_weather_config
from flashflood_data.orchestration.weather.factory import build_weather_runtime
from flashflood_data.orchestration.weather.grids import GridRegistration
from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    PlannedWeatherObject,
    ScopedWeatherObject,
)
from flashflood_data.orchestration.weather.planner import verify_weather_outcomes

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
    def extract_missing_objects(plan: dict[str, object]) -> list[dict[str, object]]:
        """Expose the mapping list through the default XCom return key required by Airflow."""
        return list(plan["missing"])

    @task(
        retries=3,
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        pool="weather_fetch",
    )
    def fetch_missing_or_revised(
        planned_document: dict[str, object],
        weather_run_id: str,
        grid_document: dict[str, object],
    ) -> dict[str, object]:
        planned = PlannedWeatherObject.model_validate(planned_document)
        attempt_no = int(get_current_context()["ti"].try_number)
        fetched = build_weather_runtime(config_path).fetch(
            planned,
            weather_run_id,
            attempt_no=attempt_no,
            grid=GridRegistration.from_document(grid_document),
        )
        return {
            "status": "available",
            "attempt_no": attempt_no,
            "fetched": fetched.model_dump(mode="json"),
        }

    @task(retries=2)
    def scope_fetched_payload(
        fetched_document: dict[str, object], grid_document: dict[str, object]
    ) -> dict[str, object]:
        if fetched_document.get("status") == "no_data":
            return fetched_document
        fetched = FetchedWeatherObject.model_validate(fetched_document["fetched"])
        scoped = build_weather_runtime(config_path).scope_fetched(
            fetched, GridRegistration.from_document(grid_document)
        )
        return {
            "status": "available",
            "attempt_no": fetched_document.get("attempt_no", 1),
            "scoped": scoped.model_dump(mode="json"),
        }

    @task(retries=3, pool="weather_raw_writer")
    def register_raw_and_meta(
        scoped_document: dict[str, object], weather_run_id: str
    ) -> dict[str, object]:
        if scoped_document.get("status") == "no_data":
            return scoped_document
        scoped = ScopedWeatherObject.model_validate(scoped_document["scoped"])
        runtime = build_weather_runtime(config_path)
        published = runtime.landing_service().publish_and_register(
            scoped,
            run_id=weather_run_id,
            attempt_no=int(scoped_document.get("attempt_no", 1)),
        )
        return {
            "status": published.status,
            "asset_id": published.asset_id,
            "object_id": published.object_id,
            "stream_id": published.stream_id,
            "product": published.product,
            "reused": published.reused,
        }

    @task(retries=0, trigger_rule="none_failed")
    def verify_contiguous_coverage(
        plan: dict[str, object], outcomes: list[dict[str, object]]
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
    def discover_unparsed_objects(
        _landing_result: dict[str, object], force_reprocess: object
    ) -> list[dict[str, str]]:
        runtime = build_weather_runtime(config_path)
        return [
            {"source_id": config.source_id, "object_id": object_id}
            for object_id in runtime.bronze_service().discover(
                config.source_id,
                parser_version=config.parser_version,
                force_reprocess=_boolean(force_reprocess),
            )
        ]

    @task(retries=2, pool="weather_bronze_writer")
    def parse_bronze(
        object_ref: dict[str, str], weather_run_id: str
    ) -> dict[str, object]:
        return build_weather_runtime(config_path).bronze_service().process_object(
            object_ref["source_id"],
            object_ref["object_id"],
            run_id=weather_run_id,
            parser_version=config.parser_version,
        )

    @task(retries=0, outlets=[WEATHER_BRONZE_ASSET])
    def publish_bronze_update(results: list[dict[str, object]]) -> dict[str, object]:
        return {
            "source_id": config.source_id,
            "published_objects": len(results),
            "asset": "weather_bronze_updated",
        }

    @task(retries=0, trigger_rule="all_done")
    def expire_transient_raw() -> dict[str, object]:
        return build_weather_runtime(config_path).expire_transient_raw(datetime.now(UTC))

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
        missing = extract_missing_objects(plan)
        fetched = fetch_missing_or_revised.partial(
            weather_run_id=weather_run_id, grid_document=grid
        ).expand(planned_document=missing)
        scoped = scope_fetched_payload.partial(grid_document=grid).expand(
            fetched_document=fetched
        )
        registered = register_raw_and_meta.partial(
            weather_run_id=weather_run_id
        ).expand(scoped_document=scoped)
        verified = verify_contiguous_coverage(plan, registered)
        return advance_cursor(plan, verified, weather_run_id)

    @task_group(group_id="bronze")
    def bronze_group(
        landing_result: dict[str, object], weather_run_id: str, force_reprocess: object
    ):
        objects = discover_unparsed_objects(landing_result, force_reprocess)
        parsed = parse_bronze.partial(weather_run_id=weather_run_id).expand(object_ref=objects)
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
        cleanup = expire_transient_raw()
        bronze_result >> cleanup

    return weather_dag()
