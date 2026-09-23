"""Airflow TaskFlow factory shared by the three dynamic weather DAGs."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, dag, get_current_context, task, task_group

from flashflood_data.orchestration.weather.config import load_weather_config
from flashflood_data.orchestration.weather.factory import build_weather_runtime
from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    PlannedWeatherObject,
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
    def load_cursor() -> dict[str, object]:
        return build_weather_runtime(config_path).cursor_document()

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
    ) -> dict[str, object]:
        return build_weather_runtime(config_path).plan_document(
            cursors,
            safe_ends,
            mode=mode,
            requested_start=requested_start,
            requested_end=requested_end,
            requested_limit=requested_limit,
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
        planned_document: dict[str, object], weather_run_id: str
    ) -> dict[str, object]:
        planned = PlannedWeatherObject.model_validate(planned_document)
        attempt_no = int(get_current_context()["ti"].try_number)
        fetched = build_weather_runtime(config_path).fetch(
            planned, weather_run_id, attempt_no=attempt_no
        )
        return {
            "status": "available",
            "attempt_no": attempt_no,
            "fetched": fetched.model_dump(mode="json"),
        }

    @task(retries=3, pool="weather_raw_writer")
    def register_raw_and_meta(
        fetched_document: dict[str, object], weather_run_id: str
    ) -> dict[str, object]:
        if fetched_document.get("status") == "no_data":
            return fetched_document
        fetched = FetchedWeatherObject.model_validate(fetched_document["fetched"])
        runtime = build_weather_runtime(config_path)
        published = runtime.landing_service().publish_and_register(
            fetched,
            run_id=weather_run_id,
            attempt_no=int(fetched_document.get("attempt_no", 1)),
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
        expected_missing = {str(item["asset_id"]) for item in plan["missing"]}
        actual = {str(item["asset_id"]) for item in outcomes}
        if expected_missing != actual:
            raise ValueError("weather fetch outcomes do not cover every planned missing object")
        if any(item.get("status") not in {"available", "no_data"} for item in outcomes):
            raise ValueError("weather coverage contains an unresolved outcome")
        return outcomes

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

    @task_group(group_id="landing_raw")
    def landing_raw_group(
        weather_run_id: str,
        mode: str,
        requested_start: str,
        requested_end: str,
        requested_limit: str,
    ):
        registry = register_dynamic_registry()
        cursors = load_cursor()
        safe_ends = determine_available_end()
        registry >> cursors
        plan = plan_expected_windows(
            cursors, safe_ends, mode, requested_start, requested_end, requested_limit
        )
        missing = extract_missing_objects(plan)
        fetched = fetch_missing_or_revised.partial(
            weather_run_id=weather_run_id
        ).expand(planned_document=missing)
        registered = register_raw_and_meta.partial(
            weather_run_id=weather_run_id
        ).expand(fetched_document=fetched)
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
        bronze_group(landed, weather_run_id, force_reprocess)

    return weather_dag()
