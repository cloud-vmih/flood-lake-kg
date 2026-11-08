"""Thin Airflow DAG for the versioned Silver weather Spark application."""

import json
from datetime import UTC, datetime

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task

from flashflood_data.pipelines.silver_weather.pipeline import WeatherBuildPlan
from flashflood_data.pipelines.silver_weather.runtime import build_weather_silver_runtime


@task(retries=1, retry_exponential_backoff=True)
def plan_weather_build_task() -> dict[str, object]:
    return build_weather_silver_runtime().plan().to_document()


@task(retries=1, retry_exponential_backoff=True, pool="silver_writer")
def execute_weather_plan(
    plan_document: dict[str, object], orchestrator_run_id: str,
) -> dict[str, object]:
    result = build_weather_silver_runtime().execute(
        WeatherBuildPlan.from_document(plan_document),
        orchestrator_run_id=orchestrator_run_id,
    )
    if result.get("status") not in {"succeeded", "skipped"}:
        raise AirflowException(json.dumps(result, sort_keys=True))
    return result


@dag(
    dag_id="silver_weather_aggregation",
    description="Normalize source weather and publish versioned L12 basin values",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1},
    tags=["silver", "weather", "spark", "iceberg"],
)
def silver_weather_aggregation_dag():
    plan = plan_weather_build_task()
    execute_weather_plan(plan, "{{ run_id }}")


silver_weather_aggregation = silver_weather_aggregation_dag()
