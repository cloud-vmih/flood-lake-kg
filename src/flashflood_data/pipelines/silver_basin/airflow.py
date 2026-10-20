"""Thin Airflow orchestration for the Silver L12 basin topology application."""

import json
from datetime import UTC, datetime

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task

from flashflood_data.pipelines.silver_basin.planner import BasinExecutionPlan
from flashflood_data.pipelines.silver_basin.runtime import build_basin_silver_runtime


@task(retries=1, retry_exponential_backoff=True)
def plan_basin_build() -> dict[str, object]:
    """Pin Bronze and return only the deterministic execution document through XCom."""
    return build_basin_silver_runtime().plan().to_document()


@task(retries=1, retry_exponential_backoff=True, pool="silver_writer")
def execute_basin_plan(
    plan_document: dict[str, object],
    orchestrator_run_id: str,
) -> dict[str, object]:
    """Submit one isolated Spark app; no DataFrame or geometry enters XCom."""
    result = build_basin_silver_runtime().execute(
        BasinExecutionPlan.from_document(plan_document),
        orchestrator_run_id=orchestrator_run_id,
    )
    if result.get("status") not in {"succeeded", "skipped"}:
        raise AirflowException(json.dumps(result, sort_keys=True))
    return result


@dag(
    dag_id="silver_basin_topology",
    description="Normalize HydroBASINS L12 and publish versioned basin topology",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1},
    tags=["silver", "basin", "spark", "sedona", "iceberg"],
)
def silver_basin_topology_dag():
    plan = plan_basin_build()
    execute_basin_plan(plan, "{{ run_id }}")


silver_basin_topology = silver_basin_topology_dag()
