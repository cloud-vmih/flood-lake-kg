"""Airflow DAG entrypoint for the Silver basin topology pipeline."""

from flashflood_data.pipelines.silver_basin.airflow import silver_basin_topology

__all__ = ["silver_basin_topology"]
