"""Airflow DAG entrypoint for Bronze DQ migration."""

from flashflood_data.pipelines.quality_migration.airflow import bronze_quality_backfill

__all__ = ["bronze_quality_backfill"]
