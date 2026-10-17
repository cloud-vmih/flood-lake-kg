"""Airflow DAG entrypoint for the static Raw-to-Bronze pipeline."""

from flashflood_data.pipelines.static_bronze.airflow import static_source_to_bronze

__all__ = ["static_source_to_bronze"]
