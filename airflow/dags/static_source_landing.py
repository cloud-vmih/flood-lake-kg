"""Airflow DAG entrypoint for the static Landing pipeline."""

from flashflood_data.pipelines.static_landing.airflow import static_source_landing

__all__ = ["static_source_landing"]
