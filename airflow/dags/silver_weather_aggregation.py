"""Airflow DAG entrypoint for Silver weather aggregation."""

from flashflood_data.pipelines.silver_weather.airflow import silver_weather_aggregation

__all__ = ["silver_weather_aggregation"]
