"""Airflow entrypoint for GSMaP Gauge NOW ingestion."""

from flashflood_data.pipelines.weather_ingest.airflow import build_project_weather_dag

gsmap_now_ingest = build_project_weather_dag("gsmap_now_ingest", "gsmap_now.yaml")
