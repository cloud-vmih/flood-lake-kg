"""Airflow entrypoint for GSMaP Gauge Standard ingestion."""

from flashflood_data.pipelines.weather_ingest.airflow import build_project_weather_dag

gsmap_standard_ingest = build_project_weather_dag("gsmap_standard_ingest", "gsmap_standard.yaml")
