"""Airflow entrypoint for IFS/Open-Meteo ingestion."""

from flashflood_data.pipelines.weather_ingest.airflow import build_project_weather_dag

ifs_ingest = build_project_weather_dag("ifs_ingest", "ifs.yaml")
