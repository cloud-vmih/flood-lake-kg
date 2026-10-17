"""Airflow entrypoint for ERA5-Land ingestion."""

from flashflood_data.pipelines.weather_ingest.airflow import build_project_weather_dag

era5_land_ingest = build_project_weather_dag("era5_land_ingest", "era5_land.yaml")
