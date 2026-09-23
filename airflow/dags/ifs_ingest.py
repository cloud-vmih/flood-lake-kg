"""IFS/Open-Meteo Single Runs restart-safe Raw-to-Bronze pipeline."""

from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.weather.airflow_factory import build_weather_dag

CONFIG_PATH = ProjectPaths.discover().root / "config" / "dynamic" / "ifs.yaml"

ifs_ingest = build_weather_dag("ifs_ingest", CONFIG_PATH)

