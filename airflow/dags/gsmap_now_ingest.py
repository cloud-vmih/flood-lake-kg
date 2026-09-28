"""GSMaP Gauge NOW restart-safe Raw-to-Bronze pipeline."""

from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.weather.airflow_factory import build_weather_dag

CONFIG_PATH = ProjectPaths.discover().root / "config" / "dynamic" / "gsmap_now.yaml"

gsmap_now_ingest = build_weather_dag("gsmap_now_ingest", CONFIG_PATH)
