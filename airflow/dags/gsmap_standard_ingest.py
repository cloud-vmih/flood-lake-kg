"""GSMaP Gauge Standard restart-safe Raw-to-Bronze pipeline."""

from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.weather.airflow_factory import build_weather_dag

CONFIG_PATH = (
    ProjectPaths.discover().root / "config" / "dynamic" / "gsmap_standard.yaml"
)

gsmap_standard_ingest = build_weather_dag("gsmap_standard_ingest", CONFIG_PATH)
