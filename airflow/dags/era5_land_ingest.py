"""ERA5-Land restart-safe Raw-to-Bronze pipeline."""

from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.weather.airflow_factory import build_weather_dag

CONFIG_PATH = ProjectPaths.discover().root / "config" / "dynamic" / "era5_land.yaml"

era5_land_ingest = build_weather_dag("era5_land_ingest", CONFIG_PATH)

