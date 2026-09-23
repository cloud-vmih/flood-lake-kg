"""IFS HRES individual-cycle acquisition through Open-Meteo Single Runs."""

import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import httpx

from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    PlannedWeatherObject,
    WeatherPipelineConfig,
    WeatherStreamConfig,
)
from flashflood_data.orchestration.weather.providers.base import target_path


def _axis(start: float, end: float, step: float) -> list[float]:
    count = max(1, round((end - start) / step) + 1)
    return [round(start + index * step, 6) for index in range(count)]


class IfsOpenMeteoProvider:
    """Download one archived IFS cycle over the configured AOI sampling grid."""

    def __init__(
        self,
        config: WeatherPipelineConfig,
        stream: WeatherStreamConfig,
        *,
        aoi_bounds: tuple[float, float, float, float],
        environment: Mapping[str, str] | None = None,
        client: object | None = None,
    ) -> None:
        self.config = config
        self.stream = stream
        self.aoi_bounds = aoi_bounds
        self.environment = os.environ if environment is None else environment
        self.client = httpx.Client(timeout=180, follow_redirects=True) if client is None else client

    def _points(self) -> list[tuple[float, float]]:
        west, south, east, north = self.aoi_bounds
        step = float(self.stream.options.get("grid_resolution_degrees", 0.1))
        return [(lat, lon) for lat in _axis(south, north, step) for lon in _axis(west, east, step)]

    def fetch(
        self, planned: PlannedWeatherObject, target_dir: Path
    ) -> FetchedWeatherObject:
        batch_size = int(self.stream.options.get("point_batch_size", 50))
        responses: list[dict[str, object]] = []
        points = self._points()
        run = (planned.model_run_time or planned.window.start).astimezone(UTC)
        for offset in range(0, len(points), batch_size):
            batch = points[offset : offset + batch_size]
            params: dict[str, object] = {
                "latitude": ",".join(str(lat) for lat, _ in batch),
                "longitude": ",".join(str(lon) for _, lon in batch),
                "hourly": ",".join(planned.variables),
                "models": str(self.stream.options.get("model", "ecmwf_ifs")),
                "run": run.strftime("%Y-%m-%dT%H:%M"),
                "timezone": "UTC",
                "forecast_hours": int(self.stream.options.get("forecast_hours", 360)),
            }
            api_key = self.environment.get("OPEN_METEO_API_KEY")
            if api_key:
                params["apikey"] = api_key
            response = self.client.get(str(self.stream.endpoint), params=params)
            response.raise_for_status()
            payload = response.json()
            responses.extend(payload if isinstance(payload, list) else [payload])
        document = {
            "schema_version": 1,
            "source_cycle_id": planned.source_cycle_id,
            "model_run_time": run.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "responses": responses,
        }
        path = target_path(target_dir, planned, self.stream.filename_suffix)
        path.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
        now = datetime.now(UTC)
        return FetchedWeatherObject(
            planned=planned,
            path=path,
            filename=path.name,
            media_type=self.stream.media_type,
            source_uri=str(self.stream.endpoint),
            retrieved_at=now,
            available_at=now,
            provider_issued_at=run,
            provider_metadata={
                "provider": "Open-Meteo",
                "model": self.stream.options.get("model", "ecmwf_ifs"),
                "point_count": len(points),
            },
        )
