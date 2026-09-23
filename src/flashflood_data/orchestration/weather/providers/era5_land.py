"""ERA5-Land hourly acquisition through the Copernicus CDS API."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    PlannedWeatherObject,
    WeatherPipelineConfig,
    WeatherStreamConfig,
)
from flashflood_data.orchestration.weather.providers.base import target_path


class Era5LandProvider:
    """Create bounded CDS requests and download provider-original NetCDF files."""

    def __init__(
        self,
        config: WeatherPipelineConfig,
        stream: WeatherStreamConfig,
        *,
        aoi_bounds: tuple[float, float, float, float],
        client: object | None = None,
    ) -> None:
        self.config = config
        self.stream = stream
        self.aoi_bounds = aoi_bounds
        if client is None:
            import cdsapi  # type: ignore[import-not-found]

            client = cdsapi.Client()
        self.client = client

    def _request(self, planned: PlannedWeatherObject) -> dict[str, object]:
        cursor = planned.window.start.astimezone(UTC)
        instants: list[datetime] = []
        while cursor < planned.window.end:
            instants.append(cursor)
            cursor += timedelta(minutes=self.stream.step_minutes)
        west, south, east, north = self.aoi_bounds
        return {
            "variable": list(planned.variables),
            "year": sorted({value.strftime("%Y") for value in instants}),
            "month": sorted({value.strftime("%m") for value in instants}),
            "day": sorted({value.strftime("%d") for value in instants}),
            "time": sorted({value.strftime("%H:%M") for value in instants}),
            "area": [north, west, south, east],
            "data_format": self.stream.options.get("data_format", "netcdf"),
            "download_format": self.stream.options.get("download_format", "unarchived"),
        }

    def fetch(
        self, planned: PlannedWeatherObject, target_dir: Path
    ) -> FetchedWeatherObject:
        path = target_path(target_dir, planned, self.stream.filename_suffix)
        dataset = str(self.stream.options.get("dataset", planned.product))
        self.client.retrieve(dataset, self._request(planned), str(path))
        if not path.is_file():
            raise RuntimeError("CDS retrieval did not create the requested file")
        now = datetime.now(UTC)
        return FetchedWeatherObject(
            planned=planned,
            path=path,
            filename=path.name,
            media_type=self.stream.media_type,
            source_uri=f"{self.stream.endpoint.rstrip('/')}/{dataset}",
            retrieved_at=now,
            available_at=now,
            provider_metadata={"provider": "Copernicus CDS", "dataset": dataset},
        )

