"""Interfaces and shared helpers for weather acquisition providers."""

from pathlib import Path
from typing import Protocol

from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    PlannedWeatherObject,
)


class WeatherProvider(Protocol):
    """Fetch one planned provider object into run-scoped local staging."""

    def fetch(
        self, planned: PlannedWeatherObject, target_dir: Path
    ) -> FetchedWeatherObject: ...


def target_path(target_dir: Path, planned: PlannedWeatherObject, suffix: str) -> Path:
    """Create a deterministic local path without embedding provider credentials."""
    directory = Path(target_dir)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{planned.asset_id}{suffix}"

