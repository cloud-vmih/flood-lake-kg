"""Load credential-free dynamic weather pipeline policies."""

from pathlib import Path

import yaml

from flashflood_data.orchestration.weather.models import WeatherPipelineConfig

_SECRET_KEYS = {"password", "api_key", "apikey", "token", "secret"}


def _contains_secret_key(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            str(key).lower() in _SECRET_KEYS or _contains_secret_key(nested)
            for key, nested in value.items()
        )
    if isinstance(value, list):
        return any(_contains_secret_key(item) for item in value)
    return False


def load_weather_config(path: Path) -> WeatherPipelineConfig:
    """Parse and validate one source policy; secrets belong only in the environment."""
    document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise TypeError("weather config must be a mapping")
    if _contains_secret_key(document):
        raise ValueError("weather config cannot contain credentials")
    return WeatherPipelineConfig.model_validate(document)
