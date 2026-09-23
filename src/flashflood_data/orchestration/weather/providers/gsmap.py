"""GSMaP Standard and Gauge NOW file acquisition."""

import os
from collections.abc import Mapping
from datetime import UTC, datetime
from ftplib import FTP
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    PlannedWeatherObject,
    WeatherPipelineConfig,
    WeatherStreamConfig,
)
from flashflood_data.orchestration.weather.providers.base import target_path


class GsmapProvider:
    """Resolve an operator-supplied JAXA URL template and download one time slice."""

    def __init__(
        self,
        config: WeatherPipelineConfig,
        stream: WeatherStreamConfig,
        *,
        environment: Mapping[str, str] | None = None,
        client: object | None = None,
        ftp_factory=None,
    ) -> None:
        self.config = config
        self.stream = stream
        self.environment = os.environ if environment is None else environment
        self.client = httpx.Client(timeout=120, follow_redirects=True) if client is None else client
        self.ftp_factory = FTP if ftp_factory is None else ftp_factory

    def _url(self, planned: PlannedWeatherObject) -> str:
        template = self.stream.endpoint
        if template is None and self.stream.endpoint_env is not None:
            template = self.environment.get(self.stream.endpoint_env)
        if not template:
            raise RuntimeError(f"missing endpoint environment: {self.stream.endpoint_env}")
        value = planned.window.start.astimezone(UTC)
        return template.format(
            year=value.strftime("%Y"),
            month=value.strftime("%m"),
            day=value.strftime("%d"),
            hour=value.strftime("%H"),
            minute=value.strftime("%M"),
            product=planned.product,
        )

    def fetch(
        self, planned: PlannedWeatherObject, target_dir: Path
    ) -> FetchedWeatherObject:
        url = self._url(planned)
        username = self.environment.get("GSMAP_USERNAME", "")
        password = self.environment.get("GSMAP_PASSWORD", "")
        path = target_path(target_dir, planned, self.stream.filename_suffix)
        parsed = urlsplit(url)
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("GSMaP URL templates cannot contain credentials")
        if parsed.scheme == "ftp":
            if not parsed.hostname:
                raise ValueError("GSMaP FTP URL has no host")
            with self.ftp_factory(parsed.hostname) as ftp, path.open("wb") as destination:
                ftp.login(username, password)
                ftp.retrbinary(f"RETR {parsed.path}", destination.write)
        elif parsed.scheme in {"http", "https"}:
            response = self.client.get(
                url,
                auth=(username, password) if username or password else None,
                follow_redirects=True,
            )
            response.raise_for_status()
            path.write_bytes(response.content)
        else:
            raise ValueError("GSMaP endpoint must use ftp, http, or https")
        now = datetime.now(UTC)
        return FetchedWeatherObject(
            planned=planned,
            path=path,
            filename=path.name,
            media_type=self.stream.media_type,
            source_uri=url,
            retrieved_at=now,
            available_at=now,
            provider_metadata={"provider": "JAXA", "product": planned.product},
        )
