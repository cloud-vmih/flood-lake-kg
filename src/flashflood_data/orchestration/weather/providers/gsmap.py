"""GSMaP Standard and Gauge NOW file acquisition."""

import os
from collections.abc import Iterable, Iterator, Mapping
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
                source_path = parsed.path
                if "*" in source_path:
                    matches = ftp.nlst(source_path)
                    if len(matches) != 1:
                        raise RuntimeError(
                            "GSMaP FTP revision wildcard must resolve exactly one file"
                        )
                    source_path = matches[0]
                    url = parsed._replace(path=source_path).geturl()
                ftp.retrbinary(f"RETR {source_path}", destination.write)
        elif parsed.scheme in {"http", "https"}:
            if "*" in parsed.path:
                raise ValueError("GSMaP revision wildcards require an FTP endpoint")
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

    def fetch_many(
        self, planned_objects: Iterable[PlannedWeatherObject], target_dir: Path
    ) -> Iterator[FetchedWeatherObject]:
        """Fetch one mapped batch, reusing a single authenticated FTP session."""
        plans = tuple(planned_objects)
        if not plans:
            return
        urls = [(planned, self._url(planned)) for planned in plans]
        parsed_urls = [
            (planned, url, urlsplit(url)) for planned, url in urls
        ]
        if any(parsed.scheme != "ftp" for _planned, _url, parsed in parsed_urls):
            for planned in plans:
                yield self.fetch(planned, target_dir)
            return
        hosts = {parsed.hostname for _planned, _url, parsed in parsed_urls}
        if None in hosts or len(hosts) != 1:
            raise ValueError("one GSMaP FTP batch must use exactly one host")
        if any(
            parsed.username is not None or parsed.password is not None
            for _planned, _url, parsed in parsed_urls
        ):
            raise ValueError("GSMaP URL templates cannot contain credentials")
        username = self.environment.get("GSMAP_USERNAME", "")
        password = self.environment.get("GSMAP_PASSWORD", "")
        host = next(iter(hosts))
        assert host is not None
        with self.ftp_factory(host) as ftp:
            ftp.login(username, password)
            for planned, url, parsed in parsed_urls:
                path = target_path(target_dir, planned, self.stream.filename_suffix)
                source_path = parsed.path
                if "*" in source_path:
                    matches = ftp.nlst(source_path)
                    if len(matches) != 1:
                        raise RuntimeError(
                            "GSMaP FTP revision wildcard must resolve exactly one file"
                        )
                    source_path = matches[0]
                    url = parsed._replace(path=source_path).geturl()
                with path.open("wb") as destination:
                    ftp.retrbinary(f"RETR {source_path}", destination.write)
                now = datetime.now(UTC)
                yield FetchedWeatherObject(
                    planned=planned,
                    path=path,
                    filename=path.name,
                    media_type=self.stream.media_type,
                    source_uri=url,
                    retrieved_at=now,
                    available_at=now,
                    provider_metadata={
                        "provider": "JAXA",
                        "product": planned.product,
                    },
                )
