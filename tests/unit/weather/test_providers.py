import json
from datetime import timedelta
from pathlib import Path

from flashflood_data.orchestration.weather.config import load_weather_config
from flashflood_data.orchestration.weather.planner import plan_expected_objects
from flashflood_data.orchestration.weather.providers.era5_land import Era5LandProvider
from flashflood_data.orchestration.weather.providers.gsmap import GsmapProvider
from flashflood_data.orchestration.weather.providers.ifs_openmeteo import IfsOpenMeteoProvider

ROOT = Path(__file__).resolve().parents[3]


def _first_plan(config_path: str):
    config = load_weather_config(ROOT / config_path)
    stream = config.streams[0]
    plan = plan_expected_objects(
        source_id=config.source_id,
        source_version=config.source_version,
        stream=stream,
        start=stream.start_at,
        end=stream.start_at + timedelta(minutes=stream.chunk_minutes),
    )[0]
    return config, stream, plan


class FakeHttpResponse:
    def __init__(self, payload: bytes | dict[str, object]) -> None:
        self.content = payload if isinstance(payload, bytes) else json.dumps(payload).encode()

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return json.loads(self.content)


class FakeHttpClient:
    def __init__(self, response: FakeHttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def test_gsmap_resolves_template_and_keeps_credentials_out_of_metadata(tmp_path: Path) -> None:
    config, stream, plan = _first_plan("config/dynamic/gsmap.yaml")
    client = FakeHttpClient(FakeHttpResponse(b"compressed-rain"))
    provider = GsmapProvider(
        config,
        stream,
        environment={
            "GSMAP_STANDARD_URL_TEMPLATE": (
                "https://example.test/{year}/{month}/{day}/rain-{hour}{minute}.dat.gz"
            ),
            "GSMAP_USERNAME": "alice",
            "GSMAP_PASSWORD": "secret",
        },
        client=client,
    )

    fetched = provider.fetch(plan, tmp_path)

    assert fetched.path.read_bytes() == b"compressed-rain"
    assert fetched.source_uri == "https://example.test/2020/01/01/rain-0000.dat.gz"
    assert "alice" not in fetched.model_dump_json()
    assert "secret" not in fetched.model_dump_json()
    assert client.calls[0][1]["auth"] == ("alice", "secret")


class FakeFtp:
    def __init__(self, host: str) -> None:
        self.host = host
        self.login_values = None
        self.command = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def login(self, username: str, password: str) -> None:
        self.login_values = (username, password)

    def retrbinary(self, command: str, callback) -> None:
        self.command = command
        callback(b"ftp-rain")


def test_gsmap_supports_jaxa_ftp_without_embedding_credentials(tmp_path: Path) -> None:
    config, stream, plan = _first_plan("config/dynamic/gsmap.yaml")
    clients = []

    def ftp_factory(host: str):
        client = FakeFtp(host)
        clients.append(client)
        return client

    provider = GsmapProvider(
        config,
        stream,
        environment={
            "GSMAP_STANDARD_URL_TEMPLATE": (
                "ftp://archive.test/{year}/{month}/{day}/rain-{hour}{minute}.dat.gz"
            ),
            "GSMAP_USERNAME": "alice",
            "GSMAP_PASSWORD": "secret",
        },
        ftp_factory=ftp_factory,
    )

    fetched = provider.fetch(plan, tmp_path)

    assert fetched.path.read_bytes() == b"ftp-rain"
    assert clients[0].host == "archive.test"
    assert clients[0].login_values == ("alice", "secret")
    assert clients[0].command == "RETR /2020/01/01/rain-0000.dat.gz"
    assert "alice" not in fetched.source_uri and "secret" not in fetched.source_uri


class FakeCdsResult:
    def __init__(self, target: str) -> None:
        self.target = target

    def download(self, target: str) -> None:
        Path(target).write_bytes(b"netcdf-fixture")


class FakeCdsClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object], str]] = []

    def retrieve(self, dataset: str, request: dict[str, object], target: str):
        self.calls.append((dataset, request, target))
        Path(target).write_bytes(b"netcdf-fixture")
        return FakeCdsResult(target)


def test_era5_builds_aoi_hourly_cds_request(tmp_path: Path) -> None:
    config, stream, plan = _first_plan("config/dynamic/era5_land.yaml")
    client = FakeCdsClient()
    provider = Era5LandProvider(
        config,
        stream,
        aoi_bounds=(103.0, 20.0, 105.0, 22.0),
        client=client,
    )

    fetched = provider.fetch(plan, tmp_path)

    dataset, request, _ = client.calls[0]
    assert dataset == "reanalysis-era5-land"
    assert request["area"] == [22.0, 103.0, 20.0, 105.0]
    assert request["variable"] == list(stream.variables)
    assert fetched.path.read_bytes() == b"netcdf-fixture"


def test_ifs_requests_explicit_cycle_and_combines_point_batches(tmp_path: Path) -> None:
    config, stream, plan = _first_plan("config/dynamic/ifs.yaml")
    client = FakeHttpClient(
        FakeHttpResponse(
            {
                "latitude": 21.0,
                "longitude": 104.0,
                "hourly": {"time": ["2024-03-01T00:00"], "precipitation": [1.5]},
                "hourly_units": {"precipitation": "mm"},
            }
        )
    )
    provider = IfsOpenMeteoProvider(
        config,
        stream.model_copy(update={"options": {**stream.options, "point_batch_size": 2}}),
        aoi_bounds=(104.0, 21.0, 104.1, 21.1),
        environment={},
        client=client,
    )

    fetched = provider.fetch(plan, tmp_path)
    document = json.loads(fetched.path.read_text())

    assert client.calls
    assert all(call[1]["params"]["run"] == "2024-03-01T00:00" for call in client.calls)
    assert all(call[1]["params"]["models"] == "ecmwf_ifs" for call in client.calls)
    assert all("precipitation" in call[1]["params"]["hourly"] for call in client.calls)
    assert document["source_cycle_id"] == "20240301T0000Z"
    assert document["responses"]
