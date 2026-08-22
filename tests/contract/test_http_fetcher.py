import hashlib
import logging
import sys
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import respx

from flashflood_data.budget import StorageBudget
from flashflood_data.config import EnvironmentSettings
from flashflood_data.http import (
    BudgetRejected,
    DownloadFailed,
    ExistingAssetConflict,
    HttpFetcher,
    PayloadMismatch,
    SecretRedactionFilter,
)
from flashflood_data.models import AssetStatus, RemoteAsset


class ChunkStream(httpx.SyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self.chunks = chunks
        self.iterations = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.iterations += 1
            yield chunk


@pytest.fixture
def remote_asset() -> RemoteAsset:
    return RemoteAsset(
        asset_id="fixture-asset",
        source_id="fixture-source",
        source_version="2026",
        uri="https://example.invalid/assets/payload.bin?tile=N21E103",
        target_relative_path=Path("raw/fixture/payload.bin"),
        media_type="application/octet-stream",
        license_id="fixture-license",
        expected_size=13,
    )


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def fetcher(project_paths, catalog, sleeps: list[float]) -> HttpFetcher:
    budget = StorageBudget(
        project_paths.dataset,
        soft_cap_bytes=2**30,
        minimum_free_bytes=0,
    )
    return HttpFetcher(
        project_paths,
        catalog,
        budget,
        client=httpx.Client(),
        max_attempts=4,
        sleep=sleeps.append,
    )


@respx.mock
def test_fetcher_streams_and_publishes_only_verified_payload(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, catalog
) -> None:
    payload = b"valid-payload"
    stream = ChunkStream(b"valid-", b"payload")
    remote = remote_asset.model_copy(
        update={"expected_checksum": hashlib.sha256(payload).hexdigest()}
    )
    respx.get(remote.uri).mock(return_value=httpx.Response(200, stream=stream))

    record = fetcher.fetch(remote, "run-1")

    target = Path(record.storage_path)
    assert target.read_bytes() == payload
    assert not Path(f"{target}.partial").exists()
    assert record.status is AssetStatus.FETCHED
    assert catalog.get(remote.asset_id) == record
    assert stream.iterations == 2


@respx.mock
def test_budget_rejection_happens_before_request_or_payload_write(
    project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    route = respx.get(remote_asset.uri).mock(return_value=httpx.Response(200, content=b"ignored"))
    fetcher = HttpFetcher(
        project_paths,
        catalog,
        StorageBudget(project_paths.dataset, soft_cap_bytes=0, minimum_free_bytes=0),
        client=httpx.Client(),
    )

    with pytest.raises(BudgetRejected, match="new_raw_soft_cap"):
        fetcher.fetch(remote_asset, "run-budget")

    target = project_paths.dataset / remote_asset.target_relative_path
    assert not route.called
    assert not target.exists()
    assert not Path(f"{target}.partial").exists()
    with pytest.raises(KeyError):
        catalog.get(remote_asset.asset_id)


@pytest.mark.parametrize("status", [429, 500, 503])
@respx.mock
def test_retryable_statuses_use_bounded_exponential_backoff(
    fetcher: HttpFetcher,
    remote_asset: RemoteAsset,
    sleeps: list[float],
    status: int,
) -> None:
    route = respx.get(remote_asset.uri).mock(
        side_effect=[
            httpx.Response(status),
            httpx.Response(200, content=b"valid-payload"),
        ]
    )

    record = fetcher.fetch(remote_asset, "run-retry")

    assert record.status is AssetStatus.FETCHED
    assert route.call_count == 2
    assert sleeps == [1]


@respx.mock
def test_retry_after_is_honored_but_capped_at_sixty_seconds(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, sleeps: list[float]
) -> None:
    respx.get(remote_asset.uri).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "3600"}),
            httpx.Response(200, content=b"valid-payload"),
        ]
    )

    fetcher.fetch(remote_asset, "run-retry-after")

    assert sleeps == [60]


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ReadTimeout("fixture timeout"),
        httpx.ConnectError("fixture network error"),
    ],
)
@respx.mock
def test_timeout_and_network_failures_are_retried(
    fetcher: HttpFetcher,
    remote_asset: RemoteAsset,
    sleeps: list[float],
    failure: Exception,
) -> None:
    route = respx.get(remote_asset.uri).mock(
        side_effect=[failure, httpx.Response(200, content=b"valid-payload")]
    )

    fetcher.fetch(remote_asset, "run-network-retry")

    assert route.call_count == 2
    assert sleeps == [1]


@respx.mock
def test_permanent_client_error_is_not_retried(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, sleeps: list[float], catalog
) -> None:
    route = respx.get(remote_asset.uri).mock(return_value=httpx.Response(404))

    with pytest.raises(DownloadFailed):
        fetcher.fetch(remote_asset, "run-not-found")

    assert route.call_count == 1
    assert sleeps == []
    assert catalog.get(remote_asset.asset_id).status is AssetStatus.FAILED


@respx.mock
def test_compatible_partial_response_is_appended(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(
        update={
            "expected_size": len(payload),
            "expected_checksum": hashlib.sha256(payload).hexdigest(),
        }
    )
    target = project_paths.dataset / remote.target_relative_path
    target.parent.mkdir(parents=True)
    partial = Path(f"{target}.partial")
    partial.write_bytes(b"abc")

    def resumed(request: httpx.Request) -> httpx.Response:
        assert request.headers["Range"] == "bytes=3-"
        return httpx.Response(206, headers={"Content-Range": "bytes 3-5/6"}, content=b"def")

    respx.get(remote.uri).mock(side_effect=resumed)

    record = fetcher.fetch(remote, "run-resume")

    assert Path(record.storage_path).read_bytes() == payload
    assert not partial.exists()


@pytest.mark.parametrize(
    "status,headers",
    [
        (200, {}),
        (206, {"Content-Range": "bytes 2-4/5"}),
        (206, {}),
    ],
)
@respx.mock
def test_non_resumable_response_restarts_instead_of_appending(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    status: int,
    headers: dict[str, str],
) -> None:
    payload = b"fresh"
    remote = remote_asset.model_copy(
        update={
            "expected_size": len(payload),
            "expected_checksum": hashlib.sha256(payload).hexdigest(),
        }
    )
    target = project_paths.dataset / remote.target_relative_path
    target.parent.mkdir(parents=True)
    Path(f"{target}.partial").write_bytes(b"stale")
    respx.get(remote.uri).mock(return_value=httpx.Response(status, headers=headers, content=payload))

    record = fetcher.fetch(remote, "run-restart")

    assert Path(record.storage_path).read_bytes() == payload


@pytest.mark.parametrize(
    "update,error_code",
    [
        ({"expected_size": 999}, "size_mismatch"),
        ({"expected_checksum": "0" * 64}, "checksum_mismatch"),
    ],
)
@respx.mock
def test_payload_mismatch_is_quarantined_without_retry(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    update: dict[str, object],
    error_code: str,
) -> None:
    payload = b"valid-payload"
    remote = remote_asset.model_copy(update=update)
    route = respx.get(remote.uri).mock(return_value=httpx.Response(200, content=payload))

    with pytest.raises(PayloadMismatch, match=error_code):
        fetcher.fetch(remote, "run-quarantine")

    target = project_paths.dataset / remote.target_relative_path
    quarantine = project_paths.raw / "_quarantine" / remote.asset_id
    assert not target.exists()
    assert not Path(f"{target}.partial").exists()
    assert [path.read_bytes() for path in quarantine.iterdir()] == [payload]
    record = catalog.get(remote.asset_id)
    assert record.status is AssetStatus.QUARANTINED
    assert record.error_code == error_code
    assert route.call_count == 1


@respx.mock
def test_existing_different_final_is_never_overwritten(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset
) -> None:
    existing = b"existing-valid-payload"
    replacement = b"new-payload"
    target = project_paths.dataset / remote_asset.target_relative_path
    target.parent.mkdir(parents=True)
    target.write_bytes(existing)
    remote = remote_asset.model_copy(
        update={
            "expected_size": len(replacement),
            "expected_checksum": hashlib.sha256(replacement).hexdigest(),
        }
    )
    route = respx.get(remote.uri).mock(return_value=httpx.Response(200, content=replacement))

    with pytest.raises(ExistingAssetConflict):
        fetcher.fetch(remote, "run-conflict")

    assert target.read_bytes() == existing
    assert not route.called


@respx.mock
def test_existing_matching_final_is_idempotently_catalogued_without_request(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset, catalog
) -> None:
    payload = b"already-present"
    target = project_paths.dataset / remote_asset.target_relative_path
    target.parent.mkdir(parents=True)
    target.write_bytes(payload)
    remote = remote_asset.model_copy(
        update={
            "expected_size": len(payload),
            "expected_checksum": hashlib.sha256(payload).hexdigest(),
        }
    )
    route = respx.get(remote.uri).mock(return_value=httpx.Response(500))

    record = fetcher.fetch(remote, "run-existing")

    assert not route.called
    assert record.status is AssetStatus.FETCHED
    assert catalog.get(remote.asset_id) == record


@respx.mock
def test_second_fetch_without_upstream_checksum_reuses_catalogued_payload(
    fetcher: HttpFetcher, remote_asset: RemoteAsset
) -> None:
    route = respx.get(remote_asset.uri).mock(
        side_effect=[
            httpx.Response(200, content=b"valid-payload"),
            httpx.Response(500),
        ]
    )
    first = fetcher.fetch(remote_asset, "run-first")

    second = fetcher.fetch(remote_asset, "run-second")

    assert second == first
    assert route.call_count == 1


@respx.mock
def test_catalog_observes_fetching_then_fetched_lifecycle(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, catalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    statuses: list[AssetStatus] = []
    real_upsert = catalog.upsert

    def recording_upsert(record):
        statuses.append(record.status)
        return real_upsert(record)

    monkeypatch.setattr(catalog, "upsert", recording_upsert)
    respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    fetcher.fetch(remote_asset, "run-lifecycle")

    assert statuses == [AssetStatus.DISCOVERED, AssetStatus.FETCHING, AssetStatus.FETCHED]


def test_redaction_filter_removes_environment_and_transport_secrets() -> None:
    environment = EnvironmentSettings(
        cdse_username="fixture-user",
        cdse_password="fixture-password",
    )
    record = logging.LogRecord(
        "fixture",
        logging.ERROR,
        __file__,
        1,
        (
            "login fixture-user fixture-password; Authorization: Bearer fixture-bearer; "
            "https://url-user:url-pass@example.invalid/file?tile=N21E103&"
            "X-Amz-Signature=fixture-signature&access_token=fixture-token"
        ),
        (),
        None,
    )

    assert SecretRedactionFilter(environment).filter(record)
    rendered = record.getMessage()

    for secret in (
        "fixture-user",
        "fixture-password",
        "fixture-bearer",
        "url-user",
        "url-pass",
        "fixture-signature",
        "fixture-token",
    ):
        assert secret not in rendered
    assert rendered.count("[REDACTED]") >= 5
    assert "tile=N21E103" in rendered


def test_redaction_filter_drops_exception_details_that_could_bypass_redaction() -> None:
    try:
        raise RuntimeError("request failed with bearer fixture-exception-token")
    except RuntimeError:
        record = logging.LogRecord(
            "fixture",
            logging.ERROR,
            __file__,
            1,
            "safe summary",
            (),
            sys.exc_info(),
        )

    assert SecretRedactionFilter().filter(record)

    assert record.exc_info is None
