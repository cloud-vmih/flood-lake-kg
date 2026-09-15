import errno
import fcntl
import hashlib
import json
import logging
import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx

from flashflood_data.config import EnvironmentSettings
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, RemoteAsset
from flashflood_data.static.sources.budget import StorageBudget
from flashflood_data.storage.http import (
    BudgetRejected,
    DownloadFailed,
    DownloadLocked,
    ExistingAssetConflict,
    HttpFetcher,
    PayloadMismatch,
    SecretRedactionFilter,
)


class ChunkStream(httpx.SyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self.chunks = chunks
        self.iterations = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.iterations += 1
            yield chunk


class InterruptedStream(httpx.SyncByteStream):
    def __init__(self, prefix: bytes) -> None:
        self.prefix = prefix

    def __iter__(self) -> Iterator[bytes]:
        yield self.prefix
        raise httpx.ReadError("fixture interrupted stream")


class CloseFaultStream(httpx.SyncByteStream):
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __iter__(self) -> Iterator[bytes]:
        yield self.payload

    def close(self) -> None:
        raise httpx.ProtocolError("fixture response close failure")


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


def _single_attempt_fetcher(project_paths, catalog) -> HttpFetcher:
    return HttpFetcher(
        project_paths,
        catalog,
        StorageBudget(project_paths.dataset, soft_cap_bytes=2**30, minimum_free_bytes=0),
        client=httpx.Client(),
        max_attempts=1,
        sleep=lambda _: None,
    )


def _interrupt_owned_partial(
    project_paths,
    catalog,
    remote: RemoteAsset,
    *,
    validator_header: str | None = "ETag",
    validator_value: str = '"fixture-v1"',
) -> None:
    headers = {validator_header: validator_value} if validator_header is not None else {}
    respx.get(remote.uri).mock(
        return_value=httpx.Response(200, headers=headers, stream=InterruptedStream(b"abc"))
    )
    with pytest.raises(DownloadFailed):
        _single_attempt_fetcher(project_paths, catalog).fetch(remote, "run-interrupted")


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
def test_stale_canonical_raw_is_quarantined_then_atomically_refetched(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, catalog, project_paths
) -> None:
    first_payload = b"valid-payload"
    replacement = b"newer-payload"
    remote = remote_asset.model_copy(update={"expected_size": len(first_payload)})
    route = respx.get(remote.uri).mock(
        side_effect=[
            httpx.Response(200, content=first_payload),
            httpx.Response(200, content=replacement),
        ]
    )
    first = fetcher.fetch(remote, "run-first")
    target = Path(first.storage_path)
    target.write_bytes(b"x" * len(first_payload))
    catalog.transition(first.asset_id, AssetStatus.VALIDATED)
    catalog.transition(first.asset_id, AssetStatus.STALE)

    recovered = fetcher.fetch(remote, "run-recovery")

    quarantined = list((project_paths.raw / "_quarantine").rglob("*payload.bin*"))
    assert route.call_count == 2
    assert target.read_bytes() == replacement
    assert recovered.status is AssetStatus.FETCHED
    assert any(path.read_bytes() == b"x" * len(first_payload) for path in quarantined)


@respx.mock
def test_fetcher_preserves_ephemeral_headers_when_resuming(
    fetcher: HttpFetcher, remote_asset: RemoteAsset
) -> None:
    """Catches dropping an authenticated header when a partial CDSE download is resumed."""
    requests: list[httpx.Request] = []

    def first_response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, headers={"ETag": '"fixture-v1"'}, stream=InterruptedStream(b"valid-"))

    def resumed_response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 6-12/13", "ETag": '"fixture-v1"'},
            stream=ChunkStream(b"payload"),
        )

    respx.get(remote_asset.uri).mock(side_effect=[first_response, resumed_response])

    record = fetcher.fetch(remote_asset, "run-resume-auth", headers={"Authorization": "Bearer ephemeral"})

    assert Path(record.storage_path).read_bytes() == b"valid-payload"
    assert [request.headers.get("Authorization") for request in requests] == ["Bearer ephemeral", "Bearer ephemeral"]
    assert requests[1].headers["Range"] == "bytes=6-"


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


@respx.mock
def test_missing_download_bound_rejects_before_request_write_or_catalog_mutation(
    fetcher: HttpFetcher, project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    remote = remote_asset.model_copy(update={"expected_size": None})
    route = respx.get(remote.uri).mock(return_value=httpx.Response(200, content=b"unknown"))

    with pytest.raises(BudgetRejected, match="missing_download_size_bound"):
        fetcher.fetch(remote, "run-unbounded")

    target = project_paths.dataset / remote.target_relative_path
    assert not route.called
    assert not target.exists()
    assert not Path(f"{target}.partial").exists()
    with pytest.raises(KeyError):
        catalog.get(remote.asset_id)


@respx.mock
def test_stream_aborts_and_quarantines_before_exceeding_conservative_bound(
    fetcher: HttpFetcher, project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    stream = ChunkStream(b"1234", b"56")
    remote = remote_asset.model_copy(
        update={"expected_size": None, "budget_size_bytes": 5}
    )
    respx.get(remote.uri).mock(return_value=httpx.Response(200, stream=stream))

    with pytest.raises(PayloadMismatch, match="budget_size_exceeded"):
        fetcher.fetch(remote, "run-bound-overflow")

    assert stream.iterations == 2
    assert not (project_paths.dataset / remote.target_relative_path).exists()
    record = catalog.get(remote.asset_id)
    assert record.status is AssetStatus.QUARANTINED
    assert record.error_code == "budget_size_exceeded"
    assert record.size_bytes <= 5


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


@pytest.mark.parametrize("retry_after", ["nan", "inf", "-inf", "-1", "not-a-delay"])
@respx.mock
def test_invalid_retry_after_falls_back_to_exponential_backoff(
    fetcher: HttpFetcher,
    remote_asset: RemoteAsset,
    sleeps: list[float],
    retry_after: str,
) -> None:
    respx.get(remote_asset.uri).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": retry_after}),
            httpx.Response(200, content=b"valid-payload"),
        ]
    )

    fetcher.fetch(remote_asset, "run-invalid-retry-after")

    assert sleeps == [1]


@respx.mock
def test_http_date_retry_after_is_capped(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, sleeps: list[float]
) -> None:
    future = format_datetime(datetime.now(UTC) + timedelta(minutes=5), usegmt=True)
    respx.get(remote_asset.uri).mock(
        side_effect=[
            httpx.Response(503, headers={"Retry-After": future}),
            httpx.Response(200, content=b"valid-payload"),
        ]
    )

    fetcher.fetch(remote_asset, "run-date-retry-after")

    assert sleeps == [60]


@respx.mock
def test_sleep_failure_records_failed_instead_of_leaving_fetching(
    project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    def broken_sleep(_: float) -> None:
        raise ValueError("fixture sleep rejected non-finite delay")

    fetcher = HttpFetcher(
        project_paths,
        catalog,
        StorageBudget(project_paths.dataset, soft_cap_bytes=2**30, minimum_free_bytes=0),
        client=httpx.Client(),
        max_attempts=2,
        sleep=broken_sleep,
    )
    respx.get(remote_asset.uri).mock(return_value=httpx.Response(500))

    with pytest.raises(DownloadFailed):
        fetcher.fetch(remote_asset, "run-sleep-failure")

    assert catalog.get(remote_asset.asset_id).status is AssetStatus.FAILED


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
def test_response_close_fault_discards_resume_state_and_later_restarts_without_range(
    fetcher: HttpFetcher,
    project_paths,
    catalog,
    remote_asset: RemoteAsset,
) -> None:
    payload = b"valid-payload"
    respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(
            200,
            headers={"ETag": '"fixture-v1"'},
            stream=CloseFaultStream(payload),
        )
    )

    with pytest.raises(DownloadFailed, match="download failed"):
        fetcher.fetch(remote_asset, "run-close-fault")

    target = project_paths.dataset / remote_asset.target_relative_path
    partial = Path(f"{target}.partial")
    partial_existed_after_failure = partial.exists()
    state_existed_after_failure = bool(
        list((project_paths.catalog / "download_state").glob("*.json"))
    )
    assert catalog.get(remote_asset.asset_id).status is AssetStatus.FAILED
    requests: list[httpx.Request] = []

    def restarted(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=payload)

    respx.get(remote_asset.uri).mock(side_effect=restarted)

    record = fetcher.fetch(remote_asset, "run-after-close-fault")

    assert not partial_existed_after_failure
    assert not state_existed_after_failure
    assert "Range" not in requests[0].headers
    assert record.status is AssetStatus.FETCHED


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
def test_partial_cleanup_fault_still_removes_sidecar_marks_failed_and_prevents_resume(
    fetcher: HttpFetcher,
    project_paths,
    catalog,
    remote_asset: RemoteAsset,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(update={"expected_size": len(payload)})
    _interrupt_owned_partial(project_paths, catalog, remote)
    target = project_paths.dataset / remote.target_relative_path
    partial = Path(f"{target}.partial")
    state_path = next((project_paths.catalog / "download_state").glob("*.json"))
    real_unlink = Path.unlink
    fail_once = True

    def fail_first_partial_unlink(path: Path, *args, **kwargs):
        nonlocal fail_once
        if path == partial and fail_once:
            fail_once = False
            raise OSError("fixture partial cleanup failure")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_first_partial_unlink)
    requests: list[httpx.Request] = []

    def fail_then_restart(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(404)
        return httpx.Response(200, content=payload)

    respx.get(remote.uri).mock(side_effect=fail_then_restart)

    with pytest.raises(DownloadFailed, match="cleanup failed"):
        fetcher.fetch(remote, "run-cleanup-fault")

    assert partial.exists()
    assert not state_path.exists()
    assert catalog.get(remote.asset_id).status is AssetStatus.FAILED

    record = fetcher.fetch(remote, "run-after-cleanup-fault")

    assert requests[0].headers["Range"] == "bytes=3-"
    assert "Range" not in requests[1].headers
    assert Path(record.storage_path).read_bytes() == payload


@respx.mock
def test_compatible_partial_response_is_appended(
    fetcher: HttpFetcher, project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(
        update={
            "expected_size": len(payload),
            "expected_checksum": hashlib.sha256(payload).hexdigest(),
        }
    )
    _interrupt_owned_partial(project_paths, catalog, remote)
    target = project_paths.dataset / remote.target_relative_path
    partial = Path(f"{target}.partial")

    def resumed(request: httpx.Request) -> httpx.Response:
        assert request.headers["Range"] == "bytes=3-"
        assert request.headers["If-Range"] == '"fixture-v1"'
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 3-5/6", "ETag": '"fixture-v1"'},
            content=b"def",
        )

    respx.get(remote.uri).mock(side_effect=resumed)

    record = fetcher.fetch(remote, "run-resume")

    assert Path(record.storage_path).read_bytes() == payload
    assert not partial.exists()


@respx.mock
def test_partial_without_sidecar_is_discarded_and_restarted(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset
) -> None:
    payload = b"fresh!"
    remote = remote_asset.model_copy(
        update={"expected_size": len(payload), "expected_checksum": hashlib.sha256(payload).hexdigest()}
    )
    target = project_paths.dataset / remote.target_relative_path
    target.parent.mkdir(parents=True)
    Path(f"{target}.partial").write_bytes(b"stale")

    def restarted(request: httpx.Request) -> httpx.Response:
        assert "Range" not in request.headers
        assert "If-Range" not in request.headers
        return httpx.Response(200, headers={"ETag": '"fresh"'}, content=payload)

    respx.get(remote.uri).mock(side_effect=restarted)

    record = fetcher.fetch(remote, "run-no-owner")

    assert Path(record.storage_path).read_bytes() == payload


@pytest.mark.parametrize(
    "sidecar_text",
    [
        "{",
        json.dumps({"remote_fingerprint": "fixture", "future_field": "unknown"}),
        json.dumps({"remote_fingerprint": 123}),
    ],
    ids=["malformed-json", "unknown-field", "type-invalid"],
)
@respx.mock
def test_corrupt_existing_sidecar_fails_closed_before_catalog_or_request(
    fetcher: HttpFetcher,
    project_paths,
    catalog,
    remote_asset: RemoteAsset,
    sidecar_text: str,
) -> None:
    target = project_paths.dataset / remote_asset.target_relative_path
    partial = Path(f"{target}.partial")
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"uncertain-bytes")
    state_path = fetcher.lock_path(remote_asset).with_suffix(".json")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(sidecar_text, encoding="utf-8")
    route = respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    with pytest.raises(ExistingAssetConflict, match="corrupt download sidecar"):
        fetcher.fetch(remote_asset, "run-corrupt-sidecar")

    assert not route.called
    assert partial.read_bytes() == b"uncertain-bytes"
    assert state_path.read_text(encoding="utf-8") == sidecar_text
    with pytest.raises(KeyError):
        catalog.get(remote_asset.asset_id)


@respx.mock
def test_unreadable_existing_sidecar_is_audit_conflict_before_request(
    fetcher: HttpFetcher,
    catalog,
    remote_asset: RemoteAsset,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = fetcher.lock_path(remote_asset).with_suffix(".json")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{}", encoding="utf-8")
    real_read_text = Path.read_text

    def unreadable_sidecar(path: Path, *args, **kwargs):
        if path == state_path:
            raise PermissionError("fixture unreadable sidecar")
        return real_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", unreadable_sidecar)
    route = respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    with pytest.raises(ExistingAssetConflict, match="corrupt download sidecar"):
        fetcher.fetch(remote_asset, "run-unreadable-sidecar")

    assert not route.called
    with pytest.raises(KeyError):
        catalog.get(remote_asset.asset_id)


@respx.mock
def test_mismatched_resume_fingerprint_discards_partial_before_request(
    fetcher: HttpFetcher, project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    payload = b"fresh!"
    remote = remote_asset.model_copy(
        update={"expected_size": len(payload), "expected_checksum": hashlib.sha256(payload).hexdigest()}
    )
    _interrupt_owned_partial(project_paths, catalog, remote)
    state_path = next((project_paths.catalog / "download_state").glob("*.json"))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["remote_fingerprint"] = "0" * 64
    state_path.write_text(json.dumps(state), encoding="utf-8")

    def restarted(request: httpx.Request) -> httpx.Response:
        assert "Range" not in request.headers
        return httpx.Response(200, headers={"ETag": '"fresh"'}, content=payload)

    respx.get(remote.uri).mock(side_effect=restarted)

    record = fetcher.fetch(remote, "run-owner-mismatch")

    assert Path(record.storage_path).read_bytes() == payload


@respx.mock
def test_server_without_validator_never_resumes_partial(
    fetcher: HttpFetcher, project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    payload = b"fresh!"
    remote = remote_asset.model_copy(update={"expected_size": len(payload)})
    _interrupt_owned_partial(project_paths, catalog, remote, validator_header=None)

    def restarted(request: httpx.Request) -> httpx.Response:
        assert "Range" not in request.headers
        return httpx.Response(200, content=payload)

    respx.get(remote.uri).mock(side_effect=restarted)

    record = fetcher.fetch(remote, "run-no-validator")

    assert Path(record.storage_path).read_bytes() == payload


@respx.mock
def test_unsolicited_partial_response_is_rejected_without_publication(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset
) -> None:
    remote = remote_asset.model_copy(update={"expected_size": 6})
    route = respx.get(remote.uri).mock(
        return_value=httpx.Response(
            206,
            headers={"Content-Range": "bytes 0-5/6", "ETag": '"fixture-v1"'},
            content=b"abcdef",
        )
    )

    with pytest.raises(DownloadFailed, match="unsolicited_partial_response"):
        fetcher.fetch(remote, "run-unsolicited")

    assert route.call_count == 1
    assert not (project_paths.dataset / remote.target_relative_path).exists()


@pytest.mark.parametrize(
    "content_range",
    ["bytes 3-5/*", "bytes 3-4/6", "bytes 3-5/7"],
)
@respx.mock
def test_incompatible_owned_range_is_discarded_then_restarted(
    fetcher: HttpFetcher,
    project_paths,
    catalog,
    remote_asset: RemoteAsset,
    content_range: str,
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(update={"expected_size": len(payload)})
    _interrupt_owned_partial(project_paths, catalog, remote)
    route = respx.get(remote.uri).mock(
        side_effect=[
            httpx.Response(
                206,
                headers={"Content-Range": content_range, "ETag": '"fixture-v1"'},
                content=b"def",
            ),
            httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=payload),
        ]
    )

    record = fetcher.fetch(remote, "run-bad-range")

    assert route.call_count == 3
    assert Path(record.storage_path).read_bytes() == payload


@respx.mock
def test_mismatched_response_validator_discards_resume_then_restarts(
    fetcher: HttpFetcher, project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(update={"expected_size": len(payload)})
    _interrupt_owned_partial(project_paths, catalog, remote)
    route = respx.get(remote.uri).mock(
        side_effect=[
            httpx.Response(
                206,
                headers={"Content-Range": "bytes 3-5/6", "ETag": '"other-v2"'},
                content=b"def",
            ),
            httpx.Response(200, headers={"ETag": '"other-v2"'}, content=payload),
        ]
    )

    record = fetcher.fetch(remote, "run-validator-mismatch")

    assert route.call_count == 3
    assert Path(record.storage_path).read_bytes() == payload


@respx.mock
def test_resume_total_may_not_exceed_conservative_budget_bound(
    fetcher: HttpFetcher, project_paths, catalog, remote_asset: RemoteAsset
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(update={"expected_size": None, "budget_size_bytes": 6})
    _interrupt_owned_partial(project_paths, catalog, remote)
    route = respx.get(remote.uri).mock(
        side_effect=[
            httpx.Response(
                206,
                headers={"Content-Range": "bytes 3-6/7", "ETag": '"fixture-v1"'},
                content=b"defg",
            ),
            httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=payload),
        ]
    )

    record = fetcher.fetch(remote, "run-bound-range")

    assert route.call_count == 3
    assert Path(record.storage_path).read_bytes() == payload


@pytest.mark.parametrize("body", [b"de", b"defg"])
@respx.mock
def test_resume_body_length_must_equal_declared_range(
    fetcher: HttpFetcher,
    project_paths,
    catalog,
    remote_asset: RemoteAsset,
    body: bytes,
) -> None:
    remote = remote_asset.model_copy(update={"expected_size": 6})
    _interrupt_owned_partial(project_paths, catalog, remote)
    respx.get(remote.uri).mock(
        return_value=httpx.Response(
            206,
            headers={"Content-Range": "bytes 3-5/6", "ETag": '"fixture-v1"'},
            content=body,
        )
    )

    with pytest.raises(PayloadMismatch, match="range_body_length_mismatch"):
        fetcher.fetch(remote, "run-range-body")

    assert not (project_paths.dataset / remote.target_relative_path).exists()


@respx.mock
def test_concurrently_locked_target_is_rejected_before_request(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset
) -> None:
    state_dir = project_paths.catalog / "download_state"
    state_dir.mkdir(parents=True)
    lock_path = fetcher.lock_path(remote_asset)
    route = respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(DownloadLocked):
            fetcher.fetch(remote_asset, "run-locked")

    assert not route.called


@respx.mock
def test_stale_unheld_lock_file_does_not_block_fetch(
    fetcher: HttpFetcher, remote_asset: RemoteAsset
) -> None:
    lock_path = fetcher.lock_path(remote_asset)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.touch()
    respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    record = fetcher.fetch(remote_asset, "run-stale-lock")

    assert record.status is AssetStatus.FETCHED


@respx.mock
def test_non_resumable_response_restarts_instead_of_appending(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
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
    respx.get(remote.uri).mock(return_value=httpx.Response(200, content=payload))

    record = fetcher.fetch(remote, "run-restart")

    assert Path(record.storage_path).read_bytes() == payload


@respx.mock
def test_full_restart_open_failure_discards_stale_owned_bytes_before_next_request(
    fetcher: HttpFetcher,
    project_paths,
    catalog,
    remote_asset: RemoteAsset,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(update={"expected_size": len(payload)})
    _interrupt_owned_partial(project_paths, catalog, remote)
    target = project_paths.dataset / remote.target_relative_path
    partial = Path(f"{target}.partial")
    real_open = Path.open
    fail_once = True

    def fail_first_truncate(path: Path, mode: str = "r", *args, **kwargs):
        nonlocal fail_once
        if path == partial and mode == "wb" and fail_once:
            fail_once = False
            raise OSError("fixture truncate failure")
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_first_truncate)
    requests: list[httpx.Request] = []

    def full_response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, headers={"ETag": '"fixture-v2"'}, content=payload)

    respx.get(remote.uri).mock(side_effect=full_response)

    with pytest.raises(DownloadFailed, match="download failed"):
        fetcher.fetch(remote, "run-truncate-failure")

    assert not partial.exists()
    assert not list((project_paths.catalog / "download_state").glob("*.json"))

    record = fetcher.fetch(remote, "run-after-truncate-failure")

    assert requests[0].headers["Range"] == "bytes=3-"
    assert "Range" not in requests[1].headers
    assert Path(record.storage_path).read_bytes() == payload


@respx.mock
def test_full_restart_sidecar_failure_leaves_no_owned_bytes_or_range_resume(
    fetcher: HttpFetcher,
    project_paths,
    catalog,
    remote_asset: RemoteAsset,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(update={"expected_size": len(payload)})
    target = project_paths.dataset / remote.target_relative_path
    partial = Path(f"{target}.partial")
    state_dir = project_paths.catalog / "download_state"
    real_replace = Path.replace
    fail_once = True

    def fail_first_state_publish(path: Path, destination: object):
        nonlocal fail_once
        destination_path = Path(destination)
        if (
            fail_once
            and destination_path.parent == state_dir
            and destination_path.suffix == ".json"
        ):
            fail_once = False
            raise OSError("fixture sidecar publication failure")
        return real_replace(path, destination)

    monkeypatch.setattr(Path, "replace", fail_first_state_publish)
    requests: list[httpx.Request] = []

    def full_response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=payload)

    respx.get(remote.uri).mock(side_effect=full_response)

    with pytest.raises(DownloadFailed, match="download failed"):
        fetcher.fetch(remote, "run-sidecar-failure")

    assert not partial.exists()
    assert not list(state_dir.glob("*.json"))

    record = fetcher.fetch(remote, "run-after-sidecar-failure")

    assert "Range" not in requests[1].headers
    assert Path(record.storage_path).read_bytes() == payload


@pytest.mark.parametrize(
    "headers",
    [
        {"ETag": 'W/"fixture-v1"'},
        {"Last-Modified": "not-an-http-date"},
    ],
    ids=["weak-etag", "invalid-last-modified"],
)
@respx.mock
def test_invalid_response_validator_cannot_own_partial_or_enable_range_resume(
    fetcher: HttpFetcher,
    project_paths,
    catalog,
    remote_asset: RemoteAsset,
    headers: dict[str, str],
) -> None:
    payload = b"abcdef"
    remote = remote_asset.model_copy(update={"expected_size": len(payload)})
    respx.get(remote.uri).mock(
        return_value=httpx.Response(200, headers=headers, stream=InterruptedStream(b"abc"))
    )

    with pytest.raises(DownloadFailed):
        _single_attempt_fetcher(project_paths, catalog).fetch(remote, "run-invalid-validator")

    state_exists_after_failure = bool(
        list((project_paths.catalog / "download_state").glob("*.json"))
    )
    requests: list[httpx.Request] = []

    def restarted(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=payload)

    respx.get(remote.uri).mock(side_effect=restarted)

    record = fetcher.fetch(remote, "run-after-invalid-validator")

    assert not state_exists_after_failure
    assert "Range" not in requests[0].headers
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
    opaque = hashlib.sha256(remote.asset_id.encode("utf-8")).hexdigest()
    quarantine = project_paths.raw / "_quarantine" / opaque
    assert not target.exists()
    assert not Path(f"{target}.partial").exists()
    assert [path.read_bytes() for path in quarantine.iterdir()] == [payload]
    record = catalog.get(remote.asset_id)
    assert record.status is AssetStatus.QUARANTINED
    assert record.error_code == error_code
    assert route.call_count == 1


@pytest.mark.parametrize(
    "target",
    [Path("harmonized/not-raw.bin"), Path("../escape.bin"), Path("raw/../../escape.bin")],
)
@respx.mock
def test_http_target_must_resolve_beneath_dataset_raw(
    fetcher: HttpFetcher, catalog, remote_asset: RemoteAsset, target: Path
) -> None:
    remote = remote_asset.model_copy(update={"target_relative_path": target})
    route = respx.get(remote.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    with pytest.raises(ValueError, match="dataset/raw"):
        fetcher.fetch(remote, "run-target-traversal")

    assert not route.called
    with pytest.raises(KeyError):
        catalog.get(remote.asset_id)


@respx.mock
def test_quarantine_uses_opaque_directory_for_unsafe_asset_id(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset
) -> None:
    remote = remote_asset.model_copy(
        update={"asset_id": "../unsafe/asset", "expected_checksum": "0" * 64}
    )
    respx.get(remote.uri).mock(return_value=httpx.Response(200, content=b"valid-payload"))

    with pytest.raises(PayloadMismatch):
        fetcher.fetch(remote, "run-unsafe-id")

    quarantine_root = (project_paths.raw / "_quarantine").resolve()
    files = [path.resolve() for path in quarantine_root.rglob("*") if path.is_file()]
    assert len(files) == 1
    assert files[0].is_relative_to(quarantine_root)
    assert files[0].read_bytes() == b"valid-payload"
    assert not (project_paths.raw / "unsafe").exists()


@respx.mock
def test_quarantine_collision_never_overwrites_existing_evidence(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    opaque = hashlib.sha256(remote.asset_id.encode("utf-8")).hexdigest()
    quarantine_dir = project_paths.raw / "_quarantine" / opaque
    quarantine_dir.mkdir(parents=True)
    collision = quarantine_dir / "payload.bin.partial.collision"
    collision.write_bytes(b"prior-evidence")
    names = iter([SimpleNamespace(hex="collision"), SimpleNamespace(hex="fresh")])
    monkeypatch.setattr("flashflood_data.storage.http.quarantine.uuid4", lambda: next(names))
    respx.get(remote.uri).mock(return_value=httpx.Response(200, content=b"valid-payload"))

    with pytest.raises(PayloadMismatch):
        fetcher.fetch(remote, "run-collision")

    assert collision.read_bytes() == b"prior-evidence"
    assert (quarantine_dir / "payload.bin.partial.fresh").read_bytes() == b"valid-payload"


@respx.mock
def test_quarantine_cross_filesystem_fallback_copies_and_verifies_evidence(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"valid-payload"
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})

    def cross_filesystem_link(source: object, destination: object) -> None:
        raise OSError(errno.EXDEV, "fixture cross-device link", destination)

    monkeypatch.setattr(os, "link", cross_filesystem_link)
    respx.get(remote.uri).mock(return_value=httpx.Response(200, content=payload))

    with pytest.raises(PayloadMismatch, match="checksum_mismatch"):
        fetcher.fetch(remote, "run-quarantine-exdev")

    record = catalog.get(remote.asset_id)
    evidence = Path(record.storage_path)
    partial = Path(f"{project_paths.dataset / remote.target_relative_path}.partial")
    assert record.status is AssetStatus.QUARANTINED
    assert evidence.read_bytes() == payload
    assert hashlib.sha256(evidence.read_bytes()).hexdigest() == record.checksum
    assert not partial.exists()


@respx.mock
def test_quarantine_copy_failure_removes_incomplete_evidence_and_owned_partial(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    target = project_paths.dataset / remote.target_relative_path
    partial = Path(f"{target}.partial")

    def cross_filesystem_link(source: object, destination: object) -> None:
        raise OSError(errno.EXDEV, "fixture cross-device link", destination)

    real_open = Path.open
    partial_read_count = 0

    def fail_copy_read(path: Path, mode: str = "r", *args, **kwargs):
        nonlocal partial_read_count
        if path == partial and mode == "rb":
            partial_read_count += 1
            if partial_read_count == 3:
                raise OSError("fixture quarantine copy read failure")
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(os, "link", cross_filesystem_link)
    monkeypatch.setattr(Path, "open", fail_copy_read)
    respx.get(remote.uri).mock(
        return_value=httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=b"valid-payload")
    )

    with pytest.raises(DownloadFailed, match="download failed"):
        fetcher.fetch(remote, "run-quarantine-copy-failure")

    opaque = hashlib.sha256(remote.asset_id.encode("utf-8")).hexdigest()
    quarantine_dir = project_paths.raw / "_quarantine" / opaque
    assert partial_read_count == 3
    assert not list(quarantine_dir.iterdir())
    assert not partial.exists()
    assert not list((project_paths.catalog / "download_state").glob("*.json"))
    assert catalog.get(remote.asset_id).status is AssetStatus.FAILED


@respx.mock
def test_pending_quarantine_reconciles_without_network_after_catalog_failure(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"valid-payload"
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    route = respx.get(remote.uri).mock(
        return_value=httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=payload)
    )
    real_transition = catalog.transition
    fail_once = True

    def failing_transition(record_id=None, target=None, **updates):
        nonlocal fail_once
        if target is AssetStatus.QUARANTINED and fail_once:
            fail_once = False
            raise RuntimeError("fixture quarantine catalog failure")
        return real_transition(record_id, target, **updates)

    monkeypatch.setattr(catalog, "transition", failing_transition)

    with pytest.raises(RuntimeError, match="quarantine catalog failure"):
        fetcher.fetch(remote, "run-quarantine-catalog-failure")

    target = project_paths.dataset / remote.target_relative_path
    partial = Path(f"{target}.partial")
    state_path = next((project_paths.catalog / "download_state").glob("*.json"))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    evidence = Path(state["pending_quarantine_path"])
    assert catalog.get(remote.asset_id).status is AssetStatus.FETCHING
    assert partial.read_bytes() == payload
    assert evidence.read_bytes() == payload
    assert state["pending_quarantine_error"] == "checksum_mismatch"
    assert state["pending_quarantine_size_bytes"] == len(payload)
    assert state["pending_quarantine_checksum"] == hashlib.sha256(payload).hexdigest()
    calls_before = route.call_count

    with pytest.raises(PayloadMismatch, match="checksum_mismatch"):
        fetcher.fetch(remote, "run-quarantine-reconcile")

    assert route.call_count == calls_before
    assert catalog.get(remote.asset_id).status is AssetStatus.QUARANTINED
    assert evidence.read_bytes() == payload
    assert not partial.exists()
    assert not state_path.exists()


@respx.mock
def test_corrupt_pending_quarantine_sidecar_fails_closed_without_transition_or_request(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    route = respx.get(remote.uri).mock(
        return_value=httpx.Response(
            200,
            headers={"ETag": '"fixture-v1"'},
            content=b"valid-payload",
        )
    )
    real_transition = catalog.transition
    fail_once = True

    def failing_transition(record_id=None, target=None, **updates):
        nonlocal fail_once
        if target is AssetStatus.QUARANTINED and fail_once:
            fail_once = False
            raise RuntimeError("fixture quarantine catalog failure")
        return real_transition(record_id, target, **updates)

    monkeypatch.setattr(catalog, "transition", failing_transition)
    with pytest.raises(RuntimeError, match="quarantine catalog failure"):
        fetcher.fetch(remote, "run-pending-corrupt")

    state_path = next((project_paths.catalog / "download_state").glob("*.json"))
    state_path.write_text("{", encoding="utf-8")
    fetching = catalog.get(remote.asset_id)
    calls_before = route.call_count

    with pytest.raises(ExistingAssetConflict, match="corrupt download sidecar"):
        fetcher.fetch(remote, "run-corrupt-pending-reconcile")

    assert route.call_count == calls_before
    assert catalog.get(remote.asset_id) == fetching


@respx.mock
def test_tampered_pending_quarantine_path_is_rejected_without_network_or_overwrite(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"valid-payload"
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    route = respx.get(remote.uri).mock(
        return_value=httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=payload)
    )
    real_transition = catalog.transition
    fail_once = True

    def failing_transition(record_id=None, target=None, **updates):
        nonlocal fail_once
        if target is AssetStatus.QUARANTINED and fail_once:
            fail_once = False
            raise RuntimeError("fixture quarantine catalog failure")
        return real_transition(record_id, target, **updates)

    monkeypatch.setattr(catalog, "transition", failing_transition)
    with pytest.raises(RuntimeError, match="quarantine catalog failure"):
        fetcher.fetch(remote, "run-quarantine-pending")

    state_path = next((project_paths.catalog / "download_state").glob("*.json"))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    legitimate_evidence = Path(state["pending_quarantine_path"])
    outside = project_paths.root / "fixture-do-not-overwrite.bin"
    outside.write_bytes(b"operator-data")
    state["pending_quarantine_path"] = str(outside)
    state_path.write_text(json.dumps(state), encoding="utf-8")
    calls_before = route.call_count

    with pytest.raises(ExistingAssetConflict, match="escaped raw quarantine"):
        fetcher.fetch(remote, "run-tampered-quarantine-state")

    assert route.call_count == calls_before
    assert outside.read_bytes() == b"operator-data"
    assert legitimate_evidence.read_bytes() == payload
    assert catalog.get(remote.asset_id).status is AssetStatus.FETCHING


@respx.mock
def test_pending_quarantine_cannot_reconcile_evidence_owned_by_another_asset(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"valid-payload"
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    route = respx.get(remote.uri).mock(
        return_value=httpx.Response(
            200,
            headers={"ETag": '"fixture-v1"'},
            content=payload,
        )
    )
    real_transition = catalog.transition
    fail_once = True

    def failing_transition(record_id=None, target=None, **updates):
        nonlocal fail_once
        if target is AssetStatus.QUARANTINED and fail_once:
            fail_once = False
            raise RuntimeError("fixture quarantine catalog failure")
        return real_transition(record_id, target, **updates)

    monkeypatch.setattr(catalog, "transition", failing_transition)
    with pytest.raises(RuntimeError, match="quarantine catalog failure"):
        fetcher.fetch(remote, "run-cross-asset-pending")

    state_path = next((project_paths.catalog / "download_state").glob("*.json"))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    other_asset_hash = hashlib.sha256(b"fixture-other-asset").hexdigest()
    other_asset_dir = project_paths.raw / "_quarantine" / other_asset_hash
    other_asset_dir.mkdir(parents=True)
    other_evidence = other_asset_dir / "payload.bin.partial.other-owner"
    other_evidence.write_bytes(payload)
    state["pending_quarantine_path"] = str(other_evidence.resolve())
    state_path.write_text(json.dumps(state), encoding="utf-8")
    fetching = catalog.get(remote.asset_id)
    calls_before = route.call_count

    with pytest.raises(ExistingAssetConflict, match="evidence ownership"):
        fetcher.fetch(remote, "run-cross-asset-reconcile")

    assert route.call_count == calls_before
    assert catalog.get(remote.asset_id) == fetching
    assert other_evidence.read_bytes() == payload


@respx.mock
def test_quarantine_retains_partial_and_pending_state_until_catalog_transition_succeeds(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"valid-payload"
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    target = project_paths.dataset / remote.target_relative_path
    partial = Path(f"{target}.partial")
    real_transition = catalog.transition
    observed_pending_state: dict[str, object] = {}

    def inspect_transition(record_id=None, target=None, **updates):
        if target is AssetStatus.QUARANTINED:
            state_path = next((project_paths.catalog / "download_state").glob("*.json"))
            observed_pending_state.update(json.loads(state_path.read_text(encoding="utf-8")))
            assert partial.read_bytes() == payload
            assert Path(str(observed_pending_state["pending_quarantine_path"])).read_bytes() == payload
        return real_transition(record_id, target, **updates)

    monkeypatch.setattr(catalog, "transition", inspect_transition)
    respx.get(remote.uri).mock(
        return_value=httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=payload)
    )

    with pytest.raises(PayloadMismatch, match="checksum_mismatch"):
        fetcher.fetch(remote, "run-quarantine-retention")

    assert observed_pending_state["pending_quarantine_error"] == "checksum_mismatch"
    assert not partial.exists()


@respx.mock
def test_cross_filesystem_quarantine_collision_retries_without_overwrite(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    opaque = hashlib.sha256(remote.asset_id.encode("utf-8")).hexdigest()
    quarantine_dir = project_paths.raw / "_quarantine" / opaque
    quarantine_dir.mkdir(parents=True)
    collision = quarantine_dir / "payload.bin.partial.collision"
    collision.write_bytes(b"prior-evidence")
    names = iter([SimpleNamespace(hex="collision"), SimpleNamespace(hex="fresh")])

    def collision_then_cross_device(source: object, destination: object) -> None:
        if Path(destination) == collision:
            raise FileExistsError(destination)
        raise OSError(errno.EXDEV, "fixture cross-device link", destination)

    monkeypatch.setattr("flashflood_data.storage.http.quarantine.uuid4", lambda: next(names))
    monkeypatch.setattr(os, "link", collision_then_cross_device)
    respx.get(remote.uri).mock(return_value=httpx.Response(200, content=b"valid-payload"))

    with pytest.raises(PayloadMismatch):
        fetcher.fetch(remote, "run-cross-device-collision")

    assert collision.read_bytes() == b"prior-evidence"
    assert (quarantine_dir / "payload.bin.partial.fresh").read_bytes() == b"valid-payload"


@respx.mock
def test_quarantine_removes_obsolete_resume_sidecar(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset
) -> None:
    remote = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    respx.get(remote.uri).mock(
        return_value=httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=b"valid-payload")
    )

    with pytest.raises(PayloadMismatch):
        fetcher.fetch(remote, "run-quarantine-state")

    assert not list((project_paths.catalog / "download_state").glob("*.json"))


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
def test_target_appearing_during_publication_is_preserved(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = project_paths.dataset / remote_asset.target_relative_path

    def competing_publication(source: object, destination: object) -> None:
        Path(destination).write_bytes(b"concurrent-valid-final")
        raise FileExistsError(destination)

    monkeypatch.setattr(
        "flashflood_data.storage.http.quarantine.os.link", competing_publication
    )
    respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    with pytest.raises(ExistingAssetConflict):
        fetcher.fetch(remote_asset, "run-race")

    assert target.read_bytes() == b"concurrent-valid-final"
    assert catalog.get(remote_asset.asset_id).status is AssetStatus.FAILED


@respx.mock
def test_catalog_finalization_failure_reconciles_without_network_on_next_run(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    route = respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, headers={"ETag": '"fixture-v1"'}, content=b"valid-payload")
    )
    real_transition = catalog.transition
    fail_once = True

    def failing_transition(record_id=None, target=None, **updates):
        nonlocal fail_once
        if target is AssetStatus.FETCHED and fail_once:
            fail_once = False
            raise RuntimeError("fixture catalog finalization failure")
        return real_transition(record_id, target, **updates)

    monkeypatch.setattr(catalog, "transition", failing_transition)

    with pytest.raises(RuntimeError, match="catalog finalization"):
        fetcher.fetch(remote_asset, "run-publish-crash")

    target = project_paths.dataset / remote_asset.target_relative_path
    partial = Path(f"{target}.partial")
    assert target.exists() and partial.exists()
    assert target.stat().st_ino == partial.stat().st_ino
    assert catalog.get(remote_asset.asset_id).status is AssetStatus.FETCHING
    state_path = next((project_paths.catalog / "download_state").glob("*.json"))
    publication_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert publication_state["verified_size_bytes"] == len(b"valid-payload")
    assert publication_state["verified_checksum"] == hashlib.sha256(b"valid-payload").hexdigest()
    calls_before = route.call_count

    record = fetcher.fetch(remote_asset, "run-reconcile")

    assert route.call_count == calls_before
    assert record.status is AssetStatus.FETCHED
    assert target.read_bytes() == b"valid-payload"
    assert not partial.exists()


@respx.mock
def test_partial_cleanup_failure_cannot_turn_catalogued_success_into_failure(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = project_paths.dataset / remote_asset.target_relative_path
    partial = Path(f"{target}.partial")
    real_unlink = Path.unlink

    def failing_partial_unlink(path: Path, *args, **kwargs) -> None:
        if path == partial and target.exists():
            raise OSError("fixture cleanup failure")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", failing_partial_unlink)
    respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    record = fetcher.fetch(remote_asset, "run-cleanup-failure")

    assert record.status is AssetStatus.FETCHED
    assert catalog.get(remote_asset.asset_id).status is AssetStatus.FETCHED
    assert target.read_bytes() == b"valid-payload"
    assert partial.exists()


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


@pytest.mark.parametrize(
    "update",
    [
        {"uri": "https://example.invalid/assets/other.bin"},
        {"media_type": "application/x-other"},
        {"license_id": "other-license"},
        {"source_valid_time": "2026-01-01"},
        {"target_relative_path": Path("raw/fixture/other.bin")},
        {"source_id": "other-source"},
        {"source_version": "other-version"},
    ],
)
@respx.mock
def test_conflicting_provenance_never_overwrites_existing_catalog_row(
    fetcher: HttpFetcher,
    project_paths,
    remote_asset: RemoteAsset,
    catalog,
    update: dict[str, object],
) -> None:
    payload = b"valid-payload"
    initial = remote_asset.model_copy(
        update={"expected_checksum": hashlib.sha256(payload).hexdigest()}
    )
    respx.get(initial.uri).mock(return_value=httpx.Response(200, content=payload))
    fetcher.fetch(initial, "run-original")
    original = catalog.get(initial.asset_id)
    conflicting = initial.model_copy(update=update)
    route = respx.request(conflicting.request_method, conflicting.uri).mock(
        return_value=httpx.Response(200, content=payload)
    )
    calls_before = route.call_count

    with pytest.raises(ExistingAssetConflict, match="provenance"):
        fetcher.fetch(conflicting, "run-conflict")

    assert route.call_count == calls_before
    assert catalog.get(initial.asset_id) == original
    assert Path(original.storage_path).read_bytes() == payload


@respx.mock
def test_non_raw_catalog_kind_is_a_provenance_conflict(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, catalog
) -> None:
    payload = b"valid-payload"
    remote = remote_asset.model_copy(
        update={"expected_checksum": hashlib.sha256(payload).hexdigest()}
    )
    respx.get(remote.uri).mock(return_value=httpx.Response(200, content=payload))
    fetcher.fetch(remote, "run-original")
    conflicting = catalog.get(remote.asset_id).model_copy(update={"kind": AssetKind.HARMONIZED})
    catalog.upsert(conflicting)

    with pytest.raises(ExistingAssetConflict, match="provenance"):
        fetcher.fetch(remote, "run-kind-conflict")

    assert catalog.get(remote.asset_id) == conflicting


@respx.mock
def test_quarantined_asset_id_cannot_be_reset_by_a_later_fetch(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, catalog
) -> None:
    bad = remote_asset.model_copy(update={"expected_checksum": "0" * 64})
    respx.get(bad.uri).mock(return_value=httpx.Response(200, content=b"valid-payload"))
    with pytest.raises(PayloadMismatch):
        fetcher.fetch(bad, "run-quarantine")
    quarantined = catalog.get(bad.asset_id)

    corrected = bad.model_copy(
        update={"expected_checksum": hashlib.sha256(b"valid-payload").hexdigest()}
    )
    route = respx.get(corrected.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )
    calls_before = route.call_count

    with pytest.raises(ExistingAssetConflict, match="operator action"):
        fetcher.fetch(corrected, "run-reset-attempt")

    assert route.call_count == calls_before
    assert catalog.get(bad.asset_id) == quarantined


@respx.mock
def test_failed_asset_with_same_provenance_uses_legal_retry_transition(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, catalog
) -> None:
    route = respx.get(remote_asset.uri).mock(
        side_effect=[httpx.Response(404), httpx.Response(200, content=b"valid-payload")]
    )
    with pytest.raises(DownloadFailed):
        fetcher.fetch(remote_asset, "run-failed")
    assert catalog.get(remote_asset.asset_id).status is AssetStatus.FAILED

    record = fetcher.fetch(remote_asset, "run-retry")

    assert route.call_count == 2
    assert record.status is AssetStatus.FETCHED


@respx.mock
def test_failed_asset_without_published_bytes_can_retry_corrected_remote_identity(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, catalog
) -> None:
    respx.get(remote_asset.uri).mock(return_value=httpx.Response(400))
    with pytest.raises(DownloadFailed):
        fetcher.fetch(remote_asset, "run-failed")
    failed = catalog.get(remote_asset.asset_id)
    assert failed.status is AssetStatus.FAILED
    assert not Path(failed.storage_path).exists()
    corrected = remote_asset.model_copy(
        update={"uri": f"{remote_asset.uri}&SCALESIZE=Long(100)"}
    )
    respx.get(corrected.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    record = fetcher.fetch(corrected, "run-corrected")

    assert record.status is AssetStatus.FETCHED
    assert record.source_uri == corrected.uri


@respx.mock
def test_stale_asset_with_same_provenance_uses_legal_fetching_transition(
    fetcher: HttpFetcher, remote_asset: RemoteAsset, catalog
) -> None:
    route = respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )
    fetched = fetcher.fetch(remote_asset, "run-original")
    catalog.transition(fetched.asset_id, AssetStatus.VALIDATED)
    catalog.transition(fetched.asset_id, AssetStatus.STALE)
    Path(fetched.storage_path).unlink()

    record = fetcher.fetch(remote_asset, "run-stale")

    assert route.call_count == 2
    assert record.status is AssetStatus.FETCHED


@respx.mock
def test_existing_fetching_row_with_same_provenance_continues_legally(
    fetcher: HttpFetcher, project_paths, remote_asset: RemoteAsset, catalog
) -> None:
    target = project_paths.dataset / remote_asset.target_relative_path
    discovered = AssetRecord(
        asset_id=remote_asset.asset_id,
        source_id=remote_asset.source_id,
        source_version=remote_asset.source_version,
        kind=AssetKind.RAW,
        source_uri=remote_asset.uri,
        storage_path=str(target),
        media_type=remote_asset.media_type,
        size_bytes=0,
        checksum="",
        retrieved_at=datetime.now(UTC),
        source_valid_time=remote_asset.source_valid_time,
        license_id=remote_asset.license_id,
        pipeline_run_id="run-discovered",
        status=AssetStatus.DISCOVERED,
    )
    catalog.upsert(discovered)
    catalog.transition(remote_asset.asset_id, AssetStatus.FETCHING)
    respx.get(remote_asset.uri).mock(
        return_value=httpx.Response(200, content=b"valid-payload")
    )

    record = fetcher.fetch(remote_asset, "run-continue")

    assert record.status is AssetStatus.FETCHED


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


def test_redaction_filter_handles_digest_google_signatures_and_non_string_args() -> None:
    signed_url = (
        "https://url-user:url-pass@example.invalid/file?tile=N21E103&"
        "X-Goog-Signature=fixture-google-signature&"
        "X-Goog-Credential=fixture-google-credential&safe=value"
    )
    record = logging.LogRecord(
        "fixture",
        logging.ERROR,
        __file__,
        1,
        "status=%d url=%s Authorization: Digest username=fixture-digest-user, "
        "response=fixture-digest-response; safe-header=kept",
        (403, signed_url),
        None,
    )
    record.exc_text = "Bearer fixture-exception-bearer"
    record.stack_info = "stack url=https://example.invalid/?X-Amz-Credential=fixture-stack"

    assert SecretRedactionFilter().filter(record)
    rendered = " ".join(
        value for value in (record.getMessage(), record.exc_text, record.stack_info) if value
    )

    for secret in (
        "url-user",
        "url-pass",
        "fixture-google-signature",
        "fixture-google-credential",
        "fixture-digest-user",
        "fixture-digest-response",
        "fixture-exception-bearer",
        "fixture-stack",
    ):
        assert secret not in rendered
    assert "tile=N21E103" in rendered
    assert "safe=value" in rendered
    assert "safe-header=kept" in rendered


@pytest.mark.parametrize(
    "authorization",
    [
        (
            'Authorization: Digest username="fixture-user;role=fixture-role", '
            'response="fixture-response"; X-Safe: kept'
        ),
        (
            'Authorization: Digest username="fixture-user\\\";fixture-suffix", '
            'response="fixture-response"; X-Safe: kept'
        ),
    ],
    ids=["quoted-semicolon", "escaped-quote"],
)
def test_redaction_filter_consumes_quoted_digest_values_to_unquoted_separator(
    authorization: str,
) -> None:
    record = logging.LogRecord(
        "fixture", logging.ERROR, __file__, 1, authorization, (), None
    )

    assert SecretRedactionFilter().filter(record)
    rendered = record.getMessage()

    for secret in (
        "fixture-user",
        "fixture-role",
        "fixture-suffix",
        "fixture-response",
    ):
        assert secret not in rendered
    assert rendered == "Authorization: [REDACTED]; X-Safe: kept"


def test_redaction_filter_treats_authorization_query_as_query_not_header() -> None:
    record = logging.LogRecord(
        "fixture",
        logging.ERROR,
        __file__,
        1,
        (
            "url=https://example.invalid/file?authorization=fixture-query-secret&"
            "safe=kept&tile=N21E103"
        ),
        (),
        None,
    )

    assert SecretRedactionFilter().filter(record)

    assert record.getMessage() == (
        "url=https://example.invalid/file?authorization=[REDACTED]&"
        "safe=kept&tile=N21E103"
    )


def test_redaction_filter_stops_authorization_at_multiline_header_boundary() -> None:
    record = logging.LogRecord(
        "fixture",
        logging.ERROR,
        __file__,
        1,
        (
            'Authorization: Digest username="fixture-multiline-secret"\n'
            "X-Safe: kept\n"
            "url=https://example.invalid/file?X-Goog-Signature=fixture-google&safe=yes"
        ),
        (),
        None,
    )

    assert SecretRedactionFilter().filter(record)
    rendered = record.getMessage()

    assert "fixture-multiline-secret" not in rendered
    assert "fixture-google" not in rendered
    assert "X-Safe: kept" in rendered
    assert "safe=yes" in rendered


@pytest.mark.parametrize(
    "message,expected,secret",
    [
        (
            (
                "{'Authorization': 'Digest fixture-dict-secret;fixture-suffix', "
                "'safe': 'kept'}"
            ),
            "{'Authorization': '[REDACTED]', 'safe': 'kept'}",
            "fixture-dict-secret",
        ),
        (
            '{"Authorization": "Basic fixture-json-secret", "safe": "kept"}',
            '{"Authorization": "[REDACTED]", "safe": "kept"}',
            "fixture-json-secret",
        ),
        (
            '{"Authorization": "FixtureScheme fixture-custom-secret", "safe": "kept"}',
            '{"Authorization": "[REDACTED]", "safe": "kept"}',
            "fixture-custom-secret",
        ),
    ],
    ids=["python-dict-digest", "json-basic", "json-custom-scheme"],
)
def test_redaction_filter_handles_quoted_mapping_authorization_fields(
    message: str,
    expected: str,
    secret: str,
) -> None:
    record = logging.LogRecord(
        "fixture", logging.ERROR, __file__, 1, message, (), None
    )

    assert SecretRedactionFilter().filter(record)

    assert secret not in record.getMessage()
    assert "fixture-suffix" not in record.getMessage()
    assert record.getMessage() == expected
