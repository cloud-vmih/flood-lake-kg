"""Safe streaming HTTP acquisition for immutable raw source assets."""

import hmac
import json
import logging
import os
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import httpx

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.catalog.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
)
from flashflood_data.core.config import EnvironmentSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.sources.budget import StorageBudget
from flashflood_data.storage.http.errors import (
    BoundExceeded as _BoundExceeded,
)
from flashflood_data.storage.http.errors import (
    BudgetRejected,
    DownloadFailed,
    ExistingAssetConflict,
    PayloadMismatch,
)
from flashflood_data.storage.http.errors import (
    RangeBodyMismatch as _RangeBodyMismatch,
)
from flashflood_data.storage.http.errors import (
    ResumeCleanupError as _ResumeCleanupError,
)
from flashflood_data.storage.http.errors import (
    RetryableStatus as _RetryableStatus,
)
from flashflood_data.storage.http.errors import (
    UnsafePartialResponse as _UnsafePartialResponse,
)
from flashflood_data.storage.http.models import (
    VerifiedPayload as _VerifiedPayload,
)
from flashflood_data.storage.http.quarantine import QuarantineMixin
from flashflood_data.storage.http.redaction import SecretRedactionFilter
from flashflood_data.storage.http.resume import ResumeMixin
from flashflood_data.storage.http.transfer import TransferMixin

_CONTENT_RANGE: Final = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)
_STRONG_ETAG: Final = re.compile(r'^"[\x21\x23-\x7e\x80-\xff]*"$')


class HttpFetcher(ResumeMixin, TransferMixin, QuarantineMixin):
    """Download, verify, catalogue, and atomically publish one remote asset."""

    def __init__(
        self,
        paths: ProjectPaths,
        catalog: AssetCatalog,
        budget: StorageBudget,
        *,
        client: httpx.Client | None = None,
        max_attempts: int = 4,
        sleep: Callable[[float], object] = time.sleep,
        environment: EnvironmentSettings | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        self.paths = paths
        self.catalog = catalog
        self.budget = budget
        self.client = client or httpx.Client()
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.environment = environment
        self.logger = logger or logging.getLogger(__name__)
        self.logger.addFilter(SecretRedactionFilter(environment))

    def fetch(
        self, remote: RemoteAsset, run_id: str, *, headers: dict[str, str] | None = None
    ) -> AssetRecord:
        """Fetch *remote* into its immutable dataset-relative target."""
        final_path = self._target_path(remote)
        size_bound = (
            remote.expected_size
            if remote.expected_size is not None
            else remote.budget_size_bytes
        )
        if size_bound is None:
            raise BudgetRejected("missing_download_size_bound")
        decision = self.budget.preflight(
            new_bytes=size_bound,
            temporary_bytes=size_bound,
        )
        if not decision.allowed:
            raise BudgetRejected(decision.reason)

        with self._target_lock(remote):
            return self._fetch_locked(remote, run_id, final_path, size_bound, headers or {})

    def _fetch_locked(
        self,
        remote: RemoteAsset,
        run_id: str,
        final_path: Path,
        size_bound: int,
        headers: dict[str, str],
    ) -> AssetRecord:
        self._read_resume_state(self._resume_state_path(remote))
        existing = self._existing_record(remote, final_path, run_id)
        if existing is not None:
            return existing

        partial = Path(f"{final_path}.partial")
        self._reconcile_pending_quarantine(remote, partial, run_id, size_bound)
        self._begin_catalog(remote, final_path, run_id)
        for attempt in range(1, self.max_attempts + 1):
            published = False
            try:
                retry_after = self._download_attempt(remote, partial, size_bound, headers)
                if retry_after is not None:
                    raise _RetryableStatus(retry_after[0], retry_after[1])
                verified = self._verify_or_quarantine(partial, remote, run_id)
                self._write_verified_state(remote, verified)
                self._publish_without_overwrite(partial, final_path)
                published = True
                record = self._finish_catalog(remote, final_path, run_id, verified)
                self._cleanup_success(remote, partial)
                return record
            except (httpx.TimeoutException, httpx.NetworkError, _RetryableStatus) as exc:
                if attempt == self.max_attempts:
                    self._mark_failed(remote.asset_id, "retry_exhausted")
                    raise DownloadFailed(f"download failed: {remote.asset_id}") from exc
                retry_after = exc.retry_after if isinstance(exc, _RetryableStatus) else None
                delay = retry_after if retry_after is not None else min(2 ** (attempt - 1), 16)
                try:
                    self.sleep(delay)
                except Exception as sleep_error:
                    self._discard_and_mark_failed(
                        remote, partial, "retry_delay_failed"
                    )
                    raise DownloadFailed(f"download failed: {remote.asset_id}") from sleep_error
            except _ResumeCleanupError:
                self._mark_failed(remote.asset_id, "resume_cleanup_failed")
                raise
            except httpx.HTTPStatusError as exc:
                self._discard_and_mark_failed(
                    remote, partial, f"http_{exc.response.status_code}"
                )
                raise DownloadFailed(f"download failed: {remote.asset_id}") from exc
            except PayloadMismatch:
                raise
            except _BoundExceeded:
                self._quarantine(partial, remote, run_id, "budget_size_exceeded")
                raise AssertionError("unreachable")
            except _RangeBodyMismatch:
                self._quarantine(partial, remote, run_id, "range_body_length_mismatch")
                raise AssertionError("unreachable")
            except _UnsafePartialResponse as exc:
                self._discard_and_mark_failed(remote, partial, exc.code)
                raise DownloadFailed(f"{exc.code}: {remote.asset_id}") from exc
            except ExistingAssetConflict:
                self._discard_and_mark_failed(
                    remote, partial, "existing_asset_conflict"
                )
                raise
            except Exception as exc:
                if published or self._has_pending_quarantine(remote):
                    raise
                self._discard_and_mark_failed(remote, partial, "download_error")
                raise DownloadFailed(f"download failed: {remote.asset_id}") from exc
        raise AssertionError("unreachable")








    def _existing_record(
        self, remote: RemoteAsset, final_path: Path, run_id: str
    ) -> AssetRecord | None:
        if not final_path.exists():
            return None
        actual_size = final_path.stat().st_size
        actual_checksum = sha256_file(final_path)
        size_matches = (
            remote.expected_size == actual_size
            if remote.expected_size is not None
            else remote.budget_size_bytes is not None
            and actual_size <= remote.budget_size_bytes
        )
        expected_checksum_matches = remote.expected_checksum is None or hmac.compare_digest(
            remote.expected_checksum.lower(), actual_checksum
        )
        try:
            catalogued = self.catalog.get(remote.asset_id)
        except KeyError:
            catalogued = None
        if catalogued is not None:
            if catalogued.status is AssetStatus.STALE:
                self._quarantine_stale_target(remote, final_path)
                return None
            self._require_catalog_identity(catalogued, remote, final_path)
            partial = Path(f"{final_path}.partial")
            if (
                catalogued.status is AssetStatus.FETCHING
                and partial.exists()
                and os.path.samestat(final_path.stat(), partial.stat())
                and size_matches
                and expected_checksum_matches
                and self._publication_state_matches(
                    remote, actual_size, actual_checksum
                )
            ):
                reconciled = self.catalog.transition(
                    remote.asset_id,
                    AssetStatus.FETCHED,
                    size_bytes=actual_size,
                    checksum=actual_checksum,
                    retrieved_at=datetime.now(UTC),
                    pipeline_run_id=run_id,
                    error_code=None,
                    error_message=None,
                )
                self._cleanup_success(remote, partial)
                return reconciled
            catalog_matches = (
                catalogued.size_bytes == actual_size
                and catalogued.checksum_algorithm == "sha256"
                and hmac.compare_digest(catalogued.checksum, actual_checksum)
                and catalogued.status in {AssetStatus.FETCHED, AssetStatus.VALIDATED}
            )
            if size_matches and expected_checksum_matches and catalog_matches:
                return catalogued
            raise ExistingAssetConflict(
                f"existing catalog state requires operator action: {remote.asset_id}"
            )
        if size_matches and remote.expected_checksum is not None and expected_checksum_matches:
            discovered = self._record(
                remote,
                final_path,
                run_id,
                status=AssetStatus.DISCOVERED,
                size_bytes=0,
                checksum="",
            )
            self.catalog.upsert(discovered)
            self.catalog.transition(remote.asset_id, AssetStatus.FETCHING)
            return self.catalog.transition(
                remote.asset_id,
                AssetStatus.FETCHED,
                size_bytes=actual_size,
                checksum=actual_checksum,
                retrieved_at=datetime.now(UTC),
            )
        raise ExistingAssetConflict(f"target already exists: {remote.asset_id}")

    def _begin_catalog(self, remote: RemoteAsset, final_path: Path, run_id: str) -> None:
        try:
            current = self.catalog.get(remote.asset_id)
        except KeyError:
            discovered = self._record(
                remote,
                final_path,
                run_id,
                status=AssetStatus.DISCOVERED,
                size_bytes=0,
                checksum="",
            )
            self.catalog.upsert(discovered)
            self.catalog.transition(remote.asset_id, AssetStatus.FETCHING)
            return
        if current.status is AssetStatus.QUARANTINED:
            raise ExistingAssetConflict(
                f"quarantined asset requires a new asset ID or operator action: {remote.asset_id}"
            )
        if current.status in {AssetStatus.FAILED, AssetStatus.STALE} and not (
            self._catalog_identity_matches(current, remote, final_path)
        ):
            current = self.catalog.upsert(
                self._record(
                    remote,
                    final_path,
                    run_id,
                    status=AssetStatus.DISCOVERED,
                    size_bytes=0,
                    checksum="",
                )
            )
        self._require_catalog_identity(current, remote, final_path)
        if current.status in {AssetStatus.DISCOVERED, AssetStatus.FAILED, AssetStatus.STALE}:
            self.catalog.transition(remote.asset_id, AssetStatus.FETCHING, pipeline_run_id=run_id)
        elif current.status is not AssetStatus.FETCHING:
            raise ExistingAssetConflict(
                f"existing catalog state requires operator action: {remote.asset_id}"
            )

    @staticmethod
    def _require_catalog_identity(
        record: AssetRecord, remote: RemoteAsset, final_path: Path
    ) -> None:
        if record.status is AssetStatus.QUARANTINED:
            raise ExistingAssetConflict(
                f"quarantined asset requires a new asset ID or operator action: {remote.asset_id}"
            )
        if not HttpFetcher._catalog_identity_matches(record, remote, final_path):
            raise ExistingAssetConflict(f"catalog provenance conflict: {remote.asset_id}")

    @staticmethod
    def _catalog_identity_matches(
        record: AssetRecord, remote: RemoteAsset, final_path: Path
    ) -> bool:
        return (
            record.asset_id == remote.asset_id
            and record.source_id == remote.source_id
            and record.source_version == remote.source_version
            and record.source_uri == remote.uri
            and Path(record.storage_path) == final_path
            and record.media_type == remote.media_type
            and record.license_id == remote.license_id
            and record.source_valid_time == remote.source_valid_time
            and record.kind is AssetKind.RAW
        )
























    def _finish_catalog(
        self,
        remote: RemoteAsset,
        final_path: Path,
        run_id: str,
        verified: _VerifiedPayload,
    ) -> AssetRecord:
        return self.catalog.transition(
            remote.asset_id,
            AssetStatus.FETCHED,
            storage_path=str(final_path),
            size_bytes=verified.size_bytes,
            checksum=verified.checksum,
            retrieved_at=datetime.now(UTC),
            pipeline_run_id=run_id,
            error_code=None,
            error_message=None,
        )


    def _mark_failed(self, asset_id: str, error_code: str) -> None:
        try:
            current = self.catalog.get(asset_id)
        except KeyError:
            return
        if current.status is AssetStatus.FETCHING:
            self.catalog.transition(
                asset_id,
                AssetStatus.FAILED,
                error_code=error_code,
                error_message="remote acquisition failed",
            )

    @staticmethod
    def _record(
        remote: RemoteAsset,
        path: Path,
        run_id: str,
        *,
        status: AssetStatus,
        size_bytes: int,
        checksum: str,
    ) -> AssetRecord:
        return AssetRecord(
            asset_id=remote.asset_id,
            source_id=remote.source_id,
            source_version=remote.source_version,
            kind=AssetKind.RAW,
            source_uri=remote.uri,
            storage_path=str(path),
            media_type=remote.media_type,
            size_bytes=size_bytes,
            checksum=checksum,
            retrieved_at=datetime.now(UTC),
            source_valid_time=remote.source_valid_time,
            license_id=remote.license_id,
            pipeline_run_id=run_id,
            status=status,
            metadata_json=json.dumps(
                {
                    "budget_size_bytes": remote.budget_size_bytes,
                    "expected_checksum": remote.expected_checksum,
                    "expected_size": remote.expected_size,
                    "request_form": dict(remote.request_form),
                    "request_method": remote.request_method,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
