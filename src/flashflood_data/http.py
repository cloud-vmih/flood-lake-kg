"""Safe streaming HTTP acquisition for immutable raw source assets."""

import hmac
import logging
import os
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Final
from uuid import uuid4

import httpx

from flashflood_data.budget import StorageBudget
from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import EnvironmentSettings
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, RemoteAsset
from flashflood_data.paths import ProjectPaths

_CONTENT_RANGE: Final = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)
_AUTHORIZATION: Final = re.compile(
    r"(?i)(\bauthorization\s*[:=]\s*)(?:(?:bearer|basic)\s+)?[^\s;,]+"
)
_BEARER: Final = re.compile(r"(?i)(\bbearer\s+)[^\s;,]+")
_URL_USERINFO: Final = re.compile(r"(?i)(https?://)[^/@\s]+@")
_SENSITIVE_QUERY: Final = re.compile(
    r"(?i)([?&](?:access[_-]?token|token|secret|password|signature|sig|credential|"
    r"api[_-]?key|authorization|x-amz-signature|x-amz-credential|"
    r"x-amz-security-token)=)[^&#\s]+"
)


class BudgetRejected(RuntimeError):
    """Raised before acquisition when the storage preflight rejects a payload."""


class DownloadFailed(RuntimeError):
    """Raised when a remote payload cannot be acquired safely."""


class PayloadMismatch(DownloadFailed):
    """Raised after a downloaded payload fails size or checksum validation."""


class ExistingAssetConflict(DownloadFailed):
    """Raised when a target already contains a different or unverifiable payload."""


class _RetryableStatus(Exception):
    def __init__(self, status_code: int, retry_after: float | None) -> None:
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"retryable HTTP status {status_code}")


def _redact(value: str, environment: EnvironmentSettings | None = None) -> str:
    redacted = _AUTHORIZATION.sub(r"\1[REDACTED]", value)
    redacted = _BEARER.sub(r"\1[REDACTED]", redacted)
    redacted = _URL_USERINFO.sub(r"\1[REDACTED]@", redacted)
    redacted = _SENSITIVE_QUERY.sub(r"\1[REDACTED]", redacted)
    if environment is not None:
        for secret in (environment.cdse_username, environment.cdse_password):
            if secret is not None and (plain := secret.get_secret_value()):
                redacted = redacted.replace(plain, "[REDACTED]")
    return redacted


class SecretRedactionFilter(logging.Filter):
    """Redact credentials and signed transport values before log emission."""

    def __init__(self, environment: EnvironmentSettings | None = None) -> None:
        super().__init__()
        self.environment = environment

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _redact(record.getMessage(), self.environment)
        record.args = ()
        record.exc_info = None
        if record.exc_text is not None:
            record.exc_text = _redact(record.exc_text, self.environment)
        if record.stack_info is not None:
            record.stack_info = _redact(record.stack_info, self.environment)
        return True


class HttpFetcher:
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

    def fetch(self, remote: RemoteAsset, run_id: str) -> AssetRecord:
        """Fetch *remote* into its immutable dataset-relative target."""
        final_path = self._target_path(remote)
        existing = self._existing_record(remote, final_path, run_id)
        if existing is not None:
            return existing

        expected_size = remote.expected_size or 0
        decision = self.budget.preflight(
            new_bytes=expected_size,
            temporary_bytes=expected_size,
        )
        if not decision.allowed:
            raise BudgetRejected(decision.reason)

        partial = Path(f"{final_path}.partial")
        self._begin_catalog(remote, final_path, run_id)
        for attempt in range(1, self.max_attempts + 1):
            try:
                retry_after = self._download_attempt(remote, partial)
                if retry_after is not None:
                    raise _RetryableStatus(retry_after[0], retry_after[1])
                self._verify_or_quarantine(partial, remote, run_id)
                self._publish_without_overwrite(partial, final_path)
                return self._finish_catalog(remote, final_path, run_id)
            except (httpx.TimeoutException, httpx.NetworkError, _RetryableStatus) as exc:
                if attempt == self.max_attempts:
                    self._mark_failed(remote.asset_id, "retry_exhausted")
                    raise DownloadFailed(f"download failed: {remote.asset_id}") from exc
                retry_after = exc.retry_after if isinstance(exc, _RetryableStatus) else None
                self.sleep(retry_after if retry_after is not None else min(2 ** (attempt - 1), 16))
            except httpx.HTTPStatusError as exc:
                self._mark_failed(remote.asset_id, f"http_{exc.response.status_code}")
                raise DownloadFailed(f"download failed: {remote.asset_id}") from exc
            except PayloadMismatch:
                raise
            except ExistingAssetConflict:
                self._mark_failed(remote.asset_id, "existing_asset_conflict")
                raise
            except Exception as exc:
                self._mark_failed(remote.asset_id, "download_error")
                raise DownloadFailed(f"download failed: {remote.asset_id}") from exc
        raise AssertionError("unreachable")

    def _target_path(self, remote: RemoteAsset) -> Path:
        dataset_root = self.paths.dataset.resolve()
        target = (dataset_root / remote.target_relative_path).resolve()
        try:
            target.relative_to(dataset_root)
        except ValueError as exc:
            raise ValueError("target_relative_path must stay within the dataset directory") from exc
        return target

    def _existing_record(
        self, remote: RemoteAsset, final_path: Path, run_id: str
    ) -> AssetRecord | None:
        if not final_path.exists():
            return None
        actual_size = final_path.stat().st_size
        actual_checksum = sha256_file(final_path)
        size_matches = remote.expected_size is None or remote.expected_size == actual_size
        expected_checksum_matches = remote.expected_checksum is None or hmac.compare_digest(
            remote.expected_checksum.lower(), actual_checksum
        )
        try:
            catalogued = self.catalog.get(remote.asset_id)
        except KeyError:
            catalogued = None
        catalog_matches = (
            catalogued is not None
            and catalogued.source_id == remote.source_id
            and catalogued.source_version == remote.source_version
            and Path(catalogued.storage_path) == final_path
            and catalogued.size_bytes == actual_size
            and catalogued.checksum_algorithm == "sha256"
            and hmac.compare_digest(catalogued.checksum, actual_checksum)
            and catalogued.status in {AssetStatus.FETCHED, AssetStatus.VALIDATED}
        )
        if size_matches and expected_checksum_matches and catalog_matches:
            return catalogued
        if size_matches and remote.expected_checksum is not None and expected_checksum_matches:
            record = self._record(
                remote,
                final_path,
                run_id,
                status=AssetStatus.FETCHED,
                size_bytes=actual_size,
                checksum=actual_checksum,
            )
            return self.catalog.upsert(record)
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
        if current.status in {AssetStatus.DISCOVERED, AssetStatus.FAILED, AssetStatus.STALE}:
            self.catalog.transition(remote.asset_id, AssetStatus.FETCHING, pipeline_run_id=run_id)
        elif current.status is not AssetStatus.FETCHING:
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

    def _download_attempt(
        self, remote: RemoteAsset, partial: Path
    ) -> tuple[int, float | None] | None:
        partial_size = partial.stat().st_size if partial.exists() else 0
        headers = {"Range": f"bytes={partial_size}-"} if partial_size else {}
        with self.client.stream(
            remote.request_method,
            remote.uri,
            headers=headers,
            data=remote.request_form or None,
            follow_redirects=True,
        ) as response:
            if response.status_code == 429 or response.status_code >= 500:
                return response.status_code, self._retry_after(response.headers.get("Retry-After"))
            response.raise_for_status()
            append = partial_size > 0 and self._compatible_range(response, partial_size)
            partial.parent.mkdir(parents=True, exist_ok=True)
            with partial.open("ab" if append else "wb") as handle:
                for chunk in response.iter_bytes(1024 * 1024):
                    handle.write(chunk)
        return None

    @staticmethod
    def _compatible_range(response: httpx.Response, partial_size: int) -> bool:
        if response.status_code != 206:
            return False
        match = _CONTENT_RANGE.fullmatch(response.headers.get("Content-Range", "").strip())
        if match is None:
            return False
        start, end, total = match.groups()
        if int(start) != partial_size or int(end) < int(start):
            return False
        return total == "*" or int(end) < int(total)

    @staticmethod
    def _retry_after(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            seconds = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=UTC)
                seconds = (retry_at - datetime.now(UTC)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return None
        return min(max(seconds, 0), 60)

    def _verify_or_quarantine(
        self, partial: Path, remote: RemoteAsset, run_id: str
    ) -> None:
        actual_size = partial.stat().st_size
        actual_checksum = sha256_file(partial)
        error_code: str | None = None
        if remote.expected_size is not None and actual_size != remote.expected_size:
            error_code = "size_mismatch"
        elif remote.expected_checksum is not None and not hmac.compare_digest(
            actual_checksum, remote.expected_checksum.lower()
        ):
            error_code = "checksum_mismatch"
        if error_code is None:
            return
        quarantine_dir = self.paths.raw / "_quarantine" / remote.asset_id
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine = quarantine_dir / f"{partial.name}.{uuid4().hex}"
        partial.replace(quarantine)
        self.catalog.transition(
            remote.asset_id,
            AssetStatus.QUARANTINED,
            storage_path=str(quarantine),
            size_bytes=actual_size,
            checksum=actual_checksum,
            retrieved_at=datetime.now(UTC),
            pipeline_run_id=run_id,
            error_code=error_code,
            error_message=f"downloaded payload failed {error_code.removesuffix('_mismatch')} validation",
        )
        raise PayloadMismatch(f"{error_code}: {remote.asset_id}")

    @staticmethod
    def _publish_without_overwrite(partial: Path, final_path: Path) -> None:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(partial, final_path)
        except FileExistsError as exc:
            raise ExistingAssetConflict("target appeared during download") from exc
        partial.unlink()

    def _finish_catalog(
        self, remote: RemoteAsset, final_path: Path, run_id: str
    ) -> AssetRecord:
        return self.catalog.transition(
            remote.asset_id,
            AssetStatus.FETCHED,
            storage_path=str(final_path),
            size_bytes=final_path.stat().st_size,
            checksum=sha256_file(final_path),
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
        )
