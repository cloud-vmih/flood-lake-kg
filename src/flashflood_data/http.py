"""Safe streaming HTTP acquisition for immutable raw source assets."""

import fcntl
import hmac
import json
import logging
import math
import os
import re
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from hashlib import sha256
from pathlib import Path
from typing import Final
from urllib.parse import unquote_plus
from uuid import uuid4

import httpx

from flashflood_data.budget import StorageBudget
from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import EnvironmentSettings
from flashflood_data.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    _is_sensitive_key,
)
from flashflood_data.paths import ProjectPaths

_CONTENT_RANGE: Final = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)
_AUTHORIZATION: Final = re.compile(
    r"(?i)(\bauthorization\s*[:=]\s*)[^\r\n;]+"
)
_BEARER: Final = re.compile(r"(?i)(\bbearer\s+)[^\s;,]+")
_URL_USERINFO: Final = re.compile(r"(?i)(https?://)[^/@\s]+@")
_QUERY_PARAMETER: Final = re.compile(r"([?&])([^=&#\s]+)=([^&#\s]*)")


class BudgetRejected(RuntimeError):
    """Raised before acquisition when the storage preflight rejects a payload."""


class DownloadFailed(RuntimeError):
    """Raised when a remote payload cannot be acquired safely."""


class PayloadMismatch(DownloadFailed):
    """Raised after a downloaded payload fails size or checksum validation."""


class ExistingAssetConflict(DownloadFailed):
    """Raised when a target already contains a different or unverifiable payload."""


class DownloadLocked(DownloadFailed):
    """Raised when another process owns the target's download lock."""


class _RetryableStatus(Exception):
    def __init__(self, status_code: int, retry_after: float | None) -> None:
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"retryable HTTP status {status_code}")


class _BoundExceeded(Exception):
    pass


class _RangeBodyMismatch(Exception):
    pass


class _UnsafePartialResponse(Exception):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class _ResumeState:
    remote_fingerprint: str
    validator_header: str | None = None
    validator_value: str | None = None
    verified_size_bytes: int | None = None
    verified_checksum: str | None = None


@dataclass(frozen=True)
class _VerifiedPayload:
    size_bytes: int
    checksum: str


def _redact(value: str, environment: EnvironmentSettings | None = None) -> str:
    def redact_query(match: re.Match[str]) -> str:
        key = unquote_plus(match.group(2))
        normalized = re.sub(r"[^a-z0-9]", "", key.lower())
        if _is_sensitive_key(key) or normalized == "sig":
            return f"{match.group(1)}{match.group(2)}=[REDACTED]"
        return match.group(0)

    redacted = _QUERY_PARAMETER.sub(redact_query, value)
    redacted = _AUTHORIZATION.sub(r"\1[REDACTED]", redacted)
    redacted = _BEARER.sub(r"\1[REDACTED]", redacted)
    redacted = _URL_USERINFO.sub(r"\1[REDACTED]@", redacted)
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
            return self._fetch_locked(remote, run_id, final_path, size_bound)

    def _fetch_locked(
        self, remote: RemoteAsset, run_id: str, final_path: Path, size_bound: int
    ) -> AssetRecord:
        existing = self._existing_record(remote, final_path, run_id)
        if existing is not None:
            return existing

        partial = Path(f"{final_path}.partial")
        self._begin_catalog(remote, final_path, run_id)
        for attempt in range(1, self.max_attempts + 1):
            published = False
            try:
                retry_after = self._download_attempt(remote, partial, size_bound)
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
                    self._mark_failed(remote.asset_id, "retry_delay_failed")
                    raise DownloadFailed(f"download failed: {remote.asset_id}") from sleep_error
            except httpx.HTTPStatusError as exc:
                self._mark_failed(remote.asset_id, f"http_{exc.response.status_code}")
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
                self._mark_failed(remote.asset_id, exc.code)
                raise DownloadFailed(f"{exc.code}: {remote.asset_id}") from exc
            except ExistingAssetConflict:
                self._mark_failed(remote.asset_id, "existing_asset_conflict")
                raise
            except Exception as exc:
                if published:
                    raise
                self._mark_failed(remote.asset_id, "download_error")
                raise DownloadFailed(f"download failed: {remote.asset_id}") from exc
        raise AssertionError("unreachable")

    def lock_path(self, remote: RemoteAsset) -> Path:
        """Return the opaque catalog-side lock path for *remote*'s target."""
        return self._state_dir() / f"{self._target_key(remote)}.lock"

    @contextmanager
    def _target_lock(self, remote: RemoteAsset) -> Iterator[None]:
        lock_path = self.lock_path(remote)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise DownloadLocked(f"download target is locked: {remote.asset_id}") from exc
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _state_dir(self) -> Path:
        return self.paths.catalog / "download_state"

    def _target_key(self, remote: RemoteAsset) -> str:
        return sha256(str(self._target_path(remote)).encode("utf-8")).hexdigest()

    def _resume_state_path(self, remote: RemoteAsset) -> Path:
        return self._state_dir() / f"{self._target_key(remote)}.json"

    @staticmethod
    def _remote_fingerprint(remote: RemoteAsset) -> str:
        values = remote.model_dump(mode="json")
        encoded = json.dumps(values, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return sha256(encoded).hexdigest()

    def _target_path(self, remote: RemoteAsset) -> Path:
        dataset_root = self.paths.dataset.resolve()
        raw_root = self.paths.raw.resolve()
        target = (dataset_root / remote.target_relative_path).resolve()
        try:
            target.relative_to(raw_root)
        except ValueError as exc:
            raise ValueError("target_relative_path must stay within dataset/raw") from exc
        return target

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
        matches = (
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
        if not matches:
            raise ExistingAssetConflict(f"catalog provenance conflict: {remote.asset_id}")

    def _download_attempt(
        self, remote: RemoteAsset, partial: Path, size_bound: int
    ) -> tuple[int, float | None] | None:
        resume = self._owned_resume_state(remote, partial)
        for request_number in range(2):
            partial_size = partial.stat().st_size if resume is not None else 0
            headers: dict[str, str] = {}
            if resume is not None:
                headers = {
                    "Range": f"bytes={partial_size}-",
                    "If-Range": resume.validator_value,
                }
            with self.client.stream(
                remote.request_method,
                remote.uri,
                headers=headers,
                data=remote.request_form or None,
                follow_redirects=True,
            ) as response:
                if response.status_code == 429 or response.status_code >= 500:
                    return response.status_code, self._retry_after(
                        response.headers.get("Retry-After")
                    )
                response.raise_for_status()
                if response.status_code == 206:
                    if resume is None:
                        raise _UnsafePartialResponse("unsolicited_partial_response")
                    range_length = self._compatible_range_length(
                        response, partial_size, size_bound, remote, resume
                    )
                    if range_length is None:
                        self._discard_resume(remote, partial)
                        resume = None
                        if request_number == 0:
                            continue
                        raise _UnsafePartialResponse("incompatible_partial_response")
                    self._stream_response(
                        response,
                        partial,
                        mode="ab",
                        initial_size=partial_size,
                        size_bound=size_bound,
                        expected_body_bytes=range_length,
                    )
                    return None

                validator = self._response_validator(response)
                if validator is None:
                    self._resume_state_path(remote).unlink(missing_ok=True)
                else:
                    self._write_resume_state(remote, validator)
                self._stream_response(
                    response,
                    partial,
                    mode="wb",
                    initial_size=0,
                    size_bound=size_bound,
                )
                return None
        raise AssertionError("unreachable")

    def _stream_response(
        self,
        response: httpx.Response,
        partial: Path,
        *,
        mode: str,
        initial_size: int,
        size_bound: int,
        expected_body_bytes: int | None = None,
    ) -> None:
        body_bytes = 0
        written = initial_size
        partial.parent.mkdir(parents=True, exist_ok=True)
        with partial.open(mode) as handle:
            for chunk in response.iter_raw():
                if expected_body_bytes is not None and body_bytes + len(chunk) > expected_body_bytes:
                    allowed = max(expected_body_bytes - body_bytes, 0)
                    handle.write(chunk[:allowed])
                    raise _RangeBodyMismatch
                if written + len(chunk) > size_bound:
                    handle.write(chunk[: max(size_bound - written, 0)])
                    raise _BoundExceeded
                handle.write(chunk)
                body_bytes += len(chunk)
                written += len(chunk)
        if expected_body_bytes is not None and body_bytes != expected_body_bytes:
            raise _RangeBodyMismatch

    def _owned_resume_state(
        self, remote: RemoteAsset, partial: Path
    ) -> _ResumeState | None:
        if not partial.exists() or partial.stat().st_size <= 0:
            self._discard_resume(remote, partial)
            return None
        state_path = self._resume_state_path(remote)
        state = self._read_resume_state(state_path)
        if state is None:
            self._discard_resume(remote, partial)
            return None
        if (
            state.remote_fingerprint != self._remote_fingerprint(remote)
            or not state.validator_header
            or not state.validator_value
            or state.verified_checksum is not None
        ):
            self._discard_resume(remote, partial)
            return None
        return state

    def _write_resume_state(self, remote: RemoteAsset, validator: tuple[str, str]) -> None:
        state = _ResumeState(self._remote_fingerprint(remote), validator[0], validator[1])
        self._persist_resume_state(remote, state)

    def _write_verified_state(
        self, remote: RemoteAsset, verified: _VerifiedPayload
    ) -> None:
        existing = self._read_resume_state(self._resume_state_path(remote))
        validator_header = existing.validator_header if existing is not None else None
        validator_value = existing.validator_value if existing is not None else None
        state = _ResumeState(
            remote_fingerprint=self._remote_fingerprint(remote),
            validator_header=validator_header,
            validator_value=validator_value,
            verified_size_bytes=verified.size_bytes,
            verified_checksum=verified.checksum,
        )
        self._persist_resume_state(remote, state)

    def _persist_resume_state(self, remote: RemoteAsset, state: _ResumeState) -> None:
        path = self._resume_state_path(remote)
        path.parent.mkdir(parents=True, exist_ok=True)
        from flashflood_data.io_atomic import atomic_target

        with atomic_target(path) as temporary:
            temporary.write_text(json.dumps(state.__dict__, sort_keys=True), encoding="utf-8")

    @staticmethod
    def _read_resume_state(path: Path) -> _ResumeState | None:
        try:
            return _ResumeState(**json.loads(path.read_text(encoding="utf-8")))
        except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def _publication_state_matches(
        self, remote: RemoteAsset, size_bytes: int, checksum: str
    ) -> bool:
        state = self._read_resume_state(self._resume_state_path(remote))
        return (
            state is not None
            and state.remote_fingerprint == self._remote_fingerprint(remote)
            and state.verified_size_bytes == size_bytes
            and state.verified_checksum is not None
            and hmac.compare_digest(state.verified_checksum, checksum)
        )

    def _discard_resume(self, remote: RemoteAsset, partial: Path) -> None:
        partial.unlink(missing_ok=True)
        self._resume_state_path(remote).unlink(missing_ok=True)

    @staticmethod
    def _response_validator(response: httpx.Response) -> tuple[str, str] | None:
        for header in ("ETag", "Last-Modified"):
            value = response.headers.get(header)
            if value:
                return header, value
        return None

    @staticmethod
    def _compatible_range_length(
        response: httpx.Response,
        partial_size: int,
        size_bound: int,
        remote: RemoteAsset,
        resume: _ResumeState,
    ) -> int | None:
        if response.status_code != 206:
            return None
        match = _CONTENT_RANGE.fullmatch(response.headers.get("Content-Range", "").strip())
        if match is None:
            return None
        start, end, total = match.groups()
        if total == "*":
            return None
        start_value, end_value, total_value = int(start), int(end), int(total)
        response_validator = response.headers.get(resume.validator_header)
        if (
            start_value != partial_size
            or end_value != total_value - 1
            or total_value > size_bound
            or response_validator != resume.validator_value
        ):
            return None
        if remote.expected_size is not None and total_value != remote.expected_size:
            return None
        return end_value - start_value + 1

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
        if not math.isfinite(seconds) or seconds < 0:
            return None
        return min(seconds, 60)

    def _verify_or_quarantine(
        self, partial: Path, remote: RemoteAsset, run_id: str
    ) -> _VerifiedPayload:
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
            return _VerifiedPayload(actual_size, actual_checksum)
        self._quarantine(partial, remote, run_id, error_code)
        raise AssertionError("unreachable")

    def _quarantine(
        self, partial: Path, remote: RemoteAsset, run_id: str, error_code: str
    ) -> None:
        actual_size = partial.stat().st_size
        actual_checksum = sha256_file(partial)
        opaque_asset_id = sha256(remote.asset_id.encode("utf-8")).hexdigest()
        quarantine_dir = (self.paths.raw / "_quarantine" / opaque_asset_id).resolve()
        quarantine_root = (self.paths.raw / "_quarantine").resolve()
        quarantine_dir.relative_to(quarantine_root)
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        for _ in range(100):
            quarantine = quarantine_dir / f"{partial.name}.{uuid4().hex}"
            try:
                os.link(partial, quarantine)
            except FileExistsError:
                continue
            try:
                partial.unlink()
            except OSError:
                pass
            break
        else:
            raise DownloadFailed(f"could not reserve quarantine target: {remote.asset_id}")
        try:
            self._resume_state_path(remote).unlink(missing_ok=True)
        except OSError:
            self.logger.warning("best-effort quarantine state cleanup failed")
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

    def _cleanup_success(self, remote: RemoteAsset, partial: Path) -> None:
        for path in (partial, self._resume_state_path(remote)):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                self.logger.warning("best-effort download cleanup failed")

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
