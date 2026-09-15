"""Quarantine verification and collision-safe publication."""

import errno
import hmac
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from flashflood_data.catalog import sha256_file
from flashflood_data.catalog.models import AssetStatus, RemoteAsset
from flashflood_data.storage.http.errors import (
    DownloadFailed,
    ExistingAssetConflict,
    PayloadMismatch,
)
from flashflood_data.storage.http.errors import (
    ResumeCleanupError as _ResumeCleanupError,
)
from flashflood_data.storage.http.models import (
    ResumeState as _ResumeState,
)
from flashflood_data.storage.http.models import (
    VerifiedPayload as _VerifiedPayload,
)


class QuarantineMixin:
    def _quarantine_stale_target(self, remote: RemoteAsset, final_path: Path) -> Path:
        """Preserve stale canonical bytes before reusing their atomic publication target."""
        actual_size = final_path.stat().st_size
        actual_checksum = sha256_file(final_path)
        opaque_asset_id = sha256(remote.asset_id.encode("utf-8")).hexdigest()
        quarantine_dir = (self.paths.raw / "_quarantine" / opaque_asset_id).resolve()
        quarantine_root = (self.paths.raw / "_quarantine").resolve()
        quarantine_dir.relative_to(quarantine_root)
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        evidence = self._publish_quarantine_evidence(
            final_path, quarantine_dir, actual_size, actual_checksum
        )
        final_path.unlink()
        self._resume_state_path(remote).unlink(missing_ok=True)
        return evidence

    def _has_pending_quarantine(self, remote: RemoteAsset) -> bool:
        state = self._read_resume_state(self._resume_state_path(remote))
        return (
            state is not None
            and state.remote_fingerprint == self._remote_fingerprint(remote)
            and state.pending_quarantine_path is not None
            and state.pending_quarantine_error is not None
            and state.pending_quarantine_size_bytes is not None
            and state.pending_quarantine_checksum is not None
        )

    def _reconcile_pending_quarantine(
        self,
        remote: RemoteAsset,
        partial: Path,
        run_id: str,
        size_bound: int,
    ) -> None:
        state = self._read_resume_state(self._resume_state_path(remote))
        pending_values = (
            None if state is None else state.pending_quarantine_path,
            None if state is None else state.pending_quarantine_error,
            None if state is None else state.pending_quarantine_size_bytes,
            None if state is None else state.pending_quarantine_checksum,
        )
        if all(value is None for value in pending_values):
            return
        if state is None or any(value is None for value in pending_values):
            raise ExistingAssetConflict(
                f"invalid pending quarantine state: {remote.asset_id}"
            )
        try:
            current = self.catalog.get(remote.asset_id)
        except KeyError as exc:
            raise ExistingAssetConflict(
                f"pending quarantine has no catalog row: {remote.asset_id}"
            ) from exc
        final_path = self._target_path(remote)
        self._require_catalog_identity(current, remote, final_path)
        quarantine_root = (self.paths.raw / "_quarantine").resolve()
        evidence = Path(state.pending_quarantine_path)
        resolved_evidence = evidence.resolve()
        try:
            resolved_evidence.relative_to(quarantine_root)
        except ValueError as exc:
            raise ExistingAssetConflict(
                f"pending quarantine path escaped raw quarantine: {remote.asset_id}"
            ) from exc
        expected_owner_dir = (
            quarantine_root / sha256(remote.asset_id.encode("utf-8")).hexdigest()
        ).resolve()
        if resolved_evidence.parent != expected_owner_dir:
            raise ExistingAssetConflict(
                f"pending quarantine evidence ownership conflict: {remote.asset_id}"
            )
        pending_size = state.pending_quarantine_size_bytes
        pending_checksum = state.pending_quarantine_checksum
        allowed_errors = {
            "budget_size_exceeded",
            "checksum_mismatch",
            "range_body_length_mismatch",
            "size_mismatch",
        }
        matches = (
            current.status is AssetStatus.FETCHING
            and state.remote_fingerprint == self._remote_fingerprint(remote)
            and evidence.is_absolute()
            and resolved_evidence == evidence
            and state.pending_quarantine_error in allowed_errors
            and pending_size <= size_bound
            and partial.is_file()
            and resolved_evidence.is_file()
            and partial.stat().st_size == pending_size
            and resolved_evidence.stat().st_size == pending_size
            and hmac.compare_digest(sha256_file(partial), pending_checksum)
            and hmac.compare_digest(sha256_file(resolved_evidence), pending_checksum)
        )
        if not matches:
            raise ExistingAssetConflict(
                f"pending quarantine evidence requires operator action: {remote.asset_id}"
            )
        self.catalog.transition(
            remote.asset_id,
            AssetStatus.QUARANTINED,
            storage_path=str(evidence),
            size_bytes=pending_size,
            checksum=pending_checksum,
            retrieved_at=datetime.now(UTC),
            pipeline_run_id=run_id,
            error_code=state.pending_quarantine_error,
            error_message=self._quarantine_error_message(state.pending_quarantine_error),
        )
        self._cleanup_success(remote, partial)
        raise PayloadMismatch(f"{state.pending_quarantine_error}: {remote.asset_id}")

    def _discard_and_mark_failed(
        self, remote: RemoteAsset, partial: Path, error_code: str
    ) -> None:
        try:
            self._discard_resume(remote, partial)
        except _ResumeCleanupError:
            self._mark_failed(remote.asset_id, error_code)
            raise
        self._mark_failed(remote.asset_id, error_code)

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
        try:
            quarantine = self._publish_quarantine_evidence(
                partial, quarantine_dir, actual_size, actual_checksum
            )
        except Exception:
            self._discard_resume(remote, partial)
            raise
        state = _ResumeState(
            remote_fingerprint=self._remote_fingerprint(remote),
            pending_quarantine_path=str(quarantine),
            pending_quarantine_error=error_code,
            pending_quarantine_size_bytes=actual_size,
            pending_quarantine_checksum=actual_checksum,
        )
        try:
            self._persist_resume_state(remote, state)
        except Exception:
            quarantine.unlink(missing_ok=True)
            self._discard_resume(remote, partial)
            raise
        self.catalog.transition(
            remote.asset_id,
            AssetStatus.QUARANTINED,
            storage_path=str(quarantine),
            size_bytes=actual_size,
            checksum=actual_checksum,
            retrieved_at=datetime.now(UTC),
            pipeline_run_id=run_id,
            error_code=error_code,
            error_message=self._quarantine_error_message(error_code),
        )
        self._cleanup_success(remote, partial)
        raise PayloadMismatch(f"{error_code}: {remote.asset_id}")

    @staticmethod
    def _quarantine_error_message(error_code: str) -> str:
        return (
            "downloaded payload failed "
            f"{error_code.removesuffix('_mismatch')} validation"
        )

    def _publish_quarantine_evidence(
        self,
        partial: Path,
        quarantine_dir: Path,
        expected_size: int,
        expected_checksum: str,
    ) -> Path:
        for _ in range(100):
            quarantine = quarantine_dir / f"{partial.name}.{uuid4().hex}"
            try:
                os.link(partial, quarantine)
                return quarantine
            except FileExistsError:
                continue
            except OSError as exc:
                if exc.errno != errno.EXDEV:
                    raise
            try:
                descriptor = os.open(
                    quarantine,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                continue
            try:
                destination = os.fdopen(descriptor, "wb")
                descriptor = -1
                with destination, partial.open("rb") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        destination.write(chunk)
                    destination.flush()
                    os.fsync(destination.fileno())
                if (
                    quarantine.stat().st_size != expected_size
                    or not hmac.compare_digest(
                        sha256_file(quarantine), expected_checksum
                    )
                ):
                    raise OSError("quarantine evidence verification failed")
                return quarantine
            except Exception:
                if descriptor >= 0:
                    os.close(descriptor)
                quarantine.unlink(missing_ok=True)
                raise
        raise DownloadFailed("could not reserve an exclusive quarantine target")

    @staticmethod
    def _publish_without_overwrite(partial: Path, final_path: Path) -> None:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(partial, final_path)
        except FileExistsError as exc:
            raise ExistingAssetConflict("target appeared during download") from exc
