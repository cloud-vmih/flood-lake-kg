"""Resume state and target-lock operations for HTTP acquisition."""

import fcntl
import hmac
import json
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path

from flashflood_data.catalog.models import RemoteAsset
from flashflood_data.storage.http.errors import (
    DownloadLocked,
    ExistingAssetConflict,
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


class ResumeMixin:
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
        from flashflood_data.storage.atomic import atomic_target

        with atomic_target(path) as temporary:
            temporary.write_text(json.dumps(state.__dict__, sort_keys=True), encoding="utf-8")

    @staticmethod
    def _read_resume_state(path: Path) -> _ResumeState | None:
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError) as exc:
            raise ExistingAssetConflict(
                f"corrupt download sidecar: {path.name}"
            ) from exc
        try:
            values = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExistingAssetConflict(
                f"corrupt download sidecar: {path.name}"
            ) from exc
        string_fields = {
            "pending_quarantine_checksum",
            "pending_quarantine_error",
            "pending_quarantine_path",
            "remote_fingerprint",
            "validator_header",
            "validator_value",
            "verified_checksum",
        }
        integer_fields = {
            "pending_quarantine_size_bytes",
            "verified_size_bytes",
        }
        if (
            not isinstance(values, dict)
            or "remote_fingerprint" not in values
            or not set(values) <= string_fields | integer_fields
            or any(
                value is not None and not isinstance(value, str)
                for key, value in values.items()
                if key in string_fields
            )
            or any(
                value is not None
                and (isinstance(value, bool) or not isinstance(value, int))
                for key, value in values.items()
                if key in integer_fields
            )
        ):
            raise ExistingAssetConflict(f"corrupt download sidecar: {path.name}")
        return _ResumeState(**values)

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
        first_error: Exception | None = None
        for path in (partial, self._resume_state_path(remote)):
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise _ResumeCleanupError(
                f"download cleanup failed: {remote.asset_id}"
            ) from first_error

    def _cleanup_success(self, remote: RemoteAsset, partial: Path) -> None:
        for path in (partial, self._resume_state_path(remote)):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                self.logger.warning("best-effort download cleanup failed")
