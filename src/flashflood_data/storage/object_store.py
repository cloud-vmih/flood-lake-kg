"""Immutable, checksum-verified object publication."""

import re
import shutil
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Protocol
from urllib.parse import urlsplit

from pyarrow import fs

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.orchestration.landing.models import PublishedObject

_CHUNK_SIZE = 8 * 1024 * 1024
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._=@+-]*$")
_COPY_TIMEOUT_MARKERS = ("timeout", "timed out", "curlcode: 28")


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_copy_timeout(error: OSError) -> bool:
    return any(marker in str(error).lower() for marker in _COPY_TIMEOUT_MARKERS)


class ObjectConflict(RuntimeError):
    """Raised when an immutable final key contains different bytes."""


class ObjectVerificationError(RuntimeError):
    """Raised when a staged or final object fails size/checksum verification."""


class ObjectStore(Protocol):
    """Minimal object-storage operations required by the publisher."""

    def exists(self, key: str) -> bool: ...

    def size(self, key: str) -> int: ...

    def sha256(self, key: str) -> str: ...

    def read(self, key: str) -> bytes: ...

    def download(self, key: str, local_path: Path) -> None: ...

    def upload(self, local_path: Path, key: str, metadata: Mapping[str, str]) -> None: ...

    def copy(self, source_key: str, destination_key: str) -> None: ...

    def delete(self, key: str) -> None: ...


class PyArrowS3ObjectStore:
    """MinIO-compatible object store backed by PyArrow's S3 filesystem."""

    def __init__(self, filesystem: fs.S3FileSystem) -> None:
        self.filesystem = filesystem

    @classmethod
    def from_settings(cls, settings: LakehouseSettings) -> "PyArrowS3ObjectStore":
        endpoint = urlsplit(settings.minio_endpoint)
        return cls(
            fs.S3FileSystem(
                access_key=settings.minio_access_key.get_secret_value(),
                secret_key=settings.minio_secret_key.get_secret_value(),
                region=settings.minio_region,
                scheme=endpoint.scheme,
                endpoint_override=endpoint.netloc,
                force_virtual_addressing=False,
            )
        )

    def exists(self, key: str) -> bool:
        return self.filesystem.get_file_info(key).type is fs.FileType.File

    def size(self, key: str) -> int:
        info = self.filesystem.get_file_info(key)
        if info.type is not fs.FileType.File:
            raise FileNotFoundError(key)
        return info.size

    def sha256(self, key: str) -> str:
        digest = sha256()
        with self.filesystem.open_input_stream(key, compression=None) as stream:
            for chunk in iter(lambda: stream.read(_CHUNK_SIZE), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def read(self, key: str) -> bytes:
        with self.filesystem.open_input_stream(key, compression=None) as stream:
            return stream.read()

    def download(self, key: str, local_path: Path) -> None:
        """Stream an object into a caller-owned temporary local file."""
        with self.filesystem.open_input_stream(
            key, compression=None
        ) as source, local_path.open("wb") as destination:
            shutil.copyfileobj(source, destination, length=_CHUNK_SIZE)

    def upload(self, local_path: Path, key: str, metadata: Mapping[str, str]) -> None:
        with local_path.open("rb") as source, self.filesystem.open_output_stream(
            key, compression=None, metadata=dict(metadata)
        ) as destination:
            shutil.copyfileobj(source, destination, length=_CHUNK_SIZE)

    def copy(self, source_key: str, destination_key: str) -> None:
        self.filesystem.copy_file(source_key, destination_key)

    def delete(self, key: str) -> None:
        if self.exists(key):
            self.filesystem.delete_file(key)


def _validated_key(key: str) -> str:
    if not key or key.startswith("/") or "?" in key or "#" in key or "\\" in key:
        raise ValueError("object key is not a safe relative path")
    path = PurePosixPath(key)
    segments = key.split("/")
    if path.is_absolute() or any(
        not segment or segment in {".", ".."} or not _SAFE_SEGMENT.fullmatch(segment)
        for segment in segments
    ):
        raise ValueError("object key contains an unsafe segment")
    return key


def _safe_run_id(run_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._+-]+", "_", run_id).strip("._")
    if not safe:
        raise ValueError("run_id has no safe characters")
    return safe


def _assert_verified_match(
    store: ObjectStore, key: str, expected_size: int, expected_checksum: str
) -> None:
    if store.size(key) != expected_size or store.sha256(key) != expected_checksum:
        raise ObjectVerificationError(f"object verification failed: {key}")


class ObjectPublisher:
    """Publish one immutable file through a verified run-scoped staging key."""

    def __init__(self, store: ObjectStore, bucket: str) -> None:
        self.store = store
        self.bucket = _validated_key(bucket)

    def find_existing(self, final_key: str, media_type: str) -> PublishedObject | None:
        """Describe an existing immutable object without replacing its original bytes."""
        final_key = _validated_key(final_key)
        qualified_final = f"{self.bucket}/{final_key}"
        if not self.store.exists(qualified_final):
            return None
        return self._published(
            qualified_final,
            final_key,
            media_type,
            self.store.size(qualified_final),
            self.store.sha256(qualified_final),
            reused=True,
        )

    def read_existing(self, final_key: str) -> bytes:
        """Read a validated final key from the configured bucket."""
        final_key = _validated_key(final_key)
        qualified_final = f"{self.bucket}/{final_key}"
        if not self.store.exists(qualified_final):
            raise FileNotFoundError(final_key)
        return self.store.read(qualified_final)

    def publish_file(
        self,
        local_path: Path,
        *,
        final_key: str,
        run_id: str,
        media_type: str,
    ) -> PublishedObject:
        """Upload, verify, promote, and return immutable object metadata."""
        local_path = Path(local_path)
        if not local_path.is_file():
            raise FileNotFoundError(local_path)
        final_key = _validated_key(final_key)
        filename = _validated_key(local_path.name)
        checksum = _sha256_file(local_path)
        size_bytes = local_path.stat().st_size
        qualified_final = f"{self.bucket}/{final_key}"
        staging_key = (
            f"{self.bucket}/_staging/{_safe_run_id(run_id)}/{checksum}/{filename}"
        )

        if self.store.exists(qualified_final):
            try:
                _assert_verified_match(self.store, qualified_final, size_bytes, checksum)
            except ObjectVerificationError as exc:
                raise ObjectConflict(f"immutable object conflict: {final_key}") from exc
            return self._published(
                qualified_final, final_key, media_type, size_bytes, checksum, reused=True
            )

        self.store.upload(
            local_path,
            staging_key,
            {"sha256": checksum, "content-type": media_type},
        )
        _assert_verified_match(self.store, staging_key, size_bytes, checksum)
        try:
            self.store.copy(staging_key, qualified_final)
        except OSError as error:
            if not _is_copy_timeout(error):
                raise
            if self.store.exists(qualified_final):
                try:
                    _assert_verified_match(
                        self.store, qualified_final, size_bytes, checksum
                    )
                except ObjectVerificationError as exc:
                    raise ObjectConflict(
                        f"immutable object conflict after copy timeout: {final_key}"
                    ) from exc
            else:
                self.store.upload(
                    local_path,
                    qualified_final,
                    {"sha256": checksum, "content-type": media_type},
                )
        _assert_verified_match(self.store, qualified_final, size_bytes, checksum)
        self.store.delete(staging_key)
        return self._published(
            qualified_final, final_key, media_type, size_bytes, checksum, reused=False
        )

    @staticmethod
    def _published(
        qualified_key: str,
        final_key: str,
        media_type: str,
        size_bytes: int,
        checksum: str,
        *,
        reused: bool,
    ) -> PublishedObject:
        bucket = qualified_key.split("/", 1)[0]
        return PublishedObject(
            object_key=qualified_key,
            object_uri=f"s3://{bucket}/{final_key}",
            media_type=media_type,
            size_bytes=size_bytes,
            checksum=checksum,
            reused=reused,
        )
