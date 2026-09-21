from collections.abc import Mapping
from pathlib import Path

import pytest
from pyarrow import fs

from flashflood_data.storage.object_store import (
    ObjectConflict,
    ObjectPublisher,
    PyArrowS3ObjectStore,
)


class CopyFailure(RuntimeError):
    pass


class MemoryObjectStore:
    def __init__(self, initial: dict[str, bytes] | None = None) -> None:
        self.data = dict(initial or {})
        self.fail_copy_once = False

    def exists(self, key: str) -> bool:
        return key in self.data

    def size(self, key: str) -> int:
        return len(self.data[key])

    def sha256(self, key: str) -> str:
        from hashlib import sha256

        return sha256(self.data[key]).hexdigest()

    def upload(self, local_path: Path, key: str, metadata: Mapping[str, str]) -> None:
        assert metadata["sha256"]
        self.data[key] = local_path.read_bytes()

    def copy(self, source_key: str, destination_key: str) -> None:
        if self.fail_copy_once:
            self.fail_copy_once = False
            raise CopyFailure
        self.data[destination_key] = self.data[source_key]

    def delete(self, key: str) -> None:
        self.data.pop(key, None)

    def keys(self) -> set[str]:
        return set(self.data)

    def read(self, key: str) -> bytes:
        return self.data[key]


class TimeoutCopyFilesystem:
    def __init__(self) -> None:
        self.local = fs.LocalFileSystem()

    def copy_file(self, source_key: str, destination_key: str) -> None:
        raise OSError("CopyObject timeout")

    def open_input_stream(self, key: str):
        return self.local.open_input_stream(key)

    def open_output_stream(self, key: str):
        return self.local.open_output_stream(key)

    def delete_file(self, key: str) -> None:
        self.local.delete_file(key)


class NonTimeoutCopyFilesystem(TimeoutCopyFilesystem):
    def __init__(self) -> None:
        super().__init__()
        self.opened_stream = False

    def copy_file(self, source_key: str, destination_key: str) -> None:
        raise OSError("AccessDenied")

    def open_input_stream(self, key: str):
        self.opened_stream = True
        return super().open_input_stream(key)


class InterruptedStreamFilesystem(TimeoutCopyFilesystem):
    class _InterruptedOutput:
        def __init__(self, stream) -> None:
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, exc_type, exc, traceback) -> None:
            self.stream.__exit__(exc_type, exc, traceback)

        def write(self, data: bytes) -> int:
            self.stream.write(data[:3])
            raise OSError("stream interrupted")

    def open_output_stream(self, key: str):
        return self._InterruptedOutput(super().open_output_stream(key))


def test_pyarrow_store_streams_copy_when_server_side_copy_times_out(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"large-object-fixture")
    store = PyArrowS3ObjectStore(TimeoutCopyFilesystem())

    store.copy(str(source), str(destination))

    assert destination.read_bytes() == source.read_bytes()


def test_pyarrow_store_streams_download_to_local_file(tmp_path: Path) -> None:
    source = tmp_path / "object.bin"
    destination = tmp_path / "download.bin"
    source.write_bytes(b"raw-source-payload")

    PyArrowS3ObjectStore(fs.LocalFileSystem()).download(str(source), destination)

    assert destination.read_bytes() == b"raw-source-payload"


def test_pyarrow_store_does_not_stream_for_non_timeout_copy_errors(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"large-object-fixture")
    filesystem = NonTimeoutCopyFilesystem()

    with pytest.raises(OSError, match="AccessDenied"):
        PyArrowS3ObjectStore(filesystem).copy(str(source), str(destination))

    assert filesystem.opened_stream is False
    assert not destination.exists()


def test_pyarrow_store_removes_partial_destination_after_stream_failure(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"large-object-fixture")

    with pytest.raises(OSError, match="stream interrupted"):
        PyArrowS3ObjectStore(InterruptedStreamFilesystem()).copy(
            str(source), str(destination)
        )

    assert not destination.exists()


def test_publish_uses_run_staging_and_reuses_verified_final(tmp_path: Path) -> None:
    payload = tmp_path / "asset.bin"
    payload.write_bytes(b"verified-source")
    store = MemoryObjectStore()
    publisher = ObjectPublisher(store, bucket="raw")

    first = publisher.publish_file(
        payload,
        final_key="static/source/1/asset/asset.bin",
        run_id="run-1",
        media_type="application/octet-stream",
    )
    second = publisher.publish_file(
        payload,
        final_key="static/source/1/asset/asset.bin",
        run_id="run-2",
        media_type="application/octet-stream",
    )

    assert first.reused is False
    assert second.reused is True
    assert first.object_uri == "s3://raw/static/source/1/asset/asset.bin"
    assert store.keys() == {"raw/static/source/1/asset/asset.bin"}


def test_publish_accepts_private_prefix_segment(tmp_path: Path) -> None:
    payload = tmp_path / "fixture.bin"
    payload.write_bytes(b"smoke-fixture")
    store = MemoryObjectStore()

    result = ObjectPublisher(store, "raw").publish_file(
        payload,
        final_key="_smoke/run-1/fixture.bin",
        run_id="run-1",
        media_type="application/octet-stream",
    )

    assert result.object_uri == "s3://raw/_smoke/run-1/fixture.bin"
    assert store.keys() == {"raw/_smoke/run-1/fixture.bin"}


def test_publish_fails_closed_on_final_checksum_conflict(tmp_path: Path) -> None:
    store = MemoryObjectStore({"raw/static/source/1/asset.bin": b"old"})
    payload = tmp_path / "asset.bin"
    payload.write_bytes(b"new")

    with pytest.raises(ObjectConflict):
        ObjectPublisher(store, "raw").publish_file(
            payload,
            final_key="static/source/1/asset.bin",
            run_id="run-1",
            media_type="application/octet-stream",
        )

    assert store.read("raw/static/source/1/asset.bin") == b"old"


def test_retry_replaces_only_run_staging_after_copy_failure(tmp_path: Path) -> None:
    payload = tmp_path / "asset.bin"
    payload.write_bytes(b"verified-source")
    store = MemoryObjectStore()
    store.fail_copy_once = True
    publisher = ObjectPublisher(store, "raw")

    with pytest.raises(CopyFailure):
        publisher.publish_file(
            payload,
            final_key="static/source/1/asset.bin",
            run_id="run-1",
            media_type="application/octet-stream",
        )
    assert len(store.keys()) == 1
    assert next(iter(store.keys())).startswith("raw/_staging/run-1/")

    result = publisher.publish_file(
        payload,
        final_key="static/source/1/asset.bin",
        run_id="run-1",
        media_type="application/octet-stream",
    )
    assert result.reused is False
    assert store.keys() == {"raw/static/source/1/asset.bin"}


@pytest.mark.parametrize(
    "key",
    ["/absolute/file", "static//file", "static/../file", "static/file?token=x"],
)
def test_publish_rejects_unsafe_object_keys(tmp_path: Path, key: str) -> None:
    payload = tmp_path / "asset.bin"
    payload.write_bytes(b"fixture")

    with pytest.raises(ValueError):
        ObjectPublisher(MemoryObjectStore(), "raw").publish_file(
            payload,
            final_key=key,
            run_id="run-1",
            media_type="application/octet-stream",
        )
