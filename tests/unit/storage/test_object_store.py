from collections.abc import Mapping
from pathlib import Path

import pytest

from flashflood_data.storage.object_store import ObjectConflict, ObjectPublisher


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
