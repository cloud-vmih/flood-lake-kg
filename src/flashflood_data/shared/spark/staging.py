"""Immutable run-scoped staging manifests for specialized Silver compute jobs."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol
from urllib.parse import urlsplit

from flashflood_data.shared.spark.models import PinnedSnapshot, StagedDataset

_SEGMENT = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._+-]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class StagingConflict(RuntimeError):
    """An immutable batch ID already points to different staged content."""


class StagingStore(Protocol):
    def exists(self, key: str) -> bool: ...

    def sha256(self, key: str) -> str: ...

    def read(self, key: str) -> bytes: ...

    def upload(self, local_path: Path, key: str, metadata: dict[str, str]) -> None: ...

    def delete(self, key: str) -> None: ...


def _segment(value: str, name: str) -> str:
    if _SEGMENT.fullmatch(value) is None:
        raise ValueError(f"unsafe staging {name}: {value!r}")
    return value


def _uri_key(uri: str) -> str:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"staged dataset URI must use s3://bucket/key: {uri}")
    return f"{parsed.netloc}/{parsed.path.strip('/')}"


@dataclass(frozen=True)
class StagingManifest:
    pipeline: str
    pipeline_run_id: str
    batch_id: str
    build_signature: str
    input_snapshots: tuple[PinnedSnapshot, ...]
    datasets: tuple[StagedDataset, ...]

    def __post_init__(self) -> None:
        _segment(self.pipeline, "pipeline")
        _segment(self.pipeline_run_id, "pipeline_run_id")
        _segment(self.batch_id, "batch_id")
        if not self.build_signature:
            raise ValueError("staging build signature is required")
        if not self.input_snapshots or not self.datasets:
            raise ValueError("staging manifest requires inputs and datasets")
        uris = [dataset.uri for dataset in self.datasets]
        if len(set(uris)) != len(uris):
            raise ValueError("staging dataset URIs must be unique")
        for dataset in self.datasets:
            _uri_key(dataset.uri)
            if dataset.row_count < 0 or not dataset.schema_hash:
                raise ValueError("staged dataset schema hash and non-negative row count are required")
            if _SHA256.fullmatch(dataset.checksum) is None:
                raise ValueError("staged dataset checksum must be lowercase SHA-256")

    @property
    def relative_prefix(self) -> str:
        return f"silver/{self.pipeline}/{self.pipeline_run_id}/{self.batch_id}"

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, payload: str) -> StagingManifest:
        raw = json.loads(payload)
        if not isinstance(raw, dict):
            raise TypeError("staging manifest must be a JSON object")
        raw["input_snapshots"] = tuple(
            PinnedSnapshot(**item) for item in raw.get("input_snapshots", ())
        )
        raw["datasets"] = tuple(StagedDataset(**item) for item in raw.get("datasets", ()))
        return cls(**raw)


class SparkStaging:
    """Commit and reconcile run-scoped staging batches in one object-store bucket."""

    def __init__(self, store: StagingStore, bucket: str = "staging") -> None:
        self.store = store
        self.bucket = _segment(bucket, "bucket")

    def _prefix(self, pipeline: str, run_id: str, batch_id: str) -> str:
        return (
            f"{self.bucket}/silver/{_segment(pipeline, 'pipeline')}/"
            f"{_segment(run_id, 'pipeline_run_id')}/{_segment(batch_id, 'batch_id')}"
        )

    @staticmethod
    def _write_bytes(store: StagingStore, key: str, payload: bytes, media_type: str) -> None:
        with TemporaryDirectory(prefix="flashflood-stage-") as directory:
            path = Path(directory) / "payload"
            path.write_bytes(payload)
            store.upload(path, key, {"content-type": media_type})

    def commit(self, manifest: StagingManifest) -> bool:
        """Write manifest and success marker; return true only for an identical retry."""
        prefix = self._prefix(
            manifest.pipeline, manifest.pipeline_run_id, manifest.batch_id
        )
        manifest_key = f"{prefix}/manifest.json"
        success_key = f"{prefix}/_SUCCESS"
        expected_root = f"s3://{self.bucket}/{manifest.relative_prefix}/"
        for dataset in manifest.datasets:
            if not dataset.uri.startswith(expected_root):
                raise ValueError("staged dataset URI is outside its manifest batch")
            key = _uri_key(dataset.uri)
            if not self.store.exists(key):
                raise FileNotFoundError(key)
            if self.store.sha256(key) != dataset.checksum:
                raise StagingConflict(f"staged dataset checksum mismatch: {dataset.uri}")

        if self.store.exists(success_key):
            stored = self.load_completed(
                manifest.pipeline, manifest.pipeline_run_id, manifest.batch_id
            )
            if stored != manifest:
                raise StagingConflict(f"staging batch identity conflict: {prefix}")
            return True

        payload = manifest.to_json().encode("utf-8")
        if self.store.exists(manifest_key):
            current = self.store.read(manifest_key)
            if current != payload:
                raise StagingConflict(f"staging manifest conflict: {prefix}")
        else:
            self._write_bytes(self.store, manifest_key, payload, "application/json")
        self._write_bytes(self.store, success_key, b"", "application/octet-stream")
        return False

    def load_completed(
        self, pipeline: str, pipeline_run_id: str, batch_id: str
    ) -> StagingManifest | None:
        prefix = self._prefix(pipeline, pipeline_run_id, batch_id)
        manifest_key = f"{prefix}/manifest.json"
        if not self.store.exists(f"{prefix}/_SUCCESS") or not self.store.exists(manifest_key):
            return None
        return StagingManifest.from_json(self.store.read(manifest_key).decode("utf-8"))

    def cleanup(self, manifest: StagingManifest, *, finalized: bool) -> None:
        if not finalized:
            raise ValueError("staging cleanup requires a finalized successful run")
        prefix = self._prefix(
            manifest.pipeline, manifest.pipeline_run_id, manifest.batch_id
        )
        for dataset in manifest.datasets:
            self.store.delete(_uri_key(dataset.uri))
        self.store.delete(f"{prefix}/manifest.json")
        self.store.delete(f"{prefix}/_SUCCESS")
