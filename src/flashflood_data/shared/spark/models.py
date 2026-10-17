"""Serializable contracts used across Airflow and Spark process boundaries."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

_SUPPORTED_MATRIX = ("4.1.3", "2.13", "17", "1.11.0", "1.9.1", "1.9.1-33.5")
_MEMORY = re.compile(r"^[1-9][0-9]*[mg]$", re.IGNORECASE)


@dataclass(frozen=True)
class SparkRuntimeConfig:
    spark_version: str
    scala_version: str
    java_version: str
    iceberg_version: str
    sedona_version: str
    geotools_version: str
    driver_memory: str
    executor_memory: str
    executor_cores: int
    shuffle_partitions: int
    catalog_name: str
    warehouse: str
    endpoint_env: Mapping[str, str]
    application_timeout_seconds: int

    def __post_init__(self) -> None:
        matrix = (
            self.spark_version,
            self.scala_version,
            self.java_version,
            self.iceberg_version,
            self.sedona_version,
            self.geotools_version,
        )
        if matrix != _SUPPORTED_MATRIX:
            raise ValueError(f"unsupported Spark runtime matrix: {matrix!r}")
        if not _MEMORY.fullmatch(self.driver_memory) or not _MEMORY.fullmatch(
            self.executor_memory
        ):
            raise ValueError("Spark memory must use a positive m/g quantity")
        if self.executor_cores < 1 or self.shuffle_partitions < 1:
            raise ValueError("Spark cores and shuffle partitions must be positive")
        if self.application_timeout_seconds < 1:
            raise ValueError("Spark application timeout must be positive")
        if set(self.endpoint_env) != {"catalog_uri", "minio_endpoint"}:
            raise ValueError("Spark endpoint_env must name catalog_uri and minio_endpoint")
        if any(not str(value).isidentifier() for value in self.endpoint_env.values()):
            raise ValueError("Spark endpoint references must be environment variable names")


@dataclass(frozen=True, order=True)
class PinnedSnapshot:
    table: str
    snapshot_id: int

    def __post_init__(self) -> None:
        if self.table.count(".") != 1 or self.snapshot_id < 1:
            raise ValueError("pinned snapshot requires namespace.table and a positive snapshot ID")


@dataclass(frozen=True)
class BuildSignature:
    value: str

    @classmethod
    def create(
        cls,
        *,
        snapshots: Sequence[PinnedSnapshot],
        config_hash: str,
        policy_id: str,
        algorithm_version: str,
        contract_version: str,
    ) -> BuildSignature:
        payload = {
            "snapshots": [asdict(item) for item in sorted(snapshots)],
            "config_hash": config_hash,
            "policy_id": policy_id,
            "algorithm_version": algorithm_version,
            "contract_version": contract_version,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return cls(hashlib.sha256(canonical.encode("utf-8")).hexdigest())


@dataclass(frozen=True)
class StagedDataset:
    uri: str
    schema_hash: str
    row_count: int
    checksum: str


@dataclass(frozen=True)
class PublishedSnapshot:
    table: str
    snapshot_id: int
    row_count: int
    file_count: int


@dataclass(frozen=True)
class SparkApplicationRequest:
    application_id: str
    pipeline_run_id: str
    pipeline: str
    entrypoint: str
    snapshots: tuple[PinnedSnapshot, ...]
    build_signature: str
    output_manifest_uri: str
    arguments: tuple[str, ...] = ()

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class SparkApplicationResult:
    application_id: str
    status: Literal["succeeded", "failed", "timed_out"]
    output_manifest_uri: str | None
    published_snapshots: tuple[PublishedSnapshot, ...] = ()
    error_code: str | None = None


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"Spark config {name} must be a mapping")
    return value


def load_spark_runtime_config(path: Path) -> SparkRuntimeConfig:
    """Load the local compatibility matrix without reading runtime secrets."""
    import yaml

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    root = _mapping(raw, "root")
    runtime = _mapping(root.get("runtime"), "runtime")
    resources = _mapping(root.get("resources"), "resources")
    catalog = _mapping(root.get("catalog"), "catalog")
    endpoint_env = _mapping(catalog.get("endpoint_env"), "catalog.endpoint_env")
    return SparkRuntimeConfig(
        spark_version=str(runtime.get("spark", "")),
        scala_version=str(runtime.get("scala", "")),
        java_version=str(runtime.get("java", "")),
        iceberg_version=str(runtime.get("iceberg", "")),
        sedona_version=str(runtime.get("sedona", "")),
        geotools_version=str(runtime.get("geotools", "")),
        driver_memory=str(resources.get("driver_memory", "")),
        executor_memory=str(resources.get("executor_memory", "")),
        executor_cores=int(resources.get("executor_cores", 0)),
        shuffle_partitions=int(resources.get("shuffle_partitions", 0)),
        catalog_name=str(catalog.get("name", "")),
        warehouse=str(catalog.get("warehouse", "")),
        endpoint_env={str(key): str(value) for key, value in endpoint_env.items()},
        application_timeout_seconds=int(root.get("application_timeout_seconds", 0)),
    )
