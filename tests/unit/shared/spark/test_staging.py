from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import pytest

from flashflood_data.shared.spark.models import PinnedSnapshot, StagedDataset


class _Store:
    def __init__(self):
        self.objects = {}

    def exists(self, key):
        return key in self.objects

    def size(self, key):
        return len(self.objects[key])

    def sha256(self, key):
        return sha256(self.objects[key]).hexdigest()

    def read(self, key):
        return self.objects[key]

    def upload(self, local_path: Path, key, metadata):
        self.objects[key] = local_path.read_bytes()

    def delete(self, key):
        self.objects.pop(key, None)


def _manifest(checksum: str):
    from flashflood_data.shared.spark.staging import StagingManifest

    return StagingManifest(
        pipeline="silver_basin_topology",
        pipeline_run_id="run-1",
        batch_id="batch-1",
        build_signature="sig-1",
        input_snapshots=(PinnedSnapshot("bronze.basin_polygon_raw", 17),),
        datasets=(
            StagedDataset(
                uri="s3://staging/silver/silver_basin_topology/run-1/batch-1/data.parquet",
                schema_hash="schema-1",
                row_count=2,
                checksum=checksum,
            ),
        ),
    )


def test_staging_commits_success_last_and_reuses_identical_batch() -> None:
    from flashflood_data.shared.spark.staging import SparkStaging

    store = _Store()
    data = b"parquet"
    key = "staging/silver/silver_basin_topology/run-1/batch-1/data.parquet"
    store.objects[key] = data
    staging = SparkStaging(store)
    manifest = _manifest(sha256(data).hexdigest())

    assert staging.commit(manifest) is False
    assert staging.commit(manifest) is True
    assert staging.load_completed("silver_basin_topology", "run-1", "batch-1") == manifest


def test_staging_rejects_conflicting_retry_and_requires_finalize_for_cleanup() -> None:
    from flashflood_data.shared.spark.staging import SparkStaging, StagingConflict

    store = _Store()
    key = "staging/silver/silver_basin_topology/run-1/batch-1/data.parquet"
    store.objects[key] = b"first"
    staging = SparkStaging(store)
    first = _manifest(sha256(b"first").hexdigest())
    staging.commit(first)

    with pytest.raises(StagingConflict):
        staging.commit(_manifest("0" * 64))
    with pytest.raises(ValueError, match="finalized"):
        staging.cleanup(first, finalized=False)

    staging.cleanup(first, finalized=True)
    assert staging.load_completed("silver_basin_topology", "run-1", "batch-1") is None
