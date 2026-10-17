"""Dependency graph shared by Spark Silver domain applications."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from flashflood_data.shared.spark.audit import SparkAudit, SparkMetaWriter
from flashflood_data.shared.spark.publish import SparkTablePublisher
from flashflood_data.shared.spark.snapshots import SparkSnapshotManager
from flashflood_data.shared.spark.staging import SparkStaging, StagingStore


@dataclass(frozen=True)
class SparkDependencies:
    """One composed dependency graph; domain code adds only its transform and rules."""

    spark: Any
    catalog_name: str
    snapshots: SparkSnapshotManager
    staging: SparkStaging
    audit: SparkAudit

    def publisher(self, table: str) -> SparkTablePublisher:
        parts = table.split(".")
        qualified = table if len(parts) == 3 else f"{self.catalog_name}.{table}"
        return SparkTablePublisher(self.spark, qualified)


def build_spark_dependencies(
    *,
    spark: Any,
    object_store: StagingStore,
    meta: SparkMetaWriter,
    catalog_name: str,
    staging_bucket: str = "staging",
) -> SparkDependencies:
    snapshots = SparkSnapshotManager(catalog_name)
    return SparkDependencies(
        spark=spark,
        catalog_name=catalog_name,
        snapshots=snapshots,
        staging=SparkStaging(object_store, bucket=staging_bucket),
        audit=SparkAudit(meta),
    )
