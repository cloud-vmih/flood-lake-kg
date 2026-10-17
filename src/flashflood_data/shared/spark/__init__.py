"""Stable, lazily imported public API for Spark/Sedona Silver applications."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_PUBLIC = {
    "BuildRecord": ("snapshots", "BuildRecord"),
    "BuildSignature": ("models", "BuildSignature"),
    "IncrementalBuildPlan": ("snapshots", "IncrementalBuildPlan"),
    "PinnedSnapshot": ("models", "PinnedSnapshot"),
    "PublishMode": ("publish", "PublishMode"),
    "PublishedSnapshot": ("models", "PublishedSnapshot"),
    "RESULT_PREFIX": ("submit", "RESULT_PREFIX"),
    "SparkApplicationRequest": ("models", "SparkApplicationRequest"),
    "SparkApplicationResult": ("models", "SparkApplicationResult"),
    "SparkAudit": ("audit", "SparkAudit"),
    "SparkAuditRequest": ("audit", "SparkAuditRequest"),
    "SparkDependencies": ("runtime", "SparkDependencies"),
    "SparkMetaWriter": ("audit", "SparkMetaWriter"),
    "SparkQualityOutcome": ("quality", "SparkQualityOutcome"),
    "SparkQualityPlan": ("quality", "SparkQualityPlan"),
    "SparkQualityRule": ("quality", "SparkQualityRule"),
    "SparkRuntimeConfig": ("models", "SparkRuntimeConfig"),
    "SparkSnapshotManager": ("snapshots", "SparkSnapshotManager"),
    "SparkStaging": ("staging", "SparkStaging"),
    "SparkSubmitter": ("submit", "SparkSubmitter"),
    "SparkTablePublisher": ("publish", "SparkTablePublisher"),
    "StagedDataset": ("models", "StagedDataset"),
    "StagingConflict": ("staging", "StagingConflict"),
    "StagingManifest": ("staging", "StagingManifest"),
    "StagingStore": ("staging", "StagingStore"),
    "apply_quality_plan": ("quality", "apply_quality_plan"),
    "build_quarantine_frame": ("quality", "build_quarantine_frame"),
    "build_spark_dependencies": ("runtime", "build_spark_dependencies"),
    "build_spark_session": ("session", "build_spark_session"),
    "catalog_options": ("session", "catalog_options"),
    "compile_quality_plan": ("quality", "compile_quality_plan"),
    "load_spark_runtime_config": ("models", "load_spark_runtime_config"),
    "merge_sql": ("publish", "merge_sql"),
    "plan_incremental_build": ("snapshots", "plan_incremental_build"),
    "quality_rows": ("quality", "quality_rows"),
    "quarantine_projection": ("quality", "quarantine_projection"),
    "rejection_id": ("quality", "rejection_id"),
    "session_options": ("session", "session_options"),
}

__all__ = tuple(sorted(_PUBLIC))


def __getattr__(name: str) -> Any:
    target = _PUBLIC.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attribute = target
    value = getattr(import_module(f"{__name__}.{module_name}"), attribute)
    globals()[name] = value
    return value
