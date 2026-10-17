"""Production dependency composition for the static Landing pipeline."""

from __future__ import annotations

import os
from pathlib import Path

from flashflood_data.catalog import AssetCatalog
from flashflood_data.core.config import EnvironmentSettings, load_study_area
from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.pipelines.static_landing.adapters.registry import load_source_specs
from flashflood_data.pipelines.static_landing.config import load_static_landing_config
from flashflood_data.pipelines.static_landing.pipeline import StaticSourceLandingService
from flashflood_data.shared.metadata.service import MetaRecorder
from flashflood_data.shared.quality import QuarantineRepository, load_quality_policy
from flashflood_data.shared.storage_budget import StorageBudget
from flashflood_data.storage.http import HttpFetcher
from flashflood_data.storage.iceberg import SourceObjectInventory, load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import ObjectPublisher, PyArrowS3ObjectStore


def build_static_landing_service(
    root: Path | None = None,
) -> StaticSourceLandingService:
    """Compose the production service from validated project configuration."""
    paths = ProjectPaths.discover(root)
    paths.ensure_output_dirs()
    study_area = load_study_area(paths.root / "config" / "study_area.yaml")
    environment = EnvironmentSettings(_env_file=paths.root / ".env")
    settings_values: dict[str, object] = {"project_root": paths.root}
    if "FLASHFLOOD_STAGING_ROOT" not in os.environ:
        settings_values["staging_root"] = paths.dataset / "lakehouse" / "staging"
    settings = LakehouseSettings(_env_file=paths.root / ".env", **settings_values)
    config = load_static_landing_config(paths.root / "config" / "pipelines" / "static_landing.yaml")
    source_specs = load_source_specs(paths.root / "config" / "sources")
    catalog = AssetCatalog(paths)
    fetcher = HttpFetcher(
        paths,
        catalog,
        StorageBudget.from_config(paths.dataset, study_area),
        environment=environment,
    )
    object_store = PyArrowS3ObjectStore.from_settings(settings)
    polaris_catalog = load_polaris_catalog(settings)
    table_store = IcebergTableStore(polaris_catalog)
    inventory = SourceObjectInventory(polaris_catalog)
    quality_policy = load_quality_policy(paths.root / "config" / "quality" / "raw_landing.yaml")
    return StaticSourceLandingService(
        config=config,
        publisher=ObjectPublisher(object_store, settings.raw_bucket),
        inventory=inventory,
        staging_root=settings.staging_root,
        source_specs=source_specs,
        paths=paths,
        catalog=catalog,
        fetcher=fetcher,
        study_area=study_area,
        environment=environment,
        meta=MetaRecorder(table_store),
        quarantine=QuarantineRepository(table_store),
        catalog_name=settings.polaris_catalog,
        quality_policy_id=quality_policy.policy_id,
    )
