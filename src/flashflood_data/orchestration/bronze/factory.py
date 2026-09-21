"""Production dependencies for the Airflow Bronze parse pipeline."""

from pathlib import Path

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.bronze.osm import load_osm_selection
from flashflood_data.orchestration.bronze.service import BronzeService
from flashflood_data.orchestration.meta.service import MetaRecorder
from flashflood_data.storage.iceberg import SourceObjectInventory, load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import PyArrowS3ObjectStore


def build_bronze_service(root: Path | None = None) -> BronzeService:
    """Compose one parse worker using the configured MinIO and Polaris runtime."""
    paths = ProjectPaths.discover(root)
    settings = LakehouseSettings(_env_file=paths.root / ".env", project_root=paths.root)
    catalog = load_polaris_catalog(settings)
    table_store = IcebergTableStore(catalog)
    return BronzeService(
        inventory=SourceObjectInventory(catalog),
        object_store=PyArrowS3ObjectStore.from_settings(settings),
        writer=table_store,
        meta=MetaRecorder(table_store),
        raw_bucket=settings.raw_bucket,
        staging_root=settings.staging_root,
        catalog_name=settings.polaris_catalog,
        osm_selection=load_osm_selection(paths.root / "config" / "bronze" / "osm.yaml"),
    )
