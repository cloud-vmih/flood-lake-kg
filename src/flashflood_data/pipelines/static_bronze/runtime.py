"""Production dependencies for the Airflow Bronze parse pipeline."""

from pathlib import Path

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.pipelines.static_bronze.osm import load_osm_aoi, load_osm_selection
from flashflood_data.pipelines.static_bronze.pipeline import BronzeService
from flashflood_data.shared.metadata.service import MetaRecorder
from flashflood_data.shared.quality import QuarantineRepository, load_quality_policy
from flashflood_data.storage.iceberg import SourceObjectInventory, load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import PyArrowS3ObjectStore


def build_bronze_service(root: Path | None = None) -> BronzeService:
    """Compose one parse worker using the configured MinIO and Polaris runtime."""
    paths = ProjectPaths.discover(root)
    settings = LakehouseSettings(_env_file=paths.root / ".env", project_root=paths.root)
    catalog = load_polaris_catalog(settings)
    table_store = IcebergTableStore(catalog)
    quality_policy = load_quality_policy(paths.root / "config" / "quality" / "bronze_static.yaml")
    return BronzeService(
        inventory=SourceObjectInventory(catalog),
        object_store=PyArrowS3ObjectStore.from_settings(settings),
        writer=table_store,
        meta=MetaRecorder(table_store),
        quarantine=QuarantineRepository(table_store),
        quality_policy=quality_policy,
        raw_bucket=settings.raw_bucket,
        staging_root=settings.staging_root,
        catalog_name=settings.polaris_catalog,
        osm_selection=load_osm_selection(
            paths.root / "config" / "pipelines" / "static_bronze_osm.yaml"
        ),
        osm_aoi=load_osm_aoi(paths.harmonized / "aoi" / "exposure_aoi.geoparquet"),
    )
