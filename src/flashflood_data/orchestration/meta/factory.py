"""Production Meta recording dependencies shared by Airflow pipelines."""

from pathlib import Path

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.meta.service import MetaRecorder
from flashflood_data.storage.iceberg import load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore


def build_meta_recorder(root: Path | None = None) -> MetaRecorder:
    """Build a Meta writer against the configured Polaris catalog."""
    paths = ProjectPaths.discover(root)
    settings = LakehouseSettings(_env_file=paths.root / ".env", project_root=paths.root)
    return MetaRecorder(IcebergTableStore(load_polaris_catalog(settings)))
