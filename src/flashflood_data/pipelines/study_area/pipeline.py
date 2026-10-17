"""Standalone, restart-safe construction of the five canonical L12 AOIs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import geopandas as gpd

from flashflood_data.catalog import AssetCatalog
from flashflood_data.catalog.models import RunRecord
from flashflood_data.core.config import EnvironmentSettings, load_study_area
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.pipelines.static_landing.acquisition import acquire_validated_assets
from flashflood_data.pipelines.static_landing.adapters.base import SourceContext
from flashflood_data.pipelines.static_landing.adapters.registry import (
    build_adapter,
    load_source_specs,
)
from flashflood_data.pipelines.static_landing.config import load_static_landing_config
from flashflood_data.pipelines.study_area.aoi import build_study_areas, write_study_areas
from flashflood_data.pipelines.study_area.hydro import select_basins_with_upstream
from flashflood_data.shared.storage_budget import StorageBudget
from flashflood_data.storage.http import HttpFetcher

_ADMIN_SOURCES = ("sonla_admin_2025", "gadm_vnm_4_1")


@dataclass(frozen=True)
class StudyAreaBuildResult:
    """JSON-safe outcome of one AOI build."""

    run_id: str
    status: str
    hydrobasins_level: int
    selected_basins: int
    selected_vietnam_basins: int
    outputs: tuple[str, ...]

    def to_document(self) -> dict[str, object]:
        return asdict(self)


class StudyAreaPipeline:
    """Bootstrap required administration and build canonical study areas."""

    def __init__(
        self,
        paths: ProjectPaths,
        catalog: AssetCatalog,
        fetcher: HttpFetcher,
    ) -> None:
        self.paths = paths
        self.catalog = catalog
        self.fetcher = fetcher
        self.study_area = load_study_area(paths.root / "config" / "study_area.yaml")
        self.environment = EnvironmentSettings(_env_file=paths.root / ".env")

    def _context(self, run_id: str) -> SourceContext:
        return SourceContext(
            paths=self.paths,
            catalog=self.catalog,
            study_area=self.study_area,
            environment=self.environment,
            run_id=run_id,
        )

    def _ensure_admin_inputs(self, run_id: str) -> None:
        required = (
            self.paths.harmonized / "aoi" / "core_aoi.geoparquet",
            self.paths.harmonized / "admin" / "vietnam_boundary.geoparquet",
        )
        if all(path.is_file() for path in required):
            return
        source_specs = load_source_specs(self.paths.root / "config" / "sources")
        landing_config = load_static_landing_config(
            self.paths.root / "config" / "pipelines" / "static_landing.yaml"
        )
        context = self._context(run_id)
        for source_id in _ADMIN_SOURCES:
            policy = landing_config.source(source_id)
            base_spec = source_specs[source_id]
            records = acquire_validated_assets(
                policy, base_spec, context, self.fetcher
            )
            spec = base_spec.model_copy(
                update={
                    "settings": dict(base_spec.settings)
                    | dict(policy.settings_override)
                }
            )
            build_adapter(spec).harmonize(context, list(records))

    def run(self, run_id: str | None = None) -> StudyAreaBuildResult:
        run_id = run_id or f"build-aoi-{uuid4().hex}"
        self.catalog.begin_run(
            RunRecord(
                run_id=run_id,
                command="build-aoi",
                started_at=datetime.now(UTC),
                config_fingerprint=self.study_area.model_dump_json(),
            )
        )
        try:
            self._ensure_admin_inputs(run_id)
            core_path = self.paths.harmonized / "aoi" / "core_aoi.geoparquet"
            vietnam_path = (
                self.paths.harmonized / "admin" / "vietnam_boundary.geoparquet"
            )
            core_layer = gpd.read_parquet(core_path)
            vietnam_layer = gpd.read_parquet(vietnam_path).to_crs(core_layer.crs)
            if core_layer.empty or vietnam_layer.empty or core_layer.crs is None:
                raise ValueError("administrative AOI inputs are empty or missing a CRS")
            core = core_layer.geometry.union_all()
            vietnam = vietnam_layer.geometry.union_all()
            level = self.study_area.hydrobasins_level
            basin_path = (
                self.paths.dataset
                / "hybas_as_lev01-12_v1c"
                / f"hybas_as_lev{level:02d}_v1c.shp"
            )
            basins = gpd.read_file(basin_path)
            selected = select_basins_with_upstream(
                basins, core, hops=self.study_area.upstream_hops
            )
            vietnam_selected = select_basins_with_upstream(
                basins, vietnam, hops=self.study_area.upstream_hops
            )
            areas = build_study_areas(
                core, selected, vietnam, vietnam_selected, self.study_area
            )
            outputs = write_study_areas(
                areas,
                self.paths.harmonized / "aoi",
                self.study_area.storage_crs,
                include_core=False,
            )
        except Exception:
            self.catalog.end_run(run_id, "failed", datetime.now(UTC))
            raise
        self.catalog.end_run(run_id, "succeeded", datetime.now(UTC))
        return StudyAreaBuildResult(
            run_id=run_id,
            status="completed",
            hydrobasins_level=level,
            selected_basins=len(selected),
            selected_vietnam_basins=len(vietnam_selected),
            outputs=tuple(str(path) for path in (core_path, *outputs)),
        )


def build_study_area_pipeline(root: Path | None = None) -> StudyAreaPipeline:
    """Compose the AOI pipeline from repository configuration."""
    paths = ProjectPaths.discover(root)
    paths.ensure_output_dirs()
    study_area = load_study_area(paths.root / "config" / "study_area.yaml")
    environment = EnvironmentSettings(_env_file=paths.root / ".env")
    catalog = AssetCatalog(paths)
    fetcher = HttpFetcher(
        paths,
        catalog,
        StorageBudget.from_config(paths.dataset, study_area),
        environment=environment,
    )
    return StudyAreaPipeline(paths, catalog, fetcher)
