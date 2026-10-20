"""Validated configuration for the Silver basin topology pipeline."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

_EXPECTED_FIELD_MAP = {
    "basin_id": "HYBAS_ID",
    "next_down_id": "NEXT_DOWN",
    "main_basin_id": "MAIN_BAS",
    "pfaf_id": "PFAF_ID",
    "topology_sort_hint": "SORT",
    "hydro_order": "ORDER",
    "is_coastal": "COAST",
    "is_endorheic": "ENDO",
    "basin_area_km2": "SUB_AREA",
    "upstream_area_km2": "UP_AREA",
    "distance_to_main_sink_km": "DIST_MAIN",
    "next_sink_id": "NEXT_SINK",
    "distance_to_sink_km": "DIST_SINK",
}


class BasinOutputTables(BaseModel):
    """Physical Silver tables produced by one topology build."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    basin: str
    edge: str


class BasinTopologyConfig(BaseModel):
    """Source authority, versioning, and field allowlist for L12 basins."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    pipeline_id: Literal["silver_basin_topology"]
    contract_version: str
    input_table: Literal["bronze.basin_polygon_raw"]
    hydro_source_id: str
    atlas_source_id: str
    hydrobasins_level: Literal[12]
    aoi_path: str
    authoritative_geometry_source: str
    authoritative_topology_source: str
    output_tables: BasinOutputTables
    target_crs: Literal["EPSG:4326"]
    transform_version: str
    topology_version: str
    revision_policy_version: str
    quality_policy_path: str
    field_map: dict[str, str]
    edge_statuses: tuple[Literal["internal", "exits_AOI", "terminal"], ...]

    @field_validator(
        "contract_version",
        "hydro_source_id",
        "atlas_source_id",
        "authoritative_geometry_source",
        "authoritative_topology_source",
        "transform_version",
        "topology_version",
        "revision_policy_version",
        "quality_policy_path",
        "aoi_path",
    )
    @classmethod
    def require_nonempty_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("basin topology config strings must not be empty")
        return normalized

    @field_validator("field_map")
    @classmethod
    def require_exact_field_allowlist(cls, value: dict[str, str]) -> dict[str, str]:
        if value != _EXPECTED_FIELD_MAP:
            raise ValueError("basin topology field_map must match the approved L12 allowlist")
        return value

    @field_validator("edge_statuses")
    @classmethod
    def require_each_edge_status_once(
        cls,
        value: tuple[Literal["internal", "exits_AOI", "terminal"], ...],
    ) -> tuple[Literal["internal", "exits_AOI", "terminal"], ...]:
        if len(value) != 3 or set(value) != {"internal", "exits_AOI", "terminal"}:
            raise ValueError("edge_statuses must contain each supported status exactly once")
        return value

    @model_validator(mode="after")
    def validate_source_authority_and_targets(self) -> BasinTopologyConfig:
        if self.hydro_source_id == self.atlas_source_id:
            raise ValueError("HydroBASINS and BasinATLAS source IDs must be different")
        if self.authoritative_geometry_source != self.hydro_source_id:
            raise ValueError("HydroBASINS must be the authoritative geometry source")
        if self.authoritative_topology_source != self.hydro_source_id:
            raise ValueError("HydroBASINS must be the authoritative topology source")
        if self.output_tables.basin != "silver.dim_basin":
            raise ValueError("basin output must be silver.dim_basin")
        if self.output_tables.edge != "silver.basin_edge":
            raise ValueError("edge output must be silver.basin_edge")
        policy_path = PurePosixPath(self.quality_policy_path)
        if policy_path.is_absolute() or ".." in policy_path.parts or policy_path.suffix != ".yaml":
            raise ValueError("quality_policy_path must be a repository-relative YAML path")
        aoi_path = PurePosixPath(self.aoi_path)
        if (
            aoi_path.is_absolute()
            or ".." in aoi_path.parts
            or aoi_path.suffix != ".geoparquet"
        ):
            raise ValueError("aoi_path must be a repository-relative GeoParquet path")
        return self


def load_basin_topology_config(path: Path) -> BasinTopologyConfig:
    """Read one credential-free YAML document and validate the complete contract."""
    document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise TypeError("basin topology config must be a mapping")
    return BasinTopologyConfig.model_validate(document)
