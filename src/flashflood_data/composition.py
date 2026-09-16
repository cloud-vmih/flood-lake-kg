"""Runtime discovery and default registration for the cross-source final stages."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from flashflood_data.catalog import sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.qa.checks import task17_qa_handler
from flashflood_data.qa.map import QA_MAP_BUNDLE_RELATIVE_PATHS
from flashflood_data.static.features.builder import (
    StaticPredictorInputs,
    task15_derive_handler,
)
from flashflood_data.static.features.config import load_feature_config
from flashflood_data.static.mappings.builder import Task16MapInputs, task16_map_handler

_PREFERRED_OWNER = "worldpop_vnm_2025"
_COMPOSITION_VERSION = "task19-v2"
_TASK15_OUTPUTS = {
    f"task15-{name}-features": f"{name}_features.parquet"
    for name in ("terrain", "soil", "landcover", "hydrology")
}
_TASK16_OUTPUTS = {
    **{
        f"task16-map-subbasin-{name}": f"map_subbasin_{name}.parquet"
        for name in ("commune", "river", "road", "bridge", "facility", "settlement", "population")
    },
    "task16-subbasin-static-feature": "subbasin_static_feature.geoparquet",
}
_QA_RELATIVE_OUTPUTS = (
    Path("report.html"),
    Path("report.json"),
    Path("report.parquet"),
    *QA_MAP_BUNDLE_RELATIVE_PATHS,
)


def _qa_asset_id(relative: Path) -> str:
    suffix = "-".join((*relative.parent.parts, relative.stem, relative.suffix[1:]))
    return f"task17-qa-{suffix}"


_QA_OUTPUTS = {_qa_asset_id(relative): relative for relative in _QA_RELATIVE_OUTPUTS}


def _owner_source_id(pipeline: Any) -> str | None:
    """Choose one deterministic source to own cross-source output records."""
    if _PREFERRED_OWNER in pipeline.source_specs:
        return _PREFERRED_OWNER
    return min(pipeline.source_specs, default=None)


def _required(path: Path) -> Path:
    if not path.is_file():
        raise ValueError(f"default static composition requires {path}")
    return path


def _first_required(paths: tuple[Path, ...]) -> Path:
    try:
        return next(path for path in paths if path.is_file())
    except StopIteration as error:
        raise ValueError(
            "default static composition requires one of: "
            + ", ".join(str(path) for path in paths)
        ) from error


def _raw_records(
    pipeline: Any,
    source_id: str,
    *,
    suffixes: tuple[str, ...] = (),
    filename_contains: str | None = None,
) -> tuple[AssetRecord, ...]:
    records = []
    for asset in pipeline.catalog._read_assets():
        path = Path(asset.storage_path)
        if (
            asset.source_id != source_id
            or asset.kind is not AssetKind.RAW
            or asset.status is not AssetStatus.VALIDATED
            or not path.is_file()
            or (suffixes and path.suffix.lower() not in suffixes)
            or (filename_contains is not None and filename_contains not in path.stem)
        ):
            continue
        records.append(asset)
    if not records:
        raise ValueError(f"default static composition has no validated raw {source_id} assets")
    return tuple(sorted(records, key=lambda asset: asset.asset_id))


def _raw_asset_groups(pipeline: Any) -> dict[str, tuple[str, ...]]:
    """Resolve the ultimate raw IDs consumed by each Task 15/16 product group."""
    basins = _raw_records(pipeline, "hydrobasins_v1c", filename_contains="lev10")
    atlas = _raw_records(pipeline, "basinatlas_v10", filename_contains="lev10")
    rivers = _raw_records(pipeline, "hydrorivers_v10")
    admin = _raw_records(pipeline, "sonla_admin_2025")
    dem = _raw_records(pipeline, "cop_dem_glo30_2024_1")
    soil = _raw_records(pipeline, "soilgrids_2_0", suffixes=(".tif", ".tiff"))
    worldcover = _raw_records(
        pipeline, "esa_worldcover_2021_v200", suffixes=(".tif", ".tiff")
    )
    osm = _raw_records(pipeline, "geofabrik_vietnam_snapshot", suffixes=(".pbf",))
    worldpop = _raw_records(pipeline, "worldpop_vnm_2025", suffixes=(".tif", ".tiff"))

    def ids(*groups: tuple[AssetRecord, ...]) -> tuple[str, ...]:
        return tuple(sorted({asset.asset_id for group in groups for asset in group}))

    return {
        "terrain": ids(basins, dem),
        "soil": ids(basins, soil),
        "landcover": ids(basins, worldcover),
        "hydrology": ids(basins, atlas, dem, rivers),
        "commune": ids(admin, basins),
        "river": ids(basins, rivers),
        "road": ids(basins, osm),
        "bridge": ids(basins, osm),
        "facility": ids(basins, osm),
        "settlement": ids(basins, osm),
        "population": ids(basins, worldpop),
    }


def _task15_dependency_paths(pipeline: Any) -> tuple[Path, ...]:
    paths = pipeline.paths
    semantics = load_feature_config()
    return (
        _required(paths.harmonized / "rasters" / "dem_glo30.tif"),
        _required(paths.harmonized / "rasters" / "worldcover_2021.tif"),
        _required(paths.harmonized / "hydro" / "river_reach.geoparquet"),
        _required(paths.harmonized / "hydro" / "subbasin_l10.geoparquet"),
        _required(paths.harmonized / "hydro" / "basinatlas_l10.geoparquet"),
        *(
            _required(
                paths.harmonized / "soilgrids" / property_id / depth / f"{statistic}.tif"
            )
            for property_id in semantics.soil_properties
            for depth in semantics.soil_depths
            for statistic in semantics.soil_statistics
        ),
    )


def _task16_dependency_paths(pipeline: Any) -> tuple[Path, ...]:
    paths = pipeline.paths
    exposure = paths.harmonized / "exposure"
    derived_exposure = paths.derived / "exposure"
    return (
        _required(paths.harmonized / "hydro" / "subbasin_l10.geoparquet"),
        _required(paths.harmonized / "aoi" / "core_aoi.geoparquet"),
        _required(paths.harmonized / "rasters" / "worldpop_2025.tif"),
        _required(paths.harmonized / "admin" / "admin_commune_2025.geoparquet"),
        _required(paths.harmonized / "hydro" / "river_reach.geoparquet"),
        *(_required(paths.derived / f"{name}_features.parquet") for name in ("terrain", "soil", "landcover", "hydrology")),
        _first_required(
            (
                derived_exposure / "road_segment.geoparquet",
                exposure / "road_segment.geoparquet",
            )
        ),
        _first_required((derived_exposure / "bridge.geoparquet", exposure / "bridge.geoparquet")),
        _first_required(
            (derived_exposure / "facility.geoparquet", exposure / "facility.geoparquet")
        ),
        _first_required(
            (derived_exposure / "settlement.geoparquet", exposure / "settlement.geoparquet")
        ),
    )


def _stage_fingerprint(
    pipeline: Any,
    stage: str,
    dependency_paths: tuple[Path, ...],
    source_asset_ids: Mapping[str, tuple[str, ...]],
    *,
    extra_config: Mapping[str, object] | None = None,
) -> str:
    from flashflood_data.pipeline import dependency_fingerprint

    by_id = {asset.asset_id: asset for asset in pipeline.catalog._read_assets()}
    raw_ids = sorted({asset_id for values in source_asset_ids.values() for asset_id in values})
    missing = [asset_id for asset_id in raw_ids if asset_id not in by_id]
    if missing:
        raise ValueError(f"default composition raw catalog references are missing: {missing}")
    owner = _owner_source_id(pipeline)
    checksums = [sha256_file(path) for path in dependency_paths]
    checksums.extend(by_id[asset_id].checksum for asset_id in raw_ids)
    config: dict[str, object] = {
        "composition_version": _COMPOSITION_VERSION,
        "owner": pipeline.source_specs[owner].model_dump(mode="json") if owner else None,
        "raw_assets": [
            {
                "asset_id": asset_id,
                "checksum": by_id[asset_id].checksum,
                "license_id": by_id[asset_id].license_id,
                "retrieved_at": by_id[asset_id].retrieved_at.isoformat(),
                "source_id": by_id[asset_id].source_id,
                "source_uri": by_id[asset_id].source_uri,
                "source_version": by_id[asset_id].source_version,
                "status": by_id[asset_id].status.value,
            }
            for asset_id in raw_ids
        ],
        "raw_asset_groups": source_asset_ids,
        "stage": stage,
        "study_area": pipeline.study_area.model_dump(mode="json"),
    }
    config.update(extra_config or {})
    return dependency_fingerprint(checksums, config, _COMPOSITION_VERSION)


def _outputs_reusable(
    pipeline: Any, expected: Mapping[str, Path], fingerprint: str
) -> bool:
    by_id = {asset.asset_id: asset for asset in pipeline.catalog._read_assets()}
    for asset_id, path in expected.items():
        asset = by_id.get(asset_id)
        if (
            asset is None
            or asset.status is not AssetStatus.DERIVED
            or asset.dependency_fingerprint != fingerprint
            or Path(asset.storage_path) != path
            or not path.is_file()
            or asset.checksum != sha256_file(path)
        ):
            return False
    return True


def _with_fingerprint(
    records: list[AssetRecord], fingerprint: str
) -> list[AssetRecord]:
    return [record.model_copy(update={"dependency_fingerprint": fingerprint}) for record in records]


def _task15_inputs(pipeline: Any) -> StaticPredictorInputs:
    paths = pipeline.paths
    semantics = load_feature_config()
    soil_paths = {
        (property_id, depth, statistic): _required(
            paths.harmonized / "soilgrids" / property_id / depth / f"{statistic}.tif"
        )
        for property_id in semantics.soil_properties
        for depth in semantics.soil_depths
        for statistic in semantics.soil_statistics
    }
    return StaticPredictorInputs(
        dem_path=_required(paths.harmonized / "rasters" / "dem_glo30.tif"),
        soil_raster_paths=soil_paths,
        worldcover_path=_required(paths.harmonized / "rasters" / "worldcover_2021.tif"),
        rivers=gpd.read_parquet(
            _required(paths.harmonized / "hydro" / "river_reach.geoparquet")
        ),
        basins=gpd.read_parquet(
            _required(paths.harmonized / "hydro" / "subbasin_l10.geoparquet")
        ),
        basinatlas=gpd.read_parquet(
            _required(paths.harmonized / "hydro" / "basinatlas_l10.geoparquet")
        ),
        processing_crs=pipeline.study_area.processing_crs,
        source_asset_ids=_raw_asset_groups(pipeline),
    )


def _task16_inputs(pipeline: Any) -> Task16MapInputs:
    paths = pipeline.paths
    core_layer = gpd.read_parquet(
        _required(paths.harmonized / "aoi" / "core_aoi.geoparquet")
    )
    feature_tables: dict[str, pd.DataFrame] = {
        name: pd.read_parquet(_required(paths.derived / f"{name}_features.parquet"))
        for name in ("terrain", "soil", "landcover", "hydrology")
    }
    exposure = paths.harmonized / "exposure"
    derived_exposure = paths.derived / "exposure"
    return Task16MapInputs(
        basins=gpd.read_parquet(
            _required(paths.harmonized / "hydro" / "subbasin_l10.geoparquet")
        ),
        core=core_layer.geometry.union_all(),
        worldpop=_required(paths.harmonized / "rasters" / "worldpop_2025.tif"),
        feature_tables=feature_tables,
        communes=gpd.read_parquet(
            _required(paths.harmonized / "admin" / "admin_commune_2025.geoparquet")
        ),
        rivers=gpd.read_parquet(
            _required(paths.harmonized / "hydro" / "river_reach.geoparquet")
        ),
        roads=gpd.read_parquet(
            _first_required(
                (
                    derived_exposure / "road_segment.geoparquet",
                    exposure / "road_segment.geoparquet",
                )
            )
        ),
        bridges=gpd.read_parquet(
            _first_required(
                (derived_exposure / "bridge.geoparquet", exposure / "bridge.geoparquet")
            )
        ),
        facilities=gpd.read_parquet(
            _first_required(
                (derived_exposure / "facility.geoparquet", exposure / "facility.geoparquet")
            )
        ),
        settlements=gpd.read_parquet(
            _first_required(
                (derived_exposure / "settlement.geoparquet", exposure / "settlement.geoparquet")
            )
        ),
        source_asset_ids=_raw_asset_groups(pipeline),
    )


def _derive_handler(
    pipeline: Any, stage: object, source_id: str, context: Any
) -> list[AssetRecord]:
    owner = _owner_source_id(pipeline)
    if owner is None or source_id != owner:
        return []
    groups = _raw_asset_groups(pipeline)
    fingerprint = _stage_fingerprint(
        pipeline,
        "derive",
        _task15_dependency_paths(pipeline),
        groups,
        extra_config={"feature_config": asdict(load_feature_config())},
    )
    expected = {
        asset_id: pipeline.paths.derived / filename
        for asset_id, filename in _TASK15_OUTPUTS.items()
    }
    if _outputs_reusable(pipeline, expected, fingerprint):
        return []
    records = task15_derive_handler(
        _task15_inputs(pipeline), pipeline.paths.derived, owner_source_id=owner
    )(pipeline, stage, source_id, context)
    return _with_fingerprint(records, fingerprint)


def _map_handler(
    pipeline: Any, stage: object, source_id: str, context: Any
) -> list[AssetRecord]:
    owner = _owner_source_id(pipeline)
    if owner is None or source_id != owner:
        return []
    groups = _raw_asset_groups(pipeline)
    fingerprint = _stage_fingerprint(pipeline, "map", _task16_dependency_paths(pipeline), groups)
    expected = {
        asset_id: pipeline.paths.derived / filename
        for asset_id, filename in _TASK16_OUTPUTS.items()
    }
    if _outputs_reusable(pipeline, expected, fingerprint):
        return []
    records = task16_map_handler(
        _task16_inputs(pipeline), pipeline.paths.derived, owner_source_id=owner
    )(pipeline, stage, source_id, context)
    return _with_fingerprint(records, fingerprint)


def _qa_handler(
    pipeline: Any, stage: object, source_id: str, context: Any
) -> list[AssetRecord]:
    owner = _owner_source_id(pipeline)
    if owner is None or source_id != owner:
        return []
    groups = _raw_asset_groups(pipeline)
    dependency_paths = tuple(
        path
        for root in (pipeline.paths.harmonized, pipeline.paths.derived)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    )
    lineage = [
        {
            "asset_id": asset.asset_id,
            "checksum": asset.checksum,
            "dependency_fingerprint": asset.dependency_fingerprint,
            "metadata_json": asset.metadata_json,
            "status": asset.status.value,
        }
        for asset in sorted(pipeline.catalog._read_assets(), key=lambda item: item.asset_id)
        if asset.asset_id.startswith(("task15-", "task16-"))
    ]
    fingerprint = _stage_fingerprint(
        pipeline,
        "qa",
        dependency_paths,
        groups,
        extra_config={"producer_lineage": lineage},
    )
    expected = {
        asset_id: pipeline.paths.qa / relative for asset_id, relative in _QA_OUTPUTS.items()
    }
    if _outputs_reusable(pipeline, expected, fingerprint):
        return []
    records = task17_qa_handler(owner_source_id=owner)(pipeline, stage, source_id, context)
    return _with_fingerprint(records, fingerprint)


def default_stage_handlers() -> Mapping[object, Callable[..., list[AssetRecord]]]:
    """Return the late-stage graph without resolving construction-time inputs."""
    from flashflood_data.pipeline import Stage

    return {
        Stage.DERIVE: _derive_handler,
        Stage.MAP: _map_handler,
        Stage.QA: _qa_handler,
    }
