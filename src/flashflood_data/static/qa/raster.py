"""Static quality checks split by subject."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import rasterio
from shapely.geometry import box

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa.models import CheckResult
from flashflood_data.static.qa.shared import (
    _check,
    _first,
    _geometry,
)
from flashflood_data.static.spatial.raster import raster_coverage_ratio


def _raster_checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    hydro, _ = _geometry(paths, "hydrological")
    core, _ = _geometry(paths, "core")
    rasters: dict[str, tuple[Path | None, gpd.GeoDataFrame | None]] = {
        "dem": (_first([paths.harmonized / "rasters" / "dem_glo30.tif"]), hydro),
        "worldcover": (_first([paths.harmonized / "rasters" / "worldcover_2021.tif"]), hydro),
        "worldpop": (_first([paths.harmonized / "rasters" / "worldpop_2025.tif"]), core),
    }
    checks: list[CheckResult] = []
    for name, (path, aoi) in rasters.items():
        if path is None or aoi is None or aoi.empty:
            checks.append(
                _check(
                    f"raster.{name}.coverage",
                    False,
                    "fatal",
                    f">= {config.environmental_raster_coverage_min_pct}% valid coverage of {'Core' if name == 'worldpop' else 'Hydrological'} AOI",
                    "missing",
                    "required raster or AOI is missing",
                )
            )
            continue
        try:
            valid_ratio = raster_coverage_ratio(path, aoi.geometry.union_all())
            ratio = valid_ratio
            coverage_kind = "native-grid valid-pixel"
            if name == "worldpop":
                with rasterio.open(path) as dataset:
                    core = gpd.GeoSeries(
                        [aoi.geometry.union_all()], crs=aoi.crs
                    ).to_crs(dataset.crs).iloc[0]
                    footprint = box(*dataset.bounds)
                    ratio = (
                        float(footprint.intersection(core).area) / float(core.area)
                        if core.area
                        else 0.0
                    )
                coverage_kind = "native-grid footprint"
            checks.append(
                _check(
                    f"raster.{name}.coverage",
                    ratio * 100 >= config.environmental_raster_coverage_min_pct,
                    "fatal",
                    f">= {config.environmental_raster_coverage_min_pct}% valid coverage of {'Core' if name == 'worldpop' else 'Hydrological'} AOI",
                    f"{ratio * 100:.6f}%",
                    f"{coverage_kind} AOI coverage",
                )
            )
            checks.append(
                _check(
                    f"raster.{name}.nodata",
                    valid_ratio >= 1.0,
                    "warning",
                    "0 source nodata pixels in AOI",
                    f"{(1 - valid_ratio) * 100:.6f}%",
                    "source nodata is retained as coverage evidence",
                )
            )
        except Exception as error:  # noqa: BLE001
            checks.append(
                _check(
                    f"raster.{name}.coverage",
                    False,
                    "fatal",
                    f">= {config.environmental_raster_coverage_min_pct}% valid coverage",
                    type(error).__name__,
                    "raster coverage could not be evaluated",
                )
            )
    from flashflood_data.static.features.config import load_feature_config

    semantics = load_feature_config()
    for property_id in semantics.soil_properties:
        for depth in semantics.soil_depths:
            for statistic in semantics.soil_statistics:
                path = paths.harmonized / "soilgrids" / property_id / depth / f"{statistic}.tif"
                check_id = f"raster.soilgrids.{property_id}.{depth}.{statistic}.coverage"
                if hydro is None or hydro.empty or not path.is_file():
                    checks.append(
                        _check(
                            check_id,
                            False,
                            "fatal",
                            f">= {config.environmental_raster_coverage_min_pct}% valid coverage of Hydrological AOI",
                            "missing",
                            "required configured SoilGrids product or Hydrological AOI is missing",
                        )
                    )
                    checks.append(
                        _check(
                            check_id.removesuffix(".coverage") + ".nodata",
                            False,
                            "warning",
                            "0 source nodata pixels in AOI",
                            "unavailable",
                            "configured SoilGrids nodata evidence is unavailable",
                        )
                    )
                    continue
                try:
                    ratio = raster_coverage_ratio(path, hydro.geometry.union_all())
                    checks.append(
                        _check(
                            check_id,
                            ratio * 100 >= config.environmental_raster_coverage_min_pct,
                            "fatal",
                            f">= {config.environmental_raster_coverage_min_pct}% valid coverage of Hydrological AOI",
                            f"{ratio * 100:.6f}%",
                            "configured SoilGrids product native-grid valid-pixel coverage",
                        )
                    )
                    checks.append(
                        _check(
                            check_id.removesuffix(".coverage") + ".nodata",
                            ratio >= 1.0,
                            "warning",
                            "0 source nodata pixels in AOI",
                            f"{(1 - ratio) * 100:.6f}%",
                            "configured SoilGrids source nodata is retained as evidence",
                        )
                    )
                except Exception as error:  # noqa: BLE001
                    checks.append(
                        _check(
                            check_id,
                            False,
                            "fatal",
                            f">= {config.environmental_raster_coverage_min_pct}% valid coverage of Hydrological AOI",
                            type(error).__name__,
                            "configured SoilGrids product could not be evaluated",
                        )
                    )
                    checks.append(
                        _check(
                            check_id.removesuffix(".coverage") + ".nodata",
                            False,
                            "warning",
                            "0 source nodata pixels in AOI",
                            type(error).__name__,
                            "configured SoilGrids nodata evidence could not be evaluated",
                        )
                    )
    return checks




def checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    return _raster_checks(paths, config)
