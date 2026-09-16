"""Static quality checks split by subject."""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa.models import CheckResult
from flashflood_data.static.qa.shared import (
    _check,
    _first,
    _geometry,
)


def _population_checks(paths: ProjectPaths) -> list[CheckResult]:
    path = _first(
        [
            paths.derived / "mappings" / "map_subbasin_population.parquet",
            paths.derived / "map_subbasin_population.parquet",
        ]
    )
    if path is None:
        return [
            _check(
                "population.scope",
                False,
                "fatal",
                "Core AOI only with pixel/nodata/coverage evidence",
                "missing",
                "population mapping is missing",
            ),
            _check(
                "population.no_double_count",
                False,
                "fatal",
                "each Core pixel assigned to at most one L10",
                "missing",
                "population mapping is missing",
            ),
        ]
    try:
        table = pd.read_parquet(path)
    except Exception:  # noqa: BLE001
        return [
            _check(
                "population.scope",
                False,
                "fatal",
                "Core AOI only with pixel/nodata/coverage evidence",
                "unreadable",
                "population mapping is unreadable",
            ),
            _check(
                "population.no_double_count",
                False,
                "fatal",
                "each Core pixel assigned to at most one L10",
                "unreadable",
                "population mapping is unreadable",
            ),
        ]
    required = {
        "population_scope",
        "contributing_pixel_count",
        "nodata_pixel_count",
        "aoi_pixel_count",
        "coverage_ratio",
    }
    scope = (
        required.issubset(table.columns)
        and not table.empty
        and table["population_scope"].eq("core_aoi_only").all()
    )
    mapped_ids = pd.to_numeric(table.get("HYBAS_ID", pd.Series(dtype="object")), errors="coerce")
    duplicate_ids = mapped_ids.duplicated().any() or mapped_ids.isna().any()
    values = {
        name: pd.to_numeric(table.get(name, pd.Series(dtype="float64")), errors="coerce")
        for name in (
            "contributing_pixel_count",
            "nodata_pixel_count",
            "aoi_pixel_count",
            "coverage_ratio",
        )
    }
    integer_counts = all(
        np.isfinite(values[name]).all()
        and (values[name] >= 0).all()
        and np.equal(values[name], np.floor(values[name])).all()
        for name in ("contributing_pixel_count", "nodata_pixel_count", "aoi_pixel_count")
    )
    counts_ok = (
        integer_counts
        and (values["contributing_pixel_count"] + values["nodata_pixel_count"])
        .eq(values["aoi_pixel_count"])
        .all()
    )
    positive = values["aoi_pixel_count"].gt(0)
    zero_rows = values["aoi_pixel_count"].eq(0)
    ratios_ok = (
        (
            positive
            & np.isclose(
                values["coverage_ratio"],
                values["contributing_pixel_count"] / values["aoi_pixel_count"],
                equal_nan=False,
            )
        )
        | (
            zero_rows
            & values["contributing_pixel_count"].eq(0)
            & values["nodata_pixel_count"].eq(0)
            & values["coverage_ratio"].isna()
        )
    ).all()
    l10_path = paths.harmonized / "hydro" / "subbasin_l10.geoparquet"
    try:
        selected_ids = set(
            pd.to_numeric(gpd.read_parquet(l10_path)["HYBAS_ID"], errors="raise").astype("int64")
        )
    except Exception:  # noqa: BLE001
        selected_ids = set()
    membership_ok = bool(selected_ids) and set(mapped_ids.astype("int64")) == selected_ids
    independent_ok = False
    core, _ = _geometry(paths, "core")
    worldpop = paths.harmonized / "rasters" / "worldpop_2025.tif"
    if core is not None and not core.empty and worldpop.is_file():
        try:
            from flashflood_data.static.features.spatial import geometry_in_dataset_crs

            with rasterio.open(worldpop) as dataset:
                core_geometry = geometry_in_dataset_crs(
                    core.geometry.union_all(), core.crs.to_string(), dataset.crs
                )
                valid_total = nodata_total = 0
                for _, window in dataset.block_windows(1):
                    inside = geometry_mask(
                        [core_geometry.__geo_interface__],
                        out_shape=(int(window.height), int(window.width)),
                        transform=dataset.window_transform(window),
                        invert=True,
                    )
                    valid = dataset.read_masks(1, window=window) > 0
                    valid_total += int((inside & valid).sum())
                    nodata_total += int((inside & ~valid).sum())
            independent_ok = (
                int(values["contributing_pixel_count"].sum()) == valid_total
                and int(values["nodata_pixel_count"].sum()) == nodata_total
                and int(values["aoi_pixel_count"].sum()) == valid_total + nodata_total
            )
            from flashflood_data.static.features.population import (
                aggregate_population_by_basin,
            )

            expected = aggregate_population_by_basin(
                worldpop, gpd.read_parquet(l10_path), core.geometry.union_all()
            ).set_index("HYBAS_ID")
            observed = table.assign(HYBAS_ID=mapped_ids.astype("int64")).set_index("HYBAS_ID")
            for basin_id, row in expected.iterrows():
                observed_row = observed.loc[int(basin_id)]
                independent_ok = (
                    independent_ok
                    and int(observed_row["contributing_pixel_count"])
                    == int(row["contributing_pixel_count"])
                    and int(observed_row["nodata_pixel_count"]) == int(row["nodata_pixel_count"])
                    and int(observed_row["aoi_pixel_count"]) == int(row["aoi_pixel_count"])
                )
        except Exception:  # noqa: BLE001 - malformed evidence is a fatal outcome.
            independent_ok = False
    ties = int(
        pd.to_numeric(
            table.get("boundary_center_tie_pixel_count", pd.Series(0, index=table.index)),
            errors="coerce",
        )
        .fillna(0)
        .sum()
    )
    return [
        _check(
            "population.scope",
            bool(scope),
            "fatal",
            "Core AOI only with pixel/nodata/coverage evidence",
            "core_aoi_only" if scope else "invalid",
            "WorldPop aggregation scope and evidence",
        ),
        _check(
            "population.no_double_count",
            not duplicate_ids
            and bool(counts_ok)
            and bool(ratios_ok)
            and membership_ok
            and (not worldpop.is_file() or independent_ok),
            "fatal",
            "each Core pixel assigned to at most one L10",
            "complete selected-L10 accounting"
            if not duplicate_ids
            and counts_ok
            and ratios_ok
            and membership_ok
            and (not worldpop.is_file() or independent_ok)
            else "duplicate, incomplete, or inconsistent pixel accounting",
            "population pixel assignment is exclusive with exact accounting",
        ),
        _check(
            "population.boundary_ties",
            ties == 0,
            "warning",
            "0 boundary-center ties",
            ties,
            "ties are deterministically assigned to the lowest HYBAS_ID",
        ),
    ]




def checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    del config
    return _population_checks(paths)
