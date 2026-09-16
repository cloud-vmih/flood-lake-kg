"""Unit contracts for deterministic hydro topology and study-area construction."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.static.harmonize.aoi import build_study_areas
from flashflood_data.static.harmonize.hydro import (
    build_basin_hierarchy,
    select_l10_with_upstream,
)


@pytest.fixture
def hydro_fixture_dir() -> Path:
    return Path(__file__).parents[2] / "fixtures" / "hydro"


@pytest.fixture
def l10_chain(hydro_fixture_dir: Path) -> gpd.GeoDataFrame:
    return gpd.read_file(hydro_fixture_dir / "basins_l10.geojson")


def test_one_hop_adds_only_direct_upstream(l10_chain: gpd.GeoDataFrame) -> None:
    """Changing NEXT_DOWN to skip 100 must make this selection fail."""
    selected = select_l10_with_upstream(l10_chain, box(103.01, 20.01, 103.09, 20.09), hops=1)

    assert set(selected.HYBAS_ID) == {100, 101}
    assert 102 not in set(selected.HYBAS_ID)


def test_selection_rejects_duplicate_hydrobasins_ids(l10_chain: gpd.GeoDataFrame) -> None:
    """Dropping this validation permits ambiguous basin topology into the output."""
    duplicate = l10_chain.iloc[[0]].copy()
    duplicate.loc[:, "geometry"] = duplicate.geometry.translate(xoff=1)

    with pytest.raises(ValueError, match="duplicate HYBAS_ID"):
        select_l10_with_upstream(pd.concat([l10_chain, duplicate], ignore_index=True), box(103.01, 20.01, 103.09, 20.09))


def test_hierarchy_uses_containing_parent_geometries_and_pfaf_prefix(
    hydro_fixture_dir: Path, l10_chain: gpd.GeoDataFrame
) -> None:
    """Changing either parent PFAF prefix or geometry must make this hierarchy fail."""
    hierarchy = build_basin_hierarchy(
        l10_chain.iloc[[0]],
        gpd.read_file(hydro_fixture_dir / "basins_l9.geojson"),
        gpd.read_file(hydro_fixture_dir / "basins_l8.geojson"),
    )

    assert hierarchy.loc[0, ["HYBAS_ID", "parent_l9_hybas_id", "parent_l8_hybas_id"]].tolist() == [
        100,
        90,
        80,
    ]
    assert not bool(hierarchy.loc[0, "scope_exit"])


def test_environment_can_cross_border_but_exposure_cannot(l10_chain: gpd.GeoDataFrame) -> None:
    """Removing the Vietnam intersection must make the exposure assertion fail."""
    core = box(103.01, 20.01, 103.09, 20.09)
    vietnam = box(103.0, 20.0, 103.15, 20.15)
    areas = build_study_areas(core, l10_chain.iloc[:2], vietnam, StudyAreaConfig())

    assert not areas.environmental.within(areas.vietnam)
    assert areas.exposure.difference(areas.vietnam).area == pytest.approx(0.0)
