"""Unit contracts for deterministic hydro topology and study-area construction."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import box

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.pipelines.study_area.aoi import build_study_areas
from flashflood_data.pipelines.study_area.hydro import select_basins_with_upstream


@pytest.fixture
def hydro_fixture_dir() -> Path:
    return Path(__file__).parents[3] / "fixtures" / "hydro"


@pytest.fixture
def l10_chain(hydro_fixture_dir: Path) -> gpd.GeoDataFrame:
    return gpd.read_file(hydro_fixture_dir / "basins_l10.geojson")


def test_environment_can_cross_border_but_exposure_cannot(l10_chain: gpd.GeoDataFrame) -> None:
    """Removing the Vietnam intersection must make the exposure assertion fail."""
    core = box(103.01, 20.01, 103.09, 20.09)
    vietnam = box(103.0, 20.0, 103.15, 20.15)
    national = select_basins_with_upstream(l10_chain, vietnam, hops=1)
    areas = build_study_areas(
        core,
        l10_chain.iloc[:2],
        vietnam,
        national,
        StudyAreaConfig(),
    )

    assert not areas.environmental.within(areas.vietnam)
    assert areas.exposure.difference(areas.vietnam).area == pytest.approx(0.0)
    assert areas.vietnam_hydrological.symmetric_difference(
        national.geometry.union_all()
    ).area == pytest.approx(0.0, abs=1e-12)


def test_national_hydrological_aoi_adds_one_direct_upstream_hop(
    l10_chain: gpd.GeoDataFrame,
) -> None:
    vietnam = box(103.0, 20.0, 103.15, 20.15)
    selected = select_basins_with_upstream(l10_chain, vietnam, hops=1)
    areas = build_study_areas(
        box(103.01, 20.01, 103.09, 20.09),
        l10_chain.iloc[:2],
        vietnam,
        selected,
        StudyAreaConfig(),
    )

    assert set(selected.HYBAS_ID) == {100, 101, 102}
    assert areas.vietnam_hydrological.symmetric_difference(
        l10_chain.geometry.union_all()
    ).area == pytest.approx(0.0, abs=1e-12)
