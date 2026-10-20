from datetime import UTC, datetime

from flashflood_data.pipelines.silver_basin.models import BasinEdgeRow, BasinRow


def test_basin_row_keeps_bbox_hydro_flags_and_both_source_objects() -> None:
    row = BasinRow(
        basin_id="42",
        basin_version="bv1",
        hydrobasins_level=12,
        geometry_wkb=b"wkb",
        bbox_wgs84=(103.0, 20.0, 104.0, 21.0),
        crs="EPSG:4326",
        geometry_hash="a" * 64,
        hydro_order=3,
        is_coastal=False,
        is_endorheic=False,
        basin_area_km2=10.0,
        upstream_area_km2=25.0,
        valid_from=datetime(2026, 1, 1, tzinfo=UTC),
        hydrobasins_object_id="hydro-object",
        basinatlas_object_id="atlas-object",
        quality_status="passed",
    )

    assert row.bbox_wgs84 == (103.0, 20.0, 104.0, 21.0)
    assert row.hydrobasins_object_id == "hydro-object"
    assert row.basinatlas_object_id == "atlas-object"


def test_basin_edge_has_stable_identity_rank_and_original_downstream() -> None:
    edge = BasinEdgeRow(
        basin_edge_id="b" * 64,
        topology_version="tv1",
        basin_version="bv1",
        upstream_basin_id="42",
        downstream_basin_id=None,
        source_downstream_id="99",
        edge_status="exits_AOI",
        topological_rank_upstream_first=7,
        terminal_type="non_terminal",
        cycle_check="passed",
        outlet_check="outside_aoi",
        quality_status="warning",
        source_object_id="hydro-object",
    )

    assert edge.source_downstream_id == "99"
    assert edge.topological_rank_upstream_first == 7
