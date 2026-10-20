from datetime import UTC, datetime
from pathlib import Path

from flashflood_data.pipelines.silver_basin.config import load_basin_topology_config

ROOT = Path(__file__).parents[4]


def test_basin_normalization_plan_uses_sedona_and_explicit_source_authority() -> None:
    from flashflood_data.pipelines.silver_basin.normalize import basin_normalization_plan

    config = load_basin_topology_config(ROOT / "config/pipelines/silver/basin_topology.yaml")
    plan = basin_normalization_plan(
        config,
        basin_version="bv1",
        valid_from=datetime(2026, 1, 1, tzinfo=UTC),
    )
    hydro_sql = " ".join(plan.hydro_geometry_projection)
    output_sql = " ".join(plan.output_projection)

    assert "ST_GeomFromWKB" in hydro_sql
    assert "ST_Transform" in hydro_sql
    assert "ST_MakeValid" in hydro_sql
    assert "ST_Normalize" in hydro_sql
    assert "NEXT_DOWN" in output_sql
    assert "atlas_basin_area_km2" in output_sql
    assert "AS bbox_wgs84" in output_sql
    assert "hydro_ORDER AS hydro_order" in output_sql
    assert "hydro_COAST AS is_coastal" in output_sql
    assert "hydro_ENDO AS is_endorheic" in output_sql
    assert "hydrobasins_object_id" in output_sql
    assert "basinatlas_object_id" in output_sql
    assert "AS quality_status" in output_sql
    assert "AS _geometry_was_repaired" in output_sql
    assert "object_id AS basinatlas_object_id" in " ".join(plan.atlas_projection)
    assert plan.join_key == "basin_id"


def test_basin_normalization_plan_rejects_non_utc_valid_from() -> None:
    import pytest

    from flashflood_data.pipelines.silver_basin.normalize import basin_normalization_plan

    config = load_basin_topology_config(ROOT / "config/pipelines/silver/basin_topology.yaml")
    with pytest.raises(ValueError, match="UTC"):
        basin_normalization_plan(
            config,
            basin_version="bv1",
            valid_from=datetime(2026, 1, 1),  # noqa: DTZ001 - intentional invalid input
        )


class _Column:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return f"{self.value} = {other.value}"


class _Frame:
    def __init__(self, calls, label="bronze"):
        self.calls = calls
        self.label = label

    def filter(self, expression):
        self.calls.append((self.label, "filter", expression))
        return _Frame(self.calls, self.label)

    def selectExpr(self, *expressions):
        self.calls.append((self.label, "selectExpr", expressions))
        return _Frame(self.calls, self.label)

    def alias(self, name):
        self.calls.append((self.label, "alias", name))
        return _Frame(self.calls, name)

    def __getitem__(self, name):
        return _Column(f"{self.label}.{name}")

    def join(self, other, condition, mode):
        self.calls.append((self.label, "join", other.label, condition, mode))
        return _Frame(self.calls, "joined")


def test_normalize_basins_builds_a_lazy_left_join_without_collecting_rows() -> None:
    from flashflood_data.pipelines.silver_basin.normalize import normalize_basins

    config = load_basin_topology_config(ROOT / "config/pipelines/silver/basin_topology.yaml")
    calls = []
    result = normalize_basins(
        _Frame(calls),
        config,
        basin_version="bv1",
        valid_from=datetime(2026, 1, 1, tzinfo=UTC),
        basin_ids=("101", "102"),
    )

    join = next(call for call in calls if call[1] == "join")
    assert join[-1] == "left"
    assert join[-2] == "h.basin_id = a.basin_id"
    assert result.label == "joined"
    assert not any(call[1] == "collect" for call in calls)
    scoped_filters = [str(call[2]) for call in calls if call[1] == "filter"]
    assert scoped_filters.count("basin_id IN ('101','102')") == 2
