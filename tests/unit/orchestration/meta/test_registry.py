"""Static landing seeds declared source and dataset metadata before publication."""

from pathlib import Path

from flashflood_data.orchestration.meta.registry import load_static_registry
from flashflood_data.static.sources.registry import load_source_specs

ROOT = Path(__file__).parents[4]


def test_static_registry_covers_every_landing_source_with_real_versions() -> None:
    specifications = load_source_specs(ROOT / "config" / "sources")
    sources, datasets = load_static_registry(
        ROOT / "config" / "meta" / "static.yaml", specifications,
        catalog_name="flood_lakehouse",
    )
    assert {row["source_id"] for row in sources} == set(specifications)
    hydro = next(row for row in sources if row["source_id"] == "hydrobasins_v1c")
    assert hydro["source_version"] == "1c"
    assert hydro["provider"] == "HydroSHEDS"
    assert {row["dataset_id"] for row in datasets} >= {
        "flood_lakehouse.meta.source_objects",
        "flood_lakehouse.bronze.basin_polygon_raw",
        "flood_lakehouse.bronze.admin_boundary_raw",
    }
    assert all(row["access_policy_ref"] for row in datasets)
    assert all(row["access_policy_ref"].startswith("config/meta/static.yaml#") for row in datasets)
