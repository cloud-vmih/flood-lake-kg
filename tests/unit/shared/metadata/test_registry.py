"""Static landing seeds declared source and dataset metadata before publication."""

from pathlib import Path

from flashflood_data.pipelines.static_landing.adapters.registry import load_source_specs
from flashflood_data.shared.metadata.registry import (
    load_dataset_registry,
    load_static_registry,
)

ROOT = Path(__file__).parents[4]


def test_static_registry_covers_every_landing_source_with_real_versions() -> None:
    specifications = load_source_specs(ROOT / "config" / "sources")
    sources, datasets = load_static_registry(
        ROOT / "config" / "shared" / "metadata.yaml",
        specifications,
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
    assert all(
        row["access_policy_ref"].startswith("config/shared/metadata.yaml#") for row in datasets
    )


def test_dataset_registry_declares_silver_basin_and_quarantine_contracts() -> None:
    datasets = load_dataset_registry(
        ROOT / "config" / "shared" / "metadata.yaml",
        catalog_name="flood_lakehouse",
    )
    by_id = {row["dataset_id"]: row for row in datasets}

    assert by_id["flood_lakehouse.silver.dim_basin"]["quality_policy_id"] == (
        "silver_basin_topology-v1"
    )
    assert by_id["flood_lakehouse.silver.basin_edge"]["layer"] == "silver"
    assert by_id["flood_lakehouse.quarantine.rejected_rows"]["layer"] == "quarantine"
