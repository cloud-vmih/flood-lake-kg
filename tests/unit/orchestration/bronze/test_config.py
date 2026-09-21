"""Parser versions are pinned separately from the raw landing configuration."""

from pathlib import Path

from flashflood_data.orchestration.bronze.config import load_bronze_config

ROOT = Path(__file__).parents[4]


def test_static_bronze_policy_pins_parser_versions_and_enables_osm() -> None:
    config = load_bronze_config(ROOT / "config" / "bronze" / "static.yaml")
    assert config.contract_version == "v1"
    assert config.parser_version("hydrobasins_v1c") == "v1"
    assert config.target_table("hydrobasins_v1c") == "basin_polygon_raw"
    assert config.parser_version("soilgrids_2_0") == "v1"
    assert config.target_table("soilgrids_2_0") == "raster_coverage"
    assert config.status("geofabrik_vietnam_snapshot") == "ready"
    assert config.parser_version("geofabrik_vietnam_snapshot") == "v1"
    assert "geofabrik_vietnam_snapshot" in config.ready_source_ids()
    assert "hydrobasins_v1c" in config.ready_source_ids()
    assert len(config.sources) == 11
    assert config.rules is not None
    assert "business_key_unique" in config.rules
    assert config.rules["business_key_unique"]["severity"] == "fatal"


def test_requested_source_limits_a_dag_run_to_one_ready_parser() -> None:
    config = load_bronze_config(ROOT / "config" / "bronze" / "static.yaml")

    assert config.should_process("geofabrik_vietnam_snapshot", "geofabrik_vietnam_snapshot")
    assert not config.should_process("basinatlas_v10", "geofabrik_vietnam_snapshot")
    assert config.should_process("basinatlas_v10", "")

    try:
        config.should_process("basinatlas_v10", "unknown_source")
    except ValueError as error:
        assert "unknown_source" in str(error)
    else:
        raise AssertionError("unknown DAG source selector must be rejected")


def test_force_reprocess_accepts_only_explicit_true_values() -> None:
    config = load_bronze_config(ROOT / "config" / "bronze" / "static.yaml")

    assert config.force_reprocess(True)
    assert config.force_reprocess("true")
    assert config.force_reprocess("1")
    assert not config.force_reprocess(False)
    assert not config.force_reprocess("")

    try:
        config.force_reprocess("sometimes")
    except ValueError as error:
        assert "sometimes" in str(error)
    else:
        raise AssertionError("ambiguous force_reprocess value must be rejected")
