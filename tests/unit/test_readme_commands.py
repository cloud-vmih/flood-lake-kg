from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_readme_has_current_weather_dags_and_reset_command() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    for dag_id in (
        "gsmap_now_ingest",
        "gsmap_standard_ingest",
        "era5_land_ingest",
        "ifs_ingest",
    ):
        assert dag_id in readme
    assert "bronze.weather_raster_slice" in readme
    assert "meta.object_lifecycle" in readme
    assert "silver.source_grid" in readme
    assert "7 ngày" in readme
    assert "docker compose down -v --remove-orphans" in readme
    assert "weather_grid_value" not in readme
