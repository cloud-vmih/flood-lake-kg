from pathlib import Path

from flashflood_data.core.config import load_study_area
from flashflood_data.core.paths import ProjectPaths


def test_study_area_has_approved_scopes(tmp_path: Path) -> None:
    cfg = tmp_path / "study.yaml"
    cfg.write_text(
        "province_origin_code: '14'\n"
        "hydrobasins_level: 10\n"
        "upstream_hops: 1\n"
        "raster_buffer_km: 10\n"
        "exposure_buffer_km: 10\n"
        "processing_crs: EPSG:32648\n"
        "storage_crs: EPSG:4326\n"
        "new_raw_soft_cap_gib: 8\n"
        "minimum_free_gib: 10\n",
        encoding="utf-8",
    )
    result = load_study_area(cfg)
    assert result.hydrobasins_level == 10
    assert result.processing_crs == "EPSG:32648"


def test_paths_never_escape_root(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    assert paths.raw == tmp_path / "dataset" / "raw"
    assert paths.catalog == tmp_path / "dataset" / "catalog"


def test_paths_use_configured_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLASHFLOOD_PROJECT_ROOT", str(tmp_path))

    assert ProjectPaths.discover().root == tmp_path.resolve()


def test_explicit_project_root_overrides_environment(monkeypatch, tmp_path: Path) -> None:
    configured = tmp_path / "configured"
    explicit = tmp_path / "explicit"
    monkeypatch.setenv("FLASHFLOOD_PROJECT_ROOT", str(configured))

    assert ProjectPaths.discover(explicit).root == explicit.resolve()
