"""Contract tests for current Sơn La administrative-source discovery."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.sources.admin import CurrentAdminAdapter, normalize_current_admin
from flashflood_data.sources.base import SourceContext


@pytest.fixture
def context(tmp_path: Path) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="admin-contract-test",
    )


@pytest.fixture
def adapter() -> CurrentAdminAdapter:
    return CurrentAdminAdapter(
        SourceSpec(
            source_id="sonla_admin_2025",
            adapter="admin_current",
            version="2025-07-01",
            license_id="public-administrative-reference",
            settings={
                "index_url": "https://sapnhap.bando.com.vn/p.co_dvhc",
                "index_form": {"ma": "0"},
                "geometry_url": "https://sapnhap.bando.com.vn/pread_json",
                "province_origin_code": "14",
                "resolution_page": "https://example.invalid/resolution-1681.html",
            },
        )
    )


def _available(path: Path, asset_id: str) -> AssetRecord:
    return AssetRecord(
        asset_id=asset_id,
        source_id="sonla_admin_2025",
        source_version="2025-07-01",
        kind=AssetKind.RAW,
        source_uri="https://example.invalid/source",
        storage_path=str(path),
        media_type="application/json",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 21, tzinfo=UTC),
        license_id="public-administrative-reference",
        pipeline_run_id="admin-contract-test",
        status=AssetStatus.VALIDATED,
    )


@pytest.fixture
def admin_fixture_dir() -> Path:
    return Path(__file__).parents[1] / "fixtures" / "admin"


def test_current_admin_discovers_geometry_after_index_is_available(
    adapter: CurrentAdminAdapter, context: SourceContext, admin_fixture_dir: Path
) -> None:
    first = adapter.resolve(context, [])

    assert {item.asset_id for item in first} == {
        "sonla-admin-2025-index",
        "resolution-1681-page",
    }
    index = admin_fixture_dir / "unit_index.json"

    second = adapter.resolve(context, [_available(index, "sonla-admin-2025-index")])

    assert second[0].asset_id == "sonla-admin-2025-unit-03664"
    assert second[0].request_method == "POST"
    assert second[0].request_form == {"id": "diaphanhanhchinhcapxa_2025.1338"}
    assert second[0].target_relative_path == Path("raw/admin/sonla_2025/units/03664.geojson")


def test_normalized_current_admin_keeps_legal_and_lookup_fields(admin_fixture_dir: Path) -> None:
    index = admin_fixture_dir / "unit_index.json"
    geometry = admin_fixture_dir / "unit_03664.geojson"

    result = normalize_current_admin(index, [geometry])
    row = result.iloc[0]

    assert row.current_commune_code == "03664"
    assert row.current_commune_name == "Phường Chiềng An"
    assert row.predecessors_text == "Phường Chiềng An, Xã Chiềng Xôm, Xã Chiềng Đen"
    assert row.lookup_id == "sonla-admin-2025:03664"
    assert row.valid_from == "2025-07-01"
    assert bool(row.geometry_repaired) is False
    assert str(result.crs) == "EPSG:4326"


def test_current_admin_discovers_resolution_pdf_after_page_is_available(
    adapter: CurrentAdminAdapter, context: SourceContext, admin_fixture_dir: Path
) -> None:
    page = _available(admin_fixture_dir / "resolution_page.html", "resolution-1681-page")

    discovered = adapter.resolve(context, [page])

    pdf = next(item for item in discovered if item.asset_id == "resolution-1681-pdf")
    assert pdf.uri == "https://example.invalid/files/1681-NQ-UBTVQH15.pdf"
    assert pdf.target_relative_path == Path("raw/admin/sonla_2025/resolution_1681.pdf")
