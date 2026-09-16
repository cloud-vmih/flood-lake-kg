"""Contract tests for current Sơn La administrative-source discovery."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.sources.admin import CurrentAdminAdapter, normalize_current_admin
from flashflood_data.static.sources.base import SourceConfigurationError, SourceContext


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
                "index_budget_size_bytes": 2_097_152,
                "geometry_budget_size_bytes": 16_777_216,
                "resolution_page_budget_size_bytes": 4_194_304,
                "resolution_pdf_budget_size_bytes": 67_108_864,
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
    return Path(__file__).parents[3] / "fixtures" / "admin"


def test_current_admin_discovers_geometry_after_index_is_available(
    adapter: CurrentAdminAdapter, context: SourceContext, admin_fixture_dir: Path
) -> None:
    first = adapter.resolve(context, [])

    assert {item.asset_id for item in first} == {
        "sonla-admin-2025-index",
        "resolution-1681-page",
    }
    assert {item.asset_id: item.budget_size_bytes for item in first} == {
        "sonla-admin-2025-index": 2_097_152,
        "resolution-1681-page": 4_194_304,
    }
    index = admin_fixture_dir / "unit_index.json"

    second = adapter.resolve(context, [_available(index, "sonla-admin-2025-index")])

    assert second[0].asset_id == "sonla-admin-2025-unit-03664"
    assert second[0].request_method == "POST"
    assert second[0].request_form == {"id": "diaphanhanhchinhcapxa_2025.1338"}
    assert second[0].target_relative_path == Path("raw/admin/sonla_2025/units/03664.geojson")
    assert second[0].budget_size_bytes == 16_777_216


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


def test_normalized_current_admin_resolves_live_lookup_id_to_index_code(
    tmp_path: Path, admin_fixture_dir: Path
) -> None:
    payload = json.loads((admin_fixture_dir / "unit_03664.geojson").read_text())
    payload["features"][0]["properties"]["a02_xa"] = "diaphanhanhchinhcapxa_2025.1338"
    geometry = tmp_path / "03664.geojson"
    geometry.write_text(json.dumps(payload), encoding="utf-8")

    result = normalize_current_admin(admin_fixture_dir / "unit_index.json", [geometry])

    assert result.current_commune_code.tolist() == ["03664"]
    assert result.raw_asset_id.tolist() == ["sonla-admin-2025-unit-03664"]


def test_current_admin_discovers_resolution_pdf_after_page_is_available(
    adapter: CurrentAdminAdapter, context: SourceContext, admin_fixture_dir: Path
) -> None:
    page = _available(admin_fixture_dir / "resolution_page.html", "resolution-1681-page")

    discovered = adapter.resolve(context, [page])

    pdf = next(item for item in discovered if item.asset_id == "resolution-1681-pdf")
    assert pdf.uri == "https://example.invalid/files/1681-NQ-UBTVQH15.pdf"
    assert pdf.target_relative_path == Path("raw/admin/sonla_2025/resolution_1681.pdf")
    assert pdf.budget_size_bytes == 67_108_864


def test_current_admin_rejects_missing_download_bound(
    adapter: CurrentAdminAdapter, context: SourceContext
) -> None:
    settings = dict(adapter.spec.settings)
    settings.pop("index_budget_size_bytes")
    broken = CurrentAdminAdapter(adapter.spec.model_copy(update={"settings": settings}))

    with pytest.raises(SourceConfigurationError, match="index_budget_size_bytes"):
        broken.resolve(context, [])


def test_current_admin_reads_gzip_encoded_json_payload(
    tmp_path: Path,
    adapter: CurrentAdminAdapter,
    context: SourceContext,
    admin_fixture_dir: Path,
) -> None:
    compressed = tmp_path / "unit_index.json"
    compressed.write_bytes(gzip.compress((admin_fixture_dir / "unit_index.json").read_bytes()))

    validation = adapter.validate_raw(compressed)
    discovered = adapter.resolve(context, [_available(compressed, "sonla-admin-2025-index")])

    assert validation.passed
    geometry = next(item for item in discovered if item.asset_id.startswith("sonla-admin-2025-unit-"))
    assert geometry.asset_id == "sonla-admin-2025-unit-03664"


def test_current_admin_reads_gzip_encoded_resolution_page(
    tmp_path: Path,
    adapter: CurrentAdminAdapter,
    context: SourceContext,
    admin_fixture_dir: Path,
) -> None:
    compressed = tmp_path / "resolution_page.html"
    compressed.write_bytes(gzip.compress((admin_fixture_dir / "resolution_page.html").read_bytes()))

    validation = adapter.validate_raw(compressed)
    discovered = adapter.resolve(context, [_available(compressed, "resolution-1681-page")])

    assert validation.passed
    pdf = next(item for item in discovered if item.asset_id == "resolution-1681-pdf")
    assert pdf.uri == "https://example.invalid/files/1681-NQ-UBTVQH15.pdf"
