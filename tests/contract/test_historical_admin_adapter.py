"""Contract tests for immutable GADM historical administrative acquisition."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import geopandas as gpd

from flashflood_data.models import SourceSpec
from flashflood_data.sources.admin import GadmAdminAdapter, normalize_historical_admin


def _adapter() -> GadmAdminAdapter:
    return GadmAdminAdapter(
        SourceSpec(
            source_id="gadm_vnm_4_1",
            adapter="gadm_admin",
            version="4.1",
            license_id="gadm-license",
            settings={
                "archive_url": "https://example.invalid/gadm41_VNM_shp.zip",
                "license_url": "https://gadm.org/license.html",
                "province_name": "Sơn La",
                "historical_valid_to": "2025-06-30",
            },
        )
    )


def test_gadm_adapter_declares_one_immutable_archive_without_available_assets() -> None:
    remote = _adapter().resolve(None, [])

    assert len(remote) == 1
    assert remote[0].asset_id == "gadm-vnm-4-1-archive"
    assert remote[0].target_relative_path == Path("raw/admin/gadm/4.1/gadm41_VNM_shp.zip")
    assert remote[0].media_type == "application/zip"


def test_historical_normalization_filters_son_la_and_preserves_validity_metadata() -> None:
    fixture = Path(__file__).parents[1] / "fixtures" / "admin" / "gadm_old_communes.geojson"

    result = normalize_historical_admin(
        gpd.read_file(fixture), source_version="4.1", valid_to=date(2025, 6, 30), raw_asset_id="raw"
    )

    assert result.old_admin_id.tolist() == ["VNM.14.1.1_1", "VNM.14.1.2_1", "VNM.14.1.3_1"]
    assert result.old_admin_name.tolist() == ["Chiềng An", "Chiềng Xôm", "Chiềng Đen"]
    assert result.old_district_name.tolist() == ["Thành phố Sơn La"] * 3
    assert result.valid_from.isna().all()
    assert result.valid_to.tolist() == [date(2025, 6, 30)] * 3
    assert result.raw_asset_id.tolist() == ["raw"] * 3
