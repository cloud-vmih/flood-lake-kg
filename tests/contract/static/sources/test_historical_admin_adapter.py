"""Contract tests for immutable GADM historical administrative acquisition."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import geopandas as gpd
import pytest

from flashflood_data.catalog.models import SourceSpec
from flashflood_data.static.sources.admin import (
    GadmAdminAdapter,
    build_sonla_reference_boundary,
    normalize_historical_admin,
)
from flashflood_data.static.sources.base import SourceConfigurationError


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
                "archive_budget_size_bytes": 536_870_912,
            },
        )
    )


def test_gadm_adapter_declares_one_immutable_archive_without_available_assets() -> None:
    remote = _adapter().resolve(None, [])

    assert len(remote) == 1
    assert remote[0].asset_id == "gadm-vnm-4-1-archive"
    assert remote[0].target_relative_path == Path("raw/admin/gadm/4.1/gadm41_VNM_shp.zip")
    assert remote[0].media_type == "application/zip"
    assert remote[0].budget_size_bytes == 536_870_912


def test_gadm_adapter_rejects_non_positive_download_bound() -> None:
    adapter = _adapter()
    settings = dict(adapter.spec.settings)
    settings["archive_budget_size_bytes"] = 0
    broken = GadmAdminAdapter(adapter.spec.model_copy(update={"settings": settings}))

    with pytest.raises(SourceConfigurationError, match="archive_budget_size_bytes"):
        broken.resolve(None, [])


def test_historical_normalization_filters_son_la_and_preserves_validity_metadata() -> None:
    fixture = Path(__file__).parents[3] / "fixtures" / "admin" / "gadm_old_communes.geojson"

    result = normalize_historical_admin(
        gpd.read_file(fixture), source_version="4.1", valid_to=date(2025, 6, 30), raw_asset_id="raw"
    )

    assert result.old_admin_id.tolist() == ["VNM.14.1.1_1", "VNM.14.1.2_1", "VNM.14.1.3_1"]
    assert result.old_admin_name.tolist() == ["Chiềng An", "Chiềng Xôm", "Chiềng Đen"]
    assert result.old_district_name.tolist() == ["Thành phố Sơn La"] * 3
    assert result.valid_from.isna().all()
    assert result.valid_to.tolist() == [date(2025, 6, 30)] * 3
    assert result.raw_asset_id.tolist() == ["raw"] * 3


def test_historical_son_la_dissolve_is_an_independent_reference_boundary() -> None:
    fixture = Path(__file__).parents[3] / "fixtures" / "admin" / "gadm_old_communes.geojson"
    historical = normalize_historical_admin(
        gpd.read_file(fixture), source_version="4.1", valid_to=date(2025, 6, 30), raw_asset_id="raw"
    )

    boundary = build_sonla_reference_boundary(historical)

    assert len(boundary) == 1
    assert boundary.reference_id.iloc[0] == "gadm-sonla-historical-dissolve"
    assert boundary.geometry.iloc[0].equals(historical.geometry.union_all())
