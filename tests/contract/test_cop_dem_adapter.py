"""Contract tests for authenticated, immutable Copernicus DEM acquisition."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr
from shapely.geometry import box
from shapely.ops import unary_union

from flashflood_data.budget import StorageBudget
from flashflood_data.catalog import AssetCatalog
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.http import HttpFetcher
from flashflood_data.models import AssetStatus, RemoteAsset, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.sources.base import SourceContext
from flashflood_data.sources.cop_dem import (
    CdseTokenClient,
    CopDemAdapter,
    MissingCredentials,
    UnsafeDemArchive,
    extract_dem_tiff,
    grid_ids_for_geometry,
    select_dem_product,
)


@pytest.fixture
def search_fixture() -> dict[str, object]:
    path = Path(__file__).parents[1] / "fixtures" / "cop_dem" / "search_n21_e103.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def spec() -> SourceSpec:
    return SourceSpec(
        source_id="cop_dem_glo30_2024_1",
        adapter="cop_dem",
        version="2024_1",
        license_id="COP-DEM-30",
        settings={
            "token_url": "https://identity.example.test/token",
            "catalogue_url": "https://catalogue.example.test/odata/v1/Products",
            "download_template": "https://download.example.test/odata/v1/Products({product_id})/$value",
            "dataset": "COP-DEM_GLO-30-DGED/2024_1",
            "product_type": "SAR_DGE_30_A4AD",
        },
    )


@pytest.fixture
def context(tmp_path: Path) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="cop-dem-contract-test",
    )


def test_grid_ids_cover_each_intersecting_one_degree_cell() -> None:
    """Catches a grid selection that omits an AOI-overlapping cell."""
    assert grid_ids_for_geometry(box(103.2, 20.2, 104.2, 21.2)) == [
        "N20_E103",
        "N20_E104",
        "N21_E103",
        "N21_E104",
    ]


def test_grid_ids_exclude_bbox_cells_not_intersected_by_aoi() -> None:
    """Catches downloading every bounding-box cell for a discontiguous Environmental AOI."""
    aoi = unary_union([box(103.1, 20.1, 103.2, 20.2), box(104.1, 21.1, 104.2, 21.2)])

    assert grid_ids_for_geometry(aoi) == ["N20_E103", "N21_E104"]


def test_product_selection_requires_fixed_dataset(search_fixture: dict[str, object]) -> None:
    """Catches accepting a product from a similarly named CDSE collection."""
    product = select_dem_product(search_fixture, "COP-DEM_GLO-30-DGED/2024_1", "N21_E103")

    assert product.product_id == "8b2a0b20-23ab-4d31-9a00-000000000001"
    assert product.product_type == "SAR_DGE_30_A4AD"
    assert product.dataset == "COP-DEM_GLO-30-DGED/2024_1"
    assert product.content_length == 4096


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"value": []}, "no active product"),
        (
            {
                "value": [
                    {
                        "Id": "wrong-dataset",
                        "Name": "wrong-dataset",
                        "ModificationDate": "2025-01-01T00:00:00Z",
                        "Attributes": [
                            {"Name": "gridId", "Value": "N21_E103"},
                            {"Name": "dataset", "Value": "OTHER"},
                            {"Name": "productType", "Value": "SAR_DGE_30_A4AD"},
                        ],
                    }
                ]
            },
            "wrong dataset",
        ),
    ],
)
def test_product_selection_rejects_unusable_catalogue_results(
    payload: dict[str, object], message: str
) -> None:
    """Catches resolution of a zero-result or wrong-dataset catalogue response."""
    with pytest.raises(ValueError, match=message):
        select_dem_product(payload, "COP-DEM_GLO-30-DGED/2024_1", "N21_E103")


def test_missing_cdse_credentials_stops_before_download(
    context: SourceContext, spec: SourceSpec
) -> None:
    """Catches a live resolution attempt that reaches CDSE without local credentials."""
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: (_ for _ in ()).throw(AssertionError(request.url)))
    )
    with pytest.raises(MissingCredentials, match="FLASHFLOOD_CDSE_USERNAME"):
        CopDemAdapter(spec, client=client).resolve(context, [])


def test_token_exchange_keeps_credentials_and_access_token_out_of_logs(caplog: pytest.LogCaptureFixture) -> None:
    """Catches accidental logging of CDSE password-grant values."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"access_token": "fixture-access-token"})

    environment = EnvironmentSettings(
        _env_file=None,
        cdse_username=SecretStr("fixture-user"),
        cdse_password=SecretStr("fixture-password"),
    )
    token = CdseTokenClient("https://identity.example.test/token", environment, httpx.Client(transport=httpx.MockTransport(handler)))

    assert token.get_access_token().get_secret_value() == "fixture-access-token"
    assert len(seen) == 1
    assert b"fixture-user" in seen[0].content
    assert b"fixture-password" in seen[0].content
    assert "fixture-user" not in caplog.text
    assert "fixture-password" not in caplog.text
    assert "fixture-access-token" not in caplog.text


def test_authenticated_fetch_keeps_bearer_token_out_of_raw_catalogue(
    context: SourceContext, spec: SourceSpec
) -> None:
    """Catches persisting a bearer token while downloading an immutable raw product."""
    seen_download_headers: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "identity.example.test":
            return httpx.Response(200, json={"access_token": "fixture-access-token"})
        seen_download_headers.append(request.headers.get("Authorization", ""))
        return httpx.Response(200, stream=httpx.ByteStream(b"dem!"))

    environment = EnvironmentSettings(
        _env_file=None,
        cdse_username=SecretStr("fixture-user"),
        cdse_password=SecretStr("fixture-password"),
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))
    authorized_context = SourceContext(
        paths=context.paths,
        catalog=context.catalog,
        study_area=context.study_area,
        environment=environment,
        run_id=context.run_id,
    )
    remote = RemoteAsset(
        asset_id="cop-dem-2024_1-N21_E103",
        source_id=spec.source_id,
        source_version=spec.version,
        uri="https://download.example.test/odata/v1/Products(fixture)/$value",
        target_relative_path=Path("raw/cop_dem/2024_1/N21_E103/fixture.zip"),
        media_type="application/zip",
        license_id=spec.license_id,
        expected_size=4,
    )
    fetcher = HttpFetcher(
        context.paths,
        context.catalog,
        StorageBudget(context.paths.dataset, soft_cap_bytes=2**30, minimum_free_bytes=0),
        client=client,
    )

    record = CopDemAdapter(spec, client=client).fetch_raw(fetcher, authorized_context, remote)

    assert seen_download_headers == ["Bearer fixture-access-token"]
    assert record.source_uri == remote.uri
    assert "fixture-access-token" not in record.model_dump_json()


def test_authenticated_fetch_refreshes_token_once_after_unauthorized_response(
    context: SourceContext, spec: SourceSpec
) -> None:
    """Catches a CDSE download that gives up instead of refreshing one expired bearer token."""
    token_requests = 0
    download_headers: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_requests
        if request.url.host == "identity.example.test":
            token_requests += 1
            return httpx.Response(200, json={"access_token": f"fixture-token-{token_requests}"})
        download_headers.append(request.headers.get("Authorization", ""))
        if len(download_headers) == 1:
            return httpx.Response(401)
        return httpx.Response(200, stream=httpx.ByteStream(b"dem!"))

    environment = EnvironmentSettings(
        _env_file=None,
        cdse_username=SecretStr("fixture-user"),
        cdse_password=SecretStr("fixture-password"),
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))
    authorized_context = SourceContext(
        paths=context.paths,
        catalog=context.catalog,
        study_area=context.study_area,
        environment=environment,
        run_id=context.run_id,
    )
    remote = RemoteAsset(
        asset_id="cop-dem-2024_1-N21_E103",
        source_id=spec.source_id,
        source_version=spec.version,
        uri="https://download.example.test/odata/v1/Products(fixture)/$value",
        target_relative_path=Path("raw/cop_dem/2024_1/N21_E103/fixture.zip"),
        media_type="application/zip",
        license_id=spec.license_id,
        expected_size=4,
    )
    fetcher = HttpFetcher(
        context.paths,
        context.catalog,
        StorageBudget(context.paths.dataset, soft_cap_bytes=2**30, minimum_free_bytes=0),
        client=client,
    )

    assert CopDemAdapter(spec, client=client).fetch_raw(fetcher, authorized_context, remote).status is AssetStatus.FETCHED
    assert download_headers == ["Bearer fixture-token-1", "Bearer fixture-token-2"]
    assert token_requests == 2


def test_extract_dem_tiff_rejects_zip_slip_members(tmp_path: Path) -> None:
    """Catches archive extraction that could write outside an isolated temporary directory."""
    archive = tmp_path / "unsafe.zip"
    import zipfile

    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("../escape_DEM.tif", b"not-a-raster")

    with pytest.raises(UnsafeDemArchive, match="unsafe member"):
        extract_dem_tiff(archive, tmp_path / "extract")
