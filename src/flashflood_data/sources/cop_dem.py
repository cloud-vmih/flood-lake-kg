"""Authenticated, immutable Copernicus DEM GLO-30 acquisition through CDSE OData."""

from __future__ import annotations

import json
import shutil
import tempfile
import zipfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import geopandas as gpd
import httpx
import numpy as np
import rasterio
from pydantic import BaseModel, Field, SecretStr
from rasterio.enums import Resampling
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from flashflood_data.catalog import sha256_bundle, sha256_file
from flashflood_data.catalog.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    ValidationResult,
)
from flashflood_data.http import DownloadFailed, HttpFetcher
from flashflood_data.raster import (
    COG_PROFILE,
    RasterExpectation,
    mosaic_clip_to_cog,
    raster_coverage_ratio,
    validate_raster,
)
from flashflood_data.sources.base import SourceAdapter, SourceConfigurationError, SourceContext

_DEM_DTYPES = ("int16", "uint16", "float32", "float64")
_GRID_LATITUDE_LIMIT = 90
_GRID_LONGITUDE_LIMIT = 180


class MissingCredentials(RuntimeError):
    """Raised before any CDSE request when the local password grant is unavailable."""


class UnsafeDemArchive(ValueError):
    """Raised when a native product archive cannot be safely inspected."""


class CdseProduct(BaseModel):
    """The minimum immutable product identity needed for a safe CDSE download."""

    product_id: str
    name: str
    grid_id: str
    dataset: str
    product_type: str
    modification_date: datetime
    online: bool
    content_length: int | None = Field(default=None, ge=0)
    checksum: str | None = None


def _grid_part(value: int, positive: str, negative: str, width: int) -> str:
    return f"{positive if value >= 0 else negative}{abs(value):0{width}d}"


def grid_ids_for_geometry(geometry: BaseGeometry) -> list[str]:
    """Return sorted one-degree GLO-30 cell IDs covering a WGS84 AOI."""
    if geometry.is_empty:
        raise ValueError("DEM grid selection requires a non-empty geometry")
    west, south, east, north = geometry.bounds
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("DEM grid selection requires WGS84 bounds within the world extent")
    west_cell, south_cell = int(np.floor(west)), int(np.floor(south))
    east_cell, north_cell = int(np.ceil(east)) - 1, int(np.ceil(north)) - 1
    if east_cell < west_cell or north_cell < south_cell:
        raise ValueError("DEM grid selection requires positive AOI area")
    cells: list[str] = []
    for latitude in range(south_cell, north_cell + 1):
        for longitude in range(west_cell, east_cell + 1):
            if not (-_GRID_LATITUDE_LIMIT <= latitude < _GRID_LATITUDE_LIMIT):
                continue
            if not (-_GRID_LONGITUDE_LIMIT <= longitude < _GRID_LONGITUDE_LIMIT):
                continue
            cell = box(longitude, latitude, longitude + 1, latitude + 1)
            if geometry.intersects(cell):
                cells.append(
                    f"{_grid_part(latitude, 'N', 'S', 2)}_{_grid_part(longitude, 'E', 'W', 3)}"
                )
    return cells


def _attribute_values(product: Mapping[str, Any]) -> dict[str, str]:
    attributes = product.get("Attributes", ())
    if not isinstance(attributes, list):
        raise TypeError("CDSE product is missing Attributes")
    values: dict[str, str] = {}
    for attribute in attributes:
        if not isinstance(attribute, Mapping):
            raise TypeError("CDSE product attributes are malformed")
        name, value = attribute.get("Name"), attribute.get("Value")
        if isinstance(name, str) and isinstance(value, str):
            values[name] = value
    return values


def _product_from_odata(item: object) -> CdseProduct:
    if not isinstance(item, Mapping):
        raise TypeError("CDSE product response contains a malformed item")
    attributes = _attribute_values(item)
    product_id, name, modification_date = item.get("Id"), item.get("Name"), item.get("ModificationDate")
    grid_id = attributes.get("gridId")
    dataset = attributes.get("dataset")
    product_type = attributes.get("productType")
    if not all(isinstance(value, str) and value for value in (product_id, name, modification_date, grid_id, dataset, product_type)):
        raise ValueError("CDSE product lacks immutable DEM identity metadata")
    content_length = item.get("ContentLength")
    if content_length is not None and (isinstance(content_length, bool) or not isinstance(content_length, int)):
        raise ValueError("CDSE product ContentLength is malformed")
    checksum: str | None = None
    raw_checksum = item.get("Checksum")
    if isinstance(raw_checksum, str) and raw_checksum:
        checksum = raw_checksum
    online = item.get("Online")
    if not isinstance(online, bool):
        raise TypeError("CDSE product Online status is malformed")
    return CdseProduct(
        product_id=product_id,
        name=name,
        grid_id=grid_id,
        dataset=dataset,
        product_type=product_type,
        modification_date=datetime.fromisoformat(modification_date),
        online=online,
        content_length=content_length,
        checksum=checksum,
    )


def select_dem_product(response: Mapping[str, object], dataset: str, grid_id: str) -> CdseProduct:
    """Select exactly one active, exact-dataset product from an OData response."""
    raw_products = response.get("value")
    if not isinstance(raw_products, list):
        raise TypeError("CDSE OData response is missing a product list")
    products = sorted((_product_from_odata(item) for item in raw_products), key=lambda item: item.modification_date, reverse=True)
    grid_products = [
        product for product in products if product.grid_id == grid_id and product.online
    ]
    if not grid_products:
        raise ValueError(f"CDSE has no active product for grid {grid_id}")
    datasets = {product.dataset for product in grid_products}
    if datasets != {dataset}:
        raise ValueError(f"CDSE product has wrong dataset for grid {grid_id}")
    product_types = {product.product_type for product in grid_products}
    if len(product_types) != 1:
        raise ValueError(f"CDSE product has mixed product types for grid {grid_id}")
    if len(grid_products) != 1:
        raise ValueError(f"CDSE has multiple active products for grid {grid_id}")
    return grid_products[0]


class CdseTokenClient:
    """Memory-only CDSE password-grant client with no request or secret logging."""

    def __init__(
        self, token_url: str, environment, client: httpx.Client | None = None
    ) -> None:
        self.token_url = token_url
        self.environment = environment
        self.client = client or httpx.Client()
        self._access_token: SecretStr | None = None

    def _credentials(self) -> tuple[str, str]:
        username = self.environment.cdse_username
        password = self.environment.cdse_password
        if username is None or not username.get_secret_value():
            raise MissingCredentials("CDSE credentials missing: set FLASHFLOOD_CDSE_USERNAME locally")
        if password is None or not password.get_secret_value():
            raise MissingCredentials("CDSE credentials missing: set FLASHFLOOD_CDSE_PASSWORD locally")
        return username.get_secret_value(), password.get_secret_value()

    def get_access_token(self) -> SecretStr:
        """Exchange locally supplied credentials for a cached, in-memory bearer token."""
        if self._access_token is not None:
            return self._access_token
        username, password = self._credentials()
        response = self.client.post(
            self.token_url,
            data={
                "client_id": "cdse-public",
                "grant_type": "password",
                "username": username,
                "password": password,
            },
        )
        response.raise_for_status()
        payload = response.json()
        access_token = payload.get("access_token") if isinstance(payload, Mapping) else None
        if not isinstance(access_token, str) or not access_token:
            raise ValueError("CDSE token response is missing access_token")
        self._access_token = SecretStr(access_token)
        return self._access_token

    def invalidate(self) -> None:
        """Discard an expired in-memory token before one allowed refresh attempt."""
        self._access_token = None


def _safe_product_name(name: str) -> str:
    candidate = Path(name)
    if not name or candidate.name != name or name in {".", ".."}:
        raise ValueError("CDSE product name is not a safe raw filename")
    return name


def extract_dem_tiff(archive: Path, destination: Path) -> Path:
    """Safely materialize exactly one DEM TIFF from an immutable ZIP product copy."""
    try:
        with zipfile.ZipFile(archive) as bundle:
            members = bundle.infolist()
            for member in members:
                path = PurePosixPath(member.filename)
                if path.is_absolute() or ".." in path.parts:
                    raise UnsafeDemArchive("unsafe member in Copernicus DEM archive")
            dem_members = [member for member in members if member.filename.lower().endswith("_dem.tif")]
            if len(dem_members) != 1:
                raise UnsafeDemArchive("Copernicus DEM archive must contain exactly one *_DEM.tif member")
            member = dem_members[0]
            target = destination / PurePosixPath(member.filename)
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
            return target
    except zipfile.BadZipFile as exc:
        raise UnsafeDemArchive("Copernicus DEM product is not a readable ZIP archive") from exc


class CopDemAdapter(SourceAdapter):
    """Resolve authenticated GLO-30 products, retain them raw, and publish a continuous COG."""

    def __init__(self, spec, *, client: httpx.Client | None = None) -> None:
        super().__init__(spec)
        self.client = client or httpx.Client()

    def _setting(self, key: str) -> str:
        value = self.spec.settings.get(key)
        if not isinstance(value, str) or not value:
            raise SourceConfigurationError(f"Copernicus DEM setting {key!r} must be a non-empty string")
        return value

    def _token_client(self, context: SourceContext) -> CdseTokenClient:
        return CdseTokenClient(self._setting("token_url"), context.environment, self.client)

    def _aoi(self, context: SourceContext, name: str) -> BaseGeometry:
        path = context.paths.harmonized / "aoi" / f"{name}_aoi.geoparquet"
        if not path.is_file():
            raise ValueError(f"Copernicus DEM requires {name}_aoi.geoparquet")
        layer = gpd.read_parquet(path)
        if layer.empty or layer.crs is None:
            raise ValueError(f"{name} AOI is missing geometry or CRS")
        geographic = layer.to_crs("EPSG:4326") if layer.crs.to_string() != "EPSG:4326" else layer
        geometry = geographic.geometry.union_all()
        if geometry.is_empty:
            raise ValueError(f"{name} AOI is empty")
        return geometry

    def _catalogue_response(
        self, token_client: CdseTokenClient, grid_id: str
    ) -> Mapping[str, object]:
        dataset = self._setting("dataset")
        string_filter = (
            "Online eq true and "
            "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'gridId' "
            f"and att/OData.CSC.StringAttribute/Value eq '{grid_id}') and "
            "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'dataset' "
            f"and att/OData.CSC.StringAttribute/Value eq '{dataset}')"
        )
        params = {"$filter": string_filter, "$expand": "Attributes", "$orderby": "ModificationDate desc"}
        for attempt in range(2):
            token = token_client.get_access_token().get_secret_value()
            response = self.client.get(
                self._setting("catalogue_url"), params=params, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code == 401 and attempt == 0:
                token_client.invalidate()
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, Mapping):
                raise TypeError("CDSE OData response is not a JSON object")
            return payload
        raise AssertionError("unreachable")

    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        """Query one fixed collection product for each Environmental AOI one-degree cell."""
        del available
        token_client = self._token_client(context)
        token_client._credentials()
        products = [
            select_dem_product(self._catalogue_response(token_client, grid_id), self._setting("dataset"), grid_id)
            for grid_id in grid_ids_for_geometry(self._aoi(context, "environmental"))
        ]
        expected_type = self._setting("product_type")
        remotes: list[RemoteAsset] = []
        for product in products:
            if product.product_type != expected_type:
                raise ValueError(f"CDSE product has unexpected type for grid {product.grid_id}")
            if product.content_length is None or product.content_length <= 0:
                raise ValueError(f"CDSE product has no safe ContentLength for grid {product.grid_id}")
            name = _safe_product_name(product.name)
            remotes.append(
                RemoteAsset(
                    asset_id=f"cop-dem-{self.spec.version}-{product.grid_id}",
                    source_id=self.spec.source_id,
                    source_version=self.spec.version,
                    uri=self._setting("download_template").format(product_id=product.product_id),
                    target_relative_path=Path("raw") / "cop_dem" / self.spec.version / product.grid_id / name,
                    media_type="application/zip" if name.lower().endswith(".zip") else "image/tiff",
                    license_id=self.spec.license_id,
                    expected_size=product.content_length,
                    source_valid_time=product.modification_date.astimezone(UTC).isoformat(),
                )
            )
        return sorted(remotes, key=lambda remote: remote.asset_id)

    def fetch_raw(
        self, fetcher: HttpFetcher, context: SourceContext, remote: RemoteAsset
    ) -> AssetRecord:
        """Fetch one immutable product with an ephemeral bearer header never stored in metadata."""
        if remote.source_id != self.spec.source_id:
            raise ValueError("Copernicus DEM fetch received a remote from another source")
        token_client = self._token_client(context)
        for attempt in range(2):
            token = token_client.get_access_token().get_secret_value()
            try:
                return fetcher.fetch(remote, context.run_id, headers={"Authorization": f"Bearer {token}"})
            except DownloadFailed as exc:
                cause = exc.__cause__
                if (
                    attempt == 0
                    and isinstance(cause, httpx.HTTPStatusError)
                    and cause.response.status_code == 401
                ):
                    token_client.invalidate()
                    continue
                raise
        raise AssertionError("unreachable")

    def _materialize_tile(self, raw_path: Path, destination: Path) -> Path:
        if zipfile.is_zipfile(raw_path):
            return extract_dem_tiff(raw_path, destination)
        if raw_path.suffix.lower() != ".tif":
            raise ValueError(f"Copernicus DEM raw product is neither ZIP nor TIFF: {raw_path.name}")
        return raw_path

    def validate_raw(self, path: Path) -> ValidationResult:
        """Validate a raw TIFF or safely inspect its one DEM raster archive member."""
        try:
            with tempfile.TemporaryDirectory(prefix="cop-dem-validate-") as temporary:
                tile = self._materialize_tile(path, Path(temporary))
                return validate_raster(
                    tile,
                    RasterExpectation(dtypes=_DEM_DTYPES, crs="EPSG:4326", resolution_range=None, aoi=tile_bounds(tile)),
                )
        except (OSError, ValueError, UnsafeDemArchive) as exc:
            return ValidationResult(
                passed=False,
                checks={"archive_safety": False, "readable": False},
                messages=(str(exc),),
            )

    def _float_copy(self, source: Path, destination: Path) -> Path:
        with rasterio.open(source) as dataset:
            profile = dataset.profile.copy()
            profile.update(COG_PROFILE, dtype="float32", nodata=float(dataset.nodata) if dataset.nodata is not None else None)
            with rasterio.open(destination, "w", **profile) as output:
                for _, window in dataset.block_windows(1):
                    output.write(dataset.read(window=window).astype("float32"), window=window)
                    output.write_mask(dataset.read_masks(1, window=window), window=window)
        return destination

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        """Mosaic validated native DEMs and exact-clip a float COG to the Environmental AOI."""
        environmental = self._aoi(context, "environmental")
        hydrological = self._aoi(context, "hydrological")
        raw_assets = [asset for asset in assets if asset.source_id == self.spec.source_id and asset.kind is AssetKind.RAW]
        if not raw_assets:
            raise ValueError("Copernicus DEM harmonization is missing raw products")
        raw_paths = [Path(asset.storage_path) for asset in sorted(raw_assets, key=lambda item: item.asset_id)]
        for asset, raw_path in zip(sorted(raw_assets, key=lambda item: item.asset_id), raw_paths, strict=True):
            validation = self.validate_raw(raw_path)
            if not validation.passed:
                raise ValueError(f"Copernicus DEM raw validation failed for {asset.asset_id}: {validation.messages}")
        output_path = context.paths.harmonized / "rasters" / "dem_glo30.tif"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="cop-dem-harmonize-", dir=output_path.parent) as temporary:
            temporary_path = Path(temporary)
            materialized = [self._materialize_tile(path, temporary_path / f"product-{index}") for index, path in enumerate(raw_paths)]
            float_tiles = [
                self._float_copy(path, temporary_path / f"tile-{index}.tif") for index, path in enumerate(materialized)
            ]
            mosaic_clip_to_cog(float_tiles, environmental, output_path, Resampling.bilinear)
        coverage = raster_coverage_ratio(output_path, hydrological)
        minimum = context.study_area.environmental_raster_coverage_min_pct / 100
        if coverage < minimum:
            raise ValueError(f"Copernicus DEM Hydrological AOI coverage {coverage:.2%} is below {minimum:.2%}")
        now = datetime.now(UTC)
        metadata = {
            "coverage_ratio_hydrological_aoi": coverage,
            "grid_policy": "native GLO-30 grid; bilinear only if reprojection is requested",
            "resampling": "bilinear",
            "validation": dict(validate_raster(
                output_path,
                RasterExpectation(dtypes=("float32",), crs="EPSG:4326", resolution_range=None, aoi=environmental),
            ).metrics),
        }
        output = AssetRecord(
            asset_id=f"cop-dem-{self.spec.version}-harmonized",
            source_id=self.spec.source_id,
            source_version=self.spec.version,
            kind=AssetKind.HARMONIZED,
            source_uri="generated:cop-dem-environmental-aoi-mosaic",
            storage_path=str(output_path),
            media_type="image/tiff",
            size_bytes=output_path.stat().st_size,
            checksum=sha256_file(output_path),
            retrieved_at=now,
            source_valid_time=self.spec.version,
            license_id=self.spec.license_id,
            pipeline_run_id=context.run_id,
            status=AssetStatus.HARMONIZED,
            dependency_fingerprint=sha256_bundle(raw_paths),
            metadata_json=json.dumps(metadata, sort_keys=True, separators=(",", ":")),
        )
        context.catalog.upsert(output)
        return [output]


def tile_bounds(path: Path) -> BaseGeometry:
    """Return the geographic extent of a tile for structural raw validation."""
    with rasterio.open(path) as dataset:
        return box(*dataset.bounds)
