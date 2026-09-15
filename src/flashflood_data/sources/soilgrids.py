"""Immutable AOI-bounded SoilGrids WCS acquisition and harmonization."""

from __future__ import annotations

import gzip
import io
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from xml.etree import ElementTree

import geopandas as gpd
from pyproj import Geod
from rasterio.enums import Resampling
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from flashflood_data.catalog import sha256_file
from flashflood_data.catalog.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    ValidationResult,
)
from flashflood_data.raster import RasterExpectation, mosaic_clip_to_cog, validate_raster
from flashflood_data.sources.base import SourceAdapter, SourceConfigurationError, SourceContext

DEFAULT_ENDPOINT_TEMPLATE: Final = "https://maps.isric.org/mapserv?map=/map/{property}.map"
DEFAULT_SOURCE_ID: Final = "soilgrids_2_0"
DEFAULT_SOURCE_VERSION: Final = "2.0"
DEFAULT_LICENSE_ID: Final = "CC-BY-4.0"
DEFAULT_OUTPUT_CRS: Final = "EPSG:4326"
_WORLD = box(-180, -90, 180, 90)
_MAX_CAPABILITIES_XML_BYTES = 16 * 1024 * 1024

_PROPERTY_METADATA: Final[dict[str, tuple[str, int]]] = {
    "clay": ("g/kg", 10),
    "sand": ("g/kg", 10),
    "silt": ("g/kg", 10),
    "bdod": ("cg/cm3", 100),
    "cfvo": ("cm3/dm3", 10),
    "wv0010": ("cm3/cm3", 10),
    "wv0033": ("cm3/cm3", 10),
    "wv1500": ("cm3/cm3", 10),
}


@dataclass(frozen=True)
class CoverageGrid:
    """Axis and CRS details advertised by a WCS DescribeCoverage response."""

    crs: str
    axes: tuple[str, str]


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_coverages(capabilities_xml: bytes) -> set[str]:
    """Return advertised WCS coverage identifiers without assuming XML prefixes."""
    if capabilities_xml.startswith(b"\x1f\x8b"):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(capabilities_xml)) as stream:
                capabilities_xml = stream.read(_MAX_CAPABILITIES_XML_BYTES + 1)
        except OSError as exc:
            raise ValueError("SoilGrids capabilities gzip payload is malformed") from exc
        if len(capabilities_xml) > _MAX_CAPABILITIES_XML_BYTES:
            raise ValueError("SoilGrids capabilities XML exceeds the decode limit")
    try:
        root = ElementTree.fromstring(capabilities_xml)
    except ElementTree.ParseError as exc:
        raise ValueError("SoilGrids capabilities XML is not well formed") from exc
    return {
        (element.text or "").strip()
        for element in root.iter()
        if _local_name(element.tag) == "CoverageId" and (element.text or "").strip()
    }


def parse_coverage_grid(describe_xml: bytes) -> CoverageGrid:
    """Read the advertised CRS and two WCS subset-axis labels from DescribeCoverage."""
    try:
        root = ElementTree.fromstring(describe_xml)
    except ElementTree.ParseError as exc:
        raise ValueError("SoilGrids DescribeCoverage XML is not well formed") from exc
    envelope = next((item for item in root.iter() if _local_name(item.tag) == "Envelope"), None)
    if envelope is None:
        raise ValueError("SoilGrids DescribeCoverage has no envelope")
    crs = envelope.get("srsName")
    axes = tuple((envelope.get("axisLabels") or "").split())
    if not crs or len(axes) != 2:
        raise ValueError("SoilGrids DescribeCoverage lacks a two-dimensional advertised grid")
    return CoverageGrid(crs=crs, axes=(axes[0], axes[1]))


def _format_coordinate(value: float) -> str:
    return format(value, ".15g")


def _request_uri(endpoint: str, parameters: list[tuple[str, str]]) -> str:
    parsed = urlsplit(endpoint)
    existing = parse_qsl(parsed.query, keep_blank_values=True)
    query = urlencode([*existing, *parameters], doseq=True, safe="(),:-")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _asset_id(property_id: str, depth: str, statistic: str) -> str:
    return f"soilgrids-2-0-{property_id}-{depth}-{statistic}"


def build_wcs_getcoverage(
    property_id: str,
    depth: str,
    statistic: str,
    bbox: tuple[float, float, float, float],
    *,
    endpoint_template: str = DEFAULT_ENDPOINT_TEMPLATE,
    source_id: str = DEFAULT_SOURCE_ID,
    source_version: str = DEFAULT_SOURCE_VERSION,
    license_id: str = DEFAULT_LICENSE_ID,
    output_crs: str = DEFAULT_OUTPUT_CRS,
    axes: tuple[str, str] = ("Long", "Lat"),
    target_resolution_m: float,
    budget_size_bytes: int,
) -> RemoteAsset:
    """Build one exact, URL-encoded WCS 2.0.1 GeoTIFF subset request."""
    west, south, east, north = bbox
    if west >= east or south >= north:
        raise ValueError("SoilGrids request bbox must have positive area")
    if target_resolution_m <= 0:
        raise ValueError("SoilGrids target resolution must be positive")
    geod = Geod(ellps="WGS84")
    center_lon = (west + east) / 2
    center_lat = (south + north) / 2
    width_m = abs(geod.inv(west, center_lat, east, center_lat)[2])
    height_m = abs(geod.inv(center_lon, south, center_lon, north)[2])
    width = max(1, int(width_m / target_resolution_m + 0.999999999))
    height = max(1, int(height_m / target_resolution_m + 0.999999999))
    coverage_id = f"{property_id}_{depth}_{statistic}"
    uri = _request_uri(
        endpoint_template.format(property=property_id),
        [
            ("SERVICE", "WCS"),
            ("VERSION", "2.0.1"),
            ("REQUEST", "GetCoverage"),
            ("COVERAGEID", coverage_id),
            ("FORMAT", "GEOTIFF_INT16"),
            ("OUTPUTCRS", output_crs),
            ("SUBSETTINGCRS", "EPSG:4326"),
            ("SUBSET", f"{axes[0]}({_format_coordinate(west)},{_format_coordinate(east)})"),
            ("SUBSET", f"{axes[1]}({_format_coordinate(south)},{_format_coordinate(north)})"),
            ("SCALESIZE", f"{axes[0]}({width})"),
            ("SCALESIZE", f"{axes[1]}({height})"),
        ],
    )
    return RemoteAsset(
        asset_id=_asset_id(property_id, depth, statistic),
        source_id=source_id,
        source_version=source_version,
        uri=uri,
        target_relative_path=Path(f"raw/soilgrids/{source_version}/{property_id}/{depth}/{statistic}.tif"),
        media_type="image/tiff",
        license_id=license_id,
        source_valid_time=source_version,
        budget_size_bytes=budget_size_bytes,
    )


class SoilGridsAdapter(SourceAdapter):
    """Discover verified coverages, retain raw WCS TIFFs, and clip integer COGs."""

    def _strings(self, key: str) -> tuple[str, ...]:
        value = self.spec.settings.get(key)
        if not isinstance(value, (list, tuple)) or not value or not all(isinstance(item, str) for item in value):
            raise SourceConfigurationError(f"SoilGrids setting {key!r} must be a non-empty string list")
        return tuple(value)

    def _setting(self, key: str) -> str:
        value = self.spec.settings.get(key)
        if not isinstance(value, str) or not value:
            raise SourceConfigurationError(f"SoilGrids setting {key!r} must be a non-empty string")
        return value

    def _budget_setting(self, key: str) -> int:
        value = self.spec.settings.get(key)
        if type(value) is not int or value <= 0:
            raise SourceConfigurationError(
                f"SoilGrids setting {key!r} must be a positive integer"
            )
        return value

    def _resolution_setting(self, key: str) -> float:
        value = self.spec.settings.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise SourceConfigurationError(
                f"SoilGrids setting {key!r} must be a positive number"
            )
        return float(value)

    def _capability_id(self, property_id: str) -> str:
        return f"soilgrids-2-0-{property_id}-capabilities"

    def _capabilities_remote(self, property_id: str) -> RemoteAsset:
        endpoint = self._setting("endpoint_template").format(property=property_id)
        return RemoteAsset(
            asset_id=self._capability_id(property_id),
            source_id=self.spec.source_id,
            source_version=self.spec.version,
            uri=_request_uri(endpoint, [("SERVICE", "WCS"), ("VERSION", "2.0.1"), ("REQUEST", "GetCapabilities")]),
            target_relative_path=Path(f"raw/soilgrids/{self.spec.version}/{property_id}/capabilities.xml"),
            media_type="application/xml",
            license_id=self.spec.license_id,
            source_valid_time=self.spec.version,
            budget_size_bytes=self._budget_setting("capabilities_budget_size_bytes"),
        )

    def _environmental_aoi(self, context: SourceContext) -> BaseGeometry:
        path = context.paths.harmonized / "aoi" / "environmental_aoi.geoparquet"
        if not path.is_file():
            raise ValueError("SoilGrids resolution requires environmental_aoi.geoparquet")
        layer = gpd.read_parquet(path)
        if layer.empty or layer.crs is None:
            raise ValueError("environmental AOI is missing geometry or CRS")
        output_crs = self._setting("output_crs")
        projected = layer.to_crs(output_crs) if layer.crs.to_string() != output_crs else layer
        geometry = projected.geometry.union_all()
        if geometry.is_empty:
            raise ValueError("environmental AOI is empty")
        return geometry

    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        """Fetch capabilities first, then declare only verified configured coverages."""
        properties = self._strings("properties")
        depths = self._strings("depths")
        statistics = self._strings("statistics")
        if self._setting("format") != "GEOTIFF_INT16":
            raise SourceConfigurationError("SoilGrids requires GEOTIFF_INT16 raw products")
        capabilities = {asset.asset_id: asset for asset in available if asset.asset_id.endswith("-capabilities")}
        if not capabilities:
            return [self._capabilities_remote(property_id) for property_id in properties]

        requested_by_property = {
            property_id: {f"{property_id}_{depth}_{statistic}" for depth in depths for statistic in statistics}
            for property_id in properties
        }
        for property_id, required in requested_by_property.items():
            try:
                capability = capabilities[self._capability_id(property_id)]
            except KeyError:
                continue
            advertised = parse_coverages(Path(capability.storage_path).read_bytes())
            missing = sorted(required - advertised)
            if missing:
                raise ValueError(f"SoilGrids {property_id} is missing configured coverages: {', '.join(missing)}")
        missing_capabilities = [property_id for property_id in properties if self._capability_id(property_id) not in capabilities]
        if missing_capabilities:
            return [self._capabilities_remote(property_id) for property_id in missing_capabilities]

        bounds = self._environmental_aoi(context).bounds
        output_crs = self._setting("output_crs")
        remotes = [
            build_wcs_getcoverage(
                property_id,
                depth,
                statistic,
                bounds,
                endpoint_template=self._setting("endpoint_template"),
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                license_id=self.spec.license_id,
                output_crs=output_crs,
                target_resolution_m=self._resolution_setting("target_resolution_m"),
                budget_size_bytes=self._budget_setting("coverage_budget_size_bytes"),
            )
            for property_id in properties
            for depth in depths
            for statistic in statistics
        ]
        return sorted(remotes, key=lambda asset: asset.asset_id)

    def validate_raw(self, path: Path) -> ValidationResult:
        """Validate an unchanged WCS capabilities XML or GeoTIFF payload."""
        if path.suffix.lower() == ".xml":
            try:
                advertised = parse_coverages(path.read_bytes())
                property_id = path.parent.name
                required = {
                    f"{property_id}_{depth}_{statistic}"
                    for depth in self._strings("depths")
                    for statistic in self._strings("statistics")
                }
                checks = {
                    "capabilities_xml": True,
                    "coverage_ids": bool(advertised),
                    "configured_coverages": required.issubset(advertised),
                }
                return ValidationResult(
                    passed=all(checks.values()),
                    checks=checks,
                    metrics={"coverage_count": len(advertised)},
                )
            except (OSError, ValueError) as exc:
                return ValidationResult(
                    passed=False,
                    checks={"capabilities_xml": False, "coverage_ids": False},
                    messages=(str(exc),),
                )
        return validate_raster(
            path,
            RasterExpectation(dtypes=("int16",), crs=self._setting("output_crs"), resolution_range=None, aoi=_WORLD),
        )

    def _raw_parts(self, context: SourceContext, asset: AssetRecord) -> tuple[str, str, str]:
        raw_root = context.paths.raw / "soilgrids" / self.spec.version
        try:
            property_id, depth, filename = Path(asset.storage_path).relative_to(raw_root).parts
        except ValueError as exc:
            raise ValueError(f"SoilGrids raw asset is outside its immutable source path: {asset.asset_id}") from exc
        if Path(filename).suffix != ".tif":
            raise ValueError(f"SoilGrids raw asset is not a TIFF: {asset.asset_id}")
        statistic = Path(filename).stem
        return property_id, depth, statistic

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        """Clip validated int16 raw files to the exact Environmental AOI as COGs."""
        aoi = self._environmental_aoi(context)
        expected = {
            (property_id, depth, statistic)
            for property_id in self._strings("properties")
            for depth in self._strings("depths")
            for statistic in self._strings("statistics")
        }
        raw_assets = [
            asset
            for asset in assets
            if asset.source_id == self.spec.source_id
            and asset.kind is AssetKind.RAW
            and Path(asset.storage_path).suffix.lower() in {".tif", ".tiff"}
        ]
        indexed = {self._raw_parts(context, asset): asset for asset in raw_assets}
        missing = sorted(expected - indexed.keys())
        if missing:
            raise ValueError(f"SoilGrids harmonization is missing raw coverages: {missing}")

        now = datetime.now(UTC)
        outputs: list[AssetRecord] = []
        for property_id, depth, statistic in sorted(expected):
            raw = indexed[(property_id, depth, statistic)]
            raw_path = Path(raw.storage_path)
            validation = validate_raster(
                raw_path,
                RasterExpectation(
                    dtypes=("int16",),
                    crs=self._setting("output_crs"),
                    resolution_range=None,
                    aoi=aoi,
                ),
            )
            if not validation.passed:
                raise ValueError(f"SoilGrids raw validation failed for {raw.asset_id}: {validation.messages}")
            output_path = context.paths.harmonized / "soilgrids" / property_id / depth / f"{statistic}.tif"
            mosaic_clip_to_cog([raw_path], aoi, output_path, Resampling.bilinear)
            unit, divisor = _PROPERTY_METADATA[property_id]
            metadata = {
                "depth": depth,
                "property": property_id,
                "raw_value_divisor": divisor,
                "resampling": "bilinear",
                "scale_factor": 1 / divisor,
                "statistic": statistic,
                "unit": unit,
                "validation": dict(validation.metrics),
            }
            output = AssetRecord(
                asset_id=f"soilgrids-2-0-{property_id}-{depth}-{statistic}-harmonized",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                kind=AssetKind.HARMONIZED,
                source_uri="generated:soilgrids-aoi-clip",
                storage_path=str(output_path),
                media_type="image/tiff",
                size_bytes=output_path.stat().st_size,
                checksum=sha256_file(output_path),
                retrieved_at=now,
                source_valid_time=self.spec.version,
                license_id=self.spec.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.HARMONIZED,
                dependency_fingerprint=raw.checksum,
                metadata_json=json.dumps(metadata, sort_keys=True, separators=(",", ":")),
            )
            context.catalog.upsert(output)
            outputs.append(output)
        return outputs
