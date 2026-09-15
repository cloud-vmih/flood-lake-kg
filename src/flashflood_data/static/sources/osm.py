"""Timestamped Geofabrik OSM acquisition metadata and Exposure-AOI extraction."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import geopandas as gpd
import httpx
import pandas as pd
import pyogrio
from shapely.geometry.base import BaseGeometry

from flashflood_data.catalog import sha256_bundle, sha256_file
from flashflood_data.catalog.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    ValidationResult,
)
from flashflood_data.static.sources.base import (
    SourceAdapter,
    SourceConfigurationError,
    SourceContext,
)
from flashflood_data.static.spatial.vector import write_geoparquet

_MD5 = re.compile(r"^[0-9a-fA-F]{32}$")
_FACILITY_AMENITIES = frozenset(
    {"hospital", "clinic", "school", "kindergarten", "college", "university", "fire_station", "police", "townhall", "shelter"}
)
_SETTLEMENT_PLACES = frozenset({"city", "town", "village", "hamlet", "locality"})
_TAG_EXCLUSIONS = frozenset({"geometry", "osm_id", "osm_way_id", "osm_type"})


@dataclass(frozen=True)
class SnapshotMetadata:
    """The immutable identity and upstream integrity evidence for a PBF snapshot."""

    source_valid_time: datetime
    target_name: str
    content_length: int
    md5: str


@dataclass(frozen=True)
class OSMOutputs:
    """GeoParquet products extracted for the Exposure AOI."""

    roads: Path
    bridges: Path
    facilities: Path
    settlements: Path
    water_context: Path


def parse_geofabrik_metadata(headers: httpx.Headers | dict[str, str], md5_text: str) -> SnapshotMetadata:
    """Parse a dated Geofabrik PBF identity from HEAD headers and its MD5 sidecar."""
    last_modified = headers.get("Last-Modified")
    raw_length = headers.get("Content-Length")
    if not last_modified:
        raise ValueError("Geofabrik PBF HEAD is missing Last-Modified")
    if raw_length is None:
        raise ValueError("Geofabrik PBF HEAD is missing Content-Length")
    try:
        source_valid_time = parsedate_to_datetime(last_modified).astimezone(UTC)
        content_length = int(raw_length)
    except (TypeError, ValueError) as exc:
        raise ValueError("Geofabrik PBF HEAD has invalid snapshot metadata") from exc
    if content_length <= 0:
        raise ValueError("Geofabrik PBF HEAD has no safe Content-Length")
    token = md5_text.strip().split(maxsplit=1)[0] if md5_text.strip() else ""
    if not _MD5.fullmatch(token):
        raise ValueError("Geofabrik MD5 sidecar is malformed")
    return SnapshotMetadata(
        source_valid_time=source_valid_time,
        target_name=f"vietnam-{source_valid_time:%Y%m%d}.osm.pbf",
        content_length=content_length,
        md5=token.lower(),
    )


def validate_snapshot_md5(path: Path, expected_md5: str) -> None:
    """Raise when a downloaded PBF differs from the upstream MD5 sidecar."""
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected_md5.lower():
        raise ValueError(f"Geofabrik PBF MD5 mismatch for {path.name}")


def _truthy(value: object) -> bool:
    return isinstance(value, str) and value.strip().lower() not in {"", "0", "false", "no", "none"}


def _source_identity(value: object, default_type: str) -> str:
    if value is None or bool(pd.isna(value)):
        raise ValueError("OSM feature is missing osm_id")
    text = str(value).strip()
    if not text:
        raise ValueError("OSM feature is missing osm_id")
    if re.fullmatch(r"(?:node|way|relation)/[^/]+", text):
        return text
    return f"{default_type}/{text}"


def _json_value(value: object) -> object:
    if hasattr(value, "item"):
        value = value.item()  # numpy scalar values are not JSON serializable by default.
    return value


def _tags(record: pd.Series) -> str:
    tags: dict[str, object] = {}
    for key, value in record.items():
        if key in _TAG_EXCLUSIONS or key == "other_tags" or pd.isna(value):
            continue
        tags[key] = _json_value(value)
    other = record.get("other_tags")
    if isinstance(other, str) and other:
        try:
            parsed = json.loads(other)
        except json.JSONDecodeError:
            tags["other_tags"] = other
        else:
            if isinstance(parsed, dict):
                tags.update(parsed)
            else:
                tags["other_tags"] = other
    return json.dumps(tags, sort_keys=True, separators=(",", ":"), default=str)


def _with_id(layer: gpd.GeoDataFrame, default_type: str) -> gpd.GeoDataFrame:
    if layer.empty:
        return layer.copy()
    if "osm_id" not in layer.columns and "osm_way_id" not in layer.columns:
        raise ValueError(f"OSM {default_type} layer is missing osm_id")
    result = layer.copy()
    source_type = result["osm_type"] if "osm_type" in result.columns else pd.Series(default_type, index=result.index)
    source_id = result["osm_id"] if "osm_id" in result.columns else pd.Series(None, index=result.index)
    way_id = result["osm_way_id"] if "osm_way_id" in result.columns else pd.Series(None, index=result.index)
    identities: list[str] = []
    for value, fallback_way_id, kind in zip(source_id, way_id, source_type, strict=True):
        identity_type = str(kind) if isinstance(kind, str) and kind else default_type
        if value is None or bool(pd.isna(value)):
            value = fallback_way_id
            identity_type = "way"
        identities.append(_source_identity(value, identity_type))
    result["osm_id"] = identities
    result["tags_json"] = result.apply(_tags, axis=1)
    return result


def _clip(layer: gpd.GeoDataFrame, exposure_aoi: BaseGeometry) -> gpd.GeoDataFrame:
    if layer.empty:
        return layer.copy()
    if layer.crs is None:
        raise ValueError("OSM layer is missing CRS")
    geographic = layer.to_crs("EPSG:4326") if layer.crs.to_string() != "EPSG:4326" else layer.copy()
    clipped = geographic.loc[geographic.geometry.intersects(exposure_aoi)].copy()
    clipped.geometry = clipped.geometry.intersection(exposure_aoi)
    return clipped.loc[clipped.geometry.notna() & ~clipped.geometry.is_empty].reset_index(drop=True)


def _empty(crs: object = "EPSG:4326") -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame({"osm_id": pd.Series(dtype="object"), "tags_json": pd.Series(dtype="object")}, geometry=gpd.GeoSeries([], crs=crs), crs=crs)


def normalize_roads(lines: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Explode road geometries in source order with stable OSM way/part identifiers."""
    roads = _with_id(lines, "way")
    if roads.empty:
        result = _empty(roads.crs or "EPSG:4326")
        result["segment_id"] = pd.Series(dtype="object")
        return result
    if "highway" not in roads.columns:
        return normalize_roads(roads.iloc[0:0].copy())
    rows: list[dict[str, object]] = []
    for _, feature in roads.loc[roads["highway"].notna()].iterrows():
        geometry = feature.geometry
        parts = list(geometry.geoms) if geometry.geom_type == "MultiLineString" else [geometry]
        for part_index, part in enumerate(parts):
            if part.is_empty or part.geom_type != "LineString":
                continue
            row = feature.drop(labels="geometry").to_dict()
            row["segment_id"] = f"{feature.osm_id}:{part_index:03d}"
            row["geometry"] = part
            rows.append(row)
    if not rows:
        result = _empty(roads.crs)
        result["segment_id"] = pd.Series(dtype="object")
        return result
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=roads.crs).reset_index(drop=True)


def _matches(layer: gpd.GeoDataFrame, column: str, values: frozenset[str]) -> pd.Series:
    if column not in layer.columns:
        return pd.Series(False, index=layer.index)
    return layer[column].astype("string").str.lower().isin(values).fillna(False)


def _nonempty_tag(layer: gpd.GeoDataFrame, column: str) -> pd.Series:
    return layer[column].map(_truthy) if column in layer.columns else pd.Series(False, index=layer.index)


@contextmanager
def _osm_config(path: Path) -> Iterator[None]:
    """Temporarily point GDAL's OSM driver at the committed tag configuration."""
    if not path.is_file():
        raise ValueError(f"OSM GDAL configuration is missing: {path}")
    previous = pyogrio.get_gdal_config_option("OSM_CONFIG_FILE")
    pyogrio.set_gdal_config_options({"OSM_CONFIG_FILE": str(path)})
    try:
        yield
    finally:
        pyogrio.set_gdal_config_options({"OSM_CONFIG_FILE": previous})


def _read_layers(pbf_path: Path, exposure_aoi: BaseGeometry, osmconf_path: Path) -> dict[str, gpd.GeoDataFrame]:
    with _osm_config(osmconf_path):
        return {
            layer: pyogrio.read_dataframe(pbf_path, layer=layer, bbox=exposure_aoi.bounds)
            for layer in ("lines", "multilinestrings", "points", "multipolygons")
        }


def _write(layer: gpd.GeoDataFrame, path: Path) -> Path:
    if layer.crs is None:
        layer = layer.set_crs("EPSG:4326")
    return write_geoparquet(layer, path)


def extract_osm_layers(
    pbf_path: Path,
    exposure_aoi: BaseGeometry,
    osmconf_path: Path,
    *,
    output_dir: Path | None = None,
) -> OSMOutputs:
    """Read configured PBF layers, exact-clip them, and write approved GeoParquet products."""
    if exposure_aoi.is_empty:
        raise ValueError("OSM extraction requires a non-empty Exposure AOI")
    layers = _read_layers(pbf_path, exposure_aoi, osmconf_path)
    lines = pd.concat(
        [_with_id(_clip(layers["lines"], exposure_aoi), "way"), _with_id(_clip(layers["multilinestrings"], exposure_aoi), "relation")],
        ignore_index=True,
    )
    lines = gpd.GeoDataFrame(lines, geometry="geometry", crs="EPSG:4326") if not lines.empty else _empty()
    points = _with_id(_clip(layers["points"], exposure_aoi), "node")
    polygons = _with_id(_clip(layers["multipolygons"], exposure_aoi), "relation")
    point_and_polygon = pd.concat([points, polygons], ignore_index=True)
    point_and_polygon = gpd.GeoDataFrame(point_and_polygon, geometry="geometry", crs="EPSG:4326") if not point_and_polygon.empty else _empty()

    roads = normalize_roads(lines)
    bridges = lines.loc[_nonempty_tag(lines, "bridge")].copy()
    facilities = point_and_polygon.loc[
        _matches(point_and_polygon, "amenity", _FACILITY_AMENITIES)
        | _nonempty_tag(point_and_polygon, "healthcare")
        | _nonempty_tag(point_and_polygon, "emergency")
        | _nonempty_tag(point_and_polygon, "government")
    ].copy()
    settlements = point_and_polygon.loc[_matches(point_and_polygon, "place", _SETTLEMENT_PLACES)].copy()
    water_context = pd.concat([lines, point_and_polygon], ignore_index=True)
    water_context = gpd.GeoDataFrame(water_context, geometry="geometry", crs="EPSG:4326") if not water_context.empty else _empty()
    water_context = water_context.loc[
        _nonempty_tag(water_context, "water")
        | _matches(water_context, "natural", frozenset({"water", "bay"}))
        | _matches(water_context, "landuse", frozenset({"reservoir"}))
        | _nonempty_tag(water_context, "waterway")
    ].copy()

    target = output_dir or pbf_path.parent / "exposure"
    return OSMOutputs(
        roads=_write(roads, target / "road_segment.geoparquet"),
        bridges=_write(bridges, target / "bridge.geoparquet"),
        facilities=_write(facilities, target / "facility.geoparquet"),
        settlements=_write(settlements, target / "settlement.geoparquet"),
        water_context=_write(water_context, target / "water_context.geoparquet"),
    )


class GeofabrikOsmAdapter(SourceAdapter):
    """Resolve a dated Vietnam PBF snapshot and harmonize its Exposure-AOI layers."""

    def __init__(self, spec, *, client: httpx.Client | None = None) -> None:
        super().__init__(spec)
        self.client = client or httpx.Client()

    def _setting(self, key: str) -> str:
        value = self.spec.settings.get(key)
        if not isinstance(value, str) or not value:
            raise SourceConfigurationError(f"Geofabrik setting {key!r} must be a non-empty string")
        return value

    def _pbf_metadata(self) -> SnapshotMetadata:
        pbf_url = self._setting("pbf_url")
        head = self.client.head(pbf_url, follow_redirects=True)
        head.raise_for_status()
        return parse_geofabrik_metadata(head.headers, "0" * 32)

    def _sidecar_size(self) -> int:
        response = self.client.head(self._setting("md5_url"), follow_redirects=True)
        response.raise_for_status()
        try:
            size = int(response.headers["Content-Length"])
        except (KeyError, ValueError) as exc:
            raise ValueError("Geofabrik MD5 HEAD has no safe Content-Length") from exc
        if size <= 0:
            raise ValueError("Geofabrik MD5 HEAD has no safe Content-Length")
        return size

    @staticmethod
    def _relative(metadata: SnapshotMetadata) -> Path:
        return Path("raw") / "osm" / "geofabrik" / f"{metadata.source_valid_time:%Y%m%d}"

    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        """Return MD5 first, then the exact dated PBF after the sidecar is retained."""
        del context
        metadata = self._pbf_metadata()
        date = f"{metadata.source_valid_time:%Y%m%d}"
        relative = self._relative(metadata)
        sidecar_name = f"{metadata.target_name}.md5"
        sidecar_assets = [
            asset for asset in available
            if asset.source_id == self.spec.source_id and asset.kind is AssetKind.RAW
            and Path(asset.storage_path).name == sidecar_name
        ]
        if len(sidecar_assets) > 1:
            raise ValueError("Geofabrik snapshot has multiple retained MD5 sidecars")
        if not sidecar_assets:
            return [
                RemoteAsset(
                    asset_id=f"geofabrik-osm-{date}-md5",
                    source_id=self.spec.source_id,
                    source_version=self.spec.version,
                    uri=self._setting("md5_url"),
                    target_relative_path=relative / sidecar_name,
                    media_type="text/plain",
                    license_id=self.spec.license_id,
                    expected_size=self._sidecar_size(),
                    source_valid_time=metadata.source_valid_time.isoformat(),
                )
            ]
        sidecar_path = Path(sidecar_assets[0].storage_path)
        if not sidecar_path.is_file():
            raise ValueError(f"Geofabrik retained MD5 sidecar is missing: {sidecar_path}")
        parse_geofabrik_metadata(
            {"Last-Modified": metadata.source_valid_time.strftime("%a, %d %b %Y %H:%M:%S GMT"), "Content-Length": str(metadata.content_length)},
            sidecar_path.read_text(encoding="ascii"),
        )
        return [
            RemoteAsset(
                asset_id=f"geofabrik-osm-{date}-pbf",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                uri=self._setting("pbf_url"),
                target_relative_path=relative / metadata.target_name,
                media_type="application/vnd.openstreetmap.data+pbf",
                license_id=self.spec.license_id,
                expected_size=metadata.content_length,
                source_valid_time=metadata.source_valid_time.isoformat(),
            )
        ]

    def validate_raw(self, path: Path) -> ValidationResult:
        """Validate either an upstream MD5 sidecar or its immutable PBF payload."""
        if path.suffix.lower() == ".md5":
            try:
                text = path.read_text(encoding="ascii")
                token = text.strip().split(maxsplit=1)[0] if text.strip() else ""
                checks = {
                    "md5_sidecar": path.name.endswith(".osm.pbf.md5"),
                    "md5_digest": bool(_MD5.fullmatch(token)),
                    "non_empty": path.stat().st_size > 0,
                }
            except (OSError, UnicodeError):
                checks = {"md5_sidecar": False, "md5_digest": False, "non_empty": False}
            passed = all(checks.values())
            return ValidationResult(
                passed=passed,
                checks=checks,
                metrics={"size_bytes": path.stat().st_size if path.is_file() else 0},
                messages=() if passed else ("raw Geofabrik MD5 sidecar is malformed",),
            )
        passed = path.is_file() and path.suffixes[-2:] == [".osm", ".pbf"] and path.stat().st_size > 0
        return ValidationResult(
            passed=passed,
            checks={"pbf_name": path.suffixes[-2:] == [".osm", ".pbf"], "non_empty": path.is_file() and path.stat().st_size > 0},
            metrics={"size_bytes": path.stat().st_size if path.is_file() else 0},
            messages=() if passed else ("raw Geofabrik OSM PBF is missing, misnamed, or empty",),
        )

    def _exposure_aoi(self, context: SourceContext) -> BaseGeometry:
        path = context.paths.harmonized / "aoi" / "exposure_aoi.geoparquet"
        if not path.is_file():
            raise ValueError("Geofabrik OSM requires exposure_aoi.geoparquet")
        layer = gpd.read_parquet(path)
        if layer.empty or layer.crs is None:
            raise ValueError("Exposure AOI is missing geometry or CRS")
        aoi = layer.to_crs("EPSG:4326").geometry.union_all()
        if aoi.is_empty:
            raise ValueError("Exposure AOI is empty")
        return aoi

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        """Extract only approved OSM entities without modifying the retained PBF."""
        pbf_assets = sorted(
            (asset for asset in assets if asset.source_id == self.spec.source_id and asset.kind is AssetKind.RAW and Path(asset.storage_path).suffixes[-2:] == [".osm", ".pbf"]),
            key=lambda asset: asset.asset_id,
        )
        if len(pbf_assets) != 1:
            raise ValueError("OSM harmonization requires exactly one retained dated PBF")
        pbf = Path(pbf_assets[0].storage_path)
        validation = self.validate_raw(pbf)
        if not validation.passed:
            raise ValueError(f"OSM raw validation failed: {validation.messages}")
        config = context.paths.root / "config" / "osmconf.ini"
        outputs = extract_osm_layers(pbf, self._exposure_aoi(context), config, output_dir=context.paths.harmonized / "exposure")
        now = datetime.now(UTC)
        records: list[AssetRecord] = []
        for name, path in (("road_segment", outputs.roads), ("bridge", outputs.bridges), ("facility", outputs.facilities), ("settlement", outputs.settlements), ("water_context", outputs.water_context)):
            record = AssetRecord(
                asset_id=f"geofabrik-osm-{self.spec.version}-{name}",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                kind=AssetKind.HARMONIZED,
                source_uri=f"generated:geofabrik-osm-exposure-aoi-{name}",
                storage_path=str(path),
                media_type="application/vnd.apache.parquet",
                size_bytes=path.stat().st_size,
                checksum=sha256_file(path),
                retrieved_at=now,
                source_valid_time=pbf_assets[0].source_valid_time,
                license_id=self.spec.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.HARMONIZED,
                dependency_fingerprint=sha256_bundle([pbf]),
                metadata_json=json.dumps({"crs": "EPSG:4326", "raw_pbf": str(pbf)}, sort_keys=True),
            )
            context.catalog.upsert(record)
            records.append(record)
        return records
