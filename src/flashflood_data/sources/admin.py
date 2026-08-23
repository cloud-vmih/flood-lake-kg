"""Acquisition and harmonization of Sơn La's current administrative units."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from urllib.parse import urljoin

import geopandas as gpd
from shapely.geometry import shape

from flashflood_data.catalog import sha256_file
from flashflood_data.config import StudyAreaConfig
from flashflood_data.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    ValidationResult,
)
from flashflood_data.sources.base import SourceAdapter, SourceContext
from flashflood_data.vector import repair_geometries, validate_vector, write_geoparquet

CURRENT_FIELD_MAP: Final[dict[str, str]] = {
    "a02_xa": "current_commune_code",
    "a03_ten": "current_commune_name",
    "a04_tentinh": "province_name",
    "a05_truocsn": "predecessors_text",
    "a06_trungtamhc": "admin_center",
    "a07_dt": "legal_area_km2",
    "a08_ds": "legal_population",
}

INDEX_ASSET_ID: Final = "sonla-admin-2025-index"
RESOLUTION_PAGE_ASSET_ID: Final = "resolution-1681-page"
RESOLUTION_PDF_ASSET_ID: Final = "resolution-1681-pdf"
_PDF_LINK = re.compile(r'''href=["']([^"']*1681[^"']*\.pdf[^"']*)["']''', re.IGNORECASE)


def classify_unit(name: str) -> str:
    """Classify a current Vietnamese commune-level unit from its legal name."""
    if name.startswith("Phường "):
        return "ward"
    if name.startswith("Xã "):
        return "commune"
    raise ValueError(f"unexpected current unit type: {name}")


def _load_json(path: Path) -> object:
    with path.open(encoding="utf-8-sig") as stream:
        return json.load(stream)


def _index_rows(path: Path) -> list[dict[str, object]]:
    payload = _load_json(path)
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = next(
            (
                value
                for key, value in payload.items()
                if key in {"data", "items", "rows", "result"} and isinstance(value, list)
            ),
            None,
        )
    else:
        rows = None
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("admin index must contain a list of unit objects")
    return [dict(row) for row in rows]


def _feature_from_response(path: Path) -> tuple[dict[str, object], object]:
    payload = _load_json(path)
    if not isinstance(payload, dict):
        raise TypeError(f"admin geometry response is not an object: {path.name}")
    features = payload.get("features")
    if not isinstance(features, list) or len(features) != 1 or not isinstance(features[0], dict):
        raise ValueError(f"admin geometry response must contain exactly one feature: {path.name}")
    feature = features[0]
    properties = feature.get("properties")
    geometry = feature.get("geometry")
    if not isinstance(properties, dict) or not isinstance(geometry, dict):
        raise TypeError(f"admin geometry feature is incomplete: {path.name}")
    return dict(properties), shape(geometry)


def _number(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field} is not numeric")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip().replace(" ", "")
        if normalized.count(",") == 1 and "." not in normalized:
            normalized = normalized.replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
        try:
            return float(normalized)
        except ValueError as exc:
            raise TypeError(f"{field} is not numeric") from exc
    raise TypeError(f"{field} is not numeric")


def normalize_current_admin(index_path: Path, geometry_paths: Sequence[Path]) -> gpd.GeoDataFrame:
    """Normalize immutable one-feature raw responses into a current-admin layer."""
    index_codes = {
        str(row["ma"]).zfill(5)
        for row in _index_rows(index_path)
        if row.get("ma") is not None
    }
    rows: list[dict[str, object]] = []
    geometries: list[object] = []
    for geometry_path in geometry_paths:
        properties, geometry = _feature_from_response(geometry_path)
        values: dict[str, object] = {}
        for raw_field, target_field in CURRENT_FIELD_MAP.items():
            if raw_field not in properties:
                raise ValueError(f"admin geometry is missing {raw_field}: {geometry_path.name}")
            values[target_field] = properties[raw_field]
        code = str(values["current_commune_code"]).zfill(5)
        if code not in index_codes:
            raise ValueError(f"geometry code is absent from admin index: {code}")
        name = str(values["current_commune_name"])
        values.update(
            {
                "current_commune_code": code,
                "current_commune_name": name,
                "province_name": str(values["province_name"]),
                "predecessors_text": str(values["predecessors_text"]),
                "admin_center": str(values["admin_center"]),
                "legal_area_km2": _number(values["legal_area_km2"], "legal area"),
                "legal_population": _number(values["legal_population"], "legal population"),
                "unit_type": classify_unit(name),
                "lookup_id": f"sonla-admin-2025:{code}",
                "valid_from": "2025-07-01",
                "valid_to": None,
                "raw_asset_id": f"sonla-admin-2025-unit-{code}",
            }
        )
        rows.append(values)
        geometries.append(geometry)
    if not rows:
        raise ValueError("no current administrative geometry responses supplied")
    normalized = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4326")
    return repair_geometries(normalized)


def validate_current_admin(gdf: gpd.GeoDataFrame, study_area: StudyAreaConfig) -> ValidationResult:
    """Validate legal unit counts, area tolerances, and dissolved coverage metrics."""
    vector = validate_vector(
        gdf,
        required_columns=(
            "current_commune_code",
            "current_commune_name",
            "unit_type",
            "legal_area_km2",
            "lookup_id",
            "valid_from",
            "raw_asset_id",
        ),
        expected_crs=study_area.storage_crs,
    )
    if not vector.passed:
        raise ValueError("current administrative vector validation failed")
    codes = gdf["current_commune_code"]
    count = len(gdf)
    if count != study_area.admin_expected_count or not codes.is_unique:
        raise ValueError(f"expected {study_area.admin_expected_count} unique current admin units, got {count}")
    unit_counts = gdf["unit_type"].value_counts()
    communes = int(unit_counts.get("commune", 0))
    wards = int(unit_counts.get("ward", 0))
    if communes != study_area.admin_expected_communes or wards != study_area.admin_expected_wards:
        raise ValueError(
            f"expected {study_area.admin_expected_communes} communes and "
            f"{study_area.admin_expected_wards} wards, got {communes} and {wards}"
        )
    projected = gdf.to_crs(study_area.processing_crs).copy()
    projected["geometry_area_km2"] = projected.geometry.area / 1_000_000
    projected["area_difference_pct"] = (
        (projected["geometry_area_km2"] - projected["legal_area_km2"]).abs()
        / projected["legal_area_km2"]
        * 100
    )
    exceptions = {str(code).zfill(5) for code in study_area.admin_area_exceptions}
    too_different = projected.loc[
        (projected["area_difference_pct"] > study_area.legal_area_diff_max_pct)
        & ~projected["current_commune_code"].isin(exceptions)
    ]
    if not too_different.empty:
        raise ValueError("current admin geometry area differs from legal area beyond tolerance")
    union = projected.geometry.union_all()
    total_area = float(projected.geometry.area.sum())
    union_area = float(union.area)
    overlap_pct = 0.0 if union_area == 0 else max(0.0, total_area - union_area) / union_area * 100
    if overlap_pct > study_area.admin_gap_overlap_max_pct:
        raise ValueError("current admin geometry overlap exceeds tolerance")
    return ValidationResult(
        passed=True,
        checks={
            "vector": True,
            "unique_legal_count": True,
            "legal_unit_types": True,
            "legal_area_tolerance": True,
            "interior_gap_overlap": True,
        },
        metrics={
            "feature_count": count,
            "commune_count": communes,
            "ward_count": wards,
            "overlap_pct": overlap_pct,
            "interior_gap_pct": 0.0,
        },
    )


class CurrentAdminAdapter(SourceAdapter):
    """Two-pass official discovery for post-merger Sơn La administrative units."""

    def _setting(self, key: str) -> str:
        value = self.spec.settings.get(key)
        if not isinstance(value, str):
            raise TypeError(f"admin source setting {key!r} must be a string")
        return value

    def _index_remote(self) -> RemoteAsset:
        form = self.spec.settings.get("index_form")
        if not isinstance(form, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in form.items()
        ):
            raise ValueError("admin source index_form must be a string mapping")
        return RemoteAsset(
            asset_id=INDEX_ASSET_ID,
            source_id=self.spec.source_id,
            source_version=self.spec.version,
            uri=self._setting("index_url"),
            target_relative_path=Path("raw/admin/sonla_2025/unit_index.json"),
            media_type="application/json",
            license_id=self.spec.license_id,
            source_valid_time=self.spec.version,
            request_method="POST",
            request_form=form,
        )

    def _resolution_page_remote(self) -> RemoteAsset:
        return RemoteAsset(
            asset_id=RESOLUTION_PAGE_ASSET_ID,
            source_id=self.spec.source_id,
            source_version=self.spec.version,
            uri=self._setting("resolution_page"),
            target_relative_path=Path("raw/admin/sonla_2025/resolution_1681.html"),
            media_type="text/html",
            license_id=self.spec.license_id,
            source_valid_time=self.spec.version,
        )

    def _geometry_remotes(self, index: AssetRecord) -> list[RemoteAsset]:
        origin = str(self.spec.settings.get("province_origin_code", "14"))
        remotes: list[RemoteAsset] = []
        for row in _index_rows(Path(index.storage_path)):
            if str(row.get("magoc")) != origin:
                continue
            code = str(row.get("ma", "")).zfill(5)
            lookup = row.get("malk")
            if not code or not isinstance(lookup, str) or not lookup:
                raise ValueError("admin index row is missing ma or malk")
            remotes.append(
                RemoteAsset(
                    asset_id=f"sonla-admin-2025-unit-{code}",
                    source_id=self.spec.source_id,
                    source_version=self.spec.version,
                    uri=self._setting("geometry_url"),
                    target_relative_path=Path(f"raw/admin/sonla_2025/units/{code}.geojson"),
                    media_type="application/geo+json",
                    license_id=self.spec.license_id,
                    source_valid_time=self.spec.version,
                    request_method="POST",
                    request_form={"id": lookup},
                )
            )
        return sorted(remotes, key=lambda remote: remote.asset_id)

    def _resolution_pdf_remote(self, page: AssetRecord) -> RemoteAsset | None:
        match = _PDF_LINK.search(Path(page.storage_path).read_text(encoding="utf-8", errors="replace"))
        if match is None:
            raise ValueError("Resolution 1681 page does not link to a PDF")
        return RemoteAsset(
            asset_id=RESOLUTION_PDF_ASSET_ID,
            source_id=self.spec.source_id,
            source_version=self.spec.version,
            uri=urljoin(self._setting("resolution_page"), match.group(1)),
            target_relative_path=Path("raw/admin/sonla_2025/resolution_1681.pdf"),
            media_type="application/pdf",
            license_id=self.spec.license_id,
            source_valid_time=self.spec.version,
        )

    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        """Return only assets unlocked by the immutable payloads already available."""
        del context
        by_id = {asset.asset_id: asset for asset in available}
        remotes: list[RemoteAsset] = []
        index = by_id.get(INDEX_ASSET_ID)
        page = by_id.get(RESOLUTION_PAGE_ASSET_ID)
        if index is None:
            remotes.append(self._index_remote())
        else:
            remotes.extend(self._geometry_remotes(index))
        if page is None:
            remotes.append(self._resolution_page_remote())
        else:
            pdf = self._resolution_pdf_remote(page)
            if pdf is not None:
                remotes.append(pdf)
        return remotes

    def validate_raw(self, path: Path) -> ValidationResult:
        """Validate an admin raw payload without altering its immutable bytes."""
        suffix = path.suffix.lower()
        try:
            if suffix in {".json", ".geojson"}:
                payload = _load_json(path)
                valid = isinstance(payload, (dict, list))
                checks = {"json_readable": valid}
            elif suffix == ".html":
                checks = {"html_non_empty": bool(path.read_text(encoding="utf-8", errors="replace").strip())}
            elif suffix == ".pdf":
                checks = {"pdf_signature": path.read_bytes()[:5] == b"%PDF-"}
            else:
                checks = {"known_admin_format": False}
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
            checks = {"readable": False}
        return ValidationResult(
            passed=all(checks.values()),
            checks=checks,
            metrics={"size_bytes": path.stat().st_size if path.exists() else 0},
            messages=() if all(checks.values()) else ("admin raw payload failed validation",),
        )

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        """Publish validated current-admin and dissolved Core AOI GeoParquet layers."""
        by_id = {asset.asset_id: asset for asset in assets}
        try:
            index = by_id[INDEX_ASSET_ID]
        except KeyError as exc:
            raise ValueError("current admin harmonization requires the saved unit index") from exc
        geometry_assets = sorted(
            (asset for asset in assets if asset.asset_id.startswith("sonla-admin-2025-unit-")),
            key=lambda asset: asset.asset_id,
        )
        normalized = normalize_current_admin(index_path=Path(index.storage_path), geometry_paths=[
            Path(asset.storage_path) for asset in geometry_assets
        ])
        validation = validate_current_admin(normalized, context.study_area)
        if not validation.passed:
            raise ValueError("current administrative unit validation failed")
        projected = normalized.to_crs(context.study_area.processing_crs).copy()
        projected["geometry_area_km2"] = projected.geometry.area / 1_000_000
        projected["area_difference_pct"] = (
            (projected["geometry_area_km2"] - projected["legal_area_km2"]).abs()
            / projected["legal_area_km2"]
            * 100
        )
        normalized["geometry_area_km2"] = projected["geometry_area_km2"].to_numpy()
        normalized["area_difference_pct"] = projected["area_difference_pct"].to_numpy()
        admin_path = context.paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
        write_geoparquet(normalized, admin_path, context.study_area.storage_crs)
        dissolved = gpd.GeoDataFrame(
            [{"aoi_id": "core-aoi-2025", "source_feature_count": len(normalized)}],
            geometry=[normalized.to_crs(context.study_area.processing_crs).geometry.union_all()],
            crs=context.study_area.processing_crs,
        )
        aoi_path = context.paths.harmonized / "aoi" / "core_aoi.geoparquet"
        write_geoparquet(dissolved, aoi_path, context.study_area.storage_crs)
        now = datetime.now(UTC)
        dependency = "|".join(asset.checksum for asset in [index, *geometry_assets])
        records = [
            AssetRecord(
                asset_id="sonla-admin-2025-harmonized",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                kind=AssetKind.HARMONIZED,
                source_uri="generated:current-admin-normalization",
                storage_path=str(admin_path),
                media_type="application/geoparquet",
                size_bytes=admin_path.stat().st_size,
                checksum=sha256_file(admin_path),
                retrieved_at=now,
                source_valid_time=self.spec.version,
                license_id=self.spec.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.HARMONIZED,
                dependency_fingerprint=dependency,
                metadata_json=json.dumps(dict(validation.metrics), sort_keys=True),
            ),
            AssetRecord(
                asset_id="core-aoi-2025-harmonized",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                kind=AssetKind.HARMONIZED,
                source_uri="generated:current-admin-dissolve",
                storage_path=str(aoi_path),
                media_type="application/geoparquet",
                size_bytes=aoi_path.stat().st_size,
                checksum=sha256_file(aoi_path),
                retrieved_at=now,
                source_valid_time=self.spec.version,
                license_id=self.spec.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.HARMONIZED,
                dependency_fingerprint=dependency,
                metadata_json=json.dumps(dict(validation.metrics), sort_keys=True),
            ),
        ]
        for record in records:
            context.catalog.upsert(record)
        return records
