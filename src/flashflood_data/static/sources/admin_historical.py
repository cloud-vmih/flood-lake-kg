"""Historical administrative boundaries and GADM source adapter."""

import json
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import geopandas as gpd

from flashflood_data.catalog import sha256_file
from flashflood_data.catalog.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    ValidationResult,
)
from flashflood_data.static.sources.admin_shared import GADM_ARCHIVE_ASSET_ID
from flashflood_data.static.sources.base import (
    SourceAdapter,
    SourceConfigurationError,
    SourceContext,
)
from flashflood_data.static.spatial.vector import repair_geometries, write_geoparquet


def _normalize_label(value: object) -> str:
    """Return an accent-preserving comparison form for GADM display labels."""
    return " ".join(str(value).strip().casefold().split())

def normalize_historical_admin(
    raw: gpd.GeoDataFrame,
    *,
    source_version: str,
    valid_to: date,
    raw_asset_id: str,
    province_name: str = "Sơn La",
) -> gpd.GeoDataFrame:
    """Normalize an immutable GADM ADM3 read into Sơn La's pre-reform communes."""
    required = {"GID_3", "NAME_1", "NAME_2", "NAME_3", "geometry"}
    missing = sorted(required.difference(raw.columns))
    if missing:
        raise ValueError(f"GADM ADM3 layer is missing columns: {', '.join(missing)}")
    if raw.crs is None:
        raise ValueError("GADM ADM3 layer has no CRS")
    son_la = raw.loc[raw["NAME_1"].map(_normalize_label) == _normalize_label(province_name)].copy()
    if son_la.empty:
        raise ValueError(f"GADM ADM3 layer has no records for {province_name}")
    normalized = gpd.GeoDataFrame(
        {
            "old_admin_id": son_la["GID_3"].astype(str),
            "old_province_name": son_la["NAME_1"].astype(str),
            "old_district_name": son_la["NAME_2"].astype(str),
            "old_admin_name": son_la["NAME_3"].astype(str),
            "source_version": source_version,
            "valid_from": None,
            "valid_to": valid_to,
            "raw_asset_id": raw_asset_id,
        },
        geometry=son_la.geometry,
        crs=raw.crs,
    )
    return repair_geometries(normalized)

def build_sonla_reference_boundary(historical: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Dissolve historical GADM Sơn La units into an independent QA boundary."""
    if historical.empty or historical.crs is None:
        raise ValueError("historical GADM units are required for the Sơn La reference boundary")
    return repair_geometries(
        gpd.GeoDataFrame(
            {
                "reference_id": ["gadm-sonla-historical-dissolve"],
                "source_version": [str(historical.iloc[0]["source_version"])],
                "raw_asset_id": [str(historical.iloc[0]["raw_asset_id"])],
            },
            geometry=[historical.geometry.union_all()],
            crs=historical.crs,
        )
    )

class GadmAdminAdapter(SourceAdapter):
    """Read the immutable GADM Vietnam ZIP directly into historical admin layers."""

    def _setting(self, key: str) -> str:
        value = self.spec.settings.get(key)
        if not isinstance(value, str):
            raise SourceConfigurationError(f"GADM source setting {key!r} must be a string")
        return value

    def _budget_setting(self, key: str) -> int:
        value = self.spec.settings.get(key)
        if type(value) is not int or value <= 0:
            raise SourceConfigurationError(
                f"GADM source setting {key!r} must be a positive integer"
            )
        return value

    def _archive_remote(self) -> RemoteAsset:
        return RemoteAsset(
            asset_id=GADM_ARCHIVE_ASSET_ID,
            source_id=self.spec.source_id,
            source_version=self.spec.version,
            uri=self._setting("archive_url"),
            target_relative_path=Path(f"raw/admin/gadm/{self.spec.version}/gadm41_VNM_shp.zip"),
            media_type="application/zip",
            license_id=self.spec.license_id,
            source_valid_time=self._setting("historical_valid_to"),
            budget_size_bytes=self._budget_setting("archive_budget_size_bytes"),
        )

    def resolve(
        self, context: SourceContext | None, available: list[AssetRecord]
    ) -> list[RemoteAsset]:
        """Declare the one immutable ZIP only until it is available locally."""
        del context
        if any(asset.asset_id == GADM_ARCHIVE_ASSET_ID for asset in available):
            return []
        return [self._archive_remote()]

    def validate_raw(self, path: Path) -> ValidationResult:
        """Validate the ZIP container without extracting or changing the raw payload."""
        try:
            checks = {
                "zip_signature": path.read_bytes()[:4] == b"PK\x03\x04",
                "zip_readable": zipfile.is_zipfile(path),
            }
            if checks["zip_readable"]:
                with zipfile.ZipFile(path) as archive:
                    names = set(archive.namelist())
                checks["adm0_present"] = "gadm41_VNM_0.shp" in names
                checks["adm3_present"] = "gadm41_VNM_3.shp" in names
        except OSError:
            checks = {"zip_readable": False}
        return ValidationResult(
            passed=all(checks.values()),
            checks=checks,
            metrics={"size_bytes": path.stat().st_size if path.exists() else 0},
            messages=() if all(checks.values()) else ("GADM raw archive failed validation",),
        )

    @staticmethod
    def _read_archive_layer(archive_path: Path, layer_name: str) -> gpd.GeoDataFrame:
        """Read a member via GDAL's virtual ZIP filesystem, never extracting raw files."""
        return gpd.read_file(f"/vsizip/{archive_path.resolve()}/{layer_name}")

    def _normalize_vietnam_boundary(
        self, raw: gpd.GeoDataFrame, raw_asset_id: str
    ) -> gpd.GeoDataFrame:
        if raw.empty or "geometry" not in raw.columns or raw.crs is None:
            raise ValueError("GADM ADM0 layer is incomplete")
        row = raw.iloc[0]
        country_id = str(row.get("GID_0", "VNM"))
        country_name = str(row.get("COUNTRY", row.get("NAME_0", "Vietnam")))
        boundary = gpd.GeoDataFrame(
            {
                "country_id": [country_id],
                "country_name": [country_name],
                "source_version": [self.spec.version],
                "raw_asset_id": [raw_asset_id],
            },
            geometry=[row.geometry],
            crs=raw.crs,
        )
        return repair_geometries(boundary)

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        """Publish historical communes, national clipping boundary, and conservative crosswalk."""
        from flashflood_data.derive.mappings import build_admin_crosswalk
        from flashflood_data.storage.atomic import atomic_target

        by_id = {asset.asset_id: asset for asset in assets}
        try:
            archive = by_id[GADM_ARCHIVE_ASSET_ID]
        except KeyError as exc:
            raise ValueError("GADM harmonization requires the saved Vietnam archive") from exc
        archive_path = Path(archive.storage_path)
        valid_to = date.fromisoformat(self._setting("historical_valid_to"))
        historical = normalize_historical_admin(
            self._read_archive_layer(archive_path, "gadm41_VNM_3.shp"),
            source_version=self.spec.version,
            valid_to=valid_to,
            raw_asset_id=archive.asset_id,
            province_name=self._setting("province_name"),
        )
        vietnam = self._normalize_vietnam_boundary(
            self._read_archive_layer(archive_path, "gadm41_VNM_0.shp"), archive.asset_id
        )
        sonla_reference = build_sonla_reference_boundary(historical)
        historical_path = context.paths.harmonized / "admin" / "admin_commune_historical.geoparquet"
        vietnam_path = context.paths.harmonized / "admin" / "vietnam_boundary.geoparquet"
        sonla_reference_path = (
            context.paths.harmonized / "admin" / "sonla_reference_boundary.geoparquet"
        )
        write_geoparquet(historical, historical_path, context.study_area.storage_crs)
        write_geoparquet(vietnam, vietnam_path, context.study_area.storage_crs)
        write_geoparquet(sonla_reference, sonla_reference_path, context.study_area.storage_crs)
        current_path = context.paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
        if not current_path.is_file():
            raise ValueError("GADM harmonization requires validated current admin GeoParquet")
        crosswalk = build_admin_crosswalk(gpd.read_parquet(current_path), historical)
        crosswalk_path = context.paths.derived / "mappings" / "admin_commune_crosswalk.parquet"
        with atomic_target(crosswalk_path) as partial:
            crosswalk.to_parquet(partial, index=False)
        now = datetime.now(UTC)
        dependency = f"{archive.checksum}|{sha256_file(current_path)}"
        outputs = (
            (
                "gadm-vnm-4-1-historical-harmonized",
                historical_path,
                AssetKind.HARMONIZED,
                "generated:gadm-adm3-normalization",
                "application/geoparquet",
                AssetStatus.HARMONIZED,
            ),
            (
                "vietnam-boundary-gadm-4-1-harmonized",
                vietnam_path,
                AssetKind.HARMONIZED,
                "generated:gadm-adm0-normalization",
                "application/geoparquet",
                AssetStatus.HARMONIZED,
            ),
            (
                "sonla-reference-boundary-gadm-4-1-harmonized",
                sonla_reference_path,
                AssetKind.HARMONIZED,
                "generated:gadm-adm3-sonla-dissolve",
                "application/geoparquet",
                AssetStatus.HARMONIZED,
            ),
            (
                "admin-commune-crosswalk-2025",
                crosswalk_path,
                AssetKind.DERIVED,
                "generated:historical-to-current-admin-crosswalk",
                "application/vnd.apache.parquet",
                AssetStatus.DERIVED,
            ),
        )
        records = [
            AssetRecord(
                asset_id=asset_id,
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                kind=kind,
                source_uri=source_uri,
                storage_path=str(path),
                media_type=media_type,
                size_bytes=path.stat().st_size,
                checksum=sha256_file(path),
                retrieved_at=now,
                source_valid_time=self._setting("historical_valid_to"),
                license_id=self.spec.license_id,
                pipeline_run_id=context.run_id,
                status=status,
                dependency_fingerprint=dependency,
                metadata_json=json.dumps({"source_asset_ids": [archive.asset_id]}, sort_keys=True),
            )
            for asset_id, path, kind, source_uri, media_type, status in outputs
        ]
        for record in records:
            context.catalog.upsert(record)
        return records
