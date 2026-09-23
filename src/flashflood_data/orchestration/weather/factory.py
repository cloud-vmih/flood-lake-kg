"""Production dependency composition and restart-safe weather planning."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.meta.service import MetaRecorder
from flashflood_data.orchestration.weather.bronze import WeatherBronzeService
from flashflood_data.orchestration.weather.config import load_weather_config
from flashflood_data.orchestration.weather.landing import WeatherLandingService
from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    IngestWatermark,
    PlannedWeatherObject,
    WeatherPipelineConfig,
)
from flashflood_data.orchestration.weather.planner import (
    advance_contiguous_cursor,
    operational_start,
    plan_expected_objects,
    provider_safe_end,
    select_missing_or_overlap,
)
from flashflood_data.orchestration.weather.providers.era5_land import Era5LandProvider
from flashflood_data.orchestration.weather.providers.gsmap import GsmapProvider
from flashflood_data.orchestration.weather.providers.ifs_openmeteo import IfsOpenMeteoProvider
from flashflood_data.orchestration.weather.watermarks import IngestWatermarkStore
from flashflood_data.storage.iceberg import SourceObjectInventory, load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import ObjectPublisher, PyArrowS3ObjectStore


def _datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _matches_planned_object(row, planned: PlannedWeatherObject) -> bool:
    """Require one inventory row to match the full planned request contract."""
    if (
        row.source_type != "dynamic"
        or row.asset_id != planned.asset_id
        or row.source_version != planned.source_version
        or row.product != planned.product
    ):
        return False
    try:
        selection = json.loads(row.selection_json)
    except (TypeError, json.JSONDecodeError):
        return False
    expected = {
        "stream_id": planned.stream_id,
        "window_start": planned.window.start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": planned.window.end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "variables": list(planned.variables),
        "options": planned.options,
    }
    return all(
        json.dumps(selection.get(key), sort_keys=True, separators=(",", ":"))
        == json.dumps(value, sort_keys=True, separators=(",", ":"))
        for key, value in expected.items()
    )


def _aoi_bounds(root: Path, configured: Path) -> tuple[float, float, float, float]:
    import geopandas as gpd

    path = configured if configured.is_absolute() else root / configured
    if not path.is_file():
        raise FileNotFoundError(
            f"dynamic weather AOI is missing: {path}; build the L12 AOI before enabling DAGs"
        )
    frame = gpd.read_parquet(path, columns=["geometry"])
    if frame.empty or frame.crs is None:
        raise ValueError("dynamic weather AOI must contain a georeferenced geometry")
    wgs84 = frame if frame.crs.to_epsg() == 4326 else frame.to_crs("EPSG:4326")
    west, south, east, north = map(float, wgs84.total_bounds)
    return west, south, east, north


@dataclass
class WeatherRuntime:
    """Source-independent façade used by small Airflow task boundaries."""

    root: Path
    config: WeatherPipelineConfig
    settings: LakehouseSettings
    inventory: SourceObjectInventory
    table_store: IcebergTableStore
    meta: MetaRecorder
    object_store: PyArrowS3ObjectStore

    @property
    def watermarks(self) -> IngestWatermarkStore:
        return IngestWatermarkStore(self.table_store)

    def stream(self, stream_id: str):
        try:
            return next(stream for stream in self.config.streams if stream.stream_id == stream_id)
        except StopIteration as error:
            raise ValueError(f"unknown weather stream: {stream_id}") from error

    def register_meta(self) -> dict[str, int]:
        valid_from = min(stream.start_at for stream in self.config.streams)
        source = self.meta.register_source(
            {
                "source_id": self.config.source_id,
                "source_version": self.config.source_version,
                "provider": self.config.provider,
                "dataset": ",".join(stream.product for stream in self.config.streams),
                "license_uri": self.config.license_uri,
                "coverage_ref": str(self.config.aoi_path),
                "refresh_sla_minutes": min(stream.step_minutes for stream in self.config.streams),
                "valid_from": valid_from,
                "valid_to": None,
            }
        )
        dataset = self.meta.register_dataset(
            {
                "dataset_id": "flood_lakehouse.bronze.weather_grid_value",
                "contract_version": "1.0",
                "layer": "bronze",
                "description": "Provider-grain dynamic weather grid values",
                "owner": "flashflood-data",
                "source_id": None,
                "schema_ref": "docs/schema_contract/data.md#bronzeweather_grid_value",
                "data_classification": "public",
                "license_id": None,
                "retention_policy_ref": "retain-source-revisions",
                "freshness_sla_minutes": 60,
                "quality_policy_id": "weather-bronze-v1",
                "access_policy_ref": "project-read",
                "valid_from": datetime(2026, 1, 1, tzinfo=UTC),
                "valid_to": None,
            }
        )
        return {"source_snapshot": source or 0, "dataset_snapshot": dataset or 0}

    def cursor_document(self) -> dict[str, object]:
        return {
            stream.stream_id: (
                None
                if (watermark := self.watermarks.load(
                    self.config.source_id, stream.product, stream.stream_id
                )) is None
                else watermark.model_dump(mode="json")
            )
            for stream in self.config.streams
        }

    def safe_end_document(self, now: datetime | None = None) -> dict[str, str]:
        current = datetime.now(UTC) if now is None else now
        return {
            stream.stream_id: provider_safe_end(current, stream).isoformat()
            for stream in self.config.streams
        }

    def plan_document(
        self,
        cursors: dict[str, object],
        safe_ends: dict[str, str],
        *,
        mode: str,
        requested_start: str = "",
        requested_end: str = "",
        requested_limit: str = "",
    ) -> dict[str, object]:
        if mode not in {"catchup", "backfill"}:
            raise ValueError("weather mode must be catchup or backfill")
        if mode == "backfill" and (not requested_start or not requested_end):
            raise ValueError("weather backfill requires start and end")
        limit = int(requested_limit) if requested_limit else self.config.max_objects_per_run
        if limit < 1:
            raise ValueError("weather object limit must be positive")
        bounds = _aoi_bounds(self.root, self.config.aoi_path)
        available_rows = self.inventory.available_objects(self.config.source_id)
        stream_documents: list[dict[str, object]] = []
        all_missing: list[dict[str, object]] = []
        for stream in self.config.streams:
            watermark_doc = cursors.get(stream.stream_id)
            watermark = (
                None if watermark_doc is None else IngestWatermark.model_validate(watermark_doc)
            )
            safe_end = _datetime(safe_ends[stream.stream_id])
            if mode == "backfill":
                start = max(stream.start_at, _datetime(requested_start))
                end = min(safe_end, _datetime(requested_end))
            else:
                start = operational_start(stream, watermark)
                end = safe_end
            request_options = {**stream.options, "aoi_bounds": list(bounds)}
            expected = plan_expected_objects(
                source_id=self.config.source_id,
                source_version=self.config.source_version,
                stream=stream,
                start=start,
                end=end,
                request_options=request_options,
            )[:limit]
            expected_by_asset = {item.asset_id: item for item in expected}
            matching_rows = [
                row
                for row in available_rows
                if (planned := expected_by_asset.get(row.asset_id)) is not None
                and _matches_planned_object(row, planned)
            ]
            existing = {
                row.asset_id
                for row in matching_rows
            }
            recently_committed = {
                row.asset_id
                for row in matching_rows
                if watermark is not None
                and row.first_seen_at >= watermark.updated_at
            }
            overlap = operational_start(stream, watermark) if watermark else safe_end
            missing = select_missing_or_overlap(
                expected,
                existing_asset_ids=existing,
                overlap_start=overlap,
                recently_committed_asset_ids=recently_committed,
            )
            all_missing.extend(item.model_dump(mode="json") for item in missing)
            stream_documents.append(
                {
                    "stream_id": stream.stream_id,
                    "product": stream.product,
                    "safe_end": safe_end.isoformat(),
                    "expected": [item.model_dump(mode="json") for item in expected],
                    "existing_asset_ids": sorted(existing),
                }
            )
        return {"mode": mode, "streams": stream_documents, "missing": all_missing}

    def provider(self, stream_id: str):
        stream = self.stream(stream_id)
        if self.config.provider == "gsmap":
            return GsmapProvider(self.config, stream)
        bounds = _aoi_bounds(self.root, self.config.aoi_path)
        if self.config.provider == "era5_land":
            return Era5LandProvider(self.config, stream, aoi_bounds=bounds)
        if self.config.provider == "ifs_openmeteo":
            return IfsOpenMeteoProvider(self.config, stream, aoi_bounds=bounds)
        raise ValueError(f"unsupported weather provider: {self.config.provider}")

    def fetch(
        self, planned: PlannedWeatherObject, run_id: str, *, attempt_no: int = 1
    ) -> FetchedWeatherObject:
        target = self.settings.staging_root / "weather" / run_id / planned.source_id
        started_at = datetime.now(UTC)
        attempt = {
            "ingest_run_id": run_id,
            "source_id": planned.source_id,
            "asset_id": planned.asset_id,
            "attempt_no": attempt_no,
            "request_fingerprint": planned.request_fingerprint,
            "started_at": started_at,
        }
        self.meta.record_attempt(**attempt, status="running")
        try:
            return self.provider(planned.stream_id).fetch(planned, target)
        except Exception as error:
            response = getattr(error, "response", None)
            self.meta.record_attempt(
                **attempt,
                status="failed",
                ended_at=datetime.now(UTC),
                http_status=getattr(response, "status_code", None),
                error_code=type(error).__name__,
            )
            raise

    def landing_service(self) -> WeatherLandingService:
        return WeatherLandingService(
            publisher=ObjectPublisher(self.object_store, self.settings.raw_bucket),
            inventory=self.inventory,
            meta=self.meta,
            license_id=self.config.license_id,
        )

    def advance(
        self,
        plan: dict[str, object],
        outcomes: list[dict[str, object]],
        *,
        run_id: str,
    ) -> dict[str, object]:
        if plan["mode"] == "backfill":
            return {"mode": "backfill", "watermarks_updated": 0}
        statuses = {
            str(outcome["asset_id"]): str(outcome["status"])
            for outcome in outcomes
        }
        updates = 0
        cursors: dict[str, str] = {}
        for document in plan["streams"]:
            expected = tuple(
                PlannedWeatherObject.model_validate(item) for item in document["expected"]
            )
            existing = set(document["existing_asset_ids"])
            local_statuses = {
                item.asset_id: statuses.get(
                    item.asset_id, "available" if item.asset_id in existing else "missing"
                )
                for item in expected
            }
            cursor = advance_contiguous_cursor(expected, local_statuses)
            if cursor is None:
                continue
            status = "ready" if not expected or cursor == expected[-1].window.end else "gap"
            self.watermarks.save(
                IngestWatermark(
                    source_id=self.config.source_id,
                    product=str(document["product"]),
                    stream_id=str(document["stream_id"]),
                    cursor_time=cursor,
                    last_safe_end=_datetime(document["safe_end"]),
                    last_run_id=run_id,
                    status=status,
                    updated_at=datetime.now(UTC),
                    detail_json="{}",
                )
            )
            updates += 1
            cursors[str(document["stream_id"])] = cursor.isoformat()
        return {"mode": "catchup", "watermarks_updated": updates, "cursors": cursors}

    def bronze_service(self) -> WeatherBronzeService:
        return WeatherBronzeService(
            inventory=self.inventory,
            object_store=self.object_store,
            writer=self.table_store,
            meta=self.meta,
            raw_bucket=self.settings.raw_bucket,
            staging_root=self.settings.staging_root,
            catalog_name=self.settings.polaris_catalog,
        )


def build_weather_runtime(config_path: Path, root: Path | None = None) -> WeatherRuntime:
    paths = ProjectPaths.discover(root)
    settings = LakehouseSettings(_env_file=paths.root / ".env", project_root=paths.root)
    catalog = load_polaris_catalog(settings)
    store = IcebergTableStore(catalog)
    return WeatherRuntime(
        root=paths.root,
        config=load_weather_config(config_path),
        settings=settings,
        inventory=SourceObjectInventory(catalog),
        table_store=store,
        meta=MetaRecorder(store),
        object_store=PyArrowS3ObjectStore.from_settings(settings),
    )
