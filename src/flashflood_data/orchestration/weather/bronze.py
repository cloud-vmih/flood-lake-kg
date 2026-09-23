"""Object-scoped Raw-to-Bronze service for dynamic weather sources."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from itertools import islice
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from pyiceberg.expressions import EqualTo, In

from flashflood_data.catalog import sha256_file
from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.orchestration.weather.parsers import parse_weather_object

_SOURCES = frozenset({"gsmap", "era5_land", "ifs_openmeteo"})


def _raw_key(row: SourceObjectRow, bucket: str) -> tuple[str, str]:
    uri = urlsplit(row.object_uri)
    if uri.scheme != "s3" or uri.netloc != bucket or uri.query or uri.fragment:
        raise ValueError("registered weather object is outside the Raw bucket")
    path = PurePosixPath(uri.path.lstrip("/"))
    if not path.parts or any(part in {".", ".."} for part in path.parts):
        raise ValueError("registered weather object has an unsafe path")
    return f"{bucket}/{path}", path.name


def _batches(iterator, size: int):
    while batch := list(islice(iterator, size)):
        for row in batch:
            value = row.get("value")
            if (
                row.get("variable")
                in {"precipitation", "total_precipitation", "runoff", "surface_runoff"}
                and value is not None
                and float(value) < 0
            ):
                raise ValueError("negative precipitation or runoff in weather object")
            if not row.get("unit"):
                raise ValueError("weather value has no source unit")
            if row["window_end"] < row["window_start"]:
                raise ValueError("weather value has an invalid temporal window")
        yield batch


class WeatherBronzeService:
    """Parse all registered dynamic Raw objects with object-level idempotency."""

    def __init__(
        self,
        *,
        inventory,
        object_store,
        writer,
        meta,
        raw_bucket: str,
        staging_root: Path,
        catalog_name: str = "flood_lakehouse",
        bronze_namespace: str = "bronze",
        batch_size: int = 10_000,
    ) -> None:
        if batch_size < 1:
            raise ValueError("weather Bronze batch size must be positive")
        self.inventory = inventory
        self.object_store = object_store
        self.writer = writer
        self.meta = meta
        self.raw_bucket = raw_bucket
        self.staging_root = Path(staging_root)
        self.catalog_name = catalog_name
        self.bronze_namespace = bronze_namespace
        self.batch_size = batch_size

    def _published(self, object_ids: tuple[str, ...], parser_version: str) -> set[str]:
        if not object_ids:
            return set()
        helper = getattr(self.writer, "published_weather_objects", None)
        if helper is not None:
            return set(helper(parser_version)) & set(object_ids)
        dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.weather_grid_value"
        meta_namespace = getattr(self.meta, "meta_namespace", "meta")
        lineage_table = self.writer.ensure_table((meta_namespace, "lineage_edges"))
        lineage_table.refresh()
        lineage = lineage_table.scan(
            selected_fields=(
                "pipeline_run_id",
                "input_kind",
                "input_object_id",
                "output_table",
                "mapping_version",
            )
        ).to_arrow().to_pylist()
        candidates = [
            (str(row["input_object_id"]), str(row["pipeline_run_id"]))
            for row in lineage
            if row.get("input_kind") == "raw_object"
            and row.get("input_object_id") in object_ids
            and row.get("output_table") == dataset_id
            and row.get("mapping_version") == parser_version
        ]
        run_ids = tuple(sorted({run_id for _object_id, run_id in candidates}))
        if not run_ids:
            return set()
        runs_table = self.writer.ensure_table((meta_namespace, "pipeline_runs"))
        runs_table.refresh()
        run_filter = (
            EqualTo("pipeline_run_id", run_ids[0])
            if len(run_ids) == 1
            else In("pipeline_run_id", run_ids)
        )
        runs = runs_table.scan(
            row_filter=run_filter,
            selected_fields=("pipeline_run_id", "status", "published_at"),
        ).to_arrow().to_pylist()
        published_runs = {
            str(row["pipeline_run_id"])
            for row in runs
            if row.get("status") == "succeeded" and row.get("published_at") is not None
        }
        candidates_with_published_runs = {
            object_id for object_id, run_id in candidates if run_id in published_runs
        }
        if not candidates_with_published_runs:
            return set()
        table = self.writer.ensure_table((self.bronze_namespace, "weather_grid_value"))
        table.refresh()
        bronze_ids = tuple(sorted(candidates_with_published_runs))
        expression = (
            EqualTo("object_id", bronze_ids[0])
            if len(bronze_ids) == 1
            else In("object_id", bronze_ids)
        )
        rows = table.scan(
            row_filter=expression, selected_fields=("object_id", "parser_version")
        ).to_arrow().to_pylist()
        return {
            str(row["object_id"])
            for row in rows
            if row.get("parser_version") == parser_version
            and row.get("object_id") in candidates_with_published_runs
        }

    def discover(
        self,
        source_id: str,
        *,
        parser_version: str,
        force_reprocess: bool = False,
    ) -> tuple[str, ...]:
        if source_id not in _SOURCES:
            raise ValueError(f"unsupported dynamic weather source: {source_id}")
        candidates = tuple(
            row.object_id
            for row in self.inventory.available_objects(source_id)
            if row.source_type == "dynamic"
        )
        if force_reprocess:
            return candidates
        published = self._published(candidates, parser_version)
        return tuple(object_id for object_id in candidates if object_id not in published)

    def process_object(
        self,
        source_id: str,
        object_id: str,
        *,
        run_id: str,
        parser_version: str,
    ) -> dict[str, object]:
        matches = [
            row
            for row in self.inventory.available_objects(source_id)
            if row.object_id == object_id and row.source_type == "dynamic"
        ]
        if len(matches) != 1:
            raise LookupError(f"available dynamic Raw object is not unique: {object_id}")
        row = matches[0]
        key, filename = _raw_key(row, self.raw_bucket)
        dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.weather_grid_value"
        started_at = datetime.now(UTC)
        identity = json.dumps(
            {"run_id": run_id, "object_id": object_id, "parser_version": parser_version},
            sort_keys=True,
        )
        pipeline_run_id = sha256(identity.encode("utf-8")).hexdigest()
        run_record = {
            "pipeline_run_id": pipeline_run_id,
            "orchestrator_run_id": run_id,
            "job_name": f"weather_bronze:{source_id}",
            "code_git_sha": None,
            "image_digest": None,
            "config_hash": sha256(parser_version.encode("utf-8")).hexdigest(),
            "parameter_set_id": None,
            "started_at": started_at,
            "finished_at": None,
            "published_at": None,
            "status": "running",
            "retry_count": 0,
            "input_row_count": 1,
            "output_row_count": None,
            "quality_result_json": None,
            "metrics_json": None,
            "error_code": None,
        }
        self.meta.record_run(run_record)
        try:
            self.staging_root.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(prefix="weather-bronze-", dir=self.staging_root) as temp:
                local = Path(temp) / filename
                self.object_store.download(key, local)
                if local.stat().st_size != row.size_bytes or sha256_file(local) != row.checksum:
                    raise ValueError("dynamic Raw object failed size/checksum verification")
                rows = parse_weather_object(
                    row, local, run_id=run_id, parser_version=parser_version
                )
                snapshot_id, row_count = self.writer.replace_object_batches(
                    (self.bronze_namespace, "weather_grid_value"),
                    object_id,
                    _batches(rows, self.batch_size),
                )
            now = datetime.now(UTC)
            self.meta.record_snapshot_ref(
                pipeline_run_id=pipeline_run_id,
                table_name=dataset_id,
                iceberg_snapshot_id=snapshot_id,
                role="output",
                quality_status="passed",
                created_at=now,
            )
            self.meta.record_quality(
                pipeline_run_id=pipeline_run_id,
                dataset_id=dataset_id,
                rule_id="nonempty_weather_parse",
                status="passed",
                severity="fatal",
                checked_at=now,
                check_phase="post_commit",
                scope_key=object_id,
                observed_value_json=json.dumps({"row_count": row_count}),
                failed_row_count=0,
                snapshot_table=dataset_id,
                snapshot_id=snapshot_id,
            )
            self.meta.record_lineage(
                pipeline_run_id=pipeline_run_id,
                input_object_id=object_id,
                output_table=dataset_id,
                output_snapshot_id=snapshot_id,
                transform_role="source",
                mapping_version=parser_version,
                created_at=now,
            )
            self.meta.record_run(
                {
                    **run_record,
                    "finished_at": now,
                    "published_at": now,
                    "status": "succeeded",
                    "output_row_count": row_count,
                    "quality_result_json": '{"status":"passed"}',
                }
            )
            return {
                "pipeline_run_id": pipeline_run_id,
                "object_id": object_id,
                "table_name": dataset_id,
                "snapshot_id": snapshot_id,
                "row_count": row_count,
                "status": "succeeded",
            }
        except Exception as error:
            self.meta.record_run(
                {
                    **run_record,
                    "finished_at": datetime.now(UTC),
                    "status": "failed",
                    "error_code": type(error).__name__,
                }
            )
            raise
