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


def _batches(iterator, size: int, allowed_indices: set[int]):
    while batch := list(islice(iterator, size)):
        for row in batch:
            indices = list(map(int, row.get("cell_indices", ())))
            values = list(row.get("values", ()))
            if len(indices) != len(values):
                raise ValueError("weather slice arrays have different lengths")
            if set(indices) != allowed_indices:
                raise ValueError("weather slice contains unknown or missing source-grid indices")
            if row.get("variable") in {
                "precipitation",
                "total_precipitation",
                "runoff",
                "surface_runoff",
                "sub_surface_runoff",
            } and any(value is not None and float(value) < 0 for value in values):
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
        lifecycle,
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
        self.lifecycle = lifecycle
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
        dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.weather_raster_slice"
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
        table = self.writer.ensure_table((self.bronze_namespace, "weather_raster_slice"))
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
        expired = set(self.lifecycle.expired_object_ids(candidates))
        candidates = tuple(item for item in candidates if item not in expired)
        if force_reprocess:
            return candidates
        published = self._published(candidates, parser_version)
        return tuple(object_id for object_id in candidates if object_id not in published)

    def _parse_registered_object(
        self,
        row: SourceObjectRow,
        *,
        run_id: str,
        parser_version: str,
        temporary_root: Path,
        object_index: int,
        grid_cache: dict[tuple[str, str, str], set[int]],
    ) -> list[dict[str, object]]:
        """Download and validate one object while preserving its identity in errors."""
        try:
            selection = json.loads(row.selection_json)
            source_grid_version = str(selection["source_grid_version"])
            spatial_scope_id = str(selection["spatial_scope_id"])
            grid_key = (row.source_id, source_grid_version, spatial_scope_id)
            allowed_indices = grid_cache.get(grid_key)
            if allowed_indices is None:
                helper = getattr(self.writer, "source_grid_indices", None)
                if helper is not None:
                    allowed_indices = set(helper(*grid_key))
                else:
                    grid_rows = self.writer.get_keyed_rows(
                        ("silver", "source_grid"),
                        {
                            "source_id": row.source_id,
                            "source_grid_version": source_grid_version,
                        },
                    )
                    allowed_indices = {
                        int(item["cell_index"])
                        for item in grid_rows
                        if spatial_scope_id in item["scope_ids"]
                    }
                if not allowed_indices:
                    raise ValueError(
                        "weather source grid has no cells for the registered scope"
                    )
                grid_cache[grid_key] = allowed_indices
            key, filename = _raw_key(row, self.raw_bucket)
            object_root = temporary_root / f"object-{object_index:04d}"
            object_root.mkdir()
            local = object_root / filename
            self.object_store.download(key, local)
            if (
                local.stat().st_size != row.size_bytes
                or sha256_file(local) != row.checksum
            ):
                raise ValueError("dynamic Raw object failed size/checksum verification")
            current = [
                item
                for batch in _batches(
                    parse_weather_object(
                        row,
                        local,
                        run_id=run_id,
                        parser_version=parser_version,
                    ),
                    self.batch_size,
                    allowed_indices,
                )
                for item in batch
            ]
            if not current:
                raise ValueError("Bronze parse produced no rows")
            return current
        except Exception as error:
            raise ValueError(f"{row.object_id}: {error}") from error

    def process_batch(
        self,
        source_id: str,
        object_ids: tuple[str, ...],
        *,
        run_id: str,
        parser_version: str,
    ) -> dict[str, object]:
        if source_id not in _SOURCES:
            raise ValueError(f"unsupported dynamic weather source: {source_id}")
        if not object_ids or len(set(object_ids)) != len(object_ids):
            raise ValueError("weather Bronze batch object IDs must be non-empty and unique")
        available = {
            row.object_id: row
            for row in self.inventory.available_objects(source_id)
            if row.source_type == "dynamic"
        }
        if any(object_id not in available for object_id in object_ids):
            raise LookupError("weather Bronze batch contains an unavailable Raw object")
        source_rows = [available[object_id] for object_id in object_ids]
        dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.weather_raster_slice"
        started_at = datetime.now(UTC)
        identity = json.dumps(
            {
                "run_id": run_id,
                "object_ids": list(object_ids),
                "parser_version": parser_version,
            },
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
            "input_row_count": len(object_ids),
            "output_row_count": None,
            "quality_result_json": None,
            "metrics_json": json.dumps({"object_count": len(object_ids)}),
            "error_code": None,
        }
        self.meta.record_run(run_record)
        try:
            self.staging_root.mkdir(parents=True, exist_ok=True)
            parsed_rows: list[dict[str, object]] = []
            row_counts: dict[str, int] = {}
            grid_cache: dict[tuple[str, str, str], set[int]] = {}
            with TemporaryDirectory(prefix="weather-bronze-", dir=self.staging_root) as temp:
                temporary_root = Path(temp)
                for index, row in enumerate(source_rows):
                    current = self._parse_registered_object(
                        row,
                        run_id=run_id,
                        parser_version=parser_version,
                        temporary_root=temporary_root,
                        object_index=index,
                        grid_cache=grid_cache,
                    )
                    row_counts[row.object_id] = len(current)
                    parsed_rows.extend(current)
                snapshot_id = self.writer.replace_objects_rows(
                    (self.bronze_namespace, "weather_raster_slice"),
                    object_ids,
                    parsed_rows,
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
            self.meta.record_qualities(
                [
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "dataset_id": dataset_id,
                        "rule_id": "nonempty_weather_parse",
                        "rule_version": "v1",
                        "status": "passed",
                        "severity": "fatal",
                        "checked_at": now,
                        "check_phase": "post_commit",
                        "scope_key": object_id,
                        "observed_value_json": json.dumps(
                            {"row_count": row_counts[object_id]}
                        ),
                        "expected_value_json": None,
                        "failed_row_count": 0,
                        "sample_uri": None,
                        "snapshot_table": dataset_id,
                        "snapshot_id": snapshot_id,
                    }
                    for object_id in object_ids
                ]
            )
            lineage_edge_ids = self.meta.record_lineages(
                [
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "input_object_id": object_id,
                        "output_table": dataset_id,
                        "output_snapshot_id": snapshot_id,
                        "transform_role": "source",
                        "mapping_version": parser_version,
                        "created_at": now,
                    }
                    for object_id in object_ids
                ]
            )
            self.lifecycle.mark_bronze_evidence_many(
                [
                    (object_id, snapshot_id, "passed", lineage_edge_id, now)
                    for object_id, lineage_edge_id in zip(
                        object_ids, lineage_edge_ids, strict=True
                    )
                ]
            )
            self.meta.record_run(
                {
                    **run_record,
                    "finished_at": now,
                    "published_at": now,
                    "status": "succeeded",
                    "output_row_count": len(parsed_rows),
                    "quality_result_json": '{"status":"passed"}',
                }
            )
            return {
                "pipeline_run_id": pipeline_run_id,
                "object_ids": list(object_ids),
                "table_name": dataset_id,
                "snapshot_id": snapshot_id,
                "row_count": len(parsed_rows),
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

    def process_object(
        self,
        source_id: str,
        object_id: str,
        *,
        run_id: str,
        parser_version: str,
    ) -> dict[str, object]:
        """Preserve the single-object API through the atomic batch implementation."""
        result = self.process_batch(
            source_id,
            (object_id,),
            run_id=run_id,
            parser_version=parser_version,
        )
        return {**result, "object_id": object_id}
