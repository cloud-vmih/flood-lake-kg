"""Object-scoped Raw-to-Bronze service for dynamic weather sources."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from pyiceberg.expressions import EqualTo, In

from flashflood_data.catalog import sha256_file
from flashflood_data.pipelines.weather_ingest.parsers import parse_weather_object
from flashflood_data.pipelines.weather_ingest.quality import (
    default_weather_quality_policy,
    evaluate_weather_slices,
)
from flashflood_data.shared.contracts import SourceObjectRow
from flashflood_data.shared.quality import (
    DataQualityBlock,
    ObjectRejection,
    QualityAuditContext,
    QualityEvaluation,
    QualityPolicy,
    QuarantineRepository,
    RowRejection,
    fatal_failures,
    quality_rows,
)

_SOURCES = frozenset({"gsmap", "era5_land", "ifs_openmeteo"})


def _raw_key(row: SourceObjectRow, bucket: str) -> tuple[str, str]:
    uri = urlsplit(row.object_uri)
    if uri.scheme != "s3" or uri.netloc != bucket or uri.query or uri.fragment:
        raise ValueError("registered weather object is outside the Raw bucket")
    path = PurePosixPath(uri.path.lstrip("/"))
    if not path.parts or any(part in {".", ".."} for part in path.parts):
        raise ValueError("registered weather object has an unsafe path")
    return f"{bucket}/{path}", path.name


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
        quality_policy: QualityPolicy | None = None,
        quarantine: QuarantineRepository | None = None,
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
        self.quality_policy = quality_policy or default_weather_quality_policy()
        self.quarantine = quarantine or QuarantineRepository(writer)

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
        lineage = (
            lineage_table.scan(
                selected_fields=(
                    "pipeline_run_id",
                    "input_kind",
                    "input_object_id",
                    "output_table",
                    "mapping_version",
                )
            )
            .to_arrow()
            .to_pylist()
        )
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
        runs = (
            runs_table.scan(
                row_filter=run_filter,
                selected_fields=("pipeline_run_id", "status", "published_at"),
            )
            .to_arrow()
            .to_pylist()
        )
        published_runs = {
            str(row["pipeline_run_id"])
            for row in runs
            if row.get("status") in {"succeeded", "succeeded_with_warning"}
            and row.get("published_at") is not None
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
        rows = (
            table.scan(row_filter=expression, selected_fields=("object_id", "parser_version"))
            .to_arrow()
            .to_pylist()
        )
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
    ) -> QualityEvaluation:
        """Download one object, then classify deterministic data defects."""
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
                raise ValueError("weather source grid has no cells for the registered scope")
            grid_cache[grid_key] = allowed_indices
        key, filename = _raw_key(row, self.raw_bucket)
        object_root = temporary_root / f"object-{object_index:04d}"
        object_root.mkdir()
        local = object_root / filename
        self.object_store.download(key, local)
        checksum_valid = (
            local.stat().st_size == row.size_bytes and sha256_file(local) == row.checksum
        )
        current: list[dict[str, object]] = []
        readable = checksum_valid
        parse_error: str | None = None
        if checksum_valid:
            try:
                current = list(
                    parse_weather_object(
                        row,
                        local,
                        run_id=run_id,
                        parser_version=parser_version,
                    )
                )
            except (KeyError, TypeError, ValueError) as error:
                readable = False
                parse_error = str(error)
        return evaluate_weather_slices(
            current,
            allowed_indices,
            self.quality_policy,
            object_context={
                "object_id": row.object_id,
                "checksum_valid": checksum_valid,
                "readable": readable,
                "error": parse_error,
            },
        )

    def _record_quarantine(
        self,
        *,
        evaluation: QualityEvaluation,
        pipeline_run_id: str,
        dataset_id: str,
        source_id: str,
        object_id: str,
        parser_version: str,
        rejected_at: datetime,
    ) -> tuple[int, ...]:
        snapshots: list[int] = []
        if evaluation.rejected_rows:
            _, snapshot_id = self.quarantine.reject_rows(
                [
                    RowRejection(
                        pipeline_run_id=pipeline_run_id,
                        layer="bronze",
                        target_dataset_id=dataset_id,
                        source_record_key=rejected.source_record_key,
                        quality_policy_id=self.quality_policy.policy_id,
                        findings=rejected.findings,
                        reason_codes=tuple(item.rule_id for item in rejected.findings),
                        object_id=object_id,
                        record=rejected.record,
                        record_uri=rejected.record_uri,
                        parser_version=parser_version,
                        rejected_at=rejected_at,
                    )
                    for rejected in evaluation.rejected_rows
                ]
            )
            snapshots.append(snapshot_id)
        for finding in fatal_failures(evaluation.findings):
            _, snapshot_id = self.quarantine.reject_object(
                ObjectRejection(
                    pipeline_run_id=pipeline_run_id,
                    layer="bronze",
                    target_dataset_id=dataset_id,
                    source_id=source_id,
                    object_id=object_id,
                    scope_key=finding.scope_key or object_id,
                    quality_policy_id=self.quality_policy.policy_id,
                    rule_id=finding.rule_id,
                    rule_version=finding.rule_version,
                    reason_code=finding.rule_id,
                    detail={
                        "observed_value": finding.observed_value,
                        "expected_value": finding.expected_value,
                    },
                    rejected_at=rejected_at,
                )
            )
            snapshots.append(snapshot_id)
        return tuple(snapshots)

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
            evaluations: dict[str, QualityEvaluation] = {}
            grid_cache: dict[tuple[str, str, str], set[int]] = {}
            with TemporaryDirectory(prefix="weather-bronze-", dir=self.staging_root) as temp:
                temporary_root = Path(temp)
                for index, row in enumerate(source_rows):
                    evaluations[row.object_id] = self._parse_registered_object(
                        row,
                        run_id=run_id,
                        parser_version=parser_version,
                        temporary_root=temporary_root,
                        object_index=index,
                        grid_cache=grid_cache,
                    )

            accepted_by_object = {
                object_id: list(evaluation.accepted_rows)
                for object_id, evaluation in evaluations.items()
                if evaluation.disposition != "block_object"
            }
            blocked_object_ids = tuple(
                object_id
                for object_id, evaluation in evaluations.items()
                if evaluation.disposition == "block_object"
            )
            now = datetime.now(UTC)
            for object_id, evaluation in evaluations.items():
                self._record_quarantine(
                    evaluation=evaluation,
                    pipeline_run_id=pipeline_run_id,
                    dataset_id=dataset_id,
                    source_id=source_id,
                    object_id=object_id,
                    parser_version=parser_version,
                    rejected_at=now,
                )

            if blocked_object_ids:
                self.meta.record_qualities(
                    [
                        row
                        for object_id in blocked_object_ids
                        for row in quality_rows(
                            evaluations[object_id],
                            QualityAuditContext(
                                pipeline_run_id=pipeline_run_id,
                                dataset_id=dataset_id,
                                check_phase="pre_commit",
                                checked_at=now,
                            ),
                        )
                    ]
                )
            if not accepted_by_object:
                self.writer.replace_object_partitions(
                    (self.bronze_namespace, "weather_raster_slice"),
                    {},
                    blocked_object_ids,
                )
                self.meta.record_run(
                    {
                        **run_record,
                        "finished_at": now,
                        "status": "failed_data_quality",
                        "quality_result_json": '{"status":"blocked"}',
                        "error_code": "data_quality_block",
                    }
                )
                raise DataQualityBlock(
                    "weather Bronze quality gate blocked every object in the batch",
                    evaluations[blocked_object_ids[0]],
                )

            snapshot_id = self.writer.replace_object_partitions(
                (self.bronze_namespace, "weather_raster_slice"),
                accepted_by_object,
                blocked_object_ids,
            )
            accepted_object_ids = tuple(accepted_by_object)
            parsed_rows = [
                row
                for object_rows in accepted_by_object.values()
                for row in object_rows
            ]
            rejected_count = sum(len(item.rejected_rows) for item in evaluations.values())
            warning_count = sum(
                row.get("quality_status") == "warning"
                for evaluation in evaluations.values()
                for row in evaluation.accepted_rows
            )
            status = (
                "succeeded_with_warning"
                if blocked_object_ids or rejected_count or warning_count
                else "succeeded"
            )
            self.meta.record_snapshot_ref(
                pipeline_run_id=pipeline_run_id,
                table_name=dataset_id,
                iceberg_snapshot_id=snapshot_id,
                role="output",
                quality_status="warning" if status == "succeeded_with_warning" else "passed",
                created_at=now,
            )
            self.meta.record_qualities(
                [
                    row
                    for object_id in accepted_object_ids
                    for row in quality_rows(
                        evaluations[object_id],
                        QualityAuditContext(
                            pipeline_run_id=pipeline_run_id,
                            dataset_id=dataset_id,
                            check_phase="post_commit",
                            checked_at=now,
                            snapshot_table=dataset_id,
                            snapshot_id=snapshot_id,
                        ),
                    )
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
                    for object_id in accepted_object_ids
                ]
            )
            self.lifecycle.mark_bronze_evidence_many(
                [
                    (
                        object_id,
                        snapshot_id,
                        (
                            "failed"
                            if evaluations[object_id].rejected_rows
                            else "warning"
                            if any(
                                row.get("quality_status") == "warning"
                                for row in evaluations[object_id].accepted_rows
                            )
                            else "passed"
                        ),
                        lineage_edge_id,
                        now,
                    )
                    for object_id, lineage_edge_id in zip(
                        accepted_object_ids, lineage_edge_ids, strict=True
                    )
                ]
            )
            self.meta.record_run(
                {
                    **run_record,
                    "finished_at": now,
                    "published_at": now,
                    "status": status,
                    "output_row_count": len(parsed_rows),
                    "quality_result_json": json.dumps(
                        {
                            "status": "warning" if status == "succeeded_with_warning" else "passed",
                            "blocked_object_count": len(blocked_object_ids),
                            "rejected_count": rejected_count,
                            "warning_count": warning_count,
                            "quality_policy_id": self.quality_policy.policy_id,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
            )
            return {
                "pipeline_run_id": pipeline_run_id,
                "object_ids": list(accepted_object_ids),
                "blocked_object_ids": list(blocked_object_ids),
                "table_name": dataset_id,
                "snapshot_id": snapshot_id,
                "row_count": len(parsed_rows),
                "rejected_count": rejected_count,
                "warning_count": warning_count,
                "status": status,
            }
        except DataQualityBlock:
            raise
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
