"""One-object Bronze parse transaction called by the Airflow parse DAG."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from pyiceberg.expressions import EqualTo, In

from flashflood_data.catalog import sha256_file
from flashflood_data.core.disk_index import DiskUniqueIndex
from flashflood_data.pipelines.static_bronze.osm import OsmAoi, OsmSelection, iter_osm_batches
from flashflood_data.pipelines.static_bronze.parsers import (
    iter_vector_batches,
    parse_raster,
    parse_vector,
)
from flashflood_data.pipelines.static_bronze.quality import (
    default_static_quality_policy,
    evaluate_static_object,
)
from flashflood_data.shared.contracts import SourceObjectRow
from flashflood_data.shared.metadata.service import MetaRecorder
from flashflood_data.shared.quality import (
    DataQualityBlock,
    ObjectRejection,
    QualityEvaluation,
    QualityPolicy,
    QualityResult,
    QuarantineRepository,
    RowRejection,
    fatal_failures,
)
from flashflood_data.storage.iceberg import SourceObjectInventory
from flashflood_data.storage.iceberg_schemas import BRONZE_KEYS
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import ObjectStore

_VECTOR_SOURCES = frozenset(
    {
        "hydrobasins_v1c",
        "basinatlas_v10",
        "hydrorivers_v10",
        "sonla_admin_2025",
        "gadm_vnm_4_1",
    }
)
_RASTER_SOURCES = frozenset(
    {
        "cop_dem_glo30_2024_1",
        "soilgrids_2_0",
        "esa_worldcover_2021_v200",
        "worldpop_vnm_2025",
    }
)
_OSM_SOURCE = "geofabrik_vietnam_snapshot"
_BATCHED_SOURCES = frozenset({"sonla_admin_2025", "soilgrids_2_0"})
_SOURCE_TO_TABLE = {
    "hydrobasins_v1c": "basin_polygon_raw",
    "basinatlas_v10": "basin_polygon_raw",
    "hydrorivers_v10": "river_reach_raw",
    "sonla_admin_2025": "admin_boundary_raw",
    "gadm_vnm_4_1": "admin_boundary_raw",
    "cop_dem_glo30_2024_1": "raster_coverage",
    "soilgrids_2_0": "raster_coverage",
    "esa_worldcover_2021_v200": "raster_coverage",
    "worldpop_vnm_2025": "raster_coverage",
    "geofabrik_vietnam_snapshot": "osm_feature_raw",
}


def _raw_key(row: SourceObjectRow, bucket: str) -> tuple[str, str]:
    parsed = urlsplit(row.object_uri)
    if parsed.scheme != "s3" or parsed.netloc != bucket or parsed.query or parsed.fragment:
        raise ValueError("registered raw object URI is outside the configured bucket")
    path = PurePosixPath(parsed.path.lstrip("/"))
    if not path.parts or any(part in {".", ".."} for part in path.parts):
        raise ValueError("registered raw object URI has an unsafe path")
    return f"{bucket}/{path}", path.name


def _table_for(
    row: SourceObjectRow, path: Path, run_id: str, version: str
) -> tuple[str, list[dict[str, object]]]:
    suffix = path.suffix.lower()
    if row.source_id in _VECTOR_SOURCES and suffix in {".zip", ".gpkg", ".geojson", ".shp"}:
        return parse_vector(
            row.source_id, path, object_id=row.object_id, run_id=run_id, parser_version=version
        )
    if row.source_id in _RASTER_SOURCES and suffix in {".tif", ".tiff", ".dem"}:
        return "raster_coverage", parse_raster(
            path,
            object_id=row.object_id,
            object_uri=row.object_uri,
            checksum=row.checksum,
            run_id=run_id,
            parser_version=version,
            source_id=row.source_id,
        )
    raise ValueError(f"no Bronze parser for {row.source_id} and {suffix}")


def eligible_raw_object(row: SourceObjectRow) -> bool:
    """Exclude known sidecars and unsupported formats from automatic parse mapping."""
    suffix = PurePosixPath(urlsplit(row.object_uri).path).suffix.lower()
    return (
        row.source_id in _VECTOR_SOURCES
        and suffix in {".zip", ".gpkg", ".geojson", ".shp"}
        or row.source_id in _RASTER_SOURCES
        and suffix in {".tif", ".tiff", ".dem"}
        or row.source_id == _OSM_SOURCE
        and suffix == ".pbf"
    )


def _quality_row(
    *,
    pipeline_run_id: str,
    dataset_id: str,
    object_id: str,
    result: QualityResult,
    checked_at: datetime,
    phase: str,
    snapshot_id: int | None = None,
) -> dict[str, object]:
    return {
        "pipeline_run_id": pipeline_run_id,
        "check_phase": phase,
        "dataset_id": dataset_id,
        "rule_id": result.rule_id,
        "rule_version": result.rule_version,
        "scope_key": object_id,
        "severity": result.severity,
        "status": result.status,
        "observed_value_json": json.dumps(result.observed_value, sort_keys=True),
        "expected_value_json": (
            None
            if result.expected_value is None
            else json.dumps(result.expected_value, sort_keys=True, default=str)
        ),
        "failed_row_count": result.failed_row_count,
        "sample_uri": None,
        "snapshot_table": dataset_id if snapshot_id is not None else None,
        "snapshot_id": snapshot_id,
        "checked_at": checked_at,
    }


class BronzeService:
    """Fetch one already registered raw object and publish its parsed Bronze slice."""

    def __init__(
        self,
        *,
        inventory: SourceObjectInventory,
        object_store: ObjectStore,
        writer: IcebergTableStore,
        meta: MetaRecorder,
        raw_bucket: str,
        staging_root: Path,
        catalog_name: str = "flood_lakehouse",
        bronze_namespace: str = "bronze",
        vector_batch_size: int = 5_000,
        osm_selection: OsmSelection | None = None,
        osm_aoi: OsmAoi | None = None,
        quality_policy: QualityPolicy | None = None,
        quarantine: QuarantineRepository | None = None,
    ) -> None:
        self.inventory = inventory
        self.object_store = object_store
        self.writer = writer
        self.meta = meta
        self.raw_bucket = raw_bucket
        self.staging_root = Path(staging_root)
        self.catalog_name = catalog_name
        self.bronze_namespace = bronze_namespace
        if vector_batch_size < 1:
            raise ValueError("vector_batch_size must be positive")
        self.vector_batch_size = vector_batch_size
        self.osm_selection = osm_selection
        self.osm_aoi = osm_aoi
        self.quality_policy = quality_policy or default_static_quality_policy()
        self.quarantine = quarantine or QuarantineRepository(writer)

    def _record_evaluation_quarantine(
        self,
        *,
        evaluation: QualityEvaluation,
        pipeline_run_id: str,
        dataset_id: str,
        source_id: str,
        object_id: str,
        parser_version: str,
    ) -> tuple[int, ...]:
        snapshots: list[int] = []
        now = datetime.now(UTC)
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
                        rejected_at=now,
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
                    scope_key=finding.scope_key or object_id,
                    quality_policy_id=self.quality_policy.policy_id,
                    rule_id=finding.rule_id,
                    rule_version=finding.rule_version,
                    reason_code=finding.rule_id,
                    detail={
                        "observed_value": finding.observed_value,
                        "expected_value": finding.expected_value,
                    },
                    source_id=source_id,
                    object_id=object_id,
                    rejected_at=now,
                )
            )
            snapshots.append(snapshot_id)
        return tuple(snapshots)

    def _raise_quality_block(
        self,
        *,
        evaluation: QualityEvaluation,
        run_record: dict[str, object],
        pipeline_run_id: str,
        dataset_id: str,
        source_id: str,
        object_id: str,
        parser_version: str,
        message: str = "Bronze quality gate blocked object",
    ) -> None:
        self._record_evaluation_quarantine(
            evaluation=evaluation,
            pipeline_run_id=pipeline_run_id,
            dataset_id=dataset_id,
            source_id=source_id,
            object_id=object_id,
            parser_version=parser_version,
        )
        checked_at = datetime.now(UTC)
        self.meta.record_qualities(
            [
                _quality_row(
                    pipeline_run_id=pipeline_run_id,
                    dataset_id=dataset_id,
                    object_id=object_id,
                    result=finding,
                    checked_at=checked_at,
                    phase="pre_commit",
                )
                for finding in evaluation.findings
            ]
        )
        self.meta.record_run(
            {
                **run_record,
                "status": "failed_data_quality",
                "finished_at": checked_at,
                "error_code": "data_quality_block",
                "quality_result_json": '{"status":"blocked"}',
            }
        )
        raise DataQualityBlock(message, evaluation)

    def _mapping_version(self, source_id: str, parser_version: str) -> str:
        if source_id == _OSM_SOURCE:
            if self.osm_selection is None or self.osm_aoi is None:
                raise ValueError("OSM selection policy and exposure AOI are not configured")
            return f"{parser_version}|osm:{self.osm_selection.version}|aoi:{self.osm_aoi.version}"
        return parser_version

    def _published_object_ids(
        self, source_id: str, object_ids: tuple[str, ...], parser_version: str
    ) -> set[str]:
        """Require published lineage and a surviving Bronze slice before skipping work."""
        if not object_ids:
            return set()
        table_name = _SOURCE_TO_TABLE[source_id]
        dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.{table_name}"
        mapping_version = self._mapping_version(source_id, parser_version)
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
            and row.get("mapping_version") == mapping_version
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
        published_run_ids = {
            str(row["pipeline_run_id"])
            for row in runs
            if row.get("pipeline_run_id") in run_ids
            and row.get("status") in {"succeeded", "succeeded_with_warning"}
            and row.get("published_at") is not None
        }
        candidates_with_published_runs = {
            object_id for object_id, run_id in candidates if run_id in published_run_ids
        }
        if not candidates_with_published_runs:
            return set()

        bronze_table = self.writer.ensure_table((self.bronze_namespace, table_name))
        bronze_table.refresh()
        bronze_ids = tuple(sorted(candidates_with_published_runs))
        bronze_filter = (
            EqualTo("object_id", bronze_ids[0])
            if len(bronze_ids) == 1
            else In("object_id", bronze_ids)
        )
        rows = (
            bronze_table.scan(row_filter=bronze_filter, selected_fields=("object_id",))
            .to_arrow()
            .to_pylist()
        )
        return {
            str(row["object_id"])
            for row in rows
            if row.get("object_id") in candidates_with_published_runs
        }

    def discover(
        self,
        source_id: str,
        *,
        parser_version: str = "v1",
        force_reprocess: bool = False,
    ) -> tuple[str, ...]:
        """Return eligible object IDs that lack a valid published Bronze result."""
        rows = self.inventory.available_objects(source_id)
        eligible = tuple(row.object_id for row in rows if eligible_raw_object(row))
        if force_reprocess:
            return eligible
        published = self._published_object_ids(source_id, eligible, parser_version)
        return tuple(object_id for object_id in eligible if object_id not in published)

    def process_batch(
        self,
        source_id: str,
        object_ids: tuple[str, ...],
        *,
        run_id: str,
        parser_version: str,
    ) -> dict[str, object]:
        """Parse several small raw objects and publish one atomic Bronze snapshot."""
        if source_id not in _BATCHED_SOURCES:
            raise ValueError(f"source does not support Bronze object batching: {source_id}")
        if not object_ids or len(set(object_ids)) != len(object_ids):
            raise ValueError("Bronze batch object IDs must be non-empty and unique")
        available = {row.object_id: row for row in self.inventory.available_objects(source_id)}
        if any(object_id not in available for object_id in object_ids):
            raise LookupError("Bronze batch contains an unavailable raw object")
        rows = [available[object_id] for object_id in object_ids]
        if any(not eligible_raw_object(row) for row in rows):
            raise ValueError("Bronze batch contains an ineligible raw object")

        started_at = datetime.now(UTC)
        mapping_version = self._mapping_version(source_id, parser_version)
        identity = json.dumps(
            {
                "run_id": run_id,
                "source_id": source_id,
                "object_ids": list(object_ids),
                "mapping_version": mapping_version,
            },
            sort_keys=True,
        )
        pipeline_run_id = sha256(identity.encode("utf-8")).hexdigest()
        run_record: dict[str, object] = {
            "pipeline_run_id": pipeline_run_id,
            "orchestrator_run_id": run_id,
            "job_name": f"bronze_parse:{source_id}",
            "code_git_sha": None,
            "image_digest": None,
            "config_hash": sha256(mapping_version.encode("utf-8")).hexdigest(),
            "parameter_set_id": None,
            "started_at": started_at,
            "finished_at": None,
            "published_at": None,
            "status": "running",
            "retry_count": 0,
            "input_row_count": len(rows),
            "output_row_count": None,
            "quality_result_json": None,
            "metrics_json": json.dumps({"object_count": len(rows)}, sort_keys=True),
            "error_code": None,
        }
        self.meta.record_run(run_record)
        table_name = _SOURCE_TO_TABLE[source_id]
        dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.{table_name}"
        evaluations: dict[str, QualityEvaluation] = {}
        try:
            self.staging_root.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(prefix="bronze-batch-", dir=self.staging_root) as temporary:
                temporary_root = Path(temporary)
                for index, row in enumerate(rows):
                    key, filename = _raw_key(row, self.raw_bucket)
                    object_root = temporary_root / f"object-{index:04d}"
                    object_root.mkdir()
                    local = object_root / filename
                    self.object_store.download(key, local)
                    checksum_valid = (
                        local.stat().st_size == row.size_bytes
                        and sha256_file(local) == row.checksum
                    )
                    current_rows: list[dict[str, object]] = []
                    readable = checksum_valid
                    crs_usable = checksum_valid
                    if checksum_valid:
                        try:
                            parsed_table, current_rows = _table_for(
                                row, local, run_id, parser_version
                            )
                            if parsed_table != table_name:
                                raise ValueError(
                                    "Bronze batch resolved to multiple target tables"
                                )
                        except (TypeError, ValueError) as error:
                            readable = False
                            crs_usable = "crs" not in str(error).lower()
                    evaluations[row.object_id] = evaluate_static_object(
                        table_name,
                        {
                            "object_id": row.object_id,
                            "checksum_valid": checksum_valid,
                            "readable": readable,
                            "crs_usable": crs_usable,
                        },
                        current_rows,
                        self.quality_policy,
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
            for object_id, evaluation in evaluations.items():
                self._record_evaluation_quarantine(
                    evaluation=evaluation,
                    pipeline_run_id=pipeline_run_id,
                    dataset_id=dataset_id,
                    source_id=source_id,
                    object_id=object_id,
                    parser_version=parser_version,
                )

            if blocked_object_ids:
                checked_at = datetime.now(UTC)
                self.meta.record_qualities(
                    [
                        _quality_row(
                            pipeline_run_id=pipeline_run_id,
                            dataset_id=dataset_id,
                            object_id=object_id,
                            result=finding,
                            checked_at=checked_at,
                            phase="pre_commit",
                        )
                        for object_id in blocked_object_ids
                        for finding in evaluations[object_id].findings
                    ]
                )
            if not accepted_by_object:
                self.writer.replace_object_partitions(
                    (self.bronze_namespace, table_name), {}, blocked_object_ids
                )
                now = datetime.now(UTC)
                self.meta.record_run(
                    {
                        **run_record,
                        "status": "failed_data_quality",
                        "finished_at": now,
                        "error_code": "data_quality_block",
                        "quality_result_json": '{"status":"blocked"}',
                    }
                )
                raise DataQualityBlock(
                    "Bronze quality gate blocked every object in the batch",
                    evaluations[blocked_object_ids[0]],
                )

            snapshot_id = self.writer.replace_object_partitions(
                (self.bronze_namespace, table_name),
                accepted_by_object,
                blocked_object_ids,
            )
            now = datetime.now(UTC)
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
                    _quality_row(
                        pipeline_run_id=pipeline_run_id,
                        dataset_id=dataset_id,
                        object_id=object_id,
                        result=result,
                        checked_at=now,
                        phase="post_commit",
                        snapshot_id=snapshot_id,
                    )
                    for object_id, evaluation in evaluations.items()
                    if object_id in accepted_by_object
                    for result in evaluation.findings
                ]
            )
            self.meta.record_lineages(
                [
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "input_object_id": object_id,
                        "output_table": dataset_id,
                        "output_snapshot_id": snapshot_id,
                        "transform_role": "source",
                        "mapping_version": mapping_version,
                        "created_at": now,
                    }
                    for object_id in accepted_by_object
                ]
            )
            self.meta.record_run(
                {
                    **run_record,
                    "status": status,
                    "finished_at": now,
                    "published_at": now,
                    "output_row_count": sum(map(len, accepted_by_object.values())),
                    "quality_result_json": json.dumps(
                        {
                            "status": "warning" if status == "succeeded_with_warning" else "passed",
                            "blocked_object_count": len(blocked_object_ids),
                            "rejected_row_count": rejected_count,
                        },
                        sort_keys=True,
                    ),
                }
            )
            return {
                "pipeline_run_id": pipeline_run_id,
                "object_ids": list(object_ids),
                "table_name": dataset_id,
                "snapshot_id": snapshot_id,
                "row_count": sum(map(len, accepted_by_object.values())),
                "accepted_object_count": len(accepted_by_object),
                "blocked_object_count": len(blocked_object_ids),
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
                    "status": "failed",
                    "finished_at": datetime.now(UTC),
                    "error_code": type(error).__name__,
                }
            )
            raise

    def process_object(
        self, source_id: str, object_id: str, *, run_id: str, parser_version: str
    ) -> dict[str, object]:
        matches = [
            row for row in self.inventory.available_objects(source_id) if row.object_id == object_id
        ]
        if len(matches) != 1:
            raise LookupError(f"available raw object is not unique: {object_id}")
        row = matches[0]
        if not eligible_raw_object(row):
            raise ValueError(f"raw object has no eligible Bronze parser: {object_id}")
        key, filename = _raw_key(row, self.raw_bucket)
        started_at = datetime.now(UTC)
        mapping_version = self._mapping_version(source_id, parser_version)
        identity = json.dumps(
            {"run_id": run_id, "object_id": object_id, "mapping_version": mapping_version},
            sort_keys=True,
        )
        pipeline_run_id = sha256(identity.encode("utf-8")).hexdigest()
        run_record: dict[str, object] = {
            "pipeline_run_id": pipeline_run_id,
            "orchestrator_run_id": run_id,
            "job_name": f"bronze_parse:{source_id}",
            "code_git_sha": None,
            "image_digest": None,
            "config_hash": sha256(mapping_version.encode("utf-8")).hexdigest(),
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
            with TemporaryDirectory(prefix="bronze-", dir=self.staging_root) as temporary:
                local = Path(temporary) / filename
                self.object_store.download(key, local)
                checksum_valid = (
                    local.stat().st_size == row.size_bytes
                    and sha256_file(local) == row.checksum
                )
                if not checksum_valid:
                    table = _SOURCE_TO_TABLE[source_id]
                    dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.{table}"
                    evaluation = evaluate_static_object(
                        table,
                        {
                            "object_id": object_id,
                            "checksum_valid": False,
                            "readable": True,
                            "crs_usable": True,
                        },
                        [],
                        self.quality_policy,
                    )
                    self._raise_quality_block(
                        evaluation=evaluation,
                        run_record=run_record,
                        pipeline_run_id=pipeline_run_id,
                        dataset_id=dataset_id,
                        source_id=source_id,
                        object_id=object_id,
                        parser_version=parser_version,
                        message="Bronze quality gate rejected Raw checksum",
                    )
                vector = row.source_id in _VECTOR_SOURCES and local.suffix.lower() in {
                    ".zip",
                    ".gpkg",
                    ".geojson",
                    ".shp",
                }
                osm = row.source_id == _OSM_SOURCE and local.name.lower().endswith(".osm.pbf")
                if vector or osm:
                    table = (
                        "river_reach_raw"
                        if row.source_id == "hydrorivers_v10"
                        else "admin_boundary_raw"
                        if row.source_id in {"sonla_admin_2025", "gadm_vnm_4_1"}
                        else "osm_feature_raw"
                        if osm
                        else "basin_polygon_raw"
                    )
                    dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.{table}"
                    quality_results: list[QualityResult] = []
                    checked_count = 0
                    accepted_count = 0
                    rejected_records = []
                    final_evaluation: QualityEvaluation | None = None

                    if osm and (self.osm_selection is None or self.osm_aoi is None):
                        raise ValueError(
                            "OSM selection policy and exposure AOI are not configured"
                        )

                    def checked_batches():
                        nonlocal checked_count, accepted_count, final_evaluation
                        if osm:
                            batches = iter_osm_batches(
                                local,
                                object_id=object_id,
                                run_id=run_id,
                                parser_version=parser_version,
                                selection=self.osm_selection,
                                aoi=self.osm_aoi.geometry,
                                batch_size=self.vector_batch_size,
                            )
                        else:
                            batches = iter_vector_batches(
                                row.source_id,
                                local,
                                object_id=object_id,
                                run_id=run_id,
                                parser_version=parser_version,
                                batch_size=self.vector_batch_size,
                            )
                        try:
                            with DiskUniqueIndex(directory=Path(temporary)) as seen:
                                for batch in batches:
                                    checked_count += len(batch)
                                    marked_batch: list[dict[str, object]] = []
                                    for source_row in batch:
                                        current = dict(source_row)
                                        identity = tuple(
                                            current.get(name) for name in BRONZE_KEYS[table]
                                        )
                                        if None not in identity:
                                            token = json.dumps(
                                                identity,
                                                sort_keys=True,
                                                separators=(",", ":"),
                                                default=str,
                                            )
                                            if not seen.add(token):
                                                current["_dq_duplicate_business_key"] = True
                                        marked_batch.append(current)
                                    evaluation = evaluate_static_object(
                                        table,
                                        {
                                            "object_id": object_id,
                                            "checksum_valid": True,
                                            "readable": True,
                                            "crs_usable": True,
                                        },
                                        marked_batch,
                                        self.quality_policy,
                                        require_accepted=False,
                                    )
                                    quality_results.extend(
                                        finding
                                        for finding in evaluation.findings
                                        if finding.rule_id
                                        not in {
                                            "object_checksum",
                                            "object_readable",
                                            "crs_usable",
                                            "nonempty_parse",
                                        }
                                    )
                                    rejected_records.extend(evaluation.rejected_rows)
                                    accepted_count += len(evaluation.accepted_rows)
                                    if evaluation.accepted_rows:
                                        yield evaluation.accepted_rows
                        except (TypeError, ValueError) as error:
                            failed = evaluate_static_object(
                                table,
                                {
                                    "object_id": object_id,
                                    "checksum_valid": True,
                                    "readable": False,
                                    "crs_usable": "crs" not in str(error).lower(),
                                },
                                [],
                                self.quality_policy,
                            )
                            self._raise_quality_block(
                                evaluation=failed,
                                run_record=run_record,
                                pipeline_run_id=pipeline_run_id,
                                dataset_id=dataset_id,
                                source_id=source_id,
                                object_id=object_id,
                                parser_version=parser_version,
                                message=f"Bronze parser rejected object: {error}",
                            )

                        summary = evaluate_static_object(
                            table,
                            {
                                "object_id": object_id,
                                "checksum_valid": True,
                                "readable": True,
                                "crs_usable": True,
                            },
                            [],
                            self.quality_policy,
                        )
                        object_findings = [
                            finding
                            for finding in summary.findings
                            if finding.rule_id != "nonempty_parse"
                        ]
                        object_findings.append(
                            QualityResult(
                                rule_id="nonempty_parse",
                                rule_version="v1",
                                status="passed" if checked_count else "failed",
                                severity="fatal",
                                scope_key=object_id,
                                observed_value={"row_count": checked_count},
                                expected_value={"minimum_row_count": 1},
                                failed_row_count=0 if checked_count else 1,
                            )
                        )
                        quality_results[:0] = object_findings
                        final_evaluation = QualityEvaluation.from_parts(
                            accepted_rows=(),
                            rejected_rows=rejected_records,
                            findings=quality_results,
                            input_row_count=checked_count if accepted_count == 0 else 0,
                        )
                        if accepted_count == 0:
                            self._raise_quality_block(
                                evaluation=final_evaluation,
                                run_record=run_record,
                                pipeline_run_id=pipeline_run_id,
                                dataset_id=dataset_id,
                                source_id=source_id,
                                object_id=object_id,
                                parser_version=parser_version,
                            )

                    snapshot_id, row_count = self.writer.replace_object_batches(
                        (self.bronze_namespace, table), object_id, checked_batches()
                    )
                    if final_evaluation is None:
                        raise RuntimeError("static Bronze DQ evaluation did not finish")
                    self._record_evaluation_quarantine(
                        evaluation=final_evaluation,
                        pipeline_run_id=pipeline_run_id,
                        dataset_id=dataset_id,
                        source_id=source_id,
                        object_id=object_id,
                        parser_version=parser_version,
                    )
                    has_warning = any(
                        result.severity == "warning"
                        and result.status in {"failed", "error"}
                        for result in quality_results
                    )
                    publish_status = (
                        "succeeded_with_warning"
                        if rejected_records or has_warning
                        else "succeeded"
                    )
                    snapshot_quality_status = (
                        "warning" if publish_status == "succeeded_with_warning" else "passed"
                    )
                    for result in quality_results:
                        self._record_precommit_quality(
                            pipeline_run_id, dataset_id, object_id, result
                        )
                else:
                    table, parsed_rows = _table_for(row, local, run_id, parser_version)
                    dataset_id = f"{self.catalog_name}.{self.bronze_namespace}.{table}"
                    evaluation = evaluate_static_object(
                        table,
                        {
                            "object_id": object_id,
                            "checksum_valid": True,
                            "readable": True,
                            "crs_usable": True,
                        },
                        parsed_rows,
                        self.quality_policy,
                    )
                    quality_results = list(evaluation.findings)
                    if evaluation.disposition == "block_object":
                        self._raise_quality_block(
                            evaluation=evaluation,
                            run_record=run_record,
                            pipeline_run_id=pipeline_run_id,
                            dataset_id=dataset_id,
                            source_id=source_id,
                            object_id=object_id,
                            parser_version=parser_version,
                        )
                    self._record_evaluation_quarantine(
                        evaluation=evaluation,
                        pipeline_run_id=pipeline_run_id,
                        dataset_id=dataset_id,
                        source_id=source_id,
                        object_id=object_id,
                        parser_version=parser_version,
                    )
                    snapshot_id = self.writer.replace_object_rows(
                        (self.bronze_namespace, table),
                        object_id,
                        evaluation.accepted_rows,
                    )
                    row_count = len(evaluation.accepted_rows)
                    publish_status = (
                        "succeeded_with_warning"
                        if evaluation.disposition != "pass"
                        else "succeeded"
                    )
                    snapshot_quality_status = (
                        "warning" if publish_status == "succeeded_with_warning" else "passed"
                    )
            now = datetime.now(UTC)
            self.meta.record_snapshot_ref(
                pipeline_run_id=pipeline_run_id,
                table_name=dataset_id,
                iceberg_snapshot_id=snapshot_id,
                role="output",
                quality_status=snapshot_quality_status,
                created_at=now,
            )
            for result in quality_results:
                self.meta.record_quality(
                    pipeline_run_id=pipeline_run_id,
                    dataset_id=dataset_id,
                    rule_id=result.rule_id,
                    status=result.status,
                    severity=result.severity,
                    checked_at=now,
                    check_phase="post_commit",
                    rule_version=result.rule_version,
                    scope_key=result.scope_key or object_id,
                    observed_value_json=json.dumps(result.observed_value, sort_keys=True),
                    expected_value_json=(
                        None
                        if result.expected_value is None
                        else json.dumps(result.expected_value, sort_keys=True, default=str)
                    ),
                    failed_row_count=result.failed_row_count,
                    snapshot_table=dataset_id,
                    snapshot_id=snapshot_id,
                )
            self.meta.record_lineage(
                pipeline_run_id=pipeline_run_id,
                input_object_id=object_id,
                output_table=dataset_id,
                output_snapshot_id=snapshot_id,
                transform_role="source",
                mapping_version=mapping_version,
                created_at=now,
            )
            self.meta.record_run(
                {
                    **run_record,
                    "status": publish_status,
                    "finished_at": now,
                    "published_at": now,
                    "output_row_count": row_count,
                    "quality_result_json": json.dumps(
                        {"status": snapshot_quality_status}, sort_keys=True
                    ),
                }
            )
            return {
                "pipeline_run_id": pipeline_run_id,
                "object_id": object_id,
                "table_name": dataset_id,
                "snapshot_id": snapshot_id,
                "row_count": row_count,
                "status": publish_status,
            }
        except DataQualityBlock:
            if "table" in locals():
                self.writer.replace_object_partitions(
                    (self.bronze_namespace, table), {}, (object_id,)
                )
            raise
        except Exception as error:
            self.meta.record_run(
                {
                    **run_record,
                    "status": "failed",
                    "finished_at": datetime.now(UTC),
                    "error_code": type(error).__name__,
                }
            )
            raise

    def _record_precommit_quality(
        self, pipeline_run_id: str, dataset_id: str, object_id: str, result: QualityResult
    ) -> None:
        self.meta.record_quality(
            pipeline_run_id=pipeline_run_id,
            dataset_id=dataset_id,
            rule_id=result.rule_id,
            status=result.status,
            severity=result.severity,
            checked_at=datetime.now(UTC),
            check_phase="pre_commit",
            scope_key=object_id,
            observed_value_json=json.dumps(result.observed_value, sort_keys=True),
            failed_row_count=result.failed_row_count,
        )

    def reconcile(self, source_id: str | None = None) -> dict[str, object]:
        """Produce a read-only reconciliation report between raw objects, Bronze tables, and Meta."""
        sources_to_check = [source_id] if source_id is not None else sorted(_SOURCE_TO_TABLE)

        available_by_source: dict[str, list[str]] = {}
        all_available_object_ids: set[str] = set()
        for src in sources_to_check:
            try:
                raw_objects = self.inventory.available_objects(src)
                eligible = [obj.object_id for obj in raw_objects if eligible_raw_object(obj)]
                available_by_source[src] = eligible
                all_available_object_ids.update(obj.object_id for obj in raw_objects)
            except Exception:  # noqa: BLE001
                available_by_source[src] = []

        target_tables = sorted(
            {_SOURCE_TO_TABLE[src] for src in sources_to_check if src in _SOURCE_TO_TABLE}
        )
        bronze_rows_by_table: dict[str, list[dict[str, object]]] = {}
        for table_name in target_tables:
            try:
                table = self.writer.ensure_table((self.bronze_namespace, table_name))
                table.refresh()
                bronze_rows_by_table[table_name] = table.scan().to_arrow().to_pylist()
            except Exception:  # noqa: BLE001
                bronze_rows_by_table[table_name] = []

        available_without_bronze: dict[str, list[str]] = {}
        for src in sources_to_check:
            table_name = _SOURCE_TO_TABLE.get(src)
            if not table_name:
                continue
            table_rows = bronze_rows_by_table.get(table_name, [])
            committed_object_ids = {
                str(row["object_id"]) for row in table_rows if "object_id" in row
            }
            missing = [
                obj_id
                for obj_id in available_by_source.get(src, [])
                if obj_id not in committed_object_ids
            ]
            if missing:
                available_without_bronze[src] = missing

        orphaned_bronze_rows: list[dict[str, object]] = []
        for table_name, rows in bronze_rows_by_table.items():
            for row in rows:
                obj_id = str(row.get("object_id"))
                if obj_id not in all_available_object_ids:
                    orphaned_bronze_rows.append({"table": table_name, "object_id": obj_id})

        duplicate_business_keys: dict[str, list[dict[str, object]]] = {}
        for table_name, rows in bronze_rows_by_table.items():
            keys = ("object_id", *BRONZE_KEYS.get(table_name, ()))
            seen_keys: set[tuple[object, ...]] = set()
            duplicates: list[dict[str, object]] = []
            for row in rows:
                key_tuple = tuple(row.get(k) for k in keys)
                if key_tuple in seen_keys:
                    duplicates.append({k: row.get(k) for k in keys})
                else:
                    seen_keys.add(key_tuple)
            if duplicates:
                duplicate_business_keys[table_name] = duplicates

        missing_snapshot_refs: list[str] = []
        missing_lineage_edges: list[str] = []
        meta_ns = getattr(self.meta, "meta_namespace", "meta")
        try:
            runs_table = self.writer.ensure_table((meta_ns, "pipeline_runs"))
            runs_table.refresh()
            runs = runs_table.scan().to_arrow().to_pylist()

            snaps_table = self.writer.ensure_table((meta_ns, "table_snapshot_ref"))
            snaps_table.refresh()
            snaps = snaps_table.scan().to_arrow().to_pylist()
            snap_run_ids = {str(s.get("pipeline_run_id")) for s in snaps}

            lineage_table = self.writer.ensure_table((meta_ns, "lineage_edges"))
            lineage_table.refresh()
            lineage = lineage_table.scan().to_arrow().to_pylist()
            lineage_run_ids = {str(l.get("pipeline_run_id")) for l in lineage}

            published_runs = [r for r in runs if r.get("published_at") is not None]
            if source_id is not None:
                published_runs = [
                    r
                    for r in published_runs
                    if r.get("job_name")
                    in {f"bronze_parse:{source_id}", f"raw_landing:{source_id}"}
                ]

            for run in published_runs:
                pid = str(run.get("pipeline_run_id"))
                if pid not in snap_run_ids:
                    missing_snapshot_refs.append(pid)
                if pid not in lineage_run_ids and str(run.get("job_name", "")).startswith(
                    "bronze_parse:"
                ):
                    missing_lineage_edges.append(pid)
        except Exception:  # noqa: BLE001, S110
            pass

        return {
            "available_objects_without_bronze": available_without_bronze,
            "orphaned_bronze_rows": orphaned_bronze_rows,
            "duplicate_business_keys": duplicate_business_keys,
            "unreferenced_published_runs": {
                "missing_snapshot_refs": sorted(missing_snapshot_refs),
                "missing_lineage_edges": sorted(missing_lineage_edges),
            },
        }
