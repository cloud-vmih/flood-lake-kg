"""Production Iceberg/MinIO dependencies for Bronze quality migration."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from pyiceberg.expressions import EqualTo

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.pipelines.quality_migration.bronze_quality import (
    BronzeQualityMigration,
    MigrationDecision,
)
from flashflood_data.pipelines.static_bronze.config import load_bronze_config
from flashflood_data.pipelines.static_bronze.runtime import build_bronze_service
from flashflood_data.pipelines.weather_ingest.runtime import build_weather_runtime
from flashflood_data.shared.metadata.service import MetaRecorder
from flashflood_data.shared.quality import (
    ObjectRejection,
    QuarantineRepository,
    load_quality_policy,
)
from flashflood_data.storage.iceberg import load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import PyArrowS3ObjectStore


def _identifier(dataset_id: str) -> tuple[str, str]:
    parts = dataset_id.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 3:
        return parts[1], parts[2]
    raise ValueError("dataset_id must be namespace.table or catalog.namespace.table")


def _boolean(value: object) -> bool:
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


class IcebergMigrationBackend:
    """Read historical snapshots and apply only explicitly planned local repairs."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.settings = LakehouseSettings(_env_file=root / ".env", project_root=root)
        self.catalog = load_polaris_catalog(self.settings)
        self.store = IcebergTableStore(self.catalog)
        self.meta = MetaRecorder(self.store)
        self.quarantine = QuarantineRepository(self.store)
        self.object_store = PyArrowS3ObjectStore.from_settings(self.settings)

    def current_snapshot_id(self, dataset_id: str) -> int:
        table = self.catalog.load_table(_identifier(dataset_id))
        table.refresh()
        snapshot = table.current_snapshot()
        if snapshot is None:
            raise ValueError(f"Bronze table has no snapshot: {dataset_id}")
        return int(snapshot.snapshot_id)

    def bronze_rows(self, dataset_id: str, snapshot_id: int):
        table = self.catalog.load_table(_identifier(dataset_id))
        table.refresh()
        actual_snapshot = snapshot_id or self.current_snapshot_id(dataset_id)
        return table.scan(snapshot_id=actual_snapshot).to_arrow().to_pylist()

    def raw_object(self, object_id: str):
        table = self.store.ensure_table(("meta", "source_objects"))
        table.refresh()
        rows = (
            table.scan(row_filter=EqualTo("object_id", object_id))
            .to_arrow()
            .to_pylist()
        )
        if len(rows) > 1:
            raise ValueError(f"duplicate source object identity: {object_id}")
        return None if not rows else rows[0]

    def raw_exists(self, object_uri: str) -> bool:
        uri = urlsplit(object_uri)
        path = PurePosixPath(uri.path.lstrip("/"))
        if (
            uri.scheme != "s3"
            or uri.netloc != self.settings.raw_bucket
            or not path.parts
            or any(part in {".", ".."} for part in path.parts)
        ):
            return False
        return self.object_store.exists(f"{uri.netloc}/{path}")

    def checkpoint_exists(self, checkpoint_id: str) -> bool:
        rows = self.store.get_keyed_rows(
            ("meta", "quality_results"), {"pipeline_run_id": checkpoint_id}
        )
        for row in rows:
            try:
                observed = json.loads(row.get("observed_value_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            if observed.get("execution_mode") == "apply" and observed.get("completed") is True:
                return True
        return False

    def record_decisions(
        self, decisions: tuple[MigrationDecision, ...], run_id: str, mode: str
    ) -> None:
        checked_at = datetime.now(UTC)
        for decision in decisions:
            failed = decision.action == "mark_raw_unavailable"
            severity = "fatal" if failed else "info" if decision.action == "audit_only" else "warning"
            self.meta.record_quality(
                pipeline_run_id=decision.checkpoint_id,
                dataset_id=decision.dataset_id,
                rule_id="bronze_quality_migration",
                rule_version="v1",
                scope_key=decision.object_id,
                severity=severity,
                status="failed" if failed else "passed",
                checked_at=checked_at,
                check_phase="post_commit" if mode == "apply" else "pre_commit",
                observed_value_json=json.dumps(
                    {
                        "action": decision.action,
                        "completed": mode == "apply" and not failed,
                        "execution_mode": mode,
                        "reason": decision.reason,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                expected_value_json='{"quality_contract_applied":true}',
                failed_row_count=1 if failed else 0,
                snapshot_table=decision.dataset_id,
                snapshot_id=decision.input_snapshot_id,
            )
            if failed:
                self.quarantine.reject_object(
                    ObjectRejection(
                        pipeline_run_id=decision.checkpoint_id,
                        layer="bronze",
                        target_dataset_id=decision.dataset_id,
                        input_table=decision.dataset_id,
                        input_snapshot_id=decision.input_snapshot_id,
                        scope_key=decision.object_id,
                        quality_policy_id=decision.quality_policy_id,
                        rule_id="raw_unavailable",
                        rule_version="v1",
                        reason_code="raw_unavailable",
                        detail={"reason": decision.reason},
                        rejected_at=checked_at,
                    )
                )
            status = "failed_data_quality" if failed else (
                "succeeded_with_warning"
                if decision.action not in {"audit_only", "skip"}
                else "succeeded"
            )
            self.meta.record_run(
                {
                    "pipeline_run_id": decision.checkpoint_id,
                    "orchestrator_run_id": run_id,
                    "job_name": "bronze_quality_backfill",
                    "code_git_sha": None,
                    "image_digest": None,
                    "config_hash": sha256(decision.quality_policy_id.encode()).hexdigest(),
                    "parameter_set_id": None,
                    "started_at": checked_at,
                    "finished_at": checked_at,
                    "published_at": checked_at if status.startswith("succeeded") else None,
                    "status": status,
                    "retry_count": 0,
                    "input_row_count": len(decision.rows),
                    "output_row_count": len(decision.rows) if mode == "apply" else None,
                    "quality_result_json": json.dumps(
                        {"action": decision.action, "mode": mode}, separators=(",", ":")
                    ),
                    "metrics_json": None,
                    "error_code": "raw_unavailable" if failed else None,
                }
            )

    def rewrite_object(self, dataset_id: str, object_id: str, rows) -> None:
        self.store.replace_object_rows(_identifier(dataset_id), object_id, rows)

    def reprocess_object(self, dataset_id: str, object_id: str, run_id: str) -> None:
        raw = self.raw_object(object_id)
        if raw is None or not self.raw_exists(str(raw["object_uri"])):
            raise FileNotFoundError(f"Raw object unavailable for migration: {object_id}")
        source_id = str(raw["source_id"])
        if _identifier(dataset_id)[1] == "weather_raster_slice":
            selection = json.loads(str(raw["selection_json"]))
            stream_id = str(selection.get("stream_id") or "")
            filename = {
                "era5_land": "era5_land.yaml",
                "ifs_openmeteo": "ifs.yaml",
                "gsmap:gauge_now": "gsmap_now.yaml",
                "gsmap:gauge_standard": "gsmap_standard.yaml",
            }.get(f"{source_id}:{stream_id}", {"era5_land": "era5_land.yaml", "ifs_openmeteo": "ifs.yaml"}.get(source_id))
            if filename is None:
                raise ValueError(f"cannot resolve weather migration config: {source_id}/{stream_id}")
            config_path = self.root / "config" / "pipelines" / "weather" / filename
            runtime = build_weather_runtime(config_path)
            runtime.bronze_service().process_object(
                source_id,
                object_id,
                run_id=run_id,
                parser_version=runtime.config.parser_version,
            )
            return
        config = load_bronze_config(
            self.root / "config" / "pipelines" / "static_bronze.yaml"
        )
        build_bronze_service(self.root).process_object(
            source_id,
            object_id,
            run_id=run_id,
            parser_version=config.parser_version(source_id),
        )


class QualityMigrationRuntime:
    def __init__(self, backend: IcebergMigrationBackend) -> None:
        self.backend = backend
        self.migration = BronzeQualityMigration(backend)

    def run(
        self,
        *,
        dataset_id: str,
        mode: str,
        object_limit: int,
        force: bool,
        run_id: str,
        snapshot_id: int = 0,
        policy_id: str = "",
    ) -> dict[str, object]:
        actual_snapshot = snapshot_id or self.backend.current_snapshot_id(dataset_id)
        policy_file = (
            "bronze_weather.yaml"
            if dataset_id.endswith("weather_raster_slice")
            else "bronze_static.yaml"
        )
        actual_policy = policy_id or load_quality_policy(
            self.backend.root / "config" / "quality" / policy_file
        ).policy_id
        decisions = self.migration.plan(
            dataset_id,
            actual_snapshot,
            actual_policy,
            object_limit=object_limit,
            force=force,
        )
        summary = self.migration.execute(decisions, run_id, mode=mode)  # type: ignore[arg-type]
        return {
            **summary.__dict__,
            "dataset_id": dataset_id,
            "snapshot_id": actual_snapshot,
            "quality_policy_id": actual_policy,
            "decisions": [
                {
                    "object_id": item.object_id,
                    "action": item.action,
                    "reason": item.reason,
                    "raw_uri": item.raw_uri,
                }
                for item in decisions
            ],
        }


def build_quality_migration_runtime(root: Path | None = None) -> QualityMigrationRuntime:
    paths = ProjectPaths.discover(root)
    return QualityMigrationRuntime(IcebergMigrationBackend(paths.root))


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit or apply Bronze DQ migration")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--mode", choices=("audit", "apply"), default="audit")
    parser.add_argument("--object-limit", type=int, default=100)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--snapshot-id", type=int, default=0)
    arguments = parser.parse_args()
    result = build_quality_migration_runtime().run(
        dataset_id=arguments.dataset,
        mode=arguments.mode,
        object_limit=arguments.object_limit,
        force=arguments.force,
        run_id=f"cli-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}",
        snapshot_id=arguments.snapshot_id,
    )
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
