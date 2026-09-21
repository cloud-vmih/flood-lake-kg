#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
DOCKER_BIN=${DOCKER_BIN:-docker}

cd "$PROJECT_ROOT"
printf '%s\n' 'Meta Bronze smoke: running.'

"$DOCKER_BIN" compose exec -T airflow-scheduler python - <<'PY'
import json
import tempfile
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4
from zipfile import ZipFile

import geopandas as gpd
from shapely.geometry import box

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.orchestration.bronze.service import BronzeService
from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.orchestration.meta.service import MetaRecorder
from flashflood_data.storage.iceberg import SourceObjectInventory, load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore
from flashflood_data.storage.object_store import ObjectPublisher, PyArrowS3ObjectStore

run_id = uuid4().hex
namespace = f"smoke_meta_bronze_{run_id}"
table_identifier = (namespace, "basin_polygon_raw")
settings = LakehouseSettings(_env_file=None)
store = PyArrowS3ObjectStore.from_settings(settings)
publisher = ObjectPublisher(store, settings.raw_bucket)
catalog = load_polaris_catalog(settings)
published = None
published_manifest = None

try:
    with tempfile.TemporaryDirectory(prefix="meta-bronze-smoke-") as temporary:
        temp_dir = Path(temporary)
        shape_path = temp_dir / "shape.shp"
        frame = gpd.GeoDataFrame(
            {"HYBAS_ID": [42], "NEXT_DOWN": [0], "SUB_AREA": [12.5], "UP_AREA": [12.5]},
            geometry=[box(103.0, 21.0, 104.0, 22.0)],
            crs="EPSG:4326",
        )
        frame.to_file(shape_path)
        zip_path = temp_dir / "hydrobasins.zip"
        with ZipFile(zip_path, "w") as archive:
            for member in temp_dir.glob("shape.*"):
                archive.write(member, member.name)

        published = publisher.publish_file(
            zip_path,
            final_key=f"_smoke/{run_id}/hydrobasins.zip",
            run_id=run_id,
            media_type="application/zip",
        )

        object_id = sha256(
            json.dumps(
                {"run_id": run_id, "checksum": published.checksum},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        manifest_path = temp_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "object_id": object_id,
                    "object_uri": published.object_uri,
                    "checksum": published.checksum,
                    "status": "verified",
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        published_manifest = publisher.publish_file(
            manifest_path,
            final_key=f"_smoke/{run_id}/manifest.json",
            run_id=run_id,
            media_type="application/json",
        )

        now = datetime.now(UTC)
        row = SourceObjectRow(
            object_id=object_id,
            asset_id=f"smoke-{run_id}",
            source_id="hydrobasins_v1c",
            source_version="1c",
            product="hydrobasins",
            object_uri=published.object_uri,
            manifest_uri=published_manifest.object_uri,
            media_type=published.media_type,
            size_bytes=published.size_bytes,
            checksum=published.checksum,
            source_uri="https://example.invalid/hydrobasins",
            retrieved_at=now,
            first_seen_at=now,
            ingest_run_id=run_id,
        )
        inventory = SourceObjectInventory(catalog, (namespace, "source_objects"))
        reg = inventory.register_many((row,))
        if reg.snapshot_id is None:
            raise RuntimeError("smoke inventory registration failed")

        writer = IcebergTableStore(catalog)
        meta = MetaRecorder(writer, meta_namespace=namespace)
        staging_root = temp_dir / "staging"
        service = BronzeService(
            inventory=inventory,
            object_store=store,
            writer=writer,
            meta=meta,
            raw_bucket=settings.raw_bucket,
            staging_root=staging_root,
            catalog_name=catalog.name,
            bronze_namespace=namespace,
        )

        result = service.process_object(
            "hydrobasins_v1c", object_id, run_id=run_id, parser_version="v1"
        )
        if result["status"] != "succeeded" or result["row_count"] != 1:
            raise RuntimeError("first Bronze parse failed or unexpected row count")

        # Verify idempotence on identical rerun
        result2 = service.process_object(
            "hydrobasins_v1c", object_id, run_id=run_id, parser_version="v1"
        )
        if result2["row_count"] != 1 or result2["snapshot_id"] != result["snapshot_id"]:
            raise RuntimeError("Bronze rerun failed idempotency check")

        # Verify queryable Iceberg rows
        table = writer.ensure_table(table_identifier)
        table.refresh()
        rows = table.scan().to_arrow().to_pylist()
        if len(rows) != 1 or rows[0]["source_feature_id"] != "42":
            raise RuntimeError("Bronze query check failed")

        # Verify Meta audit
        qa_table = writer.ensure_table((namespace, "quality_results"))
        qa_table.refresh()
        qa_rows = qa_table.scan().to_arrow().to_pylist()
        if not any(r["rule_id"] == "nonempty_parse" and r["status"] == "passed" for r in qa_rows):
            raise RuntimeError("Meta QA audit records missing")

        lineage_table = writer.ensure_table((namespace, "lineage_edges"))
        lineage_table.refresh()
        lineage_rows = lineage_table.scan().to_arrow().to_pylist()
        if not any(l["input_object_id"] == object_id for l in lineage_rows):
            raise RuntimeError("Meta lineage records missing")

finally:
    for table_name in (
        "basin_polygon_raw", "source_objects", "quality_results",
        "table_snapshot_ref", "lineage_edges", "pipeline_runs", "ingest_attempts",
    ):
        target_id = (namespace, table_name)
        if catalog.table_exists(target_id):
            catalog.purge_table(target_id)
    if (namespace,) in catalog.list_namespaces():
        catalog.drop_namespace((namespace,))
    if published is not None:
        store.delete(published.object_key)
    if published_manifest is not None:
        store.delete(published_manifest.object_key)
PY

printf '%s\n' 'Meta Bronze smoke: passed.'
