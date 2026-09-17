#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
DOCKER_BIN=${DOCKER_BIN:-docker}

cd "$PROJECT_ROOT"
printf '%s\n' 'Source landing smoke: running.'

"$DOCKER_BIN" compose exec -T airflow-scheduler python - <<'PY'
import json
import tempfile
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from pyiceberg.expressions import EqualTo

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.storage.iceberg import SourceObjectInventory, load_polaris_catalog
from flashflood_data.storage.object_store import ObjectPublisher, PyArrowS3ObjectStore

run_id = uuid4().hex
namespace = f"smoke_{run_id}"
table_identifier = (namespace, "source_objects")
settings = LakehouseSettings(_env_file=None)
store = PyArrowS3ObjectStore.from_settings(settings)
publisher = ObjectPublisher(store, settings.raw_bucket)
catalog = load_polaris_catalog(settings)
published = None
published_manifest = None

try:
    with tempfile.TemporaryDirectory(prefix="source-landing-smoke-") as temporary:
        payload_path = Path(temporary) / "fixture.bin"
        payload_path.write_bytes(run_id.encode("ascii"))
        published = publisher.publish_file(
            payload_path,
            final_key=f"_smoke/{run_id}/fixture.bin",
            run_id=run_id,
            media_type="application/octet-stream",
        )

        object_id = sha256(
            json.dumps(
                {"run_id": run_id, "checksum": published.checksum},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        manifest_path = Path(temporary) / "manifest.json"
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
            source_id="source_landing_smoke",
            source_version="1",
            product="fixture",
            object_uri=published.object_uri,
            manifest_uri=published_manifest.object_uri,
            media_type=published.media_type,
            size_bytes=published.size_bytes,
            checksum=published.checksum,
            source_uri="https://example.invalid/source-landing-smoke",
            retrieved_at=now,
            first_seen_at=now,
            ingest_run_id=run_id,
        )
        inventory = SourceObjectInventory(catalog, table_identifier)
        first = inventory.register_many((row,))
        if first.snapshot_id is None or first.reused != 0:
            raise RuntimeError("first smoke registration did not commit one new object")

        reused_payload = publisher.publish_file(
            payload_path,
            final_key=f"_smoke/{run_id}/fixture.bin",
            run_id=run_id,
            media_type="application/octet-stream",
        )
        reused_manifest = publisher.publish_file(
            manifest_path,
            final_key=f"_smoke/{run_id}/manifest.json",
            run_id=run_id,
            media_type="application/json",
        )
        second = inventory.register_many((row,))
        if not reused_payload.reused or not reused_manifest.reused or second.reused != 1:
            raise RuntimeError("smoke retry did not reuse immutable state")
        rows = inventory.table.scan(row_filter=EqualTo("object_id", object_id)).to_arrow()
        if rows.num_rows != 1:
            raise RuntimeError("smoke inventory contains a duplicate identity")
finally:
    if catalog.table_exists(table_identifier):
        catalog.purge_table(table_identifier)
    if (namespace,) in catalog.list_namespaces():
        catalog.drop_namespace((namespace,))
    if published is not None:
        store.delete(published.object_key)
    if published_manifest is not None:
        store.delete(published_manifest.object_key)
PY

printf '%s\n' 'Source landing smoke: passed.'
