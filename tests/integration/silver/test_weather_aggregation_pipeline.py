"""Exercise Silver Weather's plan, Spark handoff, manifest, audit and skip path.

This uses an in-memory gateway and catalog boundary; a live Spark/MinIO smoke
run is separate so the test remains safe while other DAGs are active.
"""

import json
import logging
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from flashflood_data.pipelines.silver_weather import runtime as weather_runtime
from flashflood_data.pipelines.silver_weather.pipeline import WEATHER_OUTPUTS
from flashflood_data.shared.spark.models import (
    PinnedSnapshot,
    PublishedSnapshot,
    SparkApplicationResult,
)
from flashflood_data.shared.spark.snapshots import BuildRecord


@pytest.mark.parametrize("fail_once", [False, True])
def test_weather_runtime_build_audit_then_skip_same_snapshots(
    monkeypatch, caplog, fail_once,
) -> None:
    caplog.set_level(logging.INFO, logger=weather_runtime.__name__)
    snapshots = {
        "bronze.weather_raster_slice": PinnedSnapshot("bronze.weather_raster_slice", 31),
        "silver.source_grid": PinnedSnapshot("silver.source_grid", 12),
        "silver.dim_basin": PinnedSnapshot("silver.dim_basin", 7),
    }
    output_rows = (
        PublishedSnapshot(WEATHER_OUTPUTS[0], 100, 3, 1),
        PublishedSnapshot(WEATHER_OUTPUTS[1], 102, 6, 2),
    )
    audit_events: list[tuple[str, object]] = []
    manifests: dict[str, bytes] = {}
    history: list[BuildRecord] = []

    class FakeAudit:
        def __init__(self, meta):
            assert meta is not None

        def start(self, request):
            audit_events.append(("started", request.pipeline_run_id))

        def finalize_success(self, request, outputs, *, auxiliary_outputs, quality_rows):
            assert tuple(outputs) == output_rows
            assert auxiliary_outputs == ()
            assert quality_rows[0]["checked_at"].tzinfo is not None
            audit_events.append(("succeeded", request.pipeline_run_id))
            history.append(BuildRecord(
                request.pipeline_run_id, "succeeded", request.build_signature,
                tuple(item.table for item in outputs),
            ))

        def finalize_failure(self, request, error_code):
            audit_events.append(("failed", error_code))

    class FakeSubmitter:
        failed = False

        def submit(self, request):
            audit_events.append(("submitted", request.arguments))
            if fail_once and "batch_month=2020-02" in request.arguments and not self.failed:
                self.failed = True
                return SparkApplicationResult(
                    request.application_id, "failed", None, error_code="smoke_failure",
                )
            output = (
                output_rows[0] if "grid_basin_weight" in request.arguments[0]
                else PublishedSnapshot(
                    WEATHER_OUTPUTS[1],
                    101 if "batch_month=2020-01" in request.arguments else 102,
                    3 if "batch_month=2020-01" in request.arguments else 6,
                    1 if "batch_month=2020-01" in request.arguments else 2,
                )
            )
            key = weather_runtime.manifest_key(request.output_manifest_uri)
            manifests[key] = json.dumps({
                "application_id": request.application_id,
                "pipeline_run_id": request.pipeline_run_id,
                "build_signature": request.build_signature,
                "outputs": [
                    {"table": row.table, "snapshot_id": row.snapshot_id,
                     "row_count": row.row_count, "file_count": row.file_count}
                    for row in (output,)
                ],
                "quality_rows": [{"checked_at": datetime.now(UTC).isoformat()}],
            }).encode()
            return SparkApplicationResult(
                request.application_id, "succeeded", request.output_manifest_uri,
                published_snapshots=(output,),
            )

    monkeypatch.setattr(weather_runtime, "SparkAudit", FakeAudit)
    monkeypatch.setattr(weather_runtime, "MetaRecorder", lambda tables: tables)
    service = object.__new__(weather_runtime.WeatherSilverRuntime)
    service.tables = object()
    service.object_store = SimpleNamespace(
        read=manifests.__getitem__, exists=manifests.__contains__,
    )
    service.submitter = FakeSubmitter()
    service.config_hash = "c" * 64
    monkeypatch.setattr(service, "_snapshot", snapshots.__getitem__)
    monkeypatch.setattr(service, "_history", lambda: tuple(history))
    monkeypatch.setattr(service, "_prepare_outputs", lambda: None)
    monkeypatch.setattr(service, "_weather_months", lambda snapshot: ("2020-01", "2020-02"))

    first = service.plan()
    assert first.action == "build"
    if fail_once:
        with pytest.raises(RuntimeError, match="smoke_failure"):
            service.execute(first, orchestrator_run_id="manual__weather-interrupted")
        assert audit_events[-1] == ("failed", "weather_runtimeerror")
    result = service.execute(first, orchestrator_run_id="manual__weather-1")
    assert result["status"] == "succeeded"
    submitted = [arguments for event, arguments in audit_events if event == "submitted"]
    assert sum("outputs=silver.grid_basin_weight" in args for args in submitted) == 1
    assert sum("batch_month=2020-01" in args for args in submitted) == 1
    assert sum("batch_month=2020-02" in args for args in submitted) == (2 if fail_once else 1)

    # A retry after all Spark manifests exist must not submit those batches again.
    assert service.execute(first, orchestrator_run_id="manual__weather-retry")["status"] == "succeeded"
    assert sum(event[0] == "submitted" for event in audit_events) == len(submitted)
    assert "weather_silver_batch_finished" in caplog.text
    assert "batch_month=2020-01 reused=True" in caplog.text

    second = service.plan()
    assert second.action == "skip"
    assert service.execute(second, orchestrator_run_id="manual__weather-2")["status"] == "skipped"
    assert audit_events[-1][0] == "succeeded"
