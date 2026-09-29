from datetime import UTC, datetime
from pathlib import Path

from flashflood_data.orchestration.weather.fetch_audit import FetchAttemptSpool
from flashflood_data.orchestration.weather.models import FetchAttemptRecord


def _record(
    asset_id: str, attempt_no: int, status: str, run_id: str = "manual__../../unsafe"
) -> FetchAttemptRecord:
    return FetchAttemptRecord(
        ingest_run_id=run_id,
        source_id="gsmap",
        asset_id=asset_id,
        attempt_no=attempt_no,
        request_fingerprint="f" * 64,
        status=status,
        started_at=datetime(2026, 9, 29, tzinfo=UTC),
        ended_at=(
            None if status == "running" else datetime(2026, 9, 29, 0, 1, tzinfo=UTC)
        ),
        error_code=None if status == "running" else "TimeoutError",
    )


def test_fetch_attempt_spool_round_trips_batches_without_using_run_id_as_path(
    tmp_path: Path,
) -> None:
    spool = FetchAttemptSpool(tmp_path)
    run_id = "manual__../../unsafe"

    spool.write_batch(
        "gsmap-standard", run_id, batch_index=1, attempt_no=1,
        rows=[_record("b", 1, "failed")],
    )
    spool.write_batch(
        "gsmap-standard", run_id, batch_index=0, attempt_no=1,
        rows=[_record("a", 1, "running")],
    )

    rows = spool.read_run("gsmap-standard", run_id)

    assert [row.asset_id for row in rows] == ["a", "b"]
    assert rows[0].started_at == datetime(2026, 9, 29, tzinfo=UTC)
    assert not (tmp_path / "manual__.." / ".." / "unsafe").exists()


def test_fetch_attempt_spool_replaces_same_batch_attempt_and_cleans_run(
    tmp_path: Path,
) -> None:
    spool = FetchAttemptSpool(tmp_path)
    run_id = "run-1"
    spool.write_batch(
        "gsmap-standard", run_id, batch_index=0, attempt_no=1,
        rows=[_record("a", 1, "failed", run_id)],
    )
    spool.write_batch(
        "gsmap-standard", run_id, batch_index=0, attempt_no=1,
        rows=[_record("a", 1, "running", run_id)],
    )

    assert [row.status for row in spool.read_run("gsmap-standard", run_id)] == ["running"]

    spool.cleanup_run("gsmap-standard", run_id)

    assert spool.read_run("gsmap-standard", run_id) == []


def test_fetch_attempt_spool_isolates_pipelines_with_the_same_run_id(
    tmp_path: Path,
) -> None:
    spool = FetchAttemptSpool(tmp_path)
    run_id = "manual__same-id"
    spool.write_batch(
        "gsmap-standard", run_id, batch_index=0, attempt_no=1,
        rows=[_record("standard", 1, "running", run_id)],
    )
    spool.write_batch(
        "gsmap-now", run_id, batch_index=0, attempt_no=1,
        rows=[_record("now", 1, "running", run_id)],
    )

    assert [row.asset_id for row in spool.read_run("gsmap-standard", run_id)] == [
        "standard"
    ]
    assert [row.asset_id for row in spool.read_run("gsmap-now", run_id)] == ["now"]

    spool.cleanup_run("gsmap-standard", run_id)

    assert spool.read_run("gsmap-standard", run_id) == []
    assert [row.asset_id for row in spool.read_run("gsmap-now", run_id)] == ["now"]
