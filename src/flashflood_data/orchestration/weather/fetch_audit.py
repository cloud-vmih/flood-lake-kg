"""Local attempt spool separating parallel provider I/O from Iceberg commits."""

import json
import shutil
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path

from flashflood_data.orchestration.weather.models import FetchAttemptRecord


class FetchAttemptSpool:
    """Write per-mapped-task attempt files and read them for one serial commit."""

    def __init__(self, staging_root: Path) -> None:
        self.root = Path(staging_root) / "weather" / "fetch-attempts"

    def _run_dir(self, pipeline_id: str, run_id: str) -> Path:
        identity = json.dumps(
            [pipeline_id, run_id], separators=(",", ":"), ensure_ascii=True
        )
        token = sha256(identity.encode("utf-8")).hexdigest()[:24]
        return self.root / token

    def write_batch(
        self,
        pipeline_id: str,
        run_id: str,
        *,
        batch_index: int,
        attempt_no: int,
        rows: Sequence[FetchAttemptRecord],
    ) -> Path:
        if batch_index < 0 or attempt_no < 1:
            raise ValueError("fetch attempt batch coordinates are invalid")
        requested = list(rows)
        if not requested or any(row.ingest_run_id != run_id for row in requested):
            raise ValueError("fetch attempt spool rows must belong to one non-empty run")
        if not pipeline_id:
            raise ValueError("fetch attempt spool pipeline identity cannot be empty")
        run_dir = self._run_dir(pipeline_id, run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        target = run_dir / f"batch-{batch_index:06d}-attempt-{attempt_no:03d}.json"
        temporary = target.with_suffix(".json.tmp")
        document = [row.model_dump(mode="json") for row in requested]
        temporary.write_text(
            json.dumps(document, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        temporary.replace(target)
        return target

    def read_run(self, pipeline_id: str, run_id: str) -> list[FetchAttemptRecord]:
        run_dir = self._run_dir(pipeline_id, run_id)
        if not run_dir.is_dir():
            return []
        rows: list[FetchAttemptRecord] = []
        for path in sorted(run_dir.glob("batch-*-attempt-*.json")):
            document = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(document, list):
                raise TypeError(f"fetch attempt spool file is not a list: {path.name}")
            rows.extend(FetchAttemptRecord.model_validate(row) for row in document)
        return rows

    def cleanup_run(self, pipeline_id: str, run_id: str) -> None:
        shutil.rmtree(self._run_dir(pipeline_id, run_id), ignore_errors=True)
