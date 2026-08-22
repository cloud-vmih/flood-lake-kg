"""Atomic, parquet-backed catalog records for local pipeline assets."""

from collections.abc import Sequence
from datetime import datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd

from flashflood_data.io_atomic import atomic_target
from flashflood_data.models import AssetRecord, AssetStatus, RunRecord
from flashflood_data.paths import ProjectPaths

LEGAL_TRANSITIONS: dict[AssetStatus, set[AssetStatus]] = {
    AssetStatus.DISCOVERED: {AssetStatus.FETCHING, AssetStatus.VALIDATED, AssetStatus.FAILED},
    AssetStatus.FETCHING: {AssetStatus.FETCHED, AssetStatus.FAILED, AssetStatus.QUARANTINED},
    AssetStatus.FETCHED: {AssetStatus.VALIDATED, AssetStatus.FAILED, AssetStatus.QUARANTINED},
    AssetStatus.VALIDATED: {AssetStatus.HARMONIZED, AssetStatus.DERIVED, AssetStatus.STALE},
    AssetStatus.HARMONIZED: {AssetStatus.DERIVED, AssetStatus.STALE},
    AssetStatus.DERIVED: {AssetStatus.STALE},
    AssetStatus.FAILED: {AssetStatus.FETCHING},
    AssetStatus.STALE: {AssetStatus.FETCHING, AssetStatus.HARMONIZED, AssetStatus.DERIVED},
    AssetStatus.QUARANTINED: set(),
}


class IllegalTransition(ValueError):
    """Raised when an asset lifecycle change violates the transition table."""


def sha256_file(path: Path) -> str:
    """Return the SHA-256 checksum of a file without loading it all into memory."""
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bundle(paths: Sequence[Path]) -> str:
    """Fingerprint an unordered file dependency set from its member checksums."""
    digest = sha256()
    for path in sorted((Path(path) for path in paths), key=lambda item: item.as_posix()):
        digest.update(sha256_file(path).encode("ascii"))
    return digest.hexdigest()


class AssetCatalog:
    """Authoritative, atomically published catalog tables for a project."""

    _ASSET_COLUMNS = tuple(AssetRecord.model_fields)
    _RUN_COLUMNS = tuple(RunRecord.model_fields)

    def __init__(self, paths: ProjectPaths) -> None:
        self.paths = paths
        self.paths.catalog.mkdir(parents=True, exist_ok=True)
        self.assets_path = self.paths.catalog / "assets.parquet"
        self.runs_path = self.paths.catalog / "runs.parquet"
        if not self.assets_path.exists():
            self._write_rows(self.assets_path, [], self._ASSET_COLUMNS)
        if not self.runs_path.exists():
            self._write_rows(self.runs_path, [], self._RUN_COLUMNS)

    @staticmethod
    def _read_rows(path: Path) -> list[dict[str, object]]:
        return pd.read_parquet(path, engine="pyarrow").to_dict(orient="records")

    @staticmethod
    def _write_rows(path: Path, rows: list[dict[str, object]], columns: Sequence[str]) -> None:
        frame = pd.DataFrame(rows, columns=columns)
        with atomic_target(path) as partial:
            frame.to_parquet(partial, engine="pyarrow", index=False)

    def _read_assets(self) -> list[AssetRecord]:
        return [AssetRecord.model_validate(row) for row in self._read_rows(self.assets_path)]

    def _write_assets(self, records: Sequence[AssetRecord]) -> None:
        rows = [record.model_dump(mode="json") for record in records]
        self._write_rows(self.assets_path, rows, self._ASSET_COLUMNS)

    def _read_runs(self) -> list[RunRecord]:
        return [RunRecord.model_validate(row) for row in self._read_rows(self.runs_path)]

    def _write_runs(self, records: Sequence[RunRecord]) -> None:
        rows = [record.model_dump(mode="json") for record in records]
        self._write_rows(self.runs_path, rows, self._RUN_COLUMNS)

    def upsert(self, record: AssetRecord) -> AssetRecord:
        """Atomically insert or replace an asset row by its stable ID."""
        records = [item for item in self._read_assets() if item.asset_id != record.asset_id]
        records.append(record)
        self._write_assets(records)
        return record

    def get(self, asset_id: str) -> AssetRecord:
        """Return an asset record by ID, raising ``KeyError`` when it is absent."""
        for record in self._read_assets():
            if record.asset_id == asset_id:
                return record
        raise KeyError(asset_id)

    def transition(self, asset_id: str, target: AssetStatus, **updates: object) -> AssetRecord:
        """Apply a legal lifecycle transition and atomically persist the replacement."""
        record = self.get(asset_id)
        if target not in LEGAL_TRANSITIONS[record.status]:
            raise IllegalTransition(f"{record.status} -> {target}")
        changed = record.model_copy(update={"status": target, **updates})
        self.upsert(changed)
        return changed

    def find_reusable(
        self,
        source_id: str,
        source_version: str,
        dependency_fingerprint: str | None,
    ) -> AssetRecord | None:
        """Find a verified, reusable terminal asset with intact local content."""
        reusable_statuses = {AssetStatus.VALIDATED, AssetStatus.HARMONIZED, AssetStatus.DERIVED}
        for record in self._read_assets():
            if (
                record.source_id != source_id
                or record.source_version != source_version
                or record.dependency_fingerprint != dependency_fingerprint
                or record.status not in reusable_statuses
            ):
                continue
            path = Path(record.storage_path)
            if (
                path.is_file()
                and record.checksum_algorithm == "sha256"
                and sha256_file(path) == record.checksum
            ):
                return record
        return None

    def begin_run(self, record: RunRecord) -> RunRecord:
        """Atomically insert or replace a run record by run ID."""
        records = [item for item in self._read_runs() if item.run_id != record.run_id]
        records.append(record)
        self._write_runs(records)
        return record

    def end_run(self, run_id: str, status: str, ended_at: datetime) -> RunRecord:
        """Persist an explicit end time and terminal status for a recorded run."""
        records = self._read_runs()
        for index, record in enumerate(records):
            if record.run_id == run_id:
                finished = record.model_copy(update={"status": status, "ended_at": ended_at})
                records[index] = finished
                self._write_runs(records)
                return finished
        raise KeyError(run_id)
