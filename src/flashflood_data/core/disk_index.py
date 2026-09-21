"""Small disk-backed indexes for bounded-memory pipeline validation."""

import sqlite3
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import TracebackType
from typing import Self


class DiskUniqueIndex:
    """Track arbitrary text keys in a temporary SQLite unique index."""

    def __init__(self, directory: Path | None = None) -> None:
        self.directory = directory
        self.path: Path | None = None
        self._connection: sqlite3.Connection | None = None

    def __enter__(self) -> Self:
        handle = NamedTemporaryFile(
            prefix="flashflood-keys-", suffix=".sqlite", dir=self.directory, delete=False
        )
        handle.close()
        self.path = Path(handle.name)
        self._connection = sqlite3.connect(self.path)
        self._connection.execute("PRAGMA journal_mode=OFF")
        self._connection.execute("PRAGMA synchronous=OFF")
        self._connection.execute("PRAGMA temp_store=FILE")
        self._connection.execute("CREATE TABLE unique_keys (value TEXT PRIMARY KEY) WITHOUT ROWID")
        return self

    def add_many(self, values: list[str]) -> bool:
        if self._connection is None:
            raise RuntimeError("DiskUniqueIndex must be used as a context manager")
        before = self._connection.total_changes
        self._connection.executemany(
            "INSERT OR IGNORE INTO unique_keys(value) VALUES (?)",
            ((value,) for value in values),
        )
        return self._connection.total_changes - before == len(values)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self.path is not None:
            self.path.unlink(missing_ok=True)
