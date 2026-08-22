from collections import namedtuple
from datetime import UTC, datetime
from pathlib import Path

import pytest

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.paths import ProjectPaths


def make_test_asset(path: Path, *, status: AssetStatus) -> AssetRecord:
    return AssetRecord(
        asset_id="fixture-asset",
        source_id="fixture-source",
        source_version="1",
        kind=AssetKind.RAW,
        source_uri="https://example.invalid/raw.bin",
        storage_path=str(path),
        media_type="application/octet-stream",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 21, tzinfo=UTC),
        license_id="fixture-license",
        pipeline_run_id="fixture-run",
        status=status,
    )


@pytest.fixture
def project_paths(tmp_path: Path) -> ProjectPaths:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    return paths


@pytest.fixture
def catalog(project_paths: ProjectPaths) -> AssetCatalog:
    return AssetCatalog(project_paths)


@pytest.fixture
def raw_asset(tmp_path: Path) -> AssetRecord:
    payload = tmp_path / "raw.bin"
    payload.write_bytes(b"fixture")
    return make_test_asset(payload, status=AssetStatus.DISCOVERED)


@pytest.fixture
def fake_disk_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    disk_usage = namedtuple("disk_usage", "total used free")
    monkeypatch.setattr(
        "flashflood_data.budget.shutil.disk_usage",
        lambda _: disk_usage(40 * 2**30, 10 * 2**30, 30 * 2**30),
    )
