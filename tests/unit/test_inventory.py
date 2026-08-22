import json
import os
import re
import zipfile
from pathlib import Path

import pytest
from openpyxl import Workbook

from flashflood_data.catalog import AssetCatalog
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.inventory import InventoryRule, validate_known_format
from flashflood_data.models import AssetKind, AssetStatus
from flashflood_data.paths import ProjectPaths
from flashflood_data.sources.base import SourceContext
from flashflood_data.sources.existing import inventory_existing


def _context(tmp_path: Path) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="fixture-inventory-run",
    )


def _rule(
    glob: str = "**/*.bin",
    *,
    source_id: str = "fixture-source",
    media_type: str = "application/octet-stream",
    suffixes: tuple[str, ...] = (".bin",),
) -> InventoryRule:
    return InventoryRule(
        source_id=source_id,
        version="fixture-v1",
        kind=AssetKind.RAW,
        glob=glob,
        media_type=media_type,
        bundle_suffixes=suffixes,
    )


def test_inventory_excludes_pipeline_outputs_venv_and_partial_paths(tmp_path: Path) -> None:
    context = _context(tmp_path)
    candidates = {
        "legacy/keep.bin": b"keep",
        "raw/skip.bin": b"raw",
        "harmonized/skip.bin": b"harmonized",
        "derived/skip.bin": b"derived",
        "catalog/skip.bin": b"catalog",
        "qa/skip.bin": b"qa",
        "Data/venv/skip.bin": b"venv",
        "Data/work.partial/skip.bin": b"partial-directory",
        "legacy/skip.bin.partial": b"partial-file",
    }
    for relative, content in candidates.items():
        path = context.paths.dataset / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    records = inventory_existing(context, rules=(_rule(),))

    assert [
        Path(record.storage_path).relative_to(context.paths.dataset).as_posix()
        for record in records
    ] == ["legacy/keep.bin"]


def test_inventory_cache_uses_path_size_mtime_and_rehash_bypasses_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)
    payload = context.paths.dataset / "legacy" / "cached.bin"
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"cache-me")
    from flashflood_data import inventory

    real_hash = inventory.sha256_file
    hashed: list[Path] = []

    def recording_hash(path: Path) -> str:
        hashed.append(path)
        return real_hash(path)

    monkeypatch.setattr(inventory, "sha256_file", recording_hash)

    inventory_existing(context, rules=(_rule(),))
    inventory_existing(context, rules=(_rule(),))
    unchanged_hashes = len(hashed)
    current = payload.stat()
    os.utime(payload, ns=(current.st_atime_ns, current.st_mtime_ns + 1))
    inventory_existing(context, rules=(_rule(),))
    inventory_existing(context, rules=(_rule(),), rehash=True)

    assert unchanged_hashes == 1
    assert len(hashed) == 3
    cache = json.loads((context.paths.catalog / "inventory-cache.json").read_text())
    assert cache["entries"]["legacy/cached.bin"]["size"] == len(b"cache-me")
    assert cache["entries"]["legacy/cached.bin"]["mtime_ns"] == payload.stat().st_mtime_ns


def test_inventory_registers_discovered_then_validated_and_writes_deterministic_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)
    payload = context.paths.dataset / "legacy" / "valid.bin"
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"valid")
    observed: list[AssetStatus] = []
    real_upsert = context.catalog.upsert

    def recording_upsert(record):
        observed.append(record.status)
        return real_upsert(record)

    monkeypatch.setattr(context.catalog, "upsert", recording_upsert)

    records = inventory_existing(context, rules=(_rule(),))
    first_report = (context.paths.catalog / "inventory.json").read_bytes()
    inventory_existing(context, rules=(_rule(),))
    second_report = (context.paths.catalog / "inventory.json").read_bytes()

    assert records[0].status is AssetStatus.VALIDATED
    assert observed[:2] == [AssetStatus.DISCOVERED, AssetStatus.VALIDATED]
    assert context.catalog.get(records[0].asset_id).status is AssetStatus.VALIDATED
    assert first_report == second_report
    assert json.loads(first_report) == {
        "asset_count": 1,
        "duplicate_count": 0,
        "sources": {
            "fixture-source": {
                "asset_count": 1,
                "duplicate_count": 0,
                "total_bytes": 5,
            }
        },
        "status_counts": {"validated": 1},
        "total_bytes": 5,
    }


def test_inventory_marks_unreadable_known_format_failed(tmp_path: Path) -> None:
    context = _context(tmp_path)
    invalid = context.paths.dataset / "Data" / "invalid.tif"
    invalid.parent.mkdir(parents=True)
    invalid.write_bytes(b"not-a-tiff")

    records = inventory_existing(
        context,
        rules=(
            _rule(
                "Data/invalid.tif",
                media_type="image/tiff",
                suffixes=(".tif",),
            ),
        ),
    )

    assert records[0].status is AssetStatus.FAILED
    assert records[0].error_code == "known_format_validation_failed"
    assert context.catalog.get(records[0].asset_id).status is AssetStatus.FAILED


def test_known_format_validation_reads_only_the_signature(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raster = tmp_path / "large.tif"
    raster.write_bytes(b"II*\x00fixture")

    def reject_whole_file_read(_path: Path) -> bytes:
        raise AssertionError("format validation must not load a potentially huge asset")

    monkeypatch.setattr(Path, "read_bytes", reject_whole_file_read)

    result = validate_known_format(raster, "image/tiff", (raster,))

    assert result.passed


def test_known_format_validation_accepts_little_endian_bigtiff(tmp_path: Path) -> None:
    raster = tmp_path / "worldpop.tif"
    raster.write_bytes(b"II+\x00\x08\x00\x00\x00")

    result = validate_known_format(raster, "image/tiff", (raster,))

    assert result.passed


def test_workbook_validation_streams_rows_when_dimension_metadata_is_absent(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "events.xlsx"
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append(["STT", "event"])
    worksheet.append([1, "first"])
    worksheet.append([2, "second"])
    worksheet.append([])
    worksheet.append(["source note"])
    workbook.save(workbook_path)

    rebuilt = tmp_path / "events-without-dimension.xlsx"
    with (
        zipfile.ZipFile(workbook_path) as source,
        zipfile.ZipFile(rebuilt, "w", compression=zipfile.ZIP_DEFLATED) as destination,
    ):
        for member in source.infolist():
            body = source.read(member.filename)
            if member.filename == "xl/worksheets/sheet1.xml":
                body = re.sub(rb"<dimension[^>]*/>", b"", body, count=1)
            destination.writestr(member, body)

    result = validate_known_format(
        rebuilt,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        (rebuilt,),
    )

    assert result.passed
    assert result.metrics["workbook_data_rows"] == 2
