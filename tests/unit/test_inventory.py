import json
import os
import re
import zipfile
from pathlib import Path

import pandas as pd
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
    replay_context = SourceContext(
        paths=context.paths,
        catalog=context.catalog,
        study_area=context.study_area,
        environment=context.environment,
        run_id="fixture-inventory-replay",
    )
    replayed = inventory_existing(replay_context, rules=(_rule(),))
    second_report = (context.paths.catalog / "inventory.json").read_bytes()

    assert records[0].status is AssetStatus.VALIDATED
    assert replayed == records
    assert observed == [AssetStatus.DISCOVERED, AssetStatus.VALIDATED]
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


def test_inventory_rejects_primary_symlink_escape_before_hash_or_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"external")
    link = context.paths.dataset / "legacy" / "escape.bin"
    link.parent.mkdir(parents=True)
    link.symlink_to(outside)
    from flashflood_data import inventory

    hashed: list[Path] = []
    opened: list[Path] = []
    real_hash = inventory.sha256_file
    real_open = Path.open

    def recording_hash(path: Path) -> str:
        hashed.append(path)
        return real_hash(path)

    def guarded_open(path: Path, *args, **kwargs):
        if path.resolve() == outside.resolve():
            opened.append(path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(inventory, "sha256_file", recording_hash)
    monkeypatch.setattr(Path, "open", guarded_open)

    with pytest.raises(RuntimeError, match="escapes dataset"):
        inventory_existing(context, rules=(_rule("legacy/escape.bin"),))

    assert hashed == []
    assert opened == []
    assert pd.read_parquet(context.catalog.assets_path).empty
    assert not (context.paths.catalog / "inventory-cache.json").exists()


def test_inventory_rejects_bundle_member_symlink_escape_before_any_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)
    primary = context.paths.dataset / "legacy" / "bundle.shp"
    primary.parent.mkdir(parents=True)
    primary.write_bytes(b"\x00\x00\x27\x0a")
    outside = tmp_path / "outside.dbf"
    outside.write_bytes(b"external-sidecar")
    primary.with_suffix(".dbf").symlink_to(outside)
    from flashflood_data import inventory

    hashed: list[Path] = []
    opened: list[Path] = []
    real_hash = inventory.sha256_file
    real_open = Path.open

    def recording_hash(path: Path) -> str:
        hashed.append(path)
        return real_hash(path)

    def guarded_open(path: Path, *args, **kwargs):
        if path.resolve() == outside.resolve():
            opened.append(path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(inventory, "sha256_file", recording_hash)
    monkeypatch.setattr(Path, "open", guarded_open)
    rule = _rule(
        "legacy/bundle.shp",
        media_type="application/x-esri-shapefile",
        suffixes=(".shp", ".dbf"),
    )

    with pytest.raises(RuntimeError, match="escapes dataset"):
        inventory_existing(context, rules=(rule,))

    assert hashed == []
    assert opened == []
    assert pd.read_parquet(context.catalog.assets_path).empty


def test_inventory_rejects_changed_content_without_overwriting_catalog(tmp_path: Path) -> None:
    context = _context(tmp_path)
    payload = context.paths.dataset / "legacy" / "stable.bin"
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"first")
    original = inventory_existing(context, rules=(_rule("legacy/stable.bin"),))[0]
    payload.write_bytes(b"other")

    with pytest.raises(RuntimeError, match="conflicting existing inventory asset"):
        inventory_existing(context, rules=(_rule("legacy/stable.bin"),), rehash=True)

    assert context.catalog.get(original.asset_id) == original


def test_inventory_preserves_matching_quarantined_catalog_row(tmp_path: Path) -> None:
    context = _context(tmp_path)
    payload = context.paths.dataset / "legacy" / "quarantined.bin"
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"stable")
    original = inventory_existing(context, rules=(_rule("legacy/quarantined.bin"),))[0]
    quarantined = original.model_copy(update={"status": AssetStatus.QUARANTINED})
    context.catalog.upsert(quarantined)

    with pytest.raises(RuntimeError, match="quarantined existing inventory asset"):
        inventory_existing(context, rules=(_rule("legacy/quarantined.bin"),))

    assert context.catalog.get(original.asset_id) == quarantined


@pytest.mark.parametrize("location", ["header", "record"])
def test_csv_validation_rejects_oversize_header_or_record(tmp_path: Path, location: str) -> None:
    path = tmp_path / "oversize.csv"
    oversized = "x" * (1024 * 1024 + 1)
    body = f"{oversized}\n1\n" if location == "header" else f"column\n{oversized}\n"
    path.write_text(body, encoding="utf-8")

    result = validate_known_format(path, "text/csv", (path,))

    assert not result.passed


def test_python_validation_rejects_oversize_source(tmp_path: Path) -> None:
    path = tmp_path / "oversize.py"
    path.write_bytes(b"#" * (1024 * 1024 + 1))

    result = validate_known_format(path, "text/x-python", (path,))

    assert not result.passed


def test_csv_validation_rejects_malformed_quoted_record(tmp_path: Path) -> None:
    path = tmp_path / "malformed.csv"
    path.write_text('column\n"unterminated', encoding="utf-8")

    result = validate_known_format(path, "text/csv", (path,))

    assert not result.passed


def test_malformed_xlsx_xml_transitions_catalog_record_to_failed(tmp_path: Path) -> None:
    context = _context(tmp_path)
    source = tmp_path / "source.xlsx"
    workbook = Workbook()
    workbook.active.append(["header"])
    workbook.save(source)
    malformed = context.paths.dataset / "legacy" / "malformed.xlsx"
    malformed.parent.mkdir(parents=True)
    with (
        zipfile.ZipFile(source) as archive,
        zipfile.ZipFile(malformed, "w", compression=zipfile.ZIP_DEFLATED) as destination,
    ):
        for member in archive.infolist():
            body = archive.read(member.filename)
            if member.filename == "xl/worksheets/sheet1.xml":
                body = b"<worksheet><broken>"
            destination.writestr(member, body)

    records = inventory_existing(
        context,
        rules=(
            _rule(
                "legacy/malformed.xlsx",
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                suffixes=(".xlsx",),
            ),
        ),
    )

    assert records[0].status is AssetStatus.FAILED
    assert context.catalog.get(records[0].asset_id).status is AssetStatus.FAILED


@pytest.mark.parametrize("signature", [b"MM\x00*", b"MM\x00+"])
def test_tiff_validation_accepts_big_endian_signatures(tmp_path: Path, signature: bytes) -> None:
    path = tmp_path / "big-endian.tif"
    path.write_bytes(signature + b"fixture")

    assert validate_known_format(path, "image/tiff", (path,)).passed


def test_validation_accepts_valid_zip_csv_and_python(tmp_path: Path) -> None:
    archive = tmp_path / "valid.zip"
    with zipfile.ZipFile(archive, "w") as destination:
        destination.writestr("member.txt", "fixture")
    csv_path = tmp_path / "valid.csv"
    csv_path.write_text("column\nvalue\n", encoding="utf-8")
    python_path = tmp_path / "valid.py"
    python_path.write_text("value = 1\n", encoding="utf-8")

    assert validate_known_format(archive, "application/zip", (archive,)).passed
    assert validate_known_format(csv_path, "text/csv", (csv_path,)).passed
    assert validate_known_format(python_path, "text/x-python", (python_path,)).passed
