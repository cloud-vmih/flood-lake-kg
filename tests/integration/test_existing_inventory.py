import hashlib
import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from flashflood_data.catalog import AssetCatalog
from flashflood_data.cli import app
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.inventory import InventoryRule
from flashflood_data.models import AssetKind, AssetStatus
from flashflood_data.paths import ProjectPaths
from flashflood_data.sources.base import SourceAdapter, SourceContext
from flashflood_data.sources.existing import ExistingAdapter, inventory_existing

SHAPEFILE_SUFFIXES = (".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".shp.xml")


def _context(tmp_path: Path) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="fixture-existing-run",
    )


def _write_shapefile_bundle(primary: Path, *, complete: bool = True) -> None:
    primary.parent.mkdir(parents=True, exist_ok=True)
    suffixes = SHAPEFILE_SUFFIXES if complete else (".shp",)
    for suffix in suffixes:
        path = primary.with_suffix(suffix) if suffix != ".shp.xml" else Path(f"{primary}.xml")
        content = b"\x00\x00\x27\x0a" + suffix.encode("ascii")
        path.write_bytes(content)


def _clone_bundle(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    for suffix in SHAPEFILE_SUFFIXES:
        source_member = (
            source.with_suffix(suffix) if suffix != ".shp.xml" else Path(f"{source}.xml")
        )
        destination_member = (
            destination.with_suffix(suffix) if suffix != ".shp.xml" else Path(f"{destination}.xml")
        )
        destination_member.write_bytes(source_member.read_bytes())


def test_inventory_hashes_compound_shapefiles_and_chooses_lexicographic_duplicate(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    first = context.paths.dataset / "a" / "hybas_as_lev10_v1c.shp"
    second = context.paths.dataset / "copy" / "hybas_as_lev10_v1c.shp"
    _write_shapefile_bundle(first)
    _clone_bundle(first, second)
    members = [path for directory in (first.parent, second.parent) for path in directory.iterdir()]
    before = {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in members}
    rule = InventoryRule(
        "hydrobasins_v1c",
        "1c",
        AssetKind.RAW,
        "**/hybas_as_lev10_v1c.shp",
        "application/x-esri-shapefile",
        SHAPEFILE_SUFFIXES,
    )

    records = inventory_existing(context, rules=(rule,))

    assert [Path(record.storage_path) for record in records] == [first, second]
    assert records[0].checksum == records[1].checksum
    assert records[0].duplicate_of_asset_id is None
    assert records[1].duplicate_of_asset_id == records[0].asset_id
    assert all(record.status is AssetStatus.VALIDATED for record in records)
    assert {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in members} == before
    assert not any(path.is_file() for path in context.paths.raw.rglob("*"))


def test_present_bundle_members_prevent_false_lake_duplicate(tmp_path: Path) -> None:
    context = _context(tmp_path)
    complete = context.paths.dataset / "Data" / "hybas_lake_as_lev08_v1c.shp"
    standalone = context.paths.dataset / "Data" / "data" / "hybas_lake_as_lev08_v1c.shp"
    _write_shapefile_bundle(complete)
    _write_shapefile_bundle(standalone, complete=False)
    standalone.write_bytes(complete.read_bytes())
    rule = InventoryRule(
        "hydrobasins_lake_sample_v1c",
        "1c",
        AssetKind.RAW,
        "Data/**/hybas_lake_as_lev08_v1c.shp",
        "application/x-esri-shapefile",
        SHAPEFILE_SUFFIXES,
    )

    records = inventory_existing(context, rules=(rule,))

    assert len(records) == 2
    assert records[0].checksum != records[1].checksum
    assert all(record.duplicate_of_asset_id is None for record in records)


def test_single_file_duplicates_choose_lexicographic_worldpop_path(tmp_path: Path) -> None:
    context = _context(tmp_path)
    upper = context.paths.dataset / "Data" / "vnm_pop_2025_CN_100m_R2025A_v1.tif"
    nested = context.paths.dataset / "Data" / "data" / "vnm_pop_2025_CN_100m_R2025A_v1.tif"
    for path in (upper, nested):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"II*\x00fixture-tiff")
    rule = InventoryRule(
        "worldpop_vnm_2025",
        "R2025A-v1",
        AssetKind.RAW,
        "Data/**/vnm_pop_2025_CN_100m_R2025A_v1.tif",
        "image/tiff",
        (".tif",),
    )

    records = inventory_existing(context, rules=(rule,))

    assert [Path(record.storage_path) for record in records] == [nested, upper]
    assert records[0].checksum == hashlib.sha256(nested.read_bytes()).hexdigest()
    assert records[1].duplicate_of_asset_id == records[0].asset_id


def test_existing_adapter_is_real_fixed_registry_implementation() -> None:
    assert issubclass(ExistingAdapter, SourceAdapter)


def test_inventory_cli_records_sanitized_run_and_supports_rehash(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    workbook = dataset / "Lu_Son_La_2020_2026.xlsx"
    workbook.parent.mkdir(parents=True)
    pd.DataFrame({"event": ["fixture"], "year": [2026]}).to_excel(workbook, index=False)
    before = workbook.stat().st_mtime_ns
    runner = CliRunner()

    first = runner.invoke(app, ["inventory", "--root", str(tmp_path)])
    report_before = (dataset / "catalog" / "inventory.json").read_bytes()
    second = runner.invoke(app, ["inventory", "--root", str(tmp_path), "--rehash"])

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert json.loads(first.stdout)["asset_count"] == 1
    assert (dataset / "catalog" / "inventory.json").read_bytes() == report_before
    runs = pd.read_parquet(dataset / "catalog" / "runs.parquet")
    assert set(runs["command"]) == {"inventory"}
    assert str(tmp_path) not in " ".join(runs["command"])
    assert workbook.exists() and workbook.stat().st_mtime_ns == before
