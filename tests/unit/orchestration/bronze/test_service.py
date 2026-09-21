"""The Bronze pipeline reads only registered MinIO raw objects and audits commits."""

from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.orchestration.bronze.osm import OsmSelection
from flashflood_data.orchestration.bronze.service import BronzeService
from flashflood_data.orchestration.landing.models import SourceObjectRow


class _Inventory:
    def __init__(self, row: SourceObjectRow) -> None:
        self.row = row

    def available_objects(self, source_id: str):
        return (self.row,) if source_id == self.row.source_id else ()


class _ObjectStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.downloads: list[str] = []

    def download(self, key: str, local_path: Path) -> None:
        self.downloads.append(key)
        copyfile(self.path, local_path)


class _MockTable:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.rows = list(rows or [])

    def refresh(self) -> None:
        pass

    def scan(self, **kwargs):
        class _ScanResult:
            def __init__(self, rows: list[dict[str, object]]) -> None:
                self._rows = rows

            def to_arrow(self):
                import pyarrow as pa
                return pa.Table.from_pylist(self._rows) if self._rows else pa.table({})

        return _ScanResult(self.rows)


class _Writer:
    def __init__(self, events: list[str] | None = None) -> None:
        self.records = []
        self.batch_sizes: list[int] = []
        self.tables: dict[tuple[str, str], _MockTable] = {}
        self.events = events

    def replace_object_rows(self, identifier, object_id, rows):
        self.records.append((identifier, object_id, list(rows)))
        self.tables.setdefault(identifier, _MockTable()).rows.extend(rows)
        return 91

    def replace_object_batches(self, identifier, object_id, batches):
        rows = []
        for batch in batches:
            self.batch_sizes.append(len(batch))
            rows.extend(batch)
        result = self.replace_object_rows(identifier, object_id, rows), len(rows)
        if self.events is not None:
            self.events.append("bronze_commit")
        return result

    def ensure_table(self, identifier: tuple[str, str]) -> _MockTable:
        return self.tables.setdefault(identifier, _MockTable())



class _Meta:
    def __init__(self, events: list[str] | None = None) -> None:
        self.quality = []
        self.lineage = []
        self.snapshots = []
        self.runs = []
        self.events = events

    def record_quality(self, **kwargs):
        self.quality.append(kwargs)
        if self.events is not None:
            self.events.append("quality_audit")

    def record_lineage(self, **kwargs):
        self.lineage.append(kwargs)

    def record_snapshot_ref(self, **kwargs):
        self.snapshots.append(kwargs)

    def record_run(self, row):
        self.runs.append(dict(row))


def _raster_row(path: Path) -> SourceObjectRow:
    data = path.read_bytes()
    now = datetime(2026, 9, 20, tzinfo=UTC)
    return SourceObjectRow(
        object_id="raw-1", asset_id="soil-sand", source_id="soilgrids_2_0",
        source_version="2.0", product="soilgrids_2_0",
        object_uri="s3://raw/static/soilgrids_2_0/sand_0-5cm_Q0.50.tif",
        manifest_uri="s3://raw/static/soilgrids_2_0/manifest.json",
        media_type="image/tiff", size_bytes=len(data), checksum=sha256(data).hexdigest(),
        source_uri="https://example.invalid/soil", retrieved_at=now, first_seen_at=now,
        ingest_run_id="landing-1",
    )


def test_process_object_parses_registered_raw_and_records_bronze_provenance(tmp_path: Path) -> None:
    path = tmp_path / "sand_0-5cm_Q0.50.tif"
    with rasterio.open(
        path, "w", driver="GTiff", width=2, height=2, count=1, dtype="uint16",
        crs="EPSG:4326", transform=from_origin(103, 22, 0.5, 0.5),
    ) as output:
        output.write(np.ones((2, 2), dtype="uint16"), 1)
    row = _raster_row(path)
    raw = _ObjectStore(path)
    writer = _Writer()
    meta = _Meta()
    service = BronzeService(
        inventory=_Inventory(row), object_store=raw, writer=writer, meta=meta,
        raw_bucket="raw", staging_root=tmp_path / "staging",
    )

    result = service.process_object(
        "soilgrids_2_0", "raw-1", run_id="bronze-run-1", parser_version="v1"
    )

    assert result["table_name"] == "flood_lakehouse.bronze.raster_coverage"
    assert result["snapshot_id"] == 91
    assert result["row_count"] == 1
    assert raw.downloads == ["raw/static/soilgrids_2_0/sand_0-5cm_Q0.50.tif"]
    assert writer.records[0][0] == ("bronze", "raster_coverage")
    assert meta.quality[-1]["status"] == "passed"
    assert meta.lineage[0]["input_object_id"] == "raw-1"
    assert meta.lineage[0]["output_snapshot_id"] == 91
    assert meta.runs[-1]["published_at"] is not None


def test_process_object_rejects_unregistered_id_without_downloading(tmp_path: Path) -> None:
    path = tmp_path / "placeholder"
    path.write_bytes(b"nothing")
    raw = _ObjectStore(path)
    service = BronzeService(
        inventory=_Inventory(_raster_row(path)), object_store=raw, writer=_Writer(),
        meta=_Meta(), raw_bucket="raw", staging_root=tmp_path / "staging",
    )
    try:
        service.process_object("soilgrids_2_0", "unknown", run_id="run-1", parser_version="v1")
    except LookupError:
        pass
    else:
        raise AssertionError("unknown object must be rejected")
    assert raw.downloads == []


def test_vector_service_commits_bounded_batches(tmp_path: Path) -> None:
    frame = gpd.GeoDataFrame(
        {"HYBAS_ID": [1, 2, 3]},
        geometry=[box(103 + i, 21, 104 + i, 22) for i in range(3)],
        crs="EPSG:4326",
    )
    frame.to_file(tmp_path / "basins.shp")
    archive = tmp_path / "basins.zip"
    with ZipFile(archive, "w") as output:
        for member in tmp_path.glob("basins.*"):
            if member != archive:
                output.write(member, member.name)
    row = _raster_row(archive).model_copy(update={
        "source_id": "hydrobasins_v1c", "object_uri": "s3://raw/static/hydrobasins_v1c/basins.zip",
    })
    events: list[str] = []
    writer = _Writer(events)
    service = BronzeService(
        inventory=_Inventory(row), object_store=_ObjectStore(archive), writer=writer,
        meta=_Meta(events), raw_bucket="raw", staging_root=tmp_path / "staging", vector_batch_size=2,
    )

    result = service.process_object("hydrobasins_v1c", "raw-1", run_id="run-1", parser_version="v1")

    assert result["row_count"] == 3
    assert writer.batch_sizes == [2, 1]
    assert {item["source_feature_id"] for item in writer.records[0][2]} == {"1", "2", "3"}
    assert events.index("bronze_commit") < events.index("quality_audit")


def test_osm_pbf_is_discovered_and_committed_in_bounded_batches(tmp_path: Path) -> None:
    path = Path(__file__).parents[3] / "fixtures" / "osm" / "sample.osm.pbf"
    row = _raster_row(path).model_copy(update={
        "source_id": "geofabrik_vietnam_snapshot",
        "object_uri": "s3://raw/static/geofabrik_vietnam_snapshot/vietnam.osm.pbf",
    })
    writer = _Writer()
    service = BronzeService(
        inventory=_Inventory(row), object_store=_ObjectStore(path), writer=writer,
        meta=_Meta(), raw_bucket="raw", staging_root=tmp_path / "staging", vector_batch_size=1,
        osm_selection=OsmSelection(
            version="test-v1",
            groups={"critical_facility": {"amenity": ("post_box",)}},
        ),
    )

    assert service.discover("geofabrik_vietnam_snapshot") == ("raw-1",)
    result = service.process_object(
        "geofabrik_vietnam_snapshot", "raw-1", run_id="run-1", parser_version="v1"
    )

    assert result["table_name"] == "flood_lakehouse.bronze.osm_feature_raw"
    assert result["row_count"] == 1
    assert writer.batch_sizes == [1]
    assert writer.records[0][2][0]["osm_id"] == "818056434"


def test_discovery_skips_only_published_objects_still_present_in_bronze(tmp_path: Path) -> None:
    path = tmp_path / "sand.tif"
    path.write_bytes(b"raw fixture")
    row = _raster_row(path)
    writer = _Writer()
    published_at = datetime(2026, 9, 21, tzinfo=UTC)
    writer.ensure_table(("bronze", "raster_coverage")).rows.append({"object_id": "raw-1"})
    writer.ensure_table(("meta", "lineage_edges")).rows.append({
        "pipeline_run_id": "published-run",
        "input_kind": "raw_object",
        "input_object_id": "raw-1",
        "output_table": "flood_lakehouse.bronze.raster_coverage",
        "mapping_version": "v1",
    })
    writer.ensure_table(("meta", "pipeline_runs")).rows.append({
        "pipeline_run_id": "published-run",
        "status": "succeeded",
        "published_at": published_at,
    })
    service = BronzeService(
        inventory=_Inventory(row), object_store=_ObjectStore(path), writer=writer,
        meta=_Meta(), raw_bucket="raw", staging_root=tmp_path / "staging",
    )

    assert service.discover("soilgrids_2_0", parser_version="v1") == ()
    assert service.discover("soilgrids_2_0", parser_version="v2") == ("raw-1",)
    assert service.discover(
        "soilgrids_2_0", parser_version="v1", force_reprocess=True
    ) == ("raw-1",)


def test_discovery_repairs_published_meta_when_bronze_slice_is_missing(tmp_path: Path) -> None:
    path = tmp_path / "sand.tif"
    path.write_bytes(b"raw fixture")
    row = _raster_row(path)
    writer = _Writer()
    writer.ensure_table(("meta", "lineage_edges")).rows.append({
        "pipeline_run_id": "published-run",
        "input_kind": "raw_object",
        "input_object_id": "raw-1",
        "output_table": "flood_lakehouse.bronze.raster_coverage",
        "mapping_version": "v1",
    })
    writer.ensure_table(("meta", "pipeline_runs")).rows.append({
        "pipeline_run_id": "published-run",
        "status": "succeeded",
        "published_at": datetime(2026, 9, 21, tzinfo=UTC),
    })
    service = BronzeService(
        inventory=_Inventory(row), object_store=_ObjectStore(path), writer=writer,
        meta=_Meta(), raw_bucket="raw", staging_root=tmp_path / "staging",
    )

    assert service.discover("soilgrids_2_0", parser_version="v1") == ("raw-1",)


def test_fatal_bronze_quality_blocks_commit_and_publication(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "sand.tif"
    path.write_bytes(b"raw fixture")
    raw = _ObjectStore(path)
    writer = _Writer()
    meta = _Meta()
    service = BronzeService(
        inventory=_Inventory(_raster_row(path)), object_store=raw, writer=writer,
        meta=meta, raw_bucket="raw", staging_root=tmp_path / "staging",
    )
    row = {
        "object_id": "raw-1", "band_or_layer": "band-1",
        "bbox_wgs84": [104, 22, 103, 21],
    }
    monkeypatch.setattr(
        "flashflood_data.orchestration.bronze.service._table_for",
        lambda *args, **kwargs: ("raster_coverage", [row]),
    )
    with pytest.raises(ValueError, match="quality"):
        service.process_object("soilgrids_2_0", "raw-1", run_id="run-1", parser_version="v1")
    assert writer.records == []
    assert any(result["rule_id"] == "bbox_valid" and result["status"] == "failed" for result in meta.quality)
    assert meta.runs[-1]["published_at"] is None


def test_reconcile_detects_discrepancies(tmp_path: Path) -> None:
    path = tmp_path / "sand.tif"
    path.write_bytes(b"raw fixture")
    row = _raster_row(path)
    writer = _Writer()
    meta = _Meta()
    service = BronzeService(
        inventory=_Inventory(row), object_store=_ObjectStore(path), writer=writer,
        meta=meta, raw_bucket="raw", staging_root=tmp_path / "staging",
    )

    # 1. Initially, row raw-1 is available in inventory but not yet in Bronze raster_coverage
    report = service.reconcile("soilgrids_2_0")
    assert report["available_objects_without_bronze"] == {"soilgrids_2_0": ["raw-1"]}
    assert report["orphaned_bronze_rows"] == []

    # 2. Add an orphaned bronze row (object_id 'ghost') and duplicate keys
    coverage_table = writer.ensure_table(("bronze", "raster_coverage"))
    coverage_table.rows.extend([
        {"object_id": "ghost", "band_or_layer": "band-1"},
        {"object_id": "raw-1", "band_or_layer": "band-dup"},
        {"object_id": "raw-1", "band_or_layer": "band-dup"},
    ])

    # 3. Add an unreferenced published run
    runs_table = writer.ensure_table(("meta", "pipeline_runs"))
    runs_table.rows.append({
        "pipeline_run_id": "unreferenced-run",
        "job_name": "bronze_parse:soilgrids_2_0",
        "published_at": datetime(2026, 9, 20, tzinfo=UTC),
    })

    report2 = service.reconcile("soilgrids_2_0")
    assert report2["available_objects_without_bronze"] == {}
    assert {"table": "raster_coverage", "object_id": "ghost"} in report2["orphaned_bronze_rows"]
    assert len(report2["duplicate_business_keys"]["raster_coverage"]) == 1
    assert "unreferenced-run" in report2["unreferenced_published_runs"]["missing_snapshot_refs"]
    assert "unreferenced-run" in report2["unreferenced_published_runs"]["missing_lineage_edges"]
