import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from flashflood_data.catalog import sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.orchestration.landing.config import LandingSourcePolicy
from flashflood_data.orchestration.landing.sources import (
    AmbiguousSourceSelection,
    MissingSourceAssets,
    prepare_source_objects,
)

NOW = datetime(2026, 9, 16, tzinfo=UTC)


def _record(
    path: Path,
    *,
    asset_id: str,
    source_id: str = "hydrobasins_v1c",
    version: str = "1c",
    status: AssetStatus = AssetStatus.VALIDATED,
    duplicate: str | None = None,
    metadata: dict[str, object] | None = None,
) -> AssetRecord:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    return AssetRecord(
        asset_id=asset_id,
        source_id=source_id,
        source_version=version,
        kind=AssetKind.RAW,
        source_uri="https://example.invalid/source",
        storage_path=str(path),
        media_type="application/xml" if path.suffix == ".xml" else "image/tiff",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=NOW,
        license_id="fixture-license",
        pipeline_run_id="fixture-run",
        status=status,
        duplicate_of_asset_id=duplicate,
        metadata_json=json.dumps(metadata or {}),
    )


def _bundle_record(tmp_path: Path, **updates: object) -> AssetRecord:
    primary = tmp_path / "hybas_as_lev12_v1c.shp"
    primary.parent.mkdir(parents=True, exist_ok=True)
    members = []
    for suffix in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
        member = primary.with_suffix(suffix)
        member.write_bytes(suffix.encode())
        members.append(member.as_posix())
    values = {
        "path": primary,
        "asset_id": "hydro-l12",
        "metadata": {"bundle_members": members},
    }
    values.update(updates)
    return _record(**values)


def test_l12_policy_selects_complete_validated_bundle(tmp_path: Path) -> None:
    policy = LandingSourcePolicy(
        source_id="hydrobasins_v1c",
        mode="shapefile_bundle",
        filename_contains="hybas_as_lev12_v1c.shp",
        output_name="hydrobasins_l12.zip",
        selection={"basin_level": 12},
    )

    prepared = prepare_source_objects(
        policy, (_bundle_record(tmp_path),), staging_root=tmp_path / "staging", run_id="run-1"
    )

    assert [item.filename for item in prepared] == ["hydrobasins_l12.zip"]
    assert {Path(member.name).suffix for member in prepared[0].members} >= {
        ".shp",
        ".shx",
        ".dbf",
        ".prj",
    }


def test_bundle_selection_rejects_duplicate_and_non_validated_records(tmp_path: Path) -> None:
    policy = LandingSourcePolicy(
        source_id="hydrobasins_v1c",
        mode="shapefile_bundle",
        filename_contains="hybas_as_lev12_v1c.shp",
        output_name="hydrobasins_l12.zip",
    )
    duplicate = _bundle_record(tmp_path / "duplicate", duplicate="canonical")
    failed = _bundle_record(tmp_path / "failed", asset_id="failed", status=AssetStatus.FAILED)

    with pytest.raises(MissingSourceAssets):
        prepare_source_objects(
            policy, (duplicate, failed), staging_root=tmp_path / "staging", run_id="run-1"
        )


def test_bundle_selection_rejects_multiple_canonical_records(tmp_path: Path) -> None:
    policy = LandingSourcePolicy(
        source_id="hydrobasins_v1c",
        mode="shapefile_bundle",
        filename_contains="hybas_as_lev12_v1c.shp",
        output_name="hydrobasins_l12.zip",
    )
    with pytest.raises(AmbiguousSourceSelection):
        prepare_source_objects(
            policy,
            (_bundle_record(tmp_path / "a"), _bundle_record(tmp_path / "b", asset_id="b")),
            staging_root=tmp_path / "staging",
            run_id="run-1",
        )


def test_soilgrids_selects_exact_canonical_scope(tmp_path: Path) -> None:
    properties = ("clay", "sand", "silt", "bdod", "cfvo", "soc", "wv0033", "wv1500")
    depths = ("0-5cm", "5-15cm", "15-30cm")
    statistics = ("mean", "Q0.05", "Q0.5", "Q0.95")
    records = []
    for property_id in properties:
        records.append(
            _record(
                tmp_path / property_id / "capabilities.xml",
                asset_id=f"soilgrids-2-0-{property_id}-capabilities",
                source_id="soilgrids_2_0",
                version="2.0",
            )
        )
        for depth in depths:
            for statistic in statistics:
                records.append(
                    _record(
                        tmp_path / property_id / depth / f"{statistic}.tif",
                        asset_id=f"soilgrids-2-0-{property_id}-{depth}-{statistic}",
                        source_id="soilgrids_2_0",
                        version="2.0",
                    )
                )
    policy = LandingSourcePolicy(
        source_id="soilgrids_2_0",
        mode="individual",
        settings_override={
            "properties": properties,
            "depths": depths,
            "statistics": statistics,
        },
    )

    prepared = prepare_source_objects(
        policy, records, staging_root=tmp_path / "staging", run_id="run-1"
    )

    assert len(prepared) == 104
    assert sum(item.path.suffix == ".tif" for item in prepared) == 96


def test_soilgrids_reports_complete_missing_set(tmp_path: Path) -> None:
    policy = LandingSourcePolicy(
        source_id="soilgrids_2_0",
        mode="individual",
        settings_override={
            "properties": ("clay",),
            "depths": ("0-5cm",),
            "statistics": ("mean", "Q0.05"),
        },
    )
    capability = _record(
        tmp_path / "clay" / "capabilities.xml",
        asset_id="soilgrids-2-0-clay-capabilities",
        source_id="soilgrids_2_0",
        version="2.0",
    )

    with pytest.raises(MissingSourceAssets) as error:
        prepare_source_objects(
            policy, (capability,), staging_root=tmp_path / "staging", run_id="run-1"
        )

    assert "clay/0-5cm/mean.tif" in str(error.value)
    assert "clay/0-5cm/Q0.05.tif" in str(error.value)
