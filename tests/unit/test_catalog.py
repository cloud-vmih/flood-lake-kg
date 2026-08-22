from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from flashflood_data.catalog import IllegalTransition, sha256_bundle, sha256_file
from flashflood_data.models import AssetStatus, RunRecord


def test_sha256_file_and_bundle_are_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "b.bin"
    second = tmp_path / "a.bin"
    first.write_bytes(b"alpha")
    second.write_bytes(b"beta")

    first_checksum = sha256(b"alpha").hexdigest()
    second_checksum = sha256(b"beta").hexdigest()
    assert sha256_file(first) == first_checksum
    assert (
        sha256_bundle([first, second])
        == sha256((second_checksum + first_checksum).encode("ascii")).hexdigest()
    )
    assert sha256_bundle([second, first]) == sha256_bundle([first, second])


def test_catalog_round_trips_and_replaces_asset_by_id(catalog, raw_asset) -> None:
    assert pd.read_parquet(catalog.assets_path).empty
    assert pd.read_parquet(catalog.runs_path).empty

    catalog.upsert(raw_asset)
    replaced = raw_asset.model_copy(update={"media_type": "application/x-fixture"})
    catalog.upsert(replaced)

    assert catalog.assets_path.exists()
    assert catalog.get(raw_asset.asset_id) == replaced
    assert catalog.get(raw_asset.asset_id).media_type == "application/x-fixture"


def test_catalog_rejects_illegal_transition(catalog, raw_asset) -> None:
    catalog.upsert(raw_asset)

    with pytest.raises(IllegalTransition, match="discovered -> harmonized"):
        catalog.transition(raw_asset.asset_id, AssetStatus.HARMONIZED)


def test_catalog_allows_legal_transition_and_persists_it(catalog, raw_asset) -> None:
    catalog.upsert(raw_asset)

    changed = catalog.transition(raw_asset.asset_id, AssetStatus.FETCHING)

    assert changed.status is AssetStatus.FETCHING
    assert catalog.get(raw_asset.asset_id).status is AssetStatus.FETCHING


def test_catalog_reuses_only_verified_terminal_assets(catalog, raw_asset) -> None:
    reusable = raw_asset.model_copy(
        update={
            "status": AssetStatus.VALIDATED,
            "dependency_fingerprint": "dependencies-v1",
        }
    )
    catalog.upsert(reusable)

    assert catalog.find_reusable("fixture-source", "1", "dependencies-v1") == reusable

    Path(reusable.storage_path).write_bytes(b"modified")
    assert catalog.find_reusable("fixture-source", "1", "dependencies-v1") is None


def test_catalog_records_run_lifecycle(catalog) -> None:
    run = RunRecord(
        run_id="run-1",
        command="inventory",
        started_at=datetime(2026, 8, 21, tzinfo=UTC),
        config_fingerprint="config-v1",
    )

    catalog.begin_run(run)
    finished = catalog.end_run(
        "run-1", status="completed", ended_at=datetime(2026, 8, 21, 1, tzinfo=UTC)
    )

    assert catalog.runs_path.exists()
    assert finished.status == "completed"
    assert finished.ended_at == datetime(2026, 8, 21, 1, tzinfo=UTC)


def test_asset_records_are_immutable(raw_asset) -> None:
    with pytest.raises(ValidationError):
        raw_asset.status = AssetStatus.FETCHING
