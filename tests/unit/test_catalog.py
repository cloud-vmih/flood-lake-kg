from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from flashflood_data.catalog import IllegalTransition, sha256_bundle, sha256_file
from flashflood_data.catalog.models import (
    AssetStatus,
    RemoteAsset,
    RunRecord,
    SourceFile,
    SourceSpec,
    ValidationResult,
)


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


def test_catalog_transition_retains_documented_asset_id_keyword(catalog, raw_asset) -> None:
    catalog.upsert(raw_asset)

    changed = catalog.transition(asset_id=raw_asset.asset_id, target=AssetStatus.FETCHING)

    assert changed.status is AssetStatus.FETCHING


@pytest.mark.parametrize("protected_key", ["asset_id", "status"])
def test_catalog_rejects_transition_updates_to_protected_fields(
    catalog, raw_asset, protected_key: str
) -> None:
    catalog.upsert(raw_asset)

    with pytest.raises(ValueError, match="protected"):
        catalog.transition(
            raw_asset.asset_id,
            AssetStatus.FETCHING,
            **{protected_key: "replacement"},
        )

    assert catalog.get(raw_asset.asset_id) == raw_asset


def test_catalog_validates_transition_updates(catalog, raw_asset) -> None:
    catalog.upsert(raw_asset)

    with pytest.raises(ValidationError):
        catalog.transition(raw_asset.asset_id, AssetStatus.FETCHING, size_bytes=-1)

    assert catalog.get(raw_asset.asset_id) == raw_asset


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


def test_model_copy_revalidates_updates(raw_asset) -> None:
    with pytest.raises(ValidationError):
        raw_asset.model_copy(update={"size_bytes": -1})


def test_models_accept_safe_public_source_configuration() -> None:
    remote = RemoteAsset(
        asset_id="dem-glo30",
        source_id="copernicus-dem",
        source_version="2021",
        uri="https://example.invalid/files/dem.tif?tile=N20E104",
        target_relative_path=Path("raw/dem.tif"),
        media_type="image/tiff",
        license_id="copernicus",
        request_form={"product": "GLO-30"},
    )
    source = SourceSpec(
        source_id="copernicus-dem",
        adapter="copernicus",
        version="2021",
        license_id="copernicus",
        settings={"endpoint": "https://example.invalid/catalog?page=1", "retry": {"count": 2}},
    )

    assert remote.uri == "https://example.invalid/files/dem.tif?tile=N20E104"
    assert SourceFile(sources=[source]).sources == (source,)


@pytest.mark.parametrize(
    "update",
    [
        {"source_uri": "https://user:pass@example.invalid/asset.bin"},
        {"source_uri": "https://example.invalid/asset.bin?access_token=secret-value"},
        {"error_message": "download failed: password=secret-value"},
    ],
)
def test_asset_record_rejects_credential_bearing_values(raw_asset, update: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        raw_asset.model_copy(update=update)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"uri": "https://user:pass@example.invalid/dem.tif"},
        {"request_form": {"Authorization": "Bearer secret-value"}},
    ],
)
def test_remote_asset_rejects_credential_bearing_values(kwargs: dict[str, object]) -> None:
    values: dict[str, object] = {
        "asset_id": "dem-glo30",
        "source_id": "copernicus-dem",
        "source_version": "2021",
        "uri": "https://example.invalid/dem.tif",
        "target_relative_path": Path("raw/dem.tif"),
        "media_type": "image/tiff",
        "license_id": "copernicus",
    }
    values.update(kwargs)

    with pytest.raises(ValidationError):
        RemoteAsset.model_validate(values)


def test_remote_asset_accepts_only_positive_conservative_budget_bound() -> None:
    values = {
        "asset_id": "bounded",
        "source_id": "fixture",
        "source_version": "1",
        "uri": "https://example.invalid/bounded.bin",
        "target_relative_path": Path("raw/bounded.bin"),
        "media_type": "application/octet-stream",
        "license_id": "fixture",
    }

    bounded = RemoteAsset.model_validate({**values, "budget_size_bytes": 1024})

    assert bounded.budget_size_bytes == 1024
    for invalid in (0, -1):
        with pytest.raises(ValidationError):
            RemoteAsset.model_validate({**values, "budget_size_bytes": invalid})


def test_source_settings_reject_nested_credential_key() -> None:
    with pytest.raises(ValidationError):
        SourceSpec(
            source_id="dem",
            adapter="copernicus",
            version="2021",
            license_id="copernicus",
            settings={"transport": {"Api-Key": "secret-value"}},
        )


def test_source_settings_allow_public_token_endpoint_name() -> None:
    """Catches rejecting a public OAuth endpoint merely because its key contains token."""
    source = SourceSpec(
        source_id="dem",
        adapter="cop_dem",
        version="2024_1",
        license_id="COP-DEM-30",
        settings={"token_url": "https://identity.example.test/token"},
    )

    assert source.settings["token_url"] == "https://identity.example.test/token"


def test_credential_rejection_does_not_echo_secret_in_error_text() -> None:
    with pytest.raises(ValidationError) as exc_info:
        RemoteAsset(
            asset_id="dem-glo30",
            source_id="copernicus-dem",
            source_version="2021",
            uri="https://example.invalid/dem.tif?token=secret-value",
            target_relative_path=Path("raw/dem.tif"),
            media_type="image/tiff",
            license_id="copernicus",
        )

    assert "secret-value" not in str(exc_info.value)


def test_models_deep_freeze_nested_containers() -> None:
    source = SourceSpec(
        source_id="safe-source",
        adapter="fixture",
        version="1",
        license_id="fixture-license",
        settings={"nested": {"values": ["one"]}},
    )
    validation = ValidationResult(
        passed=True,
        checks={"checksum": True},
        metrics={"count": 1},
        messages=["safe"],
    )
    source_file = SourceFile(sources=[source])

    with pytest.raises(TypeError):
        source.settings["new"] = "value"  # type: ignore[index]
    with pytest.raises(TypeError):
        source.settings["nested"]["values"] = ()  # type: ignore[index]
    with pytest.raises(TypeError):
        validation.metrics["count"] = 2  # type: ignore[index]
    with pytest.raises(TypeError):
        source_file.sources[0] = source  # type: ignore[index]

    assert source.settings["nested"]["values"] == ("one",)
    assert validation.messages == ("safe",)


def test_model_copy_revalidates_and_deep_freezes_nested_containers() -> None:
    source = SourceSpec(
        source_id="safe-source",
        adapter="fixture",
        version="1",
        license_id="fixture-license",
    )
    copied = source.model_copy(update={"settings": {"nested": {"values": ["one"]}}})

    with pytest.raises(TypeError):
        copied.settings["nested"]["values"] = ()  # type: ignore[index]
    with pytest.raises(ValidationError):
        source.model_copy(update={"settings": {"token": "secret-value"}})
