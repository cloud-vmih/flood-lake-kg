from pathlib import Path

import pytest
from pydantic import ValidationError

from flashflood_data.core.lakehouse import LakehouseSettings


def test_lakehouse_settings_use_local_defaults_and_secret_aliases(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MINIO_ROOT_USER", "fixture-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "fixture-password")
    monkeypatch.setenv("POLARIS_CLIENT_ID", "fixture-client")
    monkeypatch.setenv("POLARIS_CLIENT_SECRET", "fixture-secret")

    settings = LakehouseSettings(
        _env_file=None,
        project_root=tmp_path,
        staging_root=tmp_path / "staging",
    )

    assert settings.minio_endpoint == "http://127.0.0.1:9000"
    assert settings.raw_bucket == "raw"
    assert settings.project_root == tmp_path.resolve()
    assert settings.staging_root == (tmp_path / "staging").resolve()
    assert settings.minio_access_key.get_secret_value() == "fixture-user"
    assert "fixture-password" not in repr(settings)


@pytest.mark.parametrize(
    ("field", "value"),
    [("minio_endpoint", "ftp://minio"), ("polaris_uri", "polaris:8181"), ("raw_bucket", "a/b")],
)
def test_lakehouse_settings_reject_invalid_endpoints_and_bucket(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, field: str, value: str
) -> None:
    monkeypatch.setenv("MINIO_ROOT_USER", "fixture-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "fixture-password")
    monkeypatch.setenv("POLARIS_CLIENT_ID", "fixture-client")
    monkeypatch.setenv("POLARIS_CLIENT_SECRET", "fixture-secret")
    values = {field: value, "project_root": tmp_path, "staging_root": tmp_path / "staging"}

    with pytest.raises(ValidationError):
        LakehouseSettings(_env_file=None, **values)
