"""The optional SQL reader must expose Iceberg without granting writes."""

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]


def test_trino_is_optional_local_and_read_only() -> None:
    services = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    trino = services["trino"]
    assert trino["profiles"] == ["query"]
    assert trino["ports"] == ["127.0.0.1:8083:8080"]
    assert trino["depends_on"]["polaris"]["condition"] == "service_healthy"
    assert trino["depends_on"]["minio"]["condition"] == "service_healthy"
    assert trino["healthcheck"]
    assert trino["mem_limit"]
    assert "POLARIS_CLIENT_SECRET" in trino["environment"]
    assert "MINIO_ROOT_PASSWORD" in trino["environment"]


def test_trino_catalog_uses_runtime_secrets_and_blocks_writes(tmp_path: Path) -> None:
    renderer = ROOT / "infra/services/trino/render_catalog.sh"
    env = os.environ.copy()
    env.update({
        "TRINO_CATALOG_DIR": str(tmp_path),
        "POLARIS_URI": "http://polaris:8181/api/catalog",
        "POLARIS_CATALOG": "flood_lakehouse",
        "POLARIS_CLIENT_ID": "reader_id",
        "POLARIS_CLIENT_SECRET": "test-secret",
        "MINIO_ENDPOINT": "http://minio:9000",
        "MINIO_ROOT_USER": "test-access",
        "MINIO_ROOT_PASSWORD": "test-storage-secret",
    })
    result = subprocess.run(
        ["sh", str(renderer)], env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "test-secret" not in result.stdout + result.stderr
    catalog = tmp_path / "lakehouse.properties"
    config = catalog.read_text()
    assert catalog.stat().st_mode & 0o777 == 0o600
    assert "connector.name=iceberg" in config
    assert "iceberg.catalog.type=rest" in config
    assert "iceberg.rest-catalog.warehouse=flood_lakehouse" in config
    assert "iceberg.security=READ_ONLY" in config
    assert "iceberg.rest-catalog.oauth2.credential=reader_id:test-secret" in config
    assert "s3.endpoint=http://minio:9000" in config
    assert "s3.path-style-access=true" in config
    assert "s3.aws-secret-key=test-storage-secret" in config


def test_query_commands_are_separate_from_base_stack() -> None:
    makefile = (ROOT / "Makefile").read_text()
    assert "query-up:" in makefile
    assert "query-smoke:" in makefile
    assert "query-down:" in makefile
    assert "--profile query" in makefile
