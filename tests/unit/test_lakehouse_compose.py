from pathlib import Path

import yaml


ROOT = Path(__file__).parents[2]
COMPOSE = ROOT / "compose.yaml"


def services() -> dict:
    return yaml.safe_load(COMPOSE.read_text())["services"]


def test_storage_services_are_pinned_private_and_persistent() -> None:
    items = services()
    assert items["postgres"]["image"] == "postgres:17.11-bookworm"
    assert items["minio"]["image"] == "minio/minio:RELEASE.2025-09-07T16-13-09Z"
    assert items["minio-bootstrap"]["image"] == "minio/mc:RELEASE.2025-08-13T08-35-41Z"
    assert items["postgres"]["ports"] == ["127.0.0.1:5432:5432"]
    assert items["minio"]["ports"] == ["127.0.0.1:9000:9000", "127.0.0.1:9001:9001"]
    assert "./dataset/lakehouse/postgres:/var/lib/postgresql/data:Z" in items["postgres"]["volumes"]
    assert "./dataset/lakehouse/minio:/data:Z" in items["minio"]["volumes"]
    command = " ".join(items["minio-bootstrap"]["entrypoint"])
    assert "mb --ignore-existing local/raw" in command
    assert "mb --ignore-existing local/warehouse" in command
    assert "anonymous" not in command
    assert "rm " not in command


def test_storage_services_have_health_and_memory_bounds() -> None:
    items = services()
    assert items["postgres"]["healthcheck"]
    assert items["minio"]["healthcheck"]
    assert items["postgres"]["mem_limit"] == "512m"
    assert items["minio"]["mem_limit"] == "512m"
    assert items["minio-bootstrap"]["depends_on"]["minio"]["condition"] == "service_healthy"


def test_forbidden_heavy_services_are_absent() -> None:
    assert not ({"redis", "spark", "kafka", "flink", "airflow-worker"} & services().keys())
