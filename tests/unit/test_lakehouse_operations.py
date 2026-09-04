from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_makefile_exposes_safe_lakehouse_commands() -> None:
    text = (ROOT / "Makefile").read_text()
    for target in (
        "lakehouse-init:",
        "lakehouse-up:",
        "lakehouse-status:",
        "lakehouse-smoke:",
        "lakehouse-down:",
    ):
        assert target in text
    assert "down --volumes" not in text
    assert "down -v" not in text
    assert "rm -rf" not in text
    assert (
        "docker compose up -d --wait postgres minio polaris "
        "airflow-api-server airflow-scheduler airflow-dag-processor"
    ) in text


def test_smoke_check_is_non_destructive_and_covers_every_service() -> None:
    text = (ROOT / "infra/scripts/smoke-lakehouse.sh").read_text()
    for evidence in (
        "pg_isready",
        "raw",
        "warehouse",
        "flood_lakehouse",
        "api/v2/monitor/health",
        "SchedulerJob",
        "DagProcessorJob",
    ):
        assert evidence in text
    for forbidden in ("mc rm", "DROP ", "DELETE ", "down -v", "rm -rf"):
        assert forbidden not in text


def test_readme_documents_urls_persistence_and_permission_boundary() -> None:
    text = (ROOT / "README.md").read_text()
    for phrase in (
        "make lakehouse-init",
        "make lakehouse-up",
        "make lakehouse-smoke",
        "http://127.0.0.1:9001",
        "http://127.0.0.1:8080",
        "dataset/lakehouse/",
    ):
        assert phrase in text
    assert "usermod -aG docker" in text
