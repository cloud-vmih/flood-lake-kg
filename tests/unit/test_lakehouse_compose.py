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


def test_polaris_is_rest_persistent_and_resource_bounded() -> None:
    items = services()
    polaris = items["polaris"]
    database_bootstrap = items["polaris-db-bootstrap"]
    assert database_bootstrap["image"] == "apache/polaris-admin-tool:1.7.0"
    assert database_bootstrap["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert polaris["image"] == "apache/polaris:1.7.0"
    assert polaris["mem_limit"] == "1g"
    assert polaris["ports"] == ["127.0.0.1:8181:8181", "127.0.0.1:8182:8182"]
    assert polaris["environment"]["POLARIS_PERSISTENCE_TYPE"] == "relational-jdbc"
    assert "jdbc:postgresql://postgres:5432/polaris" in polaris["environment"]["QUARKUS_DATASOURCE_JDBC_URL"]
    assert polaris["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert polaris["depends_on"]["polaris-db-bootstrap"]["condition"] == "service_completed_successfully"
    assert polaris["depends_on"]["minio-bootstrap"]["condition"] == "service_completed_successfully"
    assert polaris["healthcheck"]
    assert polaris["environment"]['polaris.readiness.ignore-severe-issues'] == "true"


def test_polaris_bootstrap_is_idempotent_and_private() -> None:
    text = (ROOT / "infra/polaris/bootstrap.sh").read_text()
    assert "flood_lakehouse" in text
    assert "s3://warehouse/" in text
    assert '"pathStyleAccess": true' in text
    assert '"endpoint": "http://minio:9000"' in text
    assert "GET" in text and "POST" in text
    assert "CLIENT_SECRET" not in "\n".join(line for line in text.splitlines() if line.startswith("echo"))


def test_airflow_uses_basic_v3_local_executor_topology() -> None:
    items = services()
    assert {"airflow-init", "airflow-api-server", "airflow-scheduler", "airflow-dag-processor"} <= items.keys()
    for name in ("airflow-api-server", "airflow-scheduler", "airflow-dag-processor"):
        assert items[name]["image"] == "apache/airflow:3.3.1-python3.11"
        assert items[name]["depends_on"]["airflow-init"]["condition"] == "service_completed_successfully"
        assert items[name]["healthcheck"]
    env = items["airflow-scheduler"]["environment"]
    assert env["AIRFLOW__CORE__EXECUTOR"] == "LocalExecutor"
    assert env["AIRFLOW__CORE__PARALLELISM"] == "2"
    assert env["AIRFLOW__CORE__LOAD_EXAMPLES"] == "false"
    assert items["airflow-api-server"]["ports"] == ["127.0.0.1:8080:8080"]
    assert "entrypoint" not in items["airflow-init"]
    assert items["airflow-init"]["command"][:2] == ["bash", "-c"]


def test_airflow_mounts_code_read_only_and_state_under_dataset() -> None:
    for name in ("airflow-api-server", "airflow-scheduler", "airflow-dag-processor"):
        volumes = services()[name]["volumes"]
        assert "./airflow/dags:/opt/airflow/dags:ro,Z" in volumes
        assert "./airflow/plugins:/opt/airflow/plugins:ro,Z" in volumes
        assert "./dataset/lakehouse/airflow/logs:/opt/airflow/logs:Z" in volumes


def test_long_running_services_fit_memory_budget() -> None:
    items = services()
    names = {
        "postgres",
        "minio",
        "polaris",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
    }

    def mib(value: str) -> int:
        return int(value[:-1]) * (1024 if value.endswith("g") else 1)

    assert sum(mib(items[name]["mem_limit"]) for name in names) <= 4608
