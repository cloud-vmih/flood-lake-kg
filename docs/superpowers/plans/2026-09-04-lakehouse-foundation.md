# Lakehouse Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run a persistent, resource-bounded Docker Compose foundation containing MinIO, PostgreSQL, Apache Polaris, and Apache Airflow without ingesting domain data.

**Architecture:** One PostgreSQL container owns isolated Airflow and Polaris databases; MinIO owns private `raw` and `warehouse` buckets; Polaris 1.7.0 persists its Iceberg REST catalog metadata in PostgreSQL and points the catalog at MinIO; Airflow 3.3.1 runs API server, scheduler with LocalExecutor, and DAG processor. One-shot bootstrap services initialize databases, buckets, catalog metadata, and the Airflow admin account idempotently, while all persistent state remains under ignored `dataset/lakehouse/` bind mounts.

**Tech Stack:** Docker Compose 5.2, PostgreSQL 17.11, MinIO 2025-09-07, Apache Polaris 1.7.0, Apache Airflow 3.3.1/Python 3.11, Bash, Pytest, PyYAML.

**Spec:** `docs/superpowers/specs/2026-09-04-lakehouse-foundation-design.md`

## Global Constraints

- Do not modify, move, copy, or delete existing stage-1 files under `dataset/`.
- Put new persistent service state only under `dataset/lakehouse/`; it remains Git-ignored.
- Never print or commit `.env` values; only blank placeholders belong in `.env.example`.
- Do not add Redis, Celery workers, Spark, Kafka, Flink, forecast code, Iceberg domain tables, or static-data imports.
- Pin every image tag; do not use `latest`.
- Bind host ports only to `127.0.0.1`.
- Use `:Z` on writable Fedora bind mounts and mount DAG/plugin source read-only.
- `lakehouse-down` must preserve bind-mounted data; do not add a volume-deleting Make target.
- Keep Airflow `LocalExecutor` parallelism at `2` and the declared idle memory limits below 4.5 GiB total.
- Preserve unrelated dirty-worktree changes, including the user's DOCX, diagram, CLI, and pipeline edits.

---

### Task 1: Local environment and secret initialization

**Files:**
- Modify: `.env.example`
- Create: `infra/scripts/init-lakehouse-env.sh`
- Create: `tests/unit/test_lakehouse_env.py`

**Interfaces:**
- Consumes: root `.env`, `ENV_FILE` override, `LAKEHOUSE_DATA_ROOT` override, `openssl`, `id`.
- Produces: `init-lakehouse-env.sh` that preserves existing nonblank keys, fills missing/blank lakehouse keys, creates local state directories, and never emits values.

- [ ] **Step 1: Write failing environment-contract tests**

```python
from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "infra/scripts/init-lakehouse-env.sh"
REQUIRED = {
    "LAKEHOUSE_POSTGRES_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "POLARIS_DB_PASSWORD",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "POLARIS_CLIENT_ID",
    "POLARIS_CLIENT_SECRET",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_API_SECRET_KEY",
    "AIRFLOW_ADMIN_USERNAME",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_UID",
}


def parse_env(path: Path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def test_example_declares_blank_lakehouse_secrets() -> None:
    values = parse_env(ROOT / ".env.example")
    assert REQUIRED <= values.keys()
    assert all(values[key] == "" for key in REQUIRED)


def test_initializer_preserves_values_and_is_idempotent(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("FLASHFLOOD_CDSE_USERNAME=kept\nMINIO_ROOT_USER=existing\n")
    data_root = tmp_path / "lakehouse"
    env = os.environ | {"ENV_FILE": str(env_file), "LAKEHOUSE_DATA_ROOT": str(data_root)}

    first = subprocess.run([SCRIPT], env=env, text=True, capture_output=True, check=True)
    before = env_file.read_bytes()
    second = subprocess.run([SCRIPT], env=env, text=True, capture_output=True, check=True)

    values = parse_env(env_file)
    assert values["FLASHFLOOD_CDSE_USERNAME"] == "kept"
    assert values["MINIO_ROOT_USER"] == "existing"
    assert REQUIRED <= values.keys()
    assert all(values[key] for key in REQUIRED)
    assert before == env_file.read_bytes()
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert first.stdout == second.stdout == "Lakehouse local environment is ready.\n"
    assert all(value not in first.stdout + first.stderr for value in values.values())
    assert {p.relative_to(data_root).as_posix() for p in data_root.rglob("*") if p.is_dir()} >= {
        "airflow", "airflow/logs", "minio", "postgres"
    }
```

- [ ] **Step 2: Run the tests to verify the contract fails**

Run: `.venv/bin/pytest tests/unit/test_lakehouse_env.py -q`

Expected: FAIL because the script and lakehouse keys do not exist.

- [ ] **Step 3: Extend the blank environment example**

Append exactly these committed placeholders to `.env.example`:

```dotenv

# Local Docker lakehouse (generated in ignored .env by make lakehouse-init)
LAKEHOUSE_POSTGRES_PASSWORD=
AIRFLOW_DB_PASSWORD=
POLARIS_DB_PASSWORD=
MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
POLARIS_CLIENT_ID=
POLARIS_CLIENT_SECRET=
AIRFLOW_FERNET_KEY=
AIRFLOW_API_SECRET_KEY=
AIRFLOW_ADMIN_USERNAME=
AIRFLOW_ADMIN_PASSWORD=
AIRFLOW_UID=
```

- [ ] **Step 4: Implement the idempotent initializer**

Implement `infra/scripts/init-lakehouse-env.sh` with `set -eu`, repository-root discovery from
the script location, `umask 077`, and overridable `ENV_FILE`/`LAKEHOUSE_DATA_ROOT`. Use a helper
that replaces a blank `KEY=` line or appends a missing key but never changes a nonblank value.
Generate URL-safe values so PostgreSQL SQLAlchemy/JDBC URLs need no escaping:

```bash
random_hex() { openssl rand -hex "$1"; }
random_urlsafe() { openssl rand -base64 "$1" | tr '+/' '-_' | tr -d '=\n'; }
fernet_key() { openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n'; }

ensure_value LAKEHOUSE_POSTGRES_PASSWORD "$(random_urlsafe 24)"
ensure_value AIRFLOW_DB_PASSWORD "$(random_urlsafe 24)"
ensure_value POLARIS_DB_PASSWORD "$(random_urlsafe 24)"
ensure_value MINIO_ROOT_USER "lakehouse_admin"
ensure_value MINIO_ROOT_PASSWORD "$(random_urlsafe 24)"
ensure_value POLARIS_CLIENT_ID "polaris_root"
ensure_value POLARIS_CLIENT_SECRET "$(random_urlsafe 32)"
ensure_value AIRFLOW_FERNET_KEY "$(fernet_key)"
ensure_value AIRFLOW_API_SECRET_KEY "$(random_hex 32)"
ensure_value AIRFLOW_ADMIN_USERNAME "admin"
ensure_value AIRFLOW_ADMIN_PASSWORD "$(random_urlsafe 24)"
ensure_value AIRFLOW_UID "$(id -u)"
```

Create `postgres`, `minio`, and `airflow/logs` beneath the data root, chmod `.env` to `0600`,
and print only `Lakehouse local environment is ready.`. Do not source `.env` or print its lines.

- [ ] **Step 5: Run focused tests**

Run: `.venv/bin/pytest tests/unit/test_lakehouse_env.py -q`

Expected: PASS.

- [ ] **Step 6: Commit Task 1**

```bash
git add .env.example infra/scripts/init-lakehouse-env.sh tests/unit/test_lakehouse_env.py
git commit -m "feat: initialize local lakehouse secrets"
```

---

### Task 2: Persistent PostgreSQL and private MinIO buckets

**Files:**
- Create: `compose.yaml`
- Create: `infra/postgres/init-multiple-databases.sh`
- Create: `tests/unit/test_lakehouse_compose.py`

**Interfaces:**
- Consumes: Task 1 `.env` keys and `dataset/lakehouse/{postgres,minio}` directories.
- Produces: healthy `postgres`, `minio`, and one-shot `minio-bootstrap` services; private `raw` and `warehouse` buckets; isolated `airflow` and `polaris` databases.

- [ ] **Step 1: Write failing Compose storage tests**

Create helpers in `tests/unit/test_lakehouse_compose.py` that load `compose.yaml` with
`yaml.safe_load`. Add these exact assertions:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/unit/test_lakehouse_compose.py -q`

Expected: FAIL because `compose.yaml` does not exist.

- [ ] **Step 3: Implement safe multi-database initialization**

Write `infra/postgres/init-multiple-databases.sh` with `set -eu`. Use `psql -v
ON_ERROR_STOP=1`, psql variables, `format(... %L ...)`, and `\gexec` so passwords are SQL-quoted
and never echoed. Create login roles `airflow` and `polaris`, then databases owned by the
matching roles only when absent. Read passwords from `AIRFLOW_DB_PASSWORD` and
`POLARIS_DB_PASSWORD`; do not hard-code them.

- [ ] **Step 4: Add the PostgreSQL and MinIO Compose services**

Create `compose.yaml` with a private `lakehouse` bridge network and required-variable
expressions such as `${MINIO_ROOT_PASSWORD:?run make lakehouse-init}`. Configure:

```yaml
postgres:
  image: postgres:17.11-bookworm
  mem_limit: 512m
  ports: ["127.0.0.1:5432:5432"]
  volumes:
    - ./dataset/lakehouse/postgres:/var/lib/postgresql/data:Z
    - ./infra/postgres/init-multiple-databases.sh:/docker-entrypoint-initdb.d/10-multiple-databases.sh:ro,Z

minio:
  image: minio/minio:RELEASE.2025-09-07T16-13-09Z
  mem_limit: 512m
  command: ["server", "/data", "--console-address", ":9001"]
  ports: ["127.0.0.1:9000:9000", "127.0.0.1:9001:9001"]
  volumes: ["./dataset/lakehouse/minio:/data:Z"]
```

Use `pg_isready` and MinIO `/minio/health/live` health checks. Configure `minio-bootstrap` to
wait through `depends_on`, set an alias, then run only:

```sh
mc mb --ignore-existing local/raw
mc mb --ignore-existing local/warehouse
```

The one-shot service must exit zero and must not set anonymous bucket policies.

- [ ] **Step 5: Validate tests and Compose interpolation**

Run:

```bash
.venv/bin/pytest tests/unit/test_lakehouse_compose.py -q
ENV_FILE=/tmp/flashflood-lakehouse-plan.env LAKEHOUSE_DATA_ROOT=/tmp/flashflood-lakehouse-plan infra/scripts/init-lakehouse-env.sh
docker compose --env-file /tmp/flashflood-lakehouse-plan.env config --quiet
```

Expected: tests PASS and Compose config exits 0 without contacting the daemon. Remove only the
two explicit `/tmp/flashflood-lakehouse-plan*` test targets after validation.

- [ ] **Step 6: Commit Task 2**

```bash
git add compose.yaml infra/postgres/init-multiple-databases.sh tests/unit/test_lakehouse_compose.py
git commit -m "feat: add persistent postgres and minio services"
```

---

### Task 3: Persistent Apache Polaris REST Catalog

**Files:**
- Modify: `compose.yaml`
- Create: `infra/polaris/bootstrap.sh`
- Modify: `tests/unit/test_lakehouse_compose.py`

**Interfaces:**
- Consumes: PostgreSQL `polaris` database, MinIO `warehouse` bucket, Polaris root client credentials.
- Produces: healthy Polaris 1.7.0 service and idempotent `polaris-bootstrap` service that creates internal catalog `flood_lakehouse` at `s3://warehouse/`.

- [ ] **Step 1: Extend contract tests for Polaris**

Add assertions:

```python
def test_polaris_is_rest_persistent_and_resource_bounded() -> None:
    items = services()
    polaris = items["polaris"]
    assert polaris["image"] == "apache/polaris:1.7.0"
    assert polaris["mem_limit"] == "1g"
    assert polaris["ports"] == ["127.0.0.1:8181:8181", "127.0.0.1:8182:8182"]
    assert polaris["environment"]["POLARIS_PERSISTENCE_TYPE"] == "relational-jdbc"
    assert "jdbc:postgresql://postgres:5432/polaris" in polaris["environment"]["QUARKUS_DATASOURCE_JDBC_URL"]
    assert polaris["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert polaris["depends_on"]["minio-bootstrap"]["condition"] == "service_completed_successfully"
    assert polaris["healthcheck"]


def test_polaris_bootstrap_is_idempotent_and_private() -> None:
    text = (ROOT / "infra/polaris/bootstrap.sh").read_text()
    assert "flood_lakehouse" in text
    assert "s3://warehouse/" in text
    assert '"pathStyleAccess": true' in text
    assert '"endpoint": "http://minio:9000"' in text
    assert "GET" in text and "POST" in text
    assert "CLIENT_SECRET" not in "\n".join(line for line in text.splitlines() if line.startswith("echo"))
```

- [ ] **Step 2: Run the Polaris tests to verify failure**

Run: `.venv/bin/pytest tests/unit/test_lakehouse_compose.py -q`

Expected: FAIL because Polaris services and bootstrap script are absent.

- [ ] **Step 3: Implement the idempotent catalog bootstrap**

Write `infra/polaris/bootstrap.sh` with `set -eu`. Obtain an OAuth token from
`/api/catalog/v1/oauth/tokens`; list management catalogs; exit successfully if
`flood_lakehouse` exists; otherwise POST this catalog body:

```json
{
  "catalog": {
    "name": "flood_lakehouse",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {"default-base-location": "s3://warehouse/"},
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://warehouse/"],
      "endpoint": "http://minio:9000",
      "endpointInternal": "http://minio:9000",
      "pathStyleAccess": true,
      "region": "us-east-1"
    }
  }
}
```

Use `curl --fail-with-body` and `jq -e`; do not enable shell tracing and do not echo response
bodies containing tokens. A non-409 POST failure exits nonzero. A preflight GET is the normal
idempotent path rather than treating every error as “already exists.”

- [ ] **Step 4: Add Polaris services to Compose**

Add `polaris` with the exact 1.7.0 JDBC environment documented by Polaris:

```yaml
POLARIS_PERSISTENCE_TYPE: relational-jdbc
POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATABASE_TYPE: postgresql
QUARKUS_DATASOURCE_JDBC_URL: jdbc:postgresql://postgres:5432/polaris
QUARKUS_DATASOURCE_USERNAME: polaris
QUARKUS_DATASOURCE_PASSWORD: ${POLARIS_DB_PASSWORD:?run make lakehouse-init}
POLARIS_REALM_CONTEXT_REALMS: POLARIS
POLARIS_BOOTSTRAP_CREDENTIALS: POLARIS,${POLARIS_CLIENT_ID:?},${POLARIS_CLIENT_SECRET:?}
AWS_REGION: us-east-1
AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER:?}
AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD:?}
```

Enable only insecure local S3 and S3 catalog storage, disable telemetry, and health-check
`http://localhost:8182/q/health`. Add `polaris-bootstrap` using pinned `alpine/curl:8.21.0`,
installing `jq` in the one-shot container before running the read-only-mounted script. It must
depend on healthy Polaris and exit after catalog creation/check.

- [ ] **Step 5: Run contract and config tests**

Run:

```bash
.venv/bin/pytest tests/unit/test_lakehouse_compose.py -q
docker compose --env-file /tmp/flashflood-lakehouse-plan.env config --quiet
```

Expected: PASS and no secret values in test output.

- [ ] **Step 6: Commit Task 3**

```bash
git add compose.yaml infra/polaris/bootstrap.sh tests/unit/test_lakehouse_compose.py
git commit -m "feat: add persistent polaris rest catalog"
```

---

### Task 4: Resource-bounded Airflow 3 LocalExecutor deployment

**Files:**
- Modify: `compose.yaml`
- Create: `airflow/dags/.gitkeep`
- Create: `airflow/plugins/.gitkeep`
- Modify: `tests/unit/test_lakehouse_compose.py`

**Interfaces:**
- Consumes: PostgreSQL `airflow` database and Airflow secrets from Task 1.
- Produces: one-shot `airflow-init`; healthy `airflow-api-server`, `airflow-scheduler`, and `airflow-dag-processor`; empty tracked DAG/plugin mounts.

- [ ] **Step 1: Extend tests for the Airflow 3 topology**

```python
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


def test_airflow_mounts_code_read_only_and_state_under_dataset() -> None:
    for name in ("airflow-api-server", "airflow-scheduler", "airflow-dag-processor"):
        volumes = services()[name]["volumes"]
        assert "./airflow/dags:/opt/airflow/dags:ro,Z" in volumes
        assert "./airflow/plugins:/opt/airflow/plugins:ro,Z" in volumes
        assert "./dataset/lakehouse/airflow/logs:/opt/airflow/logs:Z" in volumes
```

- [ ] **Step 2: Run the Airflow tests to verify failure**

Run: `.venv/bin/pytest tests/unit/test_lakehouse_compose.py -q`

Expected: FAIL because Airflow services are absent.

- [ ] **Step 3: Add common Airflow configuration**

Use YAML anchors to share pinned image, `${AIRFLOW_UID:?}:0`, environment, network, and volumes.
Configure:

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__PARALLELISM: "2"
AIRFLOW__CORE__LOAD_EXAMPLES: "false"
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg://airflow:${AIRFLOW_DB_PASSWORD:?}@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:?}
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_API_SECRET_KEY:?}
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: "true"
AIRFLOW__API__DAG_CACHE_SIZE: "16"
AIRFLOW__API__DAG_CACHE_TTL: "1800"
```

Use the FAB auth manager bundled with the reference image so the local admin account is
explicit. Do not set `_PIP_ADDITIONAL_REQUIREMENTS`.

- [ ] **Step 4: Add initialization and three daemon services**

`airflow-init` runs `airflow db migrate`, checks `airflow users list`, and invokes
`airflow users create --role Admin` only when `${AIRFLOW_ADMIN_USERNAME}` is absent. Do not
reset an existing password during normal startup.

Commands and health checks:

```text
airflow-api-server: command api-server; GET /api/v2/monitor/health; 768m
airflow-scheduler: command scheduler; airflow jobs check --job-type SchedulerJob; 1g
airflow-dag-processor: command dag-processor; airflow jobs check --job-type DagProcessorJob; 512m
```

All bind mounts and the API port follow the global constraints. Omit triggerer because this
slice has no deferrable task, and omit workers because LocalExecutor runs within scheduler.

- [ ] **Step 5: Validate Airflow contracts and total memory**

Add a test that sums parsed `mem_limit` values for long-running services and asserts the total
is at most `4608 MiB`. Run:

```bash
.venv/bin/pytest tests/unit/test_lakehouse_compose.py -q
docker compose --env-file /tmp/flashflood-lakehouse-plan.env config --quiet
```

Expected: PASS; expanded Compose contains no blank required variables and no forbidden service.

- [ ] **Step 6: Commit Task 4**

```bash
git add compose.yaml airflow/dags/.gitkeep airflow/plugins/.gitkeep tests/unit/test_lakehouse_compose.py
git commit -m "feat: add airflow local executor services"
```

---

### Task 5: Safe Make commands, smoke checks, and operator documentation

**Files:**
- Modify: `Makefile`
- Create: `infra/scripts/check-docker-access.sh`
- Create: `infra/scripts/smoke-lakehouse.sh`
- Create: `tests/unit/test_lakehouse_operations.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: all Compose services and scripts from Tasks 1–4.
- Produces: `lakehouse-init`, `lakehouse-up`, `lakehouse-status`, `lakehouse-smoke`, and `lakehouse-down` operator commands with non-destructive runtime verification.

- [ ] **Step 1: Write failing operation-safety tests**

```python
from pathlib import Path


ROOT = Path(__file__).parents[2]


def test_makefile_exposes_safe_lakehouse_commands() -> None:
    text = (ROOT / "Makefile").read_text()
    for target in ("lakehouse-init:", "lakehouse-up:", "lakehouse-status:", "lakehouse-smoke:", "lakehouse-down:"):
        assert target in text
    assert "down --volumes" not in text
    assert "down -v" not in text
    assert "rm -rf" not in text


def test_smoke_check_is_non_destructive_and_covers_every_service() -> None:
    text = (ROOT / "infra/scripts/smoke-lakehouse.sh").read_text()
    for evidence in ("pg_isready", "raw", "warehouse", "flood_lakehouse", "api/v2/monitor/health", "SchedulerJob", "DagProcessorJob"):
        assert evidence in text
    for forbidden in ("mc rm", "DROP ", "DELETE ", "down -v", "rm -rf"):
        assert forbidden not in text


def test_readme_documents_urls_persistence_and_permission_boundary() -> None:
    text = (ROOT / "README.md").read_text()
    for phrase in ("make lakehouse-init", "make lakehouse-up", "make lakehouse-smoke", "http://127.0.0.1:9001", "http://127.0.0.1:8080", "dataset/lakehouse/"):
        assert phrase in text
    assert "usermod -aG docker" in text
```

- [ ] **Step 2: Run tests to verify failure**

Run: `.venv/bin/pytest tests/unit/test_lakehouse_operations.py -q`

Expected: FAIL because operation scripts and targets are absent.

- [ ] **Step 3: Implement Docker access preflight and Make targets**

`check-docker-access.sh` checks `docker`, `docker compose version`, and `docker info`; on socket
denial it prints a concise command for the user to run manually:

```text
sudo usermod -aG docker "$USER"
```

It must explain that logout/login is required and never invoke `sudo` itself.

Append these phony targets while preserving all existing targets:

```make
lakehouse-init:
	infra/scripts/init-lakehouse-env.sh
	docker compose config --quiet

lakehouse-up: lakehouse-init
	infra/scripts/check-docker-access.sh
	docker compose up -d --wait

lakehouse-status:
	infra/scripts/check-docker-access.sh
	docker compose ps

lakehouse-smoke:
	infra/scripts/check-docker-access.sh
	infra/scripts/smoke-lakehouse.sh

lakehouse-down:
	infra/scripts/check-docker-access.sh
	docker compose down
```

- [ ] **Step 4: Implement non-destructive smoke checks**

Use `docker compose exec -T` for PostgreSQL and Airflow checks. Re-run the idempotent MinIO and
Polaris setup services to confirm both resources exist; do not create tables or objects. Check:

```text
pg_isready
psql query returns databases airflow and polaris
mc stat local/raw and local/warehouse
Polaris OAuth succeeds and management catalog list contains flood_lakehouse
Airflow API /api/v2/monitor/health returns success
airflow jobs check for SchedulerJob and DagProcessorJob
airflow dags list contains no example_* DAG
```

The script uses `set -eu`, emits one short line per component, and never enables `set -x`.

- [ ] **Step 5: Document startup, URLs, credentials, and persistence**

Add a Vietnamese `Hạ tầng Lakehouse cục bộ` README section documenting prerequisites, Docker
group warning, the five Make commands, service URLs, `.env` secrecy, resource expectations,
state location, and the fact that `lakehouse-down` preserves state. Explicitly state that this
slice contains no Spark or weather-data ingestion.

- [ ] **Step 6: Run all static verification before runtime startup**

Run:

```bash
.venv/bin/pytest tests/unit/test_lakehouse_env.py tests/unit/test_lakehouse_compose.py tests/unit/test_lakehouse_operations.py -q
.venv/bin/ruff check tests/unit/test_lakehouse_env.py tests/unit/test_lakehouse_compose.py tests/unit/test_lakehouse_operations.py
docker compose --env-file /tmp/flashflood-lakehouse-plan.env config --quiet
git diff --check
```

Expected: all tests pass, Ruff is clean, Compose config is valid, and diff check is empty.

- [ ] **Step 7: Start the real stack and run acceptance checks**

After the user grants Docker socket access and approves image downloads, run:

```bash
make lakehouse-init
docker compose pull
make lakehouse-up
make lakehouse-smoke
docker compose ps
```

Expected: all daemon services healthy; bootstrap services exited 0; no forbidden services are
present; no domain data exists.

- [ ] **Step 8: Verify restart persistence**

Run:

```bash
make lakehouse-down
make lakehouse-up
make lakehouse-smoke
```

Expected: existing PostgreSQL metadata, both buckets, Polaris catalog, and Airflow admin metadata
are reused without bootstrap errors or duplicate resources.

- [ ] **Step 9: Run the full repository regression suite**

Run:

```bash
make test
make lint
git status --short
```

Expected: all repository tests pass; lint passes; only intended implementation files plus the
user's pre-existing unrelated changes appear.

- [ ] **Step 10: Commit Task 5**

```bash
git add Makefile README.md infra/scripts/check-docker-access.sh infra/scripts/smoke-lakehouse.sh tests/unit/test_lakehouse_operations.py
git commit -m "feat: operate and verify lakehouse foundation"
```

---

## Final Acceptance Record

Capture the following in the implementation handoff without committing secrets or generated
runtime state:

```text
Docker Compose version
resolved pinned image names
healthy service list
smoke-check result
restart-persistence result
pytest count
lint result
disk usage of dataset/lakehouse
remaining filesystem capacity
```

Do not run `docker compose down --volumes`, delete Docker images, or remove bind-mounted state as
part of acceptance.
