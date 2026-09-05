# Spark Iceberg Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run an optional one-master/one-worker Spark 4.1.3 cluster that writes and reads Iceberg 1.11.0 tables through Polaris and MinIO.

**Architecture:** A custom official Spark image bakes in checksum-verified Iceberg Spark and AWS JARs. Compose profile `spark` runs a resource-bounded master and worker, while a one-shot driver submits read/write jobs; shared Python job helpers configure the existing Polaris REST catalog and MinIO S3 endpoint without storing secrets in tracked files.

**Tech Stack:** Apache Spark 4.1.3, Scala 2.13, Java 17, Apache Iceberg 1.11.0, Polaris 1.7.0, MinIO, Docker Compose, Python 3, Pytest.

**Spec:** `docs/superpowers/specs/2026-09-05-spark-iceberg-runtime-design.md`

## Global Constraints

- Run one Spark master and one worker in Docker; install no Spark/Java/PySpark on the host.
- Use `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` and Iceberg `1.11.0` exactly.
- Bake Maven artifacts into the image and verify their official SHA-512 checksums.
- Keep Spark optional under Compose profile `spark`; do not add it to `make lakehouse-up`.
- Bind Spark ports only to `127.0.0.1`.
- Never bake or print `.env`, Polaris credentials, or MinIO credentials.
- Do not modify, move, copy, or delete any existing file under `dataset/`.
- Smoke cleanup may address only its generated `smoke_<uuid>` namespace and `roundtrip` table.
- Do not add permanent Iceberg schemas, domain data, Airflow DAGs/providers, Kafka, or Flink.
- Preserve all unrelated dirty-worktree changes.

---

### Task 1: Checksum-verified Spark/Iceberg image

**Files:**
- Create: `infra/spark/Dockerfile`
- Create: `tests/unit/test_spark_runtime.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes: official Spark image and Maven Central during build.
- Produces: `flood-lakehouse-spark:4.1.3-iceberg1.11.0` with both Iceberg JARs in `/opt/spark/jars`; `make spark-build`.

- [ ] **Step 1: Write the failing image/build contract test**

Create `tests/unit/test_spark_runtime.py`. Parse the Dockerfile build arguments and exercise the
Make interface rather than importing Spark on the host:

```python
from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[2]
DOCKERFILE = ROOT / "infra/spark/Dockerfile"


def dockerfile_args() -> dict[str, str]:
    assert DOCKERFILE.is_file()
    return dict(
        line.removeprefix("ARG ").split("=", 1)
        for line in DOCKERFILE.read_text().splitlines()
        if line.startswith("ARG ") and "=" in line
    )


def test_spark_image_pins_compatible_runtime_and_checksums() -> None:
    args = dockerfile_args()
    assert args == {
        "SPARK_VERSION": "4.1.3",
        "ICEBERG_VERSION": "1.11.0",
        "ICEBERG_SPARK_SHA512": (
            "f4620bb2d20777146a769a8a939a5bed658e1d5708b6a5155e1f6e831c0bfd29"
            "bd1f82618dc59bc0f30399ffcb03add3e66656286692dc6c2be94e4e1f4e7479"
        ),
        "ICEBERG_AWS_SHA512": (
            "bb50f7dc5f36a001efecf15e4d03eff955466f06557261fbacdd7fe0d88113c82"
            "e0b9b2af96cd554cef4a1d4c2348fc950270bc3f51cf6bf1e4526f46ce336a0"
        ),
    }


def test_make_exposes_the_spark_image_build() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "spark-build"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[-1] == "docker compose --profile spark build spark-master"
```

- [ ] **Step 2: Run the test and confirm RED**

Run: `.venv/bin/pytest tests/unit/test_spark_runtime.py -q`

Expected: FAIL because the Dockerfile and Make target are absent.

- [ ] **Step 3: Implement the pinned image**

Create `infra/spark/Dockerfile`:

```dockerfile
ARG SPARK_VERSION=4.1.3
FROM apache/spark:${SPARK_VERSION}-scala2.13-java17-python3-ubuntu

ARG SPARK_VERSION=4.1.3
ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_SPARK_SHA512=f4620bb2d20777146a769a8a939a5bed658e1d5708b6a5155e1f6e831c0bfd29bd1f82618dc59bc0f30399ffcb03add3e66656286692dc6c2be94e4e1f4e7479
ARG ICEBERG_AWS_SHA512=bb50f7dc5f36a001efecf15e4d03eff955466f06557261fbacdd7fe0d88113c82e0b9b2af96cd554cef4a1d4c2348fc950270bc3f51cf6bf1e4526f46ce336a0

USER root

RUN SPARK_JAR="iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
    AWS_JAR="iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    python3 -c 'import hashlib, os, urllib.request; version=os.environ["ICEBERG_VERSION"]; files=((f"iceberg-spark-runtime-4.1_2.13-{version}.jar", os.environ["ICEBERG_SPARK_SHA512"], f"iceberg-spark-runtime-4.1_2.13/{version}"), (f"iceberg-aws-bundle-{version}.jar", os.environ["ICEBERG_AWS_SHA512"], f"iceberg-aws-bundle/{version}")); [(lambda data, name, expected: (hashlib.sha512(data).hexdigest() == expected or (_ for _ in ()).throw(RuntimeError(f"checksum mismatch: {name}")), open(f"/opt/spark/jars/{name}", "wb").write(data)))(urllib.request.urlopen(f"https://repo.maven.apache.org/maven2/org/apache/iceberg/{path}/{name}", timeout=60).read(), name, expected) for name, expected, path in files]' \
    && chown spark:spark "/opt/spark/jars/${SPARK_JAR}" "/opt/spark/jars/${AWS_JAR}"

USER spark

RUN /opt/spark/bin/spark-submit --version 2>&1 | grep "version ${SPARK_VERSION}"
```

Expose all four `ARG` values to the Python command with inline environment assignments before
`python3`; if Docker does not export build args automatically, use:

```dockerfile
RUN ICEBERG_VERSION="${ICEBERG_VERSION}" \
    ICEBERG_SPARK_SHA512="${ICEBERG_SPARK_SHA512}" \
    ICEBERG_AWS_SHA512="${ICEBERG_AWS_SHA512}" \
    python3 -c '...'
```

Keep the implementation readable by replacing the one-line downloader with a BuildKit Python
heredoc if supported by the local Docker builder. The behavior must remain: download exactly
two URLs, verify before writing, and fail on mismatch.

- [ ] **Step 4: Add the Make build target**

Add `spark-build` to `.PHONY` and implement:

```make
spark-build: lakehouse-init
	docker compose --profile spark build spark-master
```

The Compose service is added in Task 2, so until then the dry run passes but the real build is
executed after Task 2 configuration exists.

- [ ] **Step 5: Run focused tests**

Run: `.venv/bin/pytest tests/unit/test_spark_runtime.py -q`

Expected: PASS.

- [ ] **Step 6: Commit Task 1**

```bash
git add infra/spark/Dockerfile tests/unit/test_spark_runtime.py Makefile
git commit -m "build: pin Spark Iceberg runtime"
```

---

### Task 2: Optional Spark master and worker profile

**Files:**
- Modify: `compose.yaml`
- Modify: `tests/unit/test_spark_runtime.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes: Task 1 custom image and the existing Compose `lakehouse` network.
- Produces: healthy `spark-master` and `spark-worker`; `spark-up`, `spark-status`, and `spark-down` commands that do not stop the base lakehouse.

- [ ] **Step 1: Write failing Compose behavior tests**

Append to `tests/unit/test_spark_runtime.py`:

```python
import yaml


def spark_services() -> dict:
    return yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]


def test_spark_master_and_worker_are_optional_private_and_bounded() -> None:
    services = spark_services()
    master = services["spark-master"]
    worker = services["spark-worker"]
    expected_image = "flood-lakehouse-spark:4.1.3-iceberg1.11.0"
    assert master["image"] == worker["image"] == expected_image
    assert master["profiles"] == worker["profiles"] == ["spark"]
    assert master["ports"] == ["127.0.0.1:7077:7077", "127.0.0.1:8081:8080"]
    assert worker["ports"] == ["127.0.0.1:8082:8081"]
    assert master["mem_limit"] == "768m"
    assert worker["mem_limit"] == "2560m"
    assert worker["depends_on"]["spark-master"]["condition"] == "service_healthy"
    assert master["healthcheck"] and worker["healthcheck"]


def test_base_lakehouse_up_does_not_start_spark() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "lakehouse-up"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert "spark-master" not in result.stdout
    assert "spark-worker" not in result.stdout


def test_make_exposes_isolated_spark_lifecycle() -> None:
    expected = {
        "spark-up": "docker compose --profile spark up -d --wait spark-master spark-worker",
        "spark-status": "docker compose --profile spark ps spark-master spark-worker",
        "spark-down": "docker compose --profile spark stop spark-worker spark-master",
    }
    for target, command in expected.items():
        result = subprocess.run(
            ["make", "--dry-run", target], cwd=ROOT, text=True, capture_output=True
        )
        assert result.returncode == 0, result.stderr
        assert command in result.stdout
    down = subprocess.run(
        ["make", "--dry-run", "spark-down"], cwd=ROOT, text=True, capture_output=True
    ).stdout
    assert "docker compose down" not in down
    assert "--volumes" not in down
```

- [ ] **Step 2: Run focused tests and confirm RED**

Run: `.venv/bin/pytest tests/unit/test_spark_runtime.py -q`

Expected: FAIL because services and lifecycle targets are absent.

- [ ] **Step 3: Add the Spark services**

Add one build anchor and three profile services to `compose.yaml`:

```yaml
x-spark-image: &spark-image
  image: flood-lakehouse-spark:4.1.3-iceberg1.11.0
  build:
    context: .
    dockerfile: infra/spark/Dockerfile
    args:
      SPARK_VERSION: "4.1.3"
      ICEBERG_VERSION: "1.11.0"

services:
  spark-master:
    <<: *spark-image
    profiles: [spark]
    mem_limit: 768m
    entrypoint: [/opt/spark/bin/spark-class]
    command:
      - org.apache.spark.deploy.master.Master
      - --host
      - spark-master
      - --port
      - "7077"
      - --webui-port
      - "8080"
    ports:
      - "127.0.0.1:7077:7077"
      - "127.0.0.1:8081:8080"
    healthcheck:
      test: [CMD, python3, -c, "import socket; socket.create_connection(('localhost', 7077), 2).close()"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 15s
    restart: unless-stopped
    networks: [lakehouse]

  spark-worker:
    <<: *spark-image
    profiles: [spark]
    mem_limit: 2560m
    depends_on:
      spark-master:
        condition: service_healthy
    entrypoint: [/opt/spark/bin/spark-class]
    command:
      - org.apache.spark.deploy.worker.Worker
      - --cores
      - "2"
      - --memory
      - 2g
      - --webui-port
      - "8081"
      - spark://spark-master:7077
    environment:
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER:?run make lakehouse-init}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD:?run make lakehouse-init}
      AWS_REGION: us-east-1
    ports: ["127.0.0.1:8082:8081"]
    healthcheck:
      test: [CMD, python3, -c, "import urllib.request; urllib.request.urlopen('http://localhost:8081', timeout=2).close()"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 15s
    restart: unless-stopped
    networks: [lakehouse]
```

Also add `spark-submit` using the same image/profile with `mem_limit: 1536m`, `entrypoint:
[/opt/spark/bin/spark-submit]`, the Polaris and MinIO environment values, read-only
`./spark/jobs:/opt/spark/jobs:ro,Z`, and the `lakehouse` network. It is a one-shot service with
`restart: "no"`; do not publish ports.

- [ ] **Step 4: Add isolated lifecycle targets**

Add to `.PHONY` and implement:

```make
spark-up: lakehouse-up
	infra/scripts/check-docker-access.sh
	docker compose --profile spark up -d --wait spark-master spark-worker

spark-status:
	infra/scripts/check-docker-access.sh
	docker compose --profile spark ps spark-master spark-worker

spark-down:
	infra/scripts/check-docker-access.sh
	docker compose --profile spark stop spark-worker spark-master
	docker compose --profile spark rm -f spark-worker spark-master
```

- [ ] **Step 5: Validate configuration and tests**

Run:

```bash
.venv/bin/pytest tests/unit/test_spark_runtime.py tests/unit/test_lakehouse_compose.py -q
docker compose config --quiet
```

Expected: PASS without starting Spark.

- [ ] **Step 6: Build and start the real cluster**

Run:

```bash
make spark-build
make spark-up
make spark-status
```

Expected: image builds, master and worker become healthy, and master UI reports one alive
worker. Verify `make lakehouse-up` alone still omits Spark after a later `make spark-down`.

- [ ] **Step 7: Commit Task 2**

```bash
git add compose.yaml Makefile tests/unit/test_spark_runtime.py
git commit -m "feat: add optional Spark standalone cluster"
```

---

### Task 3: Iceberg round-trip job and safe lifecycle smoke

**Files:**
- Create: `spark/jobs/runtime.py`
- Create: `spark/jobs/smoke_iceberg.py`
- Create: `infra/scripts/smoke-spark.sh`
- Create: `tests/unit/test_spark_job.py`
- Modify: `tests/unit/test_spark_runtime.py`
- Modify: `Makefile`
- Modify: `README.md`

**Interfaces:**
- Consumes: running Spark profile and existing Polaris/MinIO services.
- Produces: reusable `build_spark_session(app_name: str)`, testable `run_roundtrip(spark, namespace: str)`, and `make spark-smoke`.

- [ ] **Step 1: Write failing pure-Python job contract tests**

Load `spark/jobs/runtime.py` and `spark/jobs/smoke_iceberg.py` by file path so host PySpark is not
required. Tests must cover:

```python
def test_catalog_options_require_secrets_and_redact_credentials() -> None:
    env = {
        "POLARIS_URI": "http://polaris:8181/api/catalog",
        "POLARIS_CATALOG": "flood_lakehouse",
        "POLARIS_CLIENT_ID": "client",
        "POLARIS_CLIENT_SECRET": "secret",
        "MINIO_ENDPOINT": "http://minio:9000",
    }
    options = runtime.catalog_options(env)
    prefix = "spark.sql.catalog.flood_lakehouse"
    assert options[f"{prefix}.type"] == "rest"
    assert options[f"{prefix}.warehouse"] == "flood_lakehouse"
    assert options[f"{prefix}.credential"] == "client:secret"
    assert options[f"{prefix}.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert options[f"{prefix}.s3.endpoint"] == "http://minio:9000"
    assert "credential" in options["spark.redaction.regex"].lower()


def test_smoke_namespace_is_generated_and_not_user_controlled() -> None:
    first = smoke.make_smoke_namespace()
    second = smoke.make_smoke_namespace()
    assert first.startswith("smoke_")
    assert first != second
    assert first.replace("_", "").isalnum()


def test_roundtrip_cleans_up_only_its_table_after_success() -> None:
    spark = FakeSpark(rows=[(1, "basin"), (2, "rain"), (3, "risk")])
    smoke.run_roundtrip(spark, "smoke_0123456789abcdef")
    assert spark.statements[-2:] == [
        "DROP TABLE IF EXISTS flood_lakehouse.smoke_0123456789abcdef.roundtrip",
        "DROP NAMESPACE IF EXISTS flood_lakehouse.smoke_0123456789abcdef",
    ]


def test_roundtrip_cleans_up_after_read_failure() -> None:
    spark = FakeSpark(read_error=RuntimeError("read failed"))
    with pytest.raises(RuntimeError, match="read failed"):
        smoke.run_roundtrip(spark, "smoke_0123456789abcdef")
    assert spark.statements[-2:] == [
        "DROP TABLE IF EXISTS flood_lakehouse.smoke_0123456789abcdef.roundtrip",
        "DROP NAMESPACE IF EXISTS flood_lakehouse.smoke_0123456789abcdef",
    ]
```

Implement `FakeSpark.sql()` in the test to record exact statements, return literal rows for the
SELECT, and raise only on the configured SELECT. Add a rejection test for namespace values not
matching `^smoke_[0-9a-f]{32}$`.

- [ ] **Step 2: Run job tests and confirm RED**

Run: `.venv/bin/pytest tests/unit/test_spark_job.py -q`

Expected: FAIL because job modules do not exist.

- [ ] **Step 3: Implement shared Spark session configuration**

In `spark/jobs/runtime.py`, keep PySpark import inside `build_spark_session` so pure unit tests
run on the host. `catalog_options(environ)` must require exactly the five environment keys in
the test, construct the Iceberg extensions/REST/S3 properties from literals, and set:

```python
"spark.redaction.regex": "(?i)secret|password|token|access[.]?key|credential"
```

`build_spark_session(app_name)` creates a builder, applies every option, sets driver memory to
`1g`, and returns `getOrCreate()`.

- [ ] **Step 4: Implement the temporary round-trip job**

In `spark/jobs/smoke_iceberg.py`:

- `make_smoke_namespace()` returns `f"smoke_{uuid.uuid4().hex}"`;
- `run_roundtrip(spark, namespace)` validates the regex before SQL;
- execute exact CREATE NAMESPACE, CREATE TABLE, INSERT, and sorted SELECT statements;
- compare against `[(1, "basin"), (2, "rain"), (3, "risk")]`;
- in `finally`, execute exact `DROP TABLE IF EXISTS` and `DROP NAMESPACE IF EXISTS` for the
  validated namespace;
- if primary work and cleanup both fail, retain the primary exception and report cleanup to
  stderr; if only cleanup fails, return non-zero;
- `main()` prints only the generated namespace and success/cleanup messages, builds/stops the
  Spark session, and never prints environment values.

- [ ] **Step 5: Implement cluster/smoke script and Make target**

Create executable `infra/scripts/smoke-spark.sh` with `set -eu`. It must:

1. query `http://localhost:8080/json/` inside `spark-master` and require exactly one alive worker;
2. run:

```bash
docker compose --profile spark run --no-deps --rm spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/smoke_iceberg.py
```

3. never source or print `.env`.

Add:

```make
spark-smoke: spark-up
	infra/scripts/check-docker-access.sh
	infra/scripts/smoke-spark.sh
```

Add a test that `make --dry-run spark-smoke` reaches the script and that a fake Docker binary
captures only the worker query and the exact one-shot submission arguments.

- [ ] **Step 6: Document and run live acceptance twice**

Document the five Spark commands, localhost UIs, optional resource usage, and temporary smoke
behavior in `README.md`. Then run:

```bash
make spark-smoke
make spark-smoke
make spark-down
make lakehouse-status
```

Expected: both round trips pass using different namespaces; each reports cleanup; Spark
containers are absent after shutdown; base services remain healthy.

- [ ] **Step 7: Run complete verification**

Run:

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests spark/jobs
docker compose config --quiet
git diff --check
```

Expected: all commands exit zero.

- [ ] **Step 8: Commit Task 3**

```bash
git add spark/jobs/runtime.py spark/jobs/smoke_iceberg.py \
  infra/scripts/smoke-spark.sh tests/unit/test_spark_job.py \
  tests/unit/test_spark_runtime.py Makefile README.md
git commit -m "test: verify Spark Iceberg round trip"
```

---

## Self-Review Result

- Spec coverage: image integrity, optional profile, resource limits, secret boundaries,
  master/worker topology, Polaris/MinIO round trip, isolated cleanup, lifecycle, and regression
  verification map to Tasks 1–3.
- Placeholder scan: no deferred implementation placeholder remains; permanent schemas and
  Airflow DAG integration are intentionally separate future slices.
- Type consistency: image tag, service names, catalog name, environment keys, helper function
  names, Make targets, ports, and exact versions are consistent across all tasks.
