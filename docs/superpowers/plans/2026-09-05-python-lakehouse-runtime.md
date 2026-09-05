# Python Lakehouse Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Install and verify one pinned Xarray/GRIB/PyIceberg runtime in both the project `.venv` and a custom Airflow 3.3.1 image.

**Architecture:** A single exact top-level manifest is installed additively over the existing host lock and into an Airflow image extended from the pinned project base. Host checks prove GRIB decoding is available; container checks additionally authenticate through PyIceberg to the existing Polaris REST catalog without creating data.

**Tech Stack:** Python 3.11, Xarray 2026.7.0, PyIceberg 0.12.0 with PyArrow FileIO, cfgrib 0.9.15.1, eccodes 2.48.0, Apache Airflow 3.3.1, Docker Compose, Bash, Pytest.

**Spec:** `docs/superpowers/specs/2026-09-05-python-lakehouse-runtime-design.md`

## Global Constraints

- Python remains `>=3.11,<3.13`; both target environments use Python 3.11.
- Apache Airflow remains exactly `3.3.1` on Python 3.11.
- Do not modify the existing static pipeline requirements in `requirements.lock`.
- Do not modify, move, copy, or delete existing files under `dataset/`.
- Use the same exact four direct dependency versions in `.venv` and Airflow.
- Use the constraint file shipped in the Airflow image for container transitive dependencies.
- Do not use `_PIP_ADDITIONAL_REQUIREMENTS` or install packages at container startup.
- Do not add Spark, Kafka, Flink, forecast downloads, DAGs, namespaces, or Iceberg tables.
- Never print or commit `.env` values or Polaris credentials.
- Preserve unrelated dirty-worktree changes in the user's DOCX, diagrams, CLI, and pipeline.

---

### Task 1: Shared dependency manifest and host runtime

**Files:**
- Create: `requirements/lakehouse.txt`
- Create: `infra/scripts/setup-lakehouse-python.sh`
- Create: `tests/unit/test_python_lakehouse_runtime.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes: existing `.venv`, `requirements.lock`, and Python 3.11.
- Produces: `make lakehouse-python-setup`; an idempotent host runtime with four pinned direct packages and a working ecCodes decoder.

- [ ] **Step 1: Write failing manifest and setup-contract tests**

Create `tests/unit/test_python_lakehouse_runtime.py`:

```python
from pathlib import Path

ROOT = Path(__file__).parents[2]
MANIFEST = ROOT / "requirements/lakehouse.txt"

EXPECTED = {
    "xarray==2026.7.0",
    "pyiceberg[pyarrow]==0.12.0",
    "cfgrib==0.9.15.1",
    "eccodes==2.48.0",
}


def requirement_lines() -> set[str]:
    return {
        line.strip()
        for line in MANIFEST.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def test_lakehouse_manifest_pins_only_approved_direct_packages() -> None:
    assert requirement_lines() == EXPECTED
    assert all("apache-airflow" not in line for line in requirement_lines())


def test_host_setup_is_fail_fast_and_validates_the_runtime() -> None:
    text = (ROOT / "infra/scripts/setup-lakehouse-python.sh").read_text()
    assert "set -eu" in text
    assert ".venv/bin/python" in text
    assert "requirements/lakehouse.txt" in text
    assert "-m pip check" in text
    assert "-m cfgrib selfcheck" in text
    assert "import xarray" in text
    assert "import pyiceberg" in text


def test_makefile_exposes_python_runtime_setup() -> None:
    text = (ROOT / "Makefile").read_text()
    assert "lakehouse-python-setup:" in text
    assert "infra/scripts/setup-lakehouse-python.sh" in text
```

- [ ] **Step 2: Run the focused tests and confirm failure**

Run:

```bash
.venv/bin/pytest tests/unit/test_python_lakehouse_runtime.py -q
```

Expected: FAIL because the new files and Make target do not exist.

- [ ] **Step 3: Add the exact shared manifest**

Create `requirements/lakehouse.txt` with exactly:

```text
# Shared host/Airflow ingest runtime. Keep direct versions exact.
xarray==2026.7.0
pyiceberg[pyarrow]==0.12.0
cfgrib==0.9.15.1
eccodes==2.48.0
```

- [ ] **Step 4: Implement the idempotent host installer**

Create executable `infra/scripts/setup-lakehouse-python.sh`. It must resolve the repository
root from its own path, fail with `Run 'make setup' first.` when `.venv/bin/python` is missing,
and run these operations in order:

```bash
"$PROJECT_ROOT/.venv/bin/python" -m pip install \
  --disable-pip-version-check \
  --requirement "$PROJECT_ROOT/requirements/lakehouse.txt"
"$PROJECT_ROOT/.venv/bin/python" -m pip check
"$PROJECT_ROOT/.venv/bin/python" -m cfgrib selfcheck
"$PROJECT_ROOT/.venv/bin/python" - <<'PY'
from importlib.metadata import version

import cfgrib
import eccodes
import pyiceberg
import xarray

expected = {
    "xarray": "2026.7.0",
    "pyiceberg": "0.12.0",
    "cfgrib": "0.9.15.1",
    "eccodes": "2.48.0",
}
actual = {name: version(name) for name in expected}
if actual != expected:
    raise SystemExit(f"Unexpected lakehouse versions: {actual!r}")
print("Host Python lakehouse runtime is ready.")
PY
```

Imports intentionally precede the version assertion so missing native libraries fail visibly.
Do not log environment variables or credentials.

- [ ] **Step 5: Add and execute the Make target**

Add `lakehouse-python-setup` to `.PHONY` and implement:

```make
lakehouse-python-setup:
	infra/scripts/setup-lakehouse-python.sh
```

Run:

```bash
.venv/bin/pytest tests/unit/test_python_lakehouse_runtime.py -q
make lakehouse-python-setup
```

Expected: tests PASS; pip reports a consistent environment; both ecCodes and the final host
runtime message report readiness.

- [ ] **Step 6: Commit Task 1**

```bash
git add requirements/lakehouse.txt infra/scripts/setup-lakehouse-python.sh \
  tests/unit/test_python_lakehouse_runtime.py Makefile
git commit -m "build: add host Python lakehouse runtime"
```

---

### Task 2: Immutable custom Airflow runtime

**Files:**
- Create: `infra/airflow/Dockerfile`
- Modify: `compose.yaml`
- Modify: `tests/unit/test_python_lakehouse_runtime.py`
- Modify: `tests/unit/test_lakehouse_compose.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes: Task 1 `requirements/lakehouse.txt`, the pinned Airflow base image, and its bundled constraints file.
- Produces: local image `flood-lakehouse-airflow:3.3.1-python3.11`; all four Airflow services use the same immutable runtime.

- [ ] **Step 1: Write failing Airflow-image contract tests**

Append to `tests/unit/test_python_lakehouse_runtime.py`:

```python
import yaml


def test_airflow_dockerfile_preserves_airflow_and_uses_shared_manifest() -> None:
    text = (ROOT / "infra/airflow/Dockerfile").read_text()
    assert "ARG AIRFLOW_VERSION=3.3.1" in text
    assert "FROM apache/airflow:${AIRFLOW_VERSION}-python3.11" in text
    assert "requirements/lakehouse.txt" in text
    assert 'apache-airflow==${AIRFLOW_VERSION}' in text
    assert '${HOME}/constraints.txt' in text
    assert "python -m pip check" in text
    assert "python -m cfgrib selfcheck" in text
    assert "USER root" not in text


def test_all_airflow_services_use_the_custom_build() -> None:
    data = yaml.safe_load((ROOT / "compose.yaml").read_text())
    for name in (
        "airflow-init",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
    ):
        service = data["services"][name]
        assert service["image"] == "flood-lakehouse-airflow:3.3.1-python3.11"
        assert service["build"]["dockerfile"] == "infra/airflow/Dockerfile"
        assert service["build"]["args"]["AIRFLOW_VERSION"] == "3.3.1"
```

Update the existing Airflow image assertions in `tests/unit/test_lakehouse_compose.py` to
expect `flood-lakehouse-airflow:3.3.1-python3.11`.

- [ ] **Step 2: Run the focused tests and confirm failure**

Run:

```bash
.venv/bin/pytest tests/unit/test_python_lakehouse_runtime.py \
  tests/unit/test_lakehouse_compose.py -q
```

Expected: FAIL because the Dockerfile and custom image configuration do not exist.

- [ ] **Step 3: Implement the extended Airflow image**

Create `infra/airflow/Dockerfile`:

```dockerfile
ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION

COPY --chown=airflow:root requirements/lakehouse.txt /tmp/requirements-lakehouse.txt

RUN python -m pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "${HOME}/constraints.txt" \
      --requirement /tmp/requirements-lakehouse.txt \
    && python -m pip check \
    && python -m cfgrib selfcheck \
    && python -c 'from importlib.metadata import version; expected={"xarray":"2026.7.0","pyiceberg":"0.12.0","cfgrib":"0.9.15.1","eccodes":"2.48.0","apache-airflow":"3.3.1"}; actual={name:version(name) for name in expected}; assert actual == expected, actual'
```

Do not switch to root and do not add OS packages: `eccodes>=2.43` supplies `eccodeslib` through
PyPI on Linux.

- [ ] **Step 4: Configure Compose to build and use the image**

Replace the Airflow anchor's upstream `image` with:

```yaml
image: flood-lakehouse-airflow:3.3.1-python3.11
build:
  context: .
  dockerfile: infra/airflow/Dockerfile
  args:
    AIRFLOW_VERSION: "3.3.1"
```

Add these required values to the shared Airflow environment without changing `.env`:

```yaml
POLARIS_URI: http://polaris:8181/api/catalog
POLARIS_CATALOG: flood_lakehouse
POLARIS_CLIENT_ID: ${POLARIS_CLIENT_ID:?run make lakehouse-init}
POLARIS_CLIENT_SECRET: ${POLARIS_CLIENT_SECRET:?run make lakehouse-init}
```

- [ ] **Step 5: Add the explicit image-build target**

Add `lakehouse-airflow-build` to `.PHONY` and implement:

```make
lakehouse-airflow-build: lakehouse-init
	docker compose build airflow-api-server
```

Run the focused tests and static Compose validation:

```bash
.venv/bin/pytest tests/unit/test_python_lakehouse_runtime.py \
  tests/unit/test_lakehouse_compose.py -q
docker compose config --quiet
```

Expected: PASS without starting services.

- [ ] **Step 6: Build and inspect the actual image**

Run:

```bash
make lakehouse-airflow-build
docker run --rm flood-lakehouse-airflow:3.3.1-python3.11 \
  python -c 'from importlib.metadata import version; print(version("apache-airflow")); print(version("xarray")); print(version("pyiceberg")); print(version("cfgrib")); print(version("eccodes"))'
docker run --rm flood-lakehouse-airflow:3.3.1-python3.11 python -m pip check
docker run --rm flood-lakehouse-airflow:3.3.1-python3.11 python -m cfgrib selfcheck
```

Expected versions, in order: `3.3.1`, `2026.7.0`, `0.12.0`, `0.9.15.1`, `2.48.0`; pip and
cfgrib checks succeed.

- [ ] **Step 7: Commit Task 2**

```bash
git add infra/airflow/Dockerfile compose.yaml Makefile \
  tests/unit/test_python_lakehouse_runtime.py tests/unit/test_lakehouse_compose.py
git commit -m "build: extend Airflow with lakehouse libraries"
```

---

### Task 3: Read-only runtime and Polaris acceptance smoke

**Files:**
- Create: `infra/scripts/smoke-lakehouse-python.sh`
- Modify: `tests/unit/test_python_lakehouse_runtime.py`
- Modify: `Makefile`
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 1 host runtime, Task 2 Airflow image, running Polaris service, and injected Polaris credentials.
- Produces: `make lakehouse-python-smoke`, which checks both environments and performs a read-only `list_namespaces()` call.

- [ ] **Step 1: Write the failing smoke safety test**

Append:

```python
def test_python_runtime_smoke_is_read_only_and_covers_both_environments() -> None:
    text = (ROOT / "infra/scripts/smoke-lakehouse-python.sh").read_text()
    for evidence in (
        ".venv/bin/python",
        "cfgrib selfcheck",
        "docker compose exec -T airflow-api-server",
        "load_catalog",
        "list_namespaces",
        "POLARIS_CLIENT_ID",
        "POLARIS_CLIENT_SECRET",
    ):
        assert evidence in text
    for forbidden in (
        "create_namespace",
        "create_table",
        "drop_namespace",
        "drop_table",
        "delete",
        "rm -rf",
    ):
        assert forbidden not in text.lower()


def test_makefile_and_readme_document_python_runtime_commands() -> None:
    makefile = (ROOT / "Makefile").read_text()
    readme = (ROOT / "README.md").read_text()
    for command in (
        "lakehouse-python-setup",
        "lakehouse-airflow-build",
        "lakehouse-python-smoke",
    ):
        assert f"{command}:" in makefile
        assert f"make {command}" in readme
```

- [ ] **Step 2: Run the focused test and confirm failure**

Run:

```bash
.venv/bin/pytest tests/unit/test_python_lakehouse_runtime.py -q
```

Expected: FAIL because the smoke script and documentation do not exist.

- [ ] **Step 3: Implement the read-only smoke check**

Create executable `infra/scripts/smoke-lakehouse-python.sh` with `set -eu`, repository-root
resolution, and a `.venv` precondition. It must:

1. run host `python -m pip check`;
2. run host `python -m cfgrib selfcheck`;
3. assert exact host direct-package versions;
4. use `docker compose exec -T airflow-api-server python -` for container checks;
5. assert Airflow remains 3.3.1 and direct-package versions match the manifest;
6. run cfgrib's self-check in the container;
7. call only this PyIceberg operation against injected configuration:

```python
import os
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    os.environ["POLARIS_CATALOG"],
    type="rest",
    uri=os.environ["POLARIS_URI"],
    warehouse=os.environ["POLARIS_CATALOG"],
    credential=(
        f'{os.environ["POLARIS_CLIENT_ID"]}:'
        f'{os.environ["POLARIS_CLIENT_SECRET"]}'
    ),
    scope="PRINCIPAL_ROLE:ALL",
)
catalog.list_namespaces()
print("Airflow Python runtime and Polaris REST catalog are ready.")
```

The script must never enable shell tracing, echo credentials, create namespaces, or create
tables.

- [ ] **Step 4: Wire the smoke target and document commands**

Add `lakehouse-python-smoke` to `.PHONY` and implement:

```make
lakehouse-python-smoke:
	infra/scripts/check-docker-access.sh
	infra/scripts/smoke-lakehouse-python.sh
```

Add a concise README section explaining the three commands, that the smoke requires the stack
to be running, and that it does not ingest or mutate domain data.

- [ ] **Step 5: Run live acceptance checks**

Run:

```bash
make lakehouse-up
make lakehouse-python-smoke
make lakehouse-smoke
```

Expected: both host and container runtime checks succeed; PyIceberg lists namespaces; the
existing foundation smoke remains green. If the stack was stopped before this task, stop it
again with `make lakehouse-down` after verification.

- [ ] **Step 6: Run complete regression verification**

Run:

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests
docker compose config --quiet
git diff --check
```

Expected: all tests PASS, Ruff is clean, Compose validates, and Git reports no whitespace
errors.

- [ ] **Step 7: Commit Task 3**

```bash
git add infra/scripts/smoke-lakehouse-python.sh Makefile README.md \
  tests/unit/test_python_lakehouse_runtime.py
git commit -m "test: verify Python lakehouse runtime"
```

---

## Self-Review Result

- Spec coverage: every runtime, security, version, no-data-mutation, and verification
  requirement maps to Tasks 1–3.
- Placeholder scan: no deferred implementation placeholder is present; Spark is explicitly a
  separate approved phase, not part of this plan.
- Type consistency: the catalog name, environment keys, image tag, Make targets, and exact
  versions are identical across all tasks.
