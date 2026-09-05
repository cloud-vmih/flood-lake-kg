# Python Lakehouse Runtime Design

**Date:** 2026-09-05  
**Status:** Approved for implementation planning  
**Scope:** Phase 2B Python ingest runtime only

## 1. Goal

Install one consistent set of Python libraries for forecast decoding and Iceberg access in
both the project's `.venv` and a custom Apache Airflow image. The runtime must be ready to
decode GRIB with Xarray/cfgrib and communicate with the existing Apache Polaris REST catalog.

This slice prepares the execution environment only. It does not download forecast data,
create Iceberg domain tables, import static data, add DAGs, or install Spark.

## 2. Constraints

- Python remains `>=3.11,<3.13`; both current environments use Python 3.11.
- Apache Airflow remains exactly `3.3.1` on Python 3.11.
- The existing static pipeline dependency lock and behavior must remain unchanged.
- Existing files and data under `dataset/` are immutable inputs; no raw or derived asset may
  be moved, rewritten, or deleted.
- Permanent Airflow dependencies must be baked into an image, never installed at container
  startup through `_PIP_ADDITIONAL_REQUIREMENTS`.
- The host and Airflow use the same exact top-level lakehouse package versions.
- Airflow's own constraints remain authoritative for its transitive dependencies.
- This slice must not add Spark, Kafka, Flink, forecast downloads, DAGs, or Iceberg tables.

## 3. Selected Architecture

The repository owns one small, exact direct-dependency manifest:

```text
requirements/lakehouse.txt
  xarray==2026.7.0
  pyiceberg[pyarrow]==0.12.0
  cfgrib==0.9.15.1
  eccodes==2.48.0
        |                         |
        v                         v
project .venv              custom Airflow image
existing requirements.lock  Airflow 3.3.1 constraints
```

The manifest pins the four capabilities shared between environments. The project `.venv`
keeps its existing `requirements.lock` as the base and installs the manifest additively.
The Airflow Dockerfile extends `apache/airflow:3.3.1-python3.11`, installs the same manifest as
the `airflow` user, explicitly includes `apache-airflow==3.3.1`, and applies the constraint file
already shipped in the base image. Both installations finish with `pip check`.

This arrangement avoids forcing the full static/geospatial lock into Airflow while still
keeping all directly selected forecast and Iceberg versions identical.

## 4. Dependency Responsibilities

| Package | Exact version | Responsibility |
|---|---:|---|
| Xarray | 2026.7.0 | Labeled multidimensional forecast arrays |
| PyIceberg with PyArrow FileIO | 0.12.0 | JVM-free Iceberg REST client and Parquet/FileIO support |
| cfgrib | 0.9.15.1 | Xarray GRIB engine |
| eccodes | 2.48.0 | GRIB decoder Python bindings and bundled Linux binary library |

PyArrow is already present in the host static environment. The PyIceberg `pyarrow` extra makes
the requirement explicit for the Airflow image. `s3fs` is not added because PyIceberg's
PyArrow FileIO can use the MinIO-compatible S3 endpoint without introducing a second S3
filesystem stack.

## 5. Repository Layout

```text
requirements/
  lakehouse.txt                    # shared exact direct dependencies
infra/airflow/
  Dockerfile                       # immutable extended Airflow image
infra/scripts/
  setup-lakehouse-python.sh        # idempotent host installation
  smoke-lakehouse-python.sh        # host/container runtime checks
compose.yaml                       # build and use local Airflow image
Makefile                           # setup, build, and smoke entry points
```

No runtime state is added outside the existing ignored `dataset/lakehouse/` tree.

## 6. Airflow Image and Runtime Configuration

All four Airflow services inherit one Compose anchor with:

- image name `flood-lakehouse-airflow:3.3.1-python3.11`;
- build context at the repository root;
- Dockerfile `infra/airflow/Dockerfile`;
- build argument `AIRFLOW_VERSION=3.3.1`.

The Dockerfile copies only the lakehouse manifest before installing packages, preserving the
Docker layer cache when application code changes. It remains on the image's non-root `airflow`
user and runs `pip check` during the build.

Airflow receives the Polaris bootstrap client ID and secret through existing required Compose
environment interpolation. They are used only for the local REST-catalog connectivity smoke
check and later DAG configuration; their values remain solely in ignored `.env`.

## 7. Developer Interface

The Makefile adds:

```text
make lakehouse-python-setup   # install the shared manifest into .venv and validate it
make lakehouse-airflow-build  # build the immutable custom Airflow image
make lakehouse-python-smoke   # check host GRIB runtime and Airflow/Polaris access
```

`make lakehouse-up` may build the local image automatically when it is absent. Existing
lakehouse and stage-1 commands retain their names and behavior.

## 8. Verification and Acceptance Criteria

The slice is complete only when:

1. The exact direct dependency manifest contains the four approved versions and no Airflow
   package.
2. `.venv` imports Xarray, PyIceberg, cfgrib, and eccodes successfully.
3. `.venv/bin/python -m cfgrib selfcheck` finds a usable ecCodes library.
4. `.venv/bin/python -m pip check` reports no broken requirements.
5. The custom image builds from the pinned Airflow base and retains Airflow 3.3.1.
6. The custom image imports the same four direct package versions and passes `pip check` and
   cfgrib self-check.
7. With the existing stack running, PyIceberg in an Airflow container authenticates to Polaris
   and lists namespaces from catalog `flood_lakehouse` without creating data.
8. `docker compose config --quiet`, focused infrastructure tests, the full test suite, and Ruff
   all pass.

## 9. Failure Handling

- A missing `.venv` fails with a concise instruction to run `make setup`.
- Dependency resolution or binary-library failure returns non-zero immediately.
- A custom image that changes the Airflow version or has inconsistent packages fails its build.
- Polaris authentication/connectivity failure is reported without printing credentials.
- Smoke checks are read-only and do not create namespaces, tables, metadata, or objects.

## 10. Deferred Phase

Spark plus the JVM Iceberg runtime is the next independent slice. It will receive a separate
design and implementation plan after this Python runtime passes acceptance. That phase will
pin the Spark image and Iceberg runtime JAR versions, integrate them with Polaris/MinIO, and
remain resource-bounded for the current workstation.

## 11. Authoritative References

- PyIceberg installation and REST/S3 configuration: <https://py.iceberg.apache.org/>
- Xarray installation and optional IO dependencies:
  <https://docs.xarray.dev/en/stable/installing.html>
- cfgrib Xarray engine and self-check: <https://github.com/ecmwf/cfgrib>
- ECMWF ecCodes Python installation: <https://github.com/ecmwf/eccodes-python>
- Apache Airflow custom image guidance:
  <https://airflow.apache.org/docs/docker-stack/build.html>
