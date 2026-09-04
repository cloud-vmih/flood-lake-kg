# Lakehouse Foundation Design

**Date:** 2026-09-04  
**Status:** Approved for implementation planning  
**Scope:** Giai đoạn 2A infrastructure only

## 1. Goal

Provide a real, restart-safe Docker Compose foundation for the project lakehouse on the
existing Fedora workstation. The foundation consists of MinIO object storage, Apache Polaris
as an Iceberg REST Catalog, PostgreSQL metadata storage, and Apache Airflow orchestration.

This slice does not ingest weather data, create forecast tables, import stage-1 static data,
or run Spark, Kafka, or Flink.

## 2. Constraints

- Docker Engine 29.6.1 and Docker Compose 5.2.0 are already installed.
- The host has 15 GiB RAM, about 6.3 GiB currently available, and about 15 GiB free disk.
- All persistent project data must remain below `dataset/lakehouse/` and therefore remain
  ignored by Git.
- Existing `dataset/` content and the stage-1 static pipeline are immutable inputs to this
  task; no existing raw, harmonized, derived, catalog, or QA artifact may be moved or deleted.
- Secrets belong only in the ignored root `.env`; committed examples contain blank values.
- The Compose stack is a single-machine research prototype, not a production deployment.
- The stack must start without Redis, Celery workers, Spark, Kafka, or Flink.

## 3. Selected Architecture

```text
Airflow API server :8080
Airflow scheduler (LocalExecutor, parallelism=2)
Airflow DAG processor
                |
                v
PostgreSQL 17.11 <---- Apache Polaris 1.7.0 ----> MinIO S3
  airflow DB             REST :8181              API :9000
  polaris DB             health :8182            console :9001
```

PostgreSQL is one server with isolated `airflow` and `polaris` databases and login roles.
Polaris stores catalog metadata in PostgreSQL and exposes the Iceberg REST API. MinIO stores
objects in two private buckets: `raw` and `warehouse`. Airflow 3 uses three long-running
components required by its basic architecture: API server, scheduler, and DAG processor.
LocalExecutor runs at most two task processes on the scheduler container.

The fixed images are:

- `postgres:17.11-bookworm`
- `minio/minio:RELEASE.2025-09-07T16-13-09Z`
- `minio/mc:RELEASE.2025-08-13T08-35-41Z`
- `apache/polaris:1.7.0`
- `apache/airflow:3.3.1-python3.11`

Mutable tags such as `latest` are prohibited.

## 4. Repository Layout

```text
compose.yaml
infra/
  postgres/
    init-multiple-databases.sh
  polaris/
    bootstrap.sh
  scripts/
    init-lakehouse-env.sh
    smoke-lakehouse.sh
airflow/
  dags/
    .gitkeep
  plugins/
    .gitkeep
dataset/lakehouse/                 # ignored and generated locally
  minio/
  postgres/
  airflow/logs/
```

`compose.yaml`, initialization scripts, smoke checks, and empty Airflow source directories are
tracked. Persistent service state and logs are not tracked.

## 5. Initialization and Secrets

`make lakehouse-init` performs local, idempotent initialization:

1. verify required host commands (`docker`, Docker Compose, `openssl`);
2. create missing directories under `dataset/lakehouse/`;
3. append only missing lakehouse keys to `.env`, preserving existing CDSE credentials;
4. generate random passwords, Fernet key, API secret, and Polaris bootstrap credentials;
5. set `.env` permission to `0600`;
6. validate the fully interpolated Compose configuration without printing secret values.

The script never replaces an existing value and never prints generated credentials. Required
keys are documented in `.env.example` with blank values. Compose uses required-variable syntax
so direct startup fails clearly when initialization has not run.

The PostgreSQL init script creates the two roles and databases only when the PostgreSQL data
directory is first initialized. Changing database passwords in `.env` after that point does not
silently mutate persisted roles; credential rotation is outside this slice.

## 6. Persistent Storage and SELinux

Bind mounts keep state in the project:

- `dataset/lakehouse/postgres` to `/var/lib/postgresql/data`
- `dataset/lakehouse/minio` to `/data`
- `dataset/lakehouse/airflow/logs` to `/opt/airflow/logs`

Writable Fedora bind mounts use the Compose `:Z` SELinux relabel option. Airflow DAGs and
plugins are mounted read-only. `make lakehouse-down` removes containers and the project network
but never removes bind-mounted state. No Make target invokes `docker compose down --volumes` or
deletes `dataset/lakehouse/`.

## 7. Bootstrap Behavior

Bootstrap services are idempotent and finite:

- PostgreSQL initializes the `airflow` and `polaris` databases and roles.
- MinIO client waits for MinIO health, creates private `raw` and `warehouse` buckets if absent,
  and does not delete or make either bucket public.
- Polaris bootstrap waits for the management API, creates the `flood_lakehouse` catalog backed
  by `s3://warehouse/`, and treats an already-existing catalog as success.
- Airflow init waits for PostgreSQL, runs `airflow db migrate`, and creates the local admin user
  only if it does not already exist.

Long-running services declare health checks and use long-form `depends_on` conditions. Setup
services must propagate failures instead of sleeping forever after a failed command.

## 8. Resource Policy

The stack targets an idle upper bound below 4.5 GiB:

| Service | Memory limit |
|---|---:|
| PostgreSQL | 512 MiB |
| MinIO | 512 MiB |
| Polaris | 1 GiB |
| Airflow API server | 768 MiB |
| Airflow scheduler | 1 GiB |
| Airflow DAG processor | 512 MiB |

Airflow parallelism is `2`, example DAGs are disabled, DAG parsing intervals are conservative,
and API DAG caching is bounded. One-shot initialization containers are excluded from the idle
budget. If Fedora's container runtime exhibits the documented Airflow memory issue, the stack
must fail health checks visibly rather than disabling limits.

## 9. Developer Interface

The Makefile exposes:

```text
make lakehouse-init     # initialize directories and missing local secrets
make lakehouse-up       # start and wait for healthy services
make lakehouse-status   # show Compose service state
make lakehouse-smoke    # execute non-destructive acceptance checks
make lakehouse-down     # stop containers; preserve all state
```

The existing stage-1 commands remain unchanged.

The host user must be able to access `/var/run/docker.sock`. Granting Docker-group membership is
a host prerequisite and requires explicit user approval because it is effectively root-level
access. The repository setup does not invoke `sudo` automatically.

## 10. Health and Acceptance Criteria

The implementation is complete when all of the following pass:

1. `docker compose config --quiet` validates after `make lakehouse-init`.
2. `make lakehouse-up` reaches healthy state without Redis, Spark, Kafka, or Flink containers.
3. PostgreSQL answers `pg_isready` and contains separate `airflow` and `polaris` databases.
4. MinIO health succeeds and both private buckets exist.
5. Polaris management health succeeds and its REST API lists `flood_lakehouse`.
6. Airflow API health succeeds, the scheduler and DAG processor jobs are healthy, and example
   DAGs are absent.
7. `make lakehouse-smoke` returns success without writing domain data.
8. After `make lakehouse-down` followed by `make lakehouse-up`, buckets, catalog metadata, and
   Airflow metadata remain present.
9. Repository tests covering Compose structure, secret handling, destructive-command absence,
   and smoke-script behavior pass.

## 11. Failure Handling

- Missing Docker permission produces a concise prerequisite error before startup.
- Missing/blank secrets stop Compose interpolation before containers are created.
- Initialization failures remain visible through a non-zero setup-container exit code.
- A daemon starts only after required dependencies are healthy or initialization completes.
- Existing buckets, databases, users, and catalog objects are reused; startup is idempotent.
- No health or smoke command logs passwords, tokens, database URLs containing passwords, or the
  complete `.env` file.

## 12. Deferred Work

The following require separate approved designs and implementation plans:

- Spark image and Iceberg write/read jobs;
- importing stage-1 static tables into Iceberg;
- Open-Meteo ECMWF IFS HRES current forecast ingestion;
- GSMaP and ERA5-Land ingestion or historical backfill;
- temporal forecast/observation schemas;
- Airflow ingestion DAGs;
- Kafka, Flink, threat engine, impact processing, and Knowledge Graph integration.

## 13. Authoritative References

- Apache Polaris 1.7.0 release and REST catalog documentation:
  <https://polaris.apache.org/downloads/>
- Apache Polaris JDBC deployment example:
  <https://github.com/apache/polaris/blob/main/site/content/guides/jdbc/docker-compose.yml>
- Apache Airflow 3 basic architecture:
  <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html>
- Apache Airflow 3.3.1 release:
  <https://airflow.apache.org/announcements/>
- PostgreSQL official image persistence behavior:
  <https://hub.docker.com/_/postgres>
