# Spark Iceberg Runtime Design

**Date:** 2026-09-05  
**Status:** Approved for implementation planning  
**Scope:** Phase 2C Spark and Iceberg execution runtime only

## 1. Goal

Add a real single-node Spark standalone cluster to the Docker Compose lakehouse. The cluster
must use the existing Polaris REST catalog and MinIO warehouse through a pinned Apache Iceberg
runtime, while staying optional so the base lakehouse does not pay Spark's memory cost when no
Spark job is running.

This slice validates infrastructure with a temporary round-trip table. It does not ingest
forecast or static project data, create permanent domain schemas, or add Airflow DAGs.

## 2. Constraints

- Use one Spark master and one Spark worker, not Spark local mode.
- Spark runs in Docker only; do not install Java, Scala, Spark, or PySpark on the host.
- Keep the existing base lakehouse and Airflow services unchanged when Spark is stopped.
- Spark must be opt-in through a Compose profile and separate Make targets.
- Reuse the existing private Compose network, Polaris catalog `flood_lakehouse`, and MinIO
  bucket `warehouse`.
- Pin Spark, Java, Scala, Iceberg, image tags, and Maven artifacts; do not use `latest`.
- Bake Iceberg artifacts into the image; do not download Maven packages when a job starts.
- Never bake `.env` values or credentials into an image or tracked file.
- Do not modify, move, copy, or delete existing files under `dataset/`.
- The smoke job may delete only the uniquely named namespace/table that it creates itself.

## 3. Selected Versions

| Component | Version |
|---|---:|
| Apache Spark | 4.1.3 |
| Scala binary runtime | 2.13 |
| Java runtime | 17 |
| Apache Iceberg | 1.11.0 |
| Spark base image | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` |
| Spark Iceberg artifact | `iceberg-spark-runtime-4.1_2.13:1.11.0` |
| Iceberg S3 artifact | `iceberg-aws-bundle:1.11.0` |

Iceberg 1.11.0 publishes a dedicated Spark 4.1/Scala 2.13 runtime. Java 17 is selected instead
of Java 21 to keep the runtime conservative while using an official Spark image variant.

## 4. Architecture

```text
one-shot spark-submit driver
          |
          v
Spark master :7077 / UI :8081 host
          |
          v
Spark worker (2 cores, 2 GiB executor memory) / UI :8082 host
          |
          +----------> Polaris REST :8181
          |              catalog: flood_lakehouse
          |
          +----------> MinIO S3 :9000
                         bucket: warehouse
```

The master and worker share one custom Spark image. A third Compose service based on the same
image is a one-shot `spark-submit` driver rather than a long-running daemon. Job files are
mounted read-only from `spark/jobs/` into all Spark containers.

The services use Compose profile `spark`. `make lakehouse-up` continues to start only the base
lakehouse. `make spark-up` ensures the base services are healthy, then starts the master and
worker. `make spark-down` stops/removes only Spark containers and preserves the base services
and all object/catalog data.

## 5. Custom Image

`infra/spark/Dockerfile` extends the exact official Spark image and downloads two exact Maven
artifacts during build:

- `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`;
- `iceberg-aws-bundle-1.11.0.jar`.

Both files are placed in `/opt/spark/jars/`. Build arguments expose the pinned versions only;
credentials never enter the build. The image build verifies both JARs are present and that
`spark-submit --version` reports Spark 4.1.3. Image name:

```text
flood-lakehouse-spark:4.1.3-iceberg1.11.0
```

## 6. Spark and Catalog Configuration

Each job creates its `SparkSession` with a shared helper configured as follows:

```text
spark.master                                      spark://spark-master:7077
spark.sql.extensions                              IcebergSparkSessionExtensions
spark.sql.catalog.flood_lakehouse                 SparkCatalog
spark.sql.catalog.flood_lakehouse.type            rest
spark.sql.catalog.flood_lakehouse.uri             http://polaris:8181/api/catalog
spark.sql.catalog.flood_lakehouse.warehouse       flood_lakehouse
spark.sql.catalog.flood_lakehouse.credential      <client-id>:<client-secret>
spark.sql.catalog.flood_lakehouse.scope           PRINCIPAL_ROLE:ALL
spark.sql.catalog.flood_lakehouse.io-impl          S3FileIO
spark.sql.catalog.flood_lakehouse.s3.endpoint      http://minio:9000
spark.sql.catalog.flood_lakehouse.s3.path-style-access true
spark.sql.catalog.flood_lakehouse.client.region   us-east-1
```

Polaris OAuth credentials and MinIO S3 credentials enter the one-shot driver through required
Compose environment interpolation. The worker receives only the MinIO credentials needed by
executor-side FileIO. Secrets are not placed in command-line arguments, logs, or config files.

## 7. Resources and Ports

| Service | Limit | Runtime allocation |
|---|---:|---:|
| Spark master | 768 MiB | control plane only |
| Spark worker | 2560 MiB | 2 cores, 2 GiB worker memory |
| one-shot driver | 1536 MiB | 1 GiB driver memory |

Spark adds about 3.25 GiB while idle and about 4.75 GiB during submission, only when the profile
is active. Host ports bind to localhost:

- master RPC: `127.0.0.1:7077`;
- master UI: `127.0.0.1:8081`;
- worker UI: `127.0.0.1:8082`.

No Spark state is persisted in `dataset/`; Iceberg table data remains in MinIO and catalog
metadata remains in Polaris/PostgreSQL.

## 8. Developer Interface

```text
make spark-build    # build the pinned Spark/Iceberg image
make spark-up       # ensure lakehouse is up, then start master and worker
make spark-status   # show Spark service state
make spark-smoke    # submit temporary Iceberg write/read/cleanup job
make spark-down     # remove only Spark master/worker containers
```

Future Airflow work will submit scripts from `spark/jobs/` through the same one-shot driver
contract. Adding the Airflow Spark provider or a DAG is explicitly deferred.

## 9. Round-Trip Smoke Contract

The smoke job:

1. generates a namespace `smoke_<uuid-hex>`;
2. creates `flood_lakehouse.<namespace>.roundtrip` as an Iceberg table;
3. inserts three literal rows;
4. reads and verifies the exact sorted rows;
5. drops only that table and namespace in `finally` cleanup;
6. exits non-zero on any build, catalog, S3, write, read, or cleanup failure.

The namespace identifier is generated inside the job and is never accepted from a user or
environment variable. Cleanup checks the exact identifier before issuing DROP statements.
The job must never reference the `raw` bucket or any existing project namespace.

## 10. Acceptance Criteria

1. The custom image builds with Spark 4.1.3 and both Iceberg 1.11.0 JARs present.
2. `make lakehouse-up` still starts no Spark container.
3. `make spark-up` reaches a healthy master and one registered healthy worker.
4. Spark ports bind only to `127.0.0.1` and memory limits match this design.
5. `make spark-smoke` authenticates through Polaris, writes Parquet/Iceberg objects to MinIO,
   reads the exact three rows through Spark SQL, and removes its table and namespace.
6. Re-running `make spark-smoke` succeeds with a new unique namespace.
7. `make spark-down` leaves PostgreSQL, MinIO, Polaris, and Airflow running and healthy.
8. The full project test suite, Ruff, Compose validation, and whitespace checks pass.

## 11. Failure Handling

- Missing Docker permission fails before changing service state.
- Missing or blank secrets fail Compose interpolation.
- Image/JAR download or version mismatch fails the image build.
- The worker starts only after the master is healthy.
- The smoke job reports its generated namespace and cleanup result without printing secrets.
- A failed cleanup returns non-zero and reports the exact smoke namespace for manual inspection.
- Spark shutdown never calls `docker compose down`, deletes volumes, or touches `dataset/`.

## 12. Deferred Work

- Permanent Iceberg namespaces and schemas for static, forecast, observation, feature, and
  prediction tables;
- copying stage-1 static outputs into Iceberg;
- forecast ingestion and historical backfill;
- Airflow Spark connection/provider and DAGs;
- Spark autoscaling, multiple workers, history server, and event-log persistence.

## 13. Authoritative References

- Apache Spark 4.1.3 official image metadata:
  <https://github.com/apache/spark-docker/blob/master/versions.json>
- Apache Spark standalone deployment:
  <https://spark.apache.org/docs/latest/spark-standalone.html>
- Apache Iceberg 1.11 Spark getting started:
  <https://iceberg.apache.org/docs/latest/spark-getting-started/>
- Apache Iceberg Spark REST catalog configuration:
  <https://iceberg.apache.org/docs/latest/spark-configuration/>
- Apache Iceberg S3FileIO guidance:
  <https://iceberg.apache.org/docs/latest/aws/>
