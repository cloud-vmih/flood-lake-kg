# Kế hoạch triển khai Spark foundation cho Bronze → Silver

**Mục tiêu:** Cung cấp một runtime Spark/Sedona dùng chung để Airflow có thể submit, theo dõi và
reconcile mọi pipeline Silver; Spark đọc snapshot Bronze qua Polaris/MinIO, chạy DQ/quarantine và
commit Iceberg idempotent.

**Kiến trúc:** Airflow chỉ lập plan và submit application. Spark application đọc snapshot đã pin,
transform, đọc Parquet staging từ Python specialized job khi có, rồi publish Silver/quarantine và
trả output snapshot manifest. Meta finalize chỉ đánh dấu success sau khi đủ output.

**Tài liệu quyết định:**
[docs/spark_bronze_to_silver_architecture.md](../../spark_bronze_to_silver_architecture.md)

**Trạng thái 17/10/2026:** compatibility matrix, image Compose, session/models, submit contract,
snapshot discovery/pinning, staging manifest, Spark DQ/quarantine projection, single-table publisher,
Meta audit và dependency factory đã hoàn tất ở shared foundation. Fake domain đã chạy qua
`discover → stage → publish → audit → skip`. Normalize basin đã chuyển sang Spark/Sedona. Phần còn
lại là nối Airflow với runtime submit và hoàn thiện vertical slice basin bằng `spark_job.py`, topology,
DAG cùng integration test trên Polaris/MinIO thật.

## Ràng buộc

- Không tạo `SparkSession` trong Airflow scheduler hoặc DAG parse process.
- Không truyền DataFrame/raster/full rows qua XCom.
- Version Spark, Scala, Iceberg runtime, Sedona, Hadoop AWS và Java phải được pin và test cùng nhau.
- Mọi Spark read nghiệp vụ nhận snapshot ID; không đọc lại `current` giữa run.
- Mapped Python job chỉ ghi staging; không commit bảng Silver.
- Mỗi bảng chỉ có một Spark commit application tại một thời điểm.
- Raw/Bronze hiện có không đổi engine và không phải ingest lại khi thêm Spark Silver.

## Cấu trúc file mục tiêu

```text
infra/docker/spark/
  Dockerfile

config/spark/
  local.yaml

src/flashflood_data/shared/spark/
  __init__.py
  models.py
  session.py
  submit.py
  snapshots.py
  staging.py
  quality.py
  publish.py
  audit.py
  cli.py
  runtime.py

tests/unit/shared/spark/
  test_models.py
  test_submit.py
  test_snapshots.py
  test_staging.py
  test_quality.py
  test_publish.py
  test_audit.py
  test_lifecycle.py
  test_runtime.py

tests/integration/spark/
  test_catalog_roundtrip.py
  test_snapshot_read.py
  test_idempotent_merge.py
  test_quarantine_publish.py
```

## Công việc 1 — Chốt compatibility matrix và runtime contract

**File:**

- Tạo `config/spark/local.yaml`.
- Tạo `src/flashflood_data/shared/spark/models.py`.
- Tạo `tests/unit/shared/spark/test_models.py`.
- Cập nhật dependency lock/image manifest.

Config phải chứa version/runtime profile, driver/executor resource, shuffle partitions, catalog name,
warehouse, endpoint env-key và application timeout. Secret chỉ tham chiếu tên environment variable.

Model tối thiểu:

```python
SparkApplicationRequest
PinnedSnapshot
BuildSignature
StagedDataset
SparkApplicationResult
PublishedSnapshot
```

**Nghiệm thu:** config sai version/resource bị từ chối; serialized request không chứa credential.

## Công việc 2 — Hoàn thiện image và Compose profile Spark

**File:**

- Tạo/sửa `infra/docker/spark/Dockerfile`; session options được cấu hình tập trung bằng
  `shared/spark/session.py` thay cho file `spark-defaults.conf` riêng.
- Sửa `compose.yaml`.
- Sửa `Makefile` cho `spark-build`, `spark-up`, `spark-status`, `spark-smoke`, `spark-down`.
- Tạo/sửa `tools/smoke/spark_iceberg.sh`.

Image phải có đúng Iceberg Spark runtime, Sedona shaded artifact, GeoTools wrapper khi cần và S3
FileIO dependency. Spark dùng network nội bộ đến `polaris:8181` và `minio:9000`.

**Nghiệm thu:** trên Linux và WSL2, smoke tạo bảng tạm, append, đọc bằng Spark và Trino, rồi drop;
base stack vẫn chạy khi Spark dừng.

## Công việc 3 — Tạo Spark/Sedona session dùng chung

**File:**

- Tạo `shared/spark/session.py`.
- Tạo `tests/unit/shared/spark/test_session.py`.
- Tạo `tests/integration/spark/test_catalog_roundtrip.py`.

`build_spark_session(config)` chịu trách nhiệm:

- Iceberg REST catalog qua Polaris;
- MinIO endpoint/path-style/credential provider;
- Iceberg SQL extensions;
- Sedona SQL extensions/registrator;
- UTC timezone, ANSI mode và adaptive query execution;
- application tags gồm pipeline/run/build signature.

**Nghiệm thu:** domain job không tự cấu hình catalog/S3 lần thứ hai.

## Công việc 4 — Airflow submit và application lifecycle

**File:**

- Tạo `shared/spark/submit.py`, `shared/spark/cli.py`.
- Tạo `tests/unit/shared/spark/test_submit.py`.
- Tạo một DAG smoke riêng hoặc task factory test-only.

`submit.py` tạo command/request xác định, theo dõi application ID, exit code và output manifest.
Airflow retry dùng cùng `pipeline_run_id`/`build_signature`; không tạo identity mới vì attempt mới.

**Nghiệm thu:** submit thành công, timeout, driver failure và retry đều có structured outcome; XCom chỉ
chứa ID/URI/count/status.

## Công việc 5 — Snapshot pinning và incremental build plan

**File:**

- Tạo `shared/spark/snapshots.py`.
- Tạo `tests/unit/shared/spark/test_snapshots.py`.
- Tạo `tests/integration/spark/test_snapshot_read.py`.

API phải:

- resolve table identifier đã allowlist;
- lấy và pin snapshot ID;
- đọc snapshot cụ thể bằng Spark;
- tạo `build_signature` từ snapshot/config/policy/algorithm/contract version;
- trả `build`, `skip` hoặc `repair` từ Meta/output hiện có.

**Nghiệm thu:** dữ liệu commit sau lúc pin không xuất hiện trong run đang chạy; cùng signature không
tạo build mới.

## Công việc 6 — Staging contract và specialized Python bridge

**File:**

- Tạo `shared/spark/staging.py`.
- Tạo `tests/unit/shared/spark/test_staging.py`.

Layout:

```text
s3://staging/silver/<pipeline>/<run>/<batch>/
  data.parquet
  lineage.parquet
  quality.parquet
  manifest.json
  _SUCCESS
```

Manifest chứa schema hash, row count, checksum, build signature, batch ID và input refs. Spark bỏ qua
batch thiếu `_SUCCESS`; cùng batch ID nhưng manifest khác là conflict.

**Nghiệm thu:** retry reuse batch hoàn chỉnh, repair batch thiếu, cleanup chỉ sau finalize success.

## Công việc 7 — DQ, quarantine và Iceberg publish

**File:**

- Tạo `shared/spark/quality.py`, `shared/spark/publish.py`.
- Tạo `tests/unit/shared/spark/test_quality.py`, `test_publish.py`.
- Tạo `tests/integration/spark/test_idempotent_merge.py`, `test_quarantine_publish.py`.

DQ API chia DataFrame thành valid/reject/warning, tạo `rejection_id` xác định và quality result.
Publisher hỗ trợ:

- append cho immutable version;
- `MERGE INTO` theo business key/revision policy;
- overwrite-by-filter có filter bắt buộc;
- schema/key/null validation trước commit;
- trả snapshot ID/row count/file count sau commit.

**Nghiệm thu:** row reject vào quarantine nhưng row valid vẫn publish; retry không nhân row/rejection;
fatal check không tạo output snapshot.

## Công việc 8 — Audit, lineage và vertical-slice chứng minh

**File:**

- Tạo `shared/spark/audit.py` và test.
- Chuyển plan basin topology sang `spark_job.py` để làm vertical slice đầu tiên.
- Cập nhật README/roadmap/runbook.

Audit ghi input/output snapshot refs, quality và lineage sau commit; chỉ finalize success khi output
manifest khớp bảng/snapshot thật. Basin topology phải chứng minh:

```text
Airflow -> spark-submit -> Bronze pinned snapshot
  -> Sedona normalize/join -> DQ/quarantine
  -> Silver Iceberg -> Meta audit -> Trino query
```

**Nghiệm thu:** manual rerun là no-op, snapshot input mới chỉ rebuild domain, failure giữa hai output
có thể repair và không công bố run hoàn tất.

## Thứ tự checkpoint

1. Dừng sau compatibility matrix + smoke Compose.
2. Dừng sau session + snapshot read.
3. Dừng sau submit + staging bridge.
4. Dừng sau DQ/MERGE/quarantine.
5. Dừng sau basin topology end-to-end trước khi triển khai weather/static/river/event/exposure.

## Lệnh kiểm tra tối thiểu

```bash
pytest tests/unit/shared/spark -q
pytest tests/integration/spark -q
docker compose config --quiet
make spark-smoke
ruff check src tests
```

Không bắt đầu các Spark job domain nếu smoke Polaris/MinIO, pinned snapshot read, idempotent MERGE và
quarantine integration chưa đạt.
