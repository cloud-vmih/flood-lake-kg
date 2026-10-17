# Static Landing pipeline

Đưa 10 nguồn tĩnh vào MinIO Raw, đăng ký `meta.source_objects`, rồi ghi audit, quality và lineage. Pipeline chỉ acquire, validate, scope và publish object; việc parse row thuộc `static_bronze`.

## Thứ tự đọc

1. `config/pipelines/static_landing.yaml` — danh sách source và cách đóng gói.
2. `airflow.py` — task graph `register → publish → register object → audit → cleanup`.
3. `runtime.py` — ghép config, catalog local, MinIO, Polaris, HTTP fetcher, Meta và quarantine.
4. `pipeline.py` — transaction publish/reuse/register/cleanup và idempotency.
5. `acquisition.py` — resolve, fetch/reuse, validate và chuẩn bị object.
6. `adapters/registry.py` — ánh xạ adapter cố định.
7. `adapters/<provider>.py` — logic riêng từng nguồn; `worldpop.py` giữ helper native-grid và
   `admin_crosswalk.py` giữ mapping địa giới phục vụ bootstrap AOI.
8. `models.py`, `config.py` — DTO và contract cấu hình.
9. `audit.py`, `bundle.py` — audit Meta và đóng gói bundle xác định.

## Entrypoint

- Airflow: `airflow/dags/static_source_landing.py` chỉ import DAG từ `airflow.py`.
- Runtime/CLI: `build_static_landing_service()` trong `runtime.py`.

## Input và output

- Input: `config/sources/*.yaml`, AOI L12, file local hoặc provider remote.
- Output: Raw payload/manifest trong MinIO, `meta.source_objects`, Meta audit và
  `quarantine.rejected_objects` khi payload fatal trước publish.
- Shared dependency: `flashflood_data.storage`, `flashflood_data.catalog`, `flashflood_data.shared.metadata`.

DTO truyền qua ranh giới storage nằm tại `shared/contracts/source_objects.py`; `models.py` chỉ
re-export contract và định nghĩa summary riêng của Landing để code cũ trong cùng pipeline dễ đọc.

DAG ID, task ID, object identity và Raw key được giữ ổn định khi package được refactor.
Checksum/size hoặc adapter validation failure là `DataQualityBlock` không retry; timeout/network
vẫn là operational failure để Airflow retry.
