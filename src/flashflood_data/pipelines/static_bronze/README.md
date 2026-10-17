# Static Raw-to-Bronze pipeline

Discover Raw object đã đăng ký, tải và kiểm checksum, parse theo source, chạy quality gate, rồi replace đúng lát dữ liệu của batch trong Iceberg Bronze. Object fatal được ghi vào `quarantine.rejected_objects`; feature lỗi được tách vào `quarantine.rejected_rows`; feature sửa được vẫn publish với `quality_status=warning`.

## Thứ tự đọc

1. `config/pipelines/static_bronze.yaml` — source, table, parser version và batch size.
2. `config/pipelines/static_bronze_osm.yaml` — allowlist OSM.
3. `airflow.py` — discover batch và mapped parse task.
4. `runtime.py` — ghép inventory, object store, Iceberg writer, Meta recorder, quality policy và quarantine repository.
5. `pipeline.py` — discovery, parse, commit, audit, repair và reconcile.
6. `parsers.py` — parser vector/raster static source-faithful; event parser thuộc
   `pipelines/flood_events`.
7. `osm.py` — stream/filter OSM PBF.
8. `quality.py`, `config.py` — pre-commit checks và config contract.

## Entrypoint

- Airflow: `airflow/dags/static_source_to_bronze.py` chỉ import DAG từ `airflow.py`.
- Runtime/CLI: `build_bronze_service()` trong `runtime.py`.

## Input và output

- Input: `meta.source_objects` và Raw object trong MinIO.
- Output: các bảng `bronze.*`, `quarantine.rejected_objects`,
  `quarantine.rejected_rows` và Meta run/quality/snapshot/lineage.
- Shared dependency: `shared.batching`, `shared.metadata`, `shared.quality`, `storage`.

Rerun cùng `object_id` và parser version không tạo row trùng. Đổi parser version chỉ parse lại Raw, không tải provider.
Lỗi dữ liệu tất định được đổi thành `AirflowFailException` để không retry vô ích; lỗi I/O hoặc
Iceberg vẫn được task retry theo cấu hình Airflow.
