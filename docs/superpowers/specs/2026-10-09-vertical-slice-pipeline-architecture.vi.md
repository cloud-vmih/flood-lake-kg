# Thiết kế tổ chức code theo vertical slice

## Mục tiêu

Mỗi pipeline đang chạy có một package duy nhất để người đọc đi từ Airflow entrypoint đến orchestration, adapter/parser và runtime composition mà không phải tìm qua nhiều domain kỹ thuật. Refactor giữ nguyên hành vi, DAG ID, task ID, schema, object key và config value.

## Cấu trúc đích

```text
src/flashflood_data/
  pipelines/
    static_landing/
      README.md
      airflow.py
      runtime.py
      config.py
      models.py
      pipeline.py
      acquisition.py
      bundle.py
      audit.py
      adapters/
    static_bronze/
      README.md
      airflow.py
      runtime.py
      config.py
      pipeline.py
      parsers.py
      osm.py
      quality.py
    weather_ingest/
      README.md
      airflow.py
      runtime.py
      config.py
      models.py
      planner.py
      landing.py
      subset.py
      bronze.py
      parsers.py
      grids.py
      lifecycle.py
      fetch_audit.py
      watermarks.py
      providers/
  shared/
    batching.py
    metadata/
    quality.py
```

`storage/`, `catalog/` và `core/` tiếp tục là hạ tầng dùng chung có ranh giới rõ. `static/` chỉ giữ workflow file-first cũ, harmonization, feature, mapping và QA; adapter acquisition chuyển vào pipeline static Landing.

Config runtime chuyển về:

```text
config/pipelines/static_landing.yaml
config/pipelines/static_bronze.yaml
config/pipelines/static_bronze_osm.yaml
config/pipelines/weather/*.yaml
config/shared/metadata.yaml
```

## Public API

- `flashflood_data.pipelines.static_landing.airflow.static_source_landing`
- `flashflood_data.pipelines.static_landing.runtime.build_static_landing_service`
- `flashflood_data.pipelines.static_bronze.airflow.static_source_to_bronze`
- `flashflood_data.pipelines.static_bronze.runtime.build_bronze_service`
- `flashflood_data.pipelines.weather_ingest.airflow.build_project_weather_dag`
- `flashflood_data.pipelines.weather_ingest.runtime.build_weather_runtime`
- `flashflood_data.shared.metadata.factory.build_meta_recorder`

Mỗi file trong `airflow/dags/` chỉ import một DAG hoặc gọi một DAG builder từ public API trên.

## Quy tắc dependency

1. DAG wrapper không chứa task implementation hoặc load config nghiệp vụ.
2. `pipelines/<name>` có thể import `core`, `catalog`, `storage`, `shared` và domain primitive cần thiết.
3. Pipeline không import pipeline khác.
4. `shared` không import `pipelines`.
5. CLI gọi runtime public API; pipeline không import CLI.
6. Adapter registry có allowlist cố định và load adapter trong cùng package static Landing.
7. Không giữ compatibility module ở namespace `orchestration` sau khi toàn bộ caller nội bộ đã chuyển.

## Tương thích vận hành

- Giữ nguyên sáu DAG ID hiện tại.
- Giữ nguyên task graph/task ID bằng cách di chuyển nguyên implementation vào `airflow.py`.
- Giữ nguyên nội dung YAML khi đổi đường dẫn.
- Không thay đổi table name, schema, parser version, source ID, object ID hoặc MinIO key.
- Không cần ingest lại dữ liệu.
- Airflow image cần rebuild vì Python module path thay đổi.

## Tài liệu đọc code

Mỗi package có `README.md` ghi nhiệm vụ, entrypoint, thứ tự đọc, input/output, config và dependency dùng chung. README gốc chỉ mô tả bản đồ repository và liên kết tới README của từng pipeline.

## Tiêu chí nghiệm thu

- Không còn import `flashflood_data.orchestration` trong code, test hoặc DAG hiện hành.
- Mỗi DAG wrapper tối đa 12 dòng và chỉ phụ thuộc package pipeline tương ứng.
- CLI và DAG import thành công.
- Test hiện có chạy qua sau khi cập nhật import path.
- Tài liệu/plan hiện hành không hướng người đọc tới namespace cũ.
