# Kế hoạch triển khai Silver Facilities

> **Dành cho người triển khai:** dùng `executing-plans` để thực hiện từng công việc
> và dừng ở checkpoint review nếu người dùng yêu cầu đọc từng file. Checkbox chưa
> đánh dấu là chưa triển khai; không tự commit/merge khi chưa được yêu cầu.

**Mục tiêu:** chuẩn hóa facility thiết yếu từ OSM Bronze và gán quan hệ basin L12.

**Kiến trúc:** một DAG `silver_facilities`, package `silver_facilities`; Airflow
pin input/submit Spark, Spark/Sedona normalize/overlay/DQ/publish hai bảng, Meta
chốt bundle cuối. Không chờ River Network và không gọi pipeline Static Feature.

**Công nghệ:** Airflow, Spark/Sedona, Iceberg, Pydantic, framework quality/audit hiện có.

**Thiết kế:** [Exposure tách domain](../specs/2026-11-08-silver-exposure-split-design.vi.md).

## Ràng buộc

- Input: `bronze.osm_feature_raw`, `meta.source_objects` để chọn revision và
  `silver.dim_basin`; pin mọi snapshot thật sự tham gia lựa chọn/transform.
- Chọn feature giao `exposure_aoi.geoparquet`; giữ geometry OSM gốc hợp lệ.
- Output: chỉ `silver.dim_facility`, `silver.facility_basin`; không road/population/crossing.
- `capacity`, `criticality_level`, khoảng cách tới stream/outlet giữ null nếu
  không có nguồn hoặc phương pháp cấu hình. Thiếu capacity là warning, không suy từ tên.
- Facility ID từ `(osm_type, osm_id, canonical_facility_type)`, không từ tên/vị trí
  row. `facility_version` từ revision nguồn, mapping/algorithm, scope và policy.
- Chỉ mapping tag đã được Bronze policy giữ. Node và polygon cùng tên chưa đủ
  bằng chứng để tự gộp; giữ provenance và cảnh báo candidate duplicate.
- Single writer/table, staging và manifest theo build; không một task/facility.

## Flow và thứ tự đọc file

```text
airflow/dags/silver_facilities.py
 → silver_facilities/airflow.py → runtime.py → pipeline.py (build/repair/skip)
 → shared Spark gateway → spark_job.py
 → reader.py → transform.py → quality.py → shared publisher → manifest
 → runtime.py → shared audit → Meta
```

Các đường dẫn tương đối trong bảng dưới thuộc
`src/flashflood_data/pipelines/silver_facilities/` và đều là **file dự kiến tạo**.

| Thứ tự | File | Chức năng | Được gọi bởi / dùng |
| ---: | --- | --- | --- |
| 1 | `config.py` | `FacilitiesConfig`, `load_facilities_config(path)`; source/tag mapping, CRS, AOI, version, DQ | Runtime và Spark job; YAML |
| 2 | `models.py` | `FacilityRow`, `FacilityBasinRow`, `FacilityFrames(facilities, relations)` | Transform, quality, tests |
| 3 | `reader.py` | `read_facility_inputs(spark, request, config)` trả pinned OSM/basin frames | Spark job; shared snapshot reader |
| 4 | `transform.py` | `build_facilities(osm, config)` và `link_facilities(facilities, basins, config)` | Spark job; Spark SQL/Sedona |
| 5 | `quality.py` | `facility_rules(config)`, `facility_basin_rules(config)` | Spark job; shared SparkQualityRule |
| 6 | `pipeline.py` | Signature, output ownership, build/repair/skip | Runtime; shared snapshot planning |
| 7 | `spark_job.py` | `run(request)` thực hiện transform, DQ, publish, manifest | Shared Spark CLI |
| 8 | `runtime.py` | `build_facilities_runtime(root=None)`; prepare registry, submit, đọc manifest, audit | Airflow callable |
| 9 | `airflow.py` | Factory DAG có hai task plan/execute, pool writer | DAG entrypoint |
| 10 | `__init__.py`, `README.md` | Package không import PySpark eager; hướng dẫn đọc/chạy/test | Người đọc và import DAG |

Tạo thêm `config/pipelines/silver/facilities.yaml`, `config/quality/silver_facilities.yaml`,
`airflow/dags/silver_facilities.py`. Chỉ sửa registry/schema cho hai bảng sở hữu.

## Công việc 1 — Config/model/contract

**File:** tạo `config.py`, `models.py`, `__init__.py`, hai YAML trên;
sửa `src/flashflood_data/storage/iceberg_schemas.py`, `config/shared/metadata.yaml`;
tạo `tests/unit/pipelines/silver_facilities/test_config.py` và
`tests/contract/storage/test_silver_facilities_schema.py`.

**Giao diện:** config chứa mapping tag → canonical type, allowed types, metric CRS,
AOI path/hash, quality/algorithm/contract version. Model đúng khóa trong `data.md`.

- [ ] Viết test contract chưa có schema và config từ chối mapping rỗng/CRS thiếu.

```python
def test_facility_schema_has_source_and_version():
    from flashflood_data.storage.iceberg_schemas import table_schema
    names = set(table_schema(("silver", "dim_facility")).names)
    assert {"facility_id", "facility_version", "source_object_id", "geometry_wkb"} <= names
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_facilities/test_config.py tests/contract/storage/test_silver_facilities_schema.py -q`; xác nhận fail do contract thiếu.
- [ ] Viết config/model/schema tối thiểu; fixture ghi rõ y tế, shelter, cứu hộ,
  trường học, điện/nước/liên lạc theo allowlist thực tế, không mặc định mọi building là facility.
- [ ] Chạy lại cùng lệnh, yêu cầu pass. Checkpoint review contract trước transform.

## Công việc 2 — Reader và normalize/overlay

**File:** tạo `reader.py`, `transform.py`,
`tests/unit/pipelines/silver_facilities/test_transform.py`,
`tests/integration/silver/test_facilities_transform.py`.

**Giao diện:** `build_facilities` trả Spark frame schema `FacilityRow`;
`link_facilities` trả `FacilityBasinRow`. Reader chọn revision trước spatial filter
để không vô tình dùng old revision chỉ vì geometry old còn giao AOI.

- [ ] Viết fixture Spark hai basin chung biên, hospital node, shelter polygon,
  shop ngoài mapping, geometry lỗi và facility không capacity. Kiểm tra:

```python
def test_missing_capacity_is_not_invented(facility_output):
    row = facility_output.where("facility_type = 'hospital'").first()
    assert row.capacity is None
    assert row.source_object_id is not None
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_facilities/test_transform.py tests/integration/silver/test_facilities_transform.py -q`; xác nhận fail trước khi viết transform.
- [ ] Parse tags một lần, giữ geometry hợp lệ, normalize CRS; bbox prune rồi
  `ST_Intersects`. Point trên biên có thể có nhiều relation, khóa đầy đủ không trùng;
  polygon giao nhiều basin giữ từng relation. Không ép chỉ một basin bằng row order.
- [ ] Đo khoảng cách chỉ khi có pinned input được khai báo; phiên bản đầu để null.
  Chạy lại test, kiểm tra đổi thứ tự input không đổi ID/version.

## Công việc 3 — DQ và Spark publish

**File:** tạo `quality.py`, `spark_job.py`,
`tests/unit/pipelines/silver_facilities/test_quality.py`,
`tests/integration/silver/test_facilities_publish.py`.

**Giao diện:** `run(request)` tuân DTO/result marker và manifest của shared Spark.
Hai bảng là bundle cùng `facility_version`; không ghi success sau mới một bảng.

- [ ] Test trước: lỗi CRS fatal, geometry hỏng reject, capacity thiếu warning;
  relation mất FK bị chặn, retry sau commit bảng thứ nhất không nhân facility.
- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_facilities/test_quality.py tests/integration/silver/test_facilities_publish.py -q`; xác nhận fail.
- [ ] Dùng shared quality/publisher, materialize khi source plan đọc target;
  kiểm tra unique/FK/type/CRS trước commit. Ghi reject và lineage theo source key.
- [ ] Publish bundle hoàn tất rồi ghi manifest; giữ evidence đến khi audit xong.
  Chạy test pass và kiểm tra snapshot không tăng khi replay cùng nội dung.

## Công việc 4 — Runtime/DAG và nghiệm thu

**File:** tạo `pipeline.py`, `runtime.py`, `airflow.py`, `README.md`, DAG entrypoint;
tạo `tests/unit/pipelines/silver_facilities/test_pipeline.py`,
`tests/contract/infra/test_silver_facilities_dag.py`.

- [ ] Viết test build/skip/repair, request chỉ pin input cần thiết, import DAG
  không load Spark và không yêu cầu River Network.

```python
def test_facility_dag_is_manual_and_single_run(dag):
    assert dag.dag_id == "silver_facilities"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_facilities tests/contract/infra/test_silver_facilities_dag.py -q`; xác nhận các test runtime mới fail.
- [ ] Wire factory dùng shared submit/audit; DAG manual, paused mặc định,
  `silver_writer`, log phase timing. Chạy lại test pass, smoke bảng tạm.
- [ ] Sau review mới chạy AOI thật: Trino kiểm tra unique/FK, capacity không bị
  bịa, warning có Meta evidence; replay skip. Ghi kết quả vào README và nhật ký
  performance. Commit chỉ khi người dùng yêu cầu; không merge branch teammate.
