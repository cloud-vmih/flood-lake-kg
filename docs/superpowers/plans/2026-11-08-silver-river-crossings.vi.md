# Kế hoạch triển khai Silver River Crossings

> **Dành cho người triển khai:** dùng `executing-plans`; làm sau khi Road và River
> có output contract đã nghiệm thu. Checkbox chưa đánh dấu là chưa triển khai;
> không tự commit/merge.

**Mục tiêu:** xác định vị trí road–river giao nhau, giữ evidence loại giao cắt và version.

**Kiến trúc:** DAG `silver_river_crossings` đọc bundle Road Network và River Network
đã hoàn tất, Spark/Sedona spatial join/chuẩn hóa giao điểm, DQ và ghi một bảng.
Không đọc PBF hay chạy lại pipeline Road/River; không tính ngập/passability ở Silver.

**Công nghệ:** Spark/Sedona, Iceberg/Polaris, Airflow và shared Spark audit/quality.

**Thiết kế:** [Exposure tách domain](../specs/2026-11-08-silver-exposure-split-design.vi.md).

## Ràng buộc

- Bắt buộc `silver.dim_road_edge`, `silver.dim_river_reach` với version/snapshot
  lấy từ hai run Meta thành công (bao gồm succeeded_with_warning được policy cho phép).
- `silver.dim_road_node` chỉ đọc nếu thuật toán cần endpoints; không thêm input
  snapshot không tham gia transform. Không bắt buộc Static Feature/Weather/Population.
- Output duy nhất `silver.river_crossing`. Road owner giữ evidence `bridge`,
  `tunnel`, `ford`, `osm_layer`; thiếu schema/version thì preflight fail rõ ràng.
- Crossing là road–river spatial relation, khác junction road–road. Bridge có
  thể giao sông về mặt bằng dù khác cao độ; không loại mọi bridge do khác layer.
- Tag bridge/ford mới đủ phân loại tương ứng; geometry-only là `unknown`.
  Tunnel/culvert/overlap không được suy thành bridge hoặc ford.
- Không tính deck height từ DEM mean. `deck_height_m` null nếu thiếu nguồn và
  quy ước datum. Không tự kết luận ngập hoặc đường bị cắt.
- ID có road/reach IDs+versions, processing version và canonical intersection
  fingerprint; không index array/intersection thứ tự. Một cặp có thể có nhiều crossing.

## Flow và thứ tự đọc file

```text
DAG → airflow.py → runtime.py → pipeline.py (chọn completed input bundles)
 → Spark spark_job.py → reader.py → transform.py → quality.py
 → MERGE river_crossing → publish manifest → Meta
```

File tạo trong `src/flashflood_data/pipelines/silver_crossings/`:

| Thứ tự | File | Vai trò | Caller / dependency |
| ---: | --- | --- | --- |
| 1 | `config.py` | `CrossingsConfig`, `load_crossings_config(path)`; CRS, precision/tolerance, tag priority, versions | Runtime/job |
| 2 | `models.py` | `CrossingRow`, `CrossingInputBundle` (snapshots + road/river versions) | Reader/transform/planner |
| 3 | `pipeline.py` | `plan_crossings_build(...)` chọn bundle hoàn tất, signature/repair/skip | Runtime; shared snapshots/Meta |
| 4 | `reader.py` | `read_crossing_inputs(spark, request, config)` lọc đúng versions trong pinned snapshots | Spark job |
| 5 | `transform.py` | `build_crossings(roads, rivers, config)`; candidate join, point extraction, classification, ID | Spark job; Sedona |
| 6 | `quality.py` | `crossing_rules(config)` và kiểm FK/tolerance/grade evidence | Spark job; shared quality |
| 7 | `spark_job.py` | `run(request)` transform/DQ/MERGE/manifest | Shared Spark CLI |
| 8 | `runtime.py`, `airflow.py` | `build_crossings_runtime(root=None)`, submit/audit và factory DAG | DAG entrypoint |
| 9 | `__init__.py`, `README.md` | Import nhẹ, giới hạn diễn giải crossing | Người đọc/DAG |

Tạo `config/pipelines/silver/river_crossings.yaml`,
`config/quality/silver_river_crossings.yaml`, `airflow/dags/silver_river_crossings.py`.

## Công việc 1 — Contract và discovery input hoàn tất

**File:** tạo config/models/pipeline/init, hai YAML;
sửa `src/flashflood_data/storage/iceberg_schemas.py`, `config/shared/metadata.yaml`;
tạo `tests/unit/pipelines/silver_crossings/test_config.py`,
`tests/unit/pipelines/silver_crossings/test_pipeline.py`,
`tests/contract/storage/test_silver_crossings_schema.py`.

**Giao diện:** `CrossingInputBundle` chứa đúng cặp completed run IDs, snapshots
và selected network versions; `plan_crossings_build` dùng chúng cùng config/AOI
để tạo signature. Không ghép snapshot mới nhất từng bảng khi upstream đang chạy dở.

- [ ] Test trước: Road partial run bị loại; cùng completed bundles skip; đổi
  một river version rebuild; thiếu `ford`/`osm_layer` báo contract error rõ ràng.

```python
def test_crossing_contract_links_both_network_versions():
    from flashflood_data.storage.iceberg_schemas import table_schema
    names = set(table_schema(("silver", "river_crossing")).names)
    assert {"road_version", "river_version", "crossing_type", "quality_status"} <= names
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_crossings/test_config.py tests/unit/pipelines/silver_crossings/test_pipeline.py tests/contract/storage/test_silver_crossings_schema.py -q`; xác nhận fail.
- [ ] Định nghĩa config/models/contract/registry và discovery chỉ đọc Meta bundle
  thành công; tie revision không phân giải được thì block. Chạy lại pass.

## Công việc 2 — Reader và giao cắt có evidence

**File:** tạo reader/transform;
`tests/integration/silver/test_crossing_transform.py`.

- [ ] Fixture: một bridge, một ford, một geometry-only, một tunnel, hai crossing
  trên cùng cặp line uốn, endpoint lặp do segment split, line overlap.

```python
def test_geometry_only_does_not_prove_bridge(crossing_output):
    row = crossing_output.where("road_edge_id = 'fixture-unknown'").first()
    assert row.crossing_type == "unknown"
    assert row.deck_height_m is None
```

- [ ] Chạy `.venv/bin/pytest tests/integration/silver/test_crossing_transform.py -q`; fixture giữ road ID trên, xác nhận fail.
- [ ] Bbox/index candidate join trước `ST_Intersection`, explode point components;
  line overlap vào finding/quarantine, không tự chọn centroid làm crossing.
  Canonical tọa độ theo precision config, dedup cùng cặp/version/vị trí; giữ hai
  vị trí khác nhau. Hai edge khác nhau chung endpoint vẫn là hai quan hệ riêng,
  không gộp mất FK chỉ vì cùng tọa độ.
- [ ] Phân loại theo tag precedence cấu hình; tag mâu thuẫn → unknown/warning.
  Bridge không tạo road junction mới; không sửa graph Road. Đảo input/partition
  order không đổi ID; thay processing version tạo build/version khác. Test pass.

## Công việc 3 — DQ, publish và replay

**File:** tạo quality/spark_job;
`tests/unit/pipelines/silver_crossings/test_quality.py`,
`tests/integration/silver/test_crossings_publish.py`.

- [ ] Test trước: geometry null reject, missing road/reach FK fatal nếu input
  bundle sai, point ngoài tolerance reject, unknown warning, replay không trùng.
- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_crossings/test_quality.py tests/integration/silver/test_crossings_publish.py -q`; xác nhận fail.
- [ ] Đọc đúng pinned snapshots và filter network versions; dùng shared DQ,
  quarantine và publisher; MERGE theo `crossing_id` xác định. Source/target scope
  version khi an toàn; không quét lịch sử rồi lọc bằng Python.
- [ ] Publish thành công rồi manifest. Input không có giao cắt hợp lệ là kết quả
  no-crossings được audit rõ ràng, không trả snapshot cũ làm output mới. Kiểm thử
  đường đi empty-output này trước production. Test replay/partial failure pass.

## Công việc 4 — Runtime/DAG và nghiệm thu

**File:** tạo runtime/airflow/README và entrypoint DAG;
`tests/contract/infra/test_silver_crossings_dag.py`.

- [ ] Test trước DAG manual/paused, hai input bundles bắt buộc, import không
  load Spark và không gọi service nội bộ của Road/River.

```python
def test_crossing_dag_has_independent_identity(dag):
    assert dag.dag_id == "silver_river_crossings"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_crossings tests/contract/infra/test_silver_crossings_dag.py -q`; xác nhận fail phần orchestration mới.
- [ ] Wire shared factory/submit/audit; task plan → execute, `silver_writer`,
  log candidate pairs, point/unknown/reject counts và timing từng phase. Test pass.
- [ ] Smoke bảng tạm rồi AOI nhỏ sau review: xem geometry cùng tags bằng Trino,
  kiểm FK/version, hai crossing/cặp không mất, replay skip. Ghi README kết quả
  và giới hạn bridge/ford evidence; chưa đánh giá khả năng đi qua trong Silver.
