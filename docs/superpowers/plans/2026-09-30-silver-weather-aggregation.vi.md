# Kế hoạch triển khai tổng hợp dữ liệu thời tiết theo lưu vực ở Silver


**Bản gốc:** `docs/superpowers/plans/2026-09-30-silver-weather-aggregation.md`

> **Bản tiếng Việt:** Tên file code, class, hàm, bảng, field và lệnh được giữ nguyên để khớp repository.

> **Dành cho người triển khai:** KỸ NĂNG BẮT BUỘC: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng ô đánh dấu (`- [ ]`) để theo dõi.

**Mục tiêu:** Chuyển các raster slice Bronze thành giá trị thời tiết theo lưu vực L12 có version bằng bộ trọng số giao giữa source grid và basin có thể tái sử dụng.

**Kiến trúc:** Chỉ tạo trọng số cho tổ hợp grid/basin/geometry version chưa có, ánh xạ mảng raster slice bằng `cell_index` ổn định, tổng hợp các cell hợp lệ theo diện tích giao và append idempotent bằng hash business key xác định. NOW, Standard, ERA5-Land và IFS vẫn là các quan sát riêng trong Silver.

**Công nghệ:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, NumPy, PyArrow, Shapely, PyProj, PyIceberg, pytest.

**Tài liệu thiết kế:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.vi.md`

## Ràng buộc chung

- Yêu cầu nền tảng dùng chung và `silver.dim_basin` đã có.
- `silver.source_grid` là output đã có của weather ingest và không được build lại tại đây.
- Tổng hợp theo basin không bao giờ coi cell missing/nodata là 0.
- `valid_coverage_fraction` bằng diện tích giao hợp lệ chia cho diện tích basin.
- Các cửa sổ NOW lệch nhau 30 phút vẫn là quan sát riêng; Standard không xóa hoặc overwrite NOW.
- IFS và ERA5-Land vẫn là nguồn riêng dù có biến trùng nhau.
- Identity để rebuild trọng số gồm source grid version, basin version và geometry processing version.
- Tính toán fan-out theo batch slice; mỗi bảng output chỉ có một writer task.

---

## Cấu trúc file

```text
config/silver/weather_aggregation.yaml
airflow/dags/silver_weather_aggregation.py
src/flashflood_data/orchestration/silver/weather/
  __init__.py
  config.py
  models.py
  reader.py
  weights.py
  aggregate.py
  revisions.py
  quality.py
  service.py
  factory.py
tests/unit/silver/weather/
  test_config.py
  test_weights.py
  test_aggregate.py
  test_revisions.py
  test_quality.py
  test_service.py
tests/integration/silver/test_weather_aggregation_pipeline.py
```

Service đọc ba table contract, chỉ gọi `weights.py` cho tổ hợp còn thiếu, gọi `aggregate.py` cho slice key còn thiếu rồi publish qua writer/audit chung. Package này không được gọi API provider hoặc fetch object Raw.

### Công việc 1: Thêm schema, cấu hình và contract dòng dữ liệu

**Các file:**
- Tạo: `config/silver/weather_aggregation.yaml`
- Tạo: `src/flashflood_data/orchestration/silver/weather/__init__.py`
- Tạo: `src/flashflood_data/orchestration/silver/weather/config.py`
- Tạo: `src/flashflood_data/orchestration/silver/weather/models.py`
- Sửa: `src/flashflood_data/storage/iceberg_schemas.py`
- Sửa: `config/meta/static.yaml`
- Sửa: `tests/contract/storage/test_meta_bronze_schemas.py`
- Tạo: `tests/unit/silver/weather/test_config.py`

**Giao diện:**
- Đầu ra: physical `silver.grid_basin_weight` và `silver.basin_weather_value`.
- Đầu ra: `WeatherAggregationConfig`, `GridBasinWeightRow`, `BasinWeatherValueRow`, `WeatherAggregationReport`.

- [ ] **Bước 1: Viết kiểm thử ban đầu cho schema và chính sách biến**

```python
def test_weather_config_declares_every_bronze_variable():
    config = load_weather_aggregation_config(CONFIG)
    assert config.variables["precipitation"].unit == "mm"
    assert config.variables["surface_runoff"].aggregation == "area_mean"
    assert config.variables["subsurface_runoff"].aggregation == "area_mean"
    assert config.variables["soil_moisture"].aggregation == "area_mean"

def test_basin_weather_schema_keeps_source_and_revision():
    names = set(table_schema(("silver", "basin_weather_value")).names)
    assert {"source_id", "source_product", "source_revision", "available_at",
            "valid_coverage_fraction", "business_key_hash"} <= names
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/weather/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: FAIL vì config/models/schemas are absent.

- [ ] **Bước 3: Triển khai chính sách đã validate và contract vật lý**

YAML định nghĩa `geometry_processing_version`, CRS bảo toàn diện tích, ngưỡng coverage tối thiểu để publish, slice batch size, unit chuẩn và cách aggregate cho precipitation, các lớp soil moisture, surface runoff và subsurface runoff. Từ chối biến thiếu unit hoặc dùng cách aggregate khác `area_mean`.

Thêm toàn bộ cột đã duyệt từ `docs/schema_contract/data.md`. Đăng ký cả hai dataset với `quality_policy_id: silver-weather-v1`.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/weather/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit contracts**

```bash
git add config/silver/weather_aggregation.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/weather src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/weather/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define Silver weather contracts"
```

### Công việc 2: Xây dựng trọng số grid–basin có thể tái sử dụng

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/weather/reader.py`
- Tạo: `src/flashflood_data/orchestration/silver/weather/weights.py`
- Tạo: `tests/unit/silver/weather/test_weights.py`

**Giao diện:**
- Đầu vào: các dòng từ `silver.source_grid`, `silver.dim_basin`, và hiện có weights.
- Đầu ra: `WeatherSilverInputs` bound to exact snapshots.
- Đầu ra: `build_grid_basin_weights(grid_rows, basin_rows, config) -> list[GridBasinWeightRow]`.

- [ ] **Bước 1: Viết kiểm thử trọng số theo diện tích chính xác**

```python
def test_two_equal_cells_cover_one_basin():
    rows = build_grid_basin_weights(two_equal_cells(), one_basin(), config)
    assert sum(row.weight_by_basin for row in rows) == pytest.approx(1.0)
    assert all(row.weight_by_grid == pytest.approx(1.0) for row in rows)

def test_partial_cell_uses_intersection_area():
    row = build_grid_basin_weights(one_half_intersection(), one_basin(), config)[0]
    assert row.weight_by_grid == pytest.approx(0.5, rel=1e-3)
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/weather/test_weights.py -q`  
Mong đợi: FAIL vì reader/weights chưa tồn tại.

- [ ] **Bước 3: Triển khai chồng lớp có index trong CRS bảo toàn diện tích**

Giải mã WKB, sửa hoặc từ chối geometry lỗi theo cấu hình, chiếu cả hai lớp sang CRS bảo toàn diện tích, dùng spatial index để tránh Cartesian join toàn phần rồi tính:

```python
intersection_area_m2 = intersection.area
weight_by_basin = intersection_area_m2 / basin.area
weight_by_grid = intersection_area_m2 / grid_cell.area
```

Bỏ qua tiếp xúc có diện tích 0. Tạo `geometry_processing_version` từ thuật toán và CRS đã cấu hình, đồng thời giữ source/basin version trong mọi dòng.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/weather/test_weights.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit weights**

```bash
git add src/flashflood_data/orchestration/silver/weather/reader.py src/flashflood_data/orchestration/silver/weather/weights.py tests/unit/silver/weather/test_weights.py
git commit -m "feat: build source-grid basin weights"
```

### Công việc 3: Tổng hợp slice và giữ đúng ngữ nghĩa revision

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/weather/aggregate.py`
- Tạo: `src/flashflood_data/orchestration/silver/weather/revisions.py`
- Tạo: `src/flashflood_data/orchestration/silver/weather/quality.py`
- Tạo: `tests/unit/silver/weather/test_aggregate.py`
- Tạo: `tests/unit/silver/weather/test_revisions.py`
- Tạo: `tests/unit/silver/weather/test_quality.py`

**Giao diện:**
- Đầu vào: one `weather_raster_slice`, cell-index lookup, relevant weights, basin/version, variable policy.
- Đầu ra: `aggregate_slice(slice_row: Mapping[str, object], weights: Sequence[GridBasinWeightRow], context: AggregationContext) -> tuple[list[BasinWeatherValueRow], WeatherAggregationReport]`.
- Đầu ra: `weather_business_key(row_without_hash) -> str`.
- Đầu ra: `check_weather_values(rows: Sequence[BasinWeatherValueRow], report: WeatherAggregationReport, config: WeatherAggregationConfig) -> list[QualityResult]`.

- [ ] **Bước 1: Viết kiểm thử cho tổng hợp, dữ liệu thiếu và window**

```python
def test_area_mean_renormalizes_over_valid_coverage():
    result, report = aggregate_slice(slice_values([10.0, float("nan")]), weights([0.5, 0.5]), context)
    assert result[0].value == pytest.approx(10.0)
    assert result[0].valid_coverage_fraction == pytest.approx(0.5)

def test_now_and_standard_same_window_have_different_business_keys():
    assert weather_business_key(now_row()) != weather_business_key(standard_row())

def test_shifted_now_windows_are_both_retained():
    assert weather_business_key(now_at("10:00")) != weather_business_key(now_at("10:30"))
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/weather/test_aggregate.py tests/unit/silver/weather/test_revisions.py tests/unit/silver/weather/test_quality.py -q`  
Mong đợi: FAIL vì aggregation/revision/DQ code is absent.

- [ ] **Bước 3: Triển khai tổng hợp mảng đã căn chỉnh và identity nghiệp vụ đầy đủ**

Tạo lookup `cell_index -> value` từ các mảng căn chỉnh theo vị trí và từ chối khi lệch độ dài hoặc trùng index. Với mỗi basin:

```python
valid_weight = sum(weight_by_basin for valid cells)
value = sum(cell_value * weight_by_basin for valid cells) / valid_weight
coverage = valid_weight
```

Hash business key gồm basin/version, source/product/grid version, variable/level/cycle, model run, valid time, window start/end và revision. Không đưa QA flag có thể thay đổi vào hash. Nếu thiếu `available_at`, lấy từ record source object trong Meta; từ chối dòng khi không có mốc availability đáng tin cậy.

DQ nghiêm trọng gồm mảng lệch vị trí, cell không tồn tại, business key trùng, unit/value-kind không khớp, time window lỗi và coverage ngoài 0–1. Coverage thấp là warning và đặt `value=None` khi thấp hơn ngưỡng publish.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/weather/test_aggregate.py tests/unit/silver/weather/test_revisions.py tests/unit/silver/weather/test_quality.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit tổng hợp**

```bash
git add src/flashflood_data/orchestration/silver/weather/aggregate.py src/flashflood_data/orchestration/silver/weather/revisions.py src/flashflood_data/orchestration/silver/weather/quality.py tests/unit/silver/weather/test_aggregate.py tests/unit/silver/weather/test_revisions.py tests/unit/silver/weather/test_quality.py
git commit -m "feat: aggregate weather slices by basin"
```

### Công việc 4: Thêm service tăng dần, DAG và kiểm thử tích hợp

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/weather/service.py`
- Tạo: `src/flashflood_data/orchestration/silver/weather/factory.py`
- Tạo: `airflow/dags/silver_weather_aggregation.py`
- Tạo: `tests/unit/silver/weather/test_service.py`
- Tạo: `tests/integration/silver/test_weather_aggregation_pipeline.py`
- Sửa: `tests/contract/infra/test_silver_dags.py`
- Sửa: `README.md`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`

**Giao diện:**
- Đầu ra: `WeatherSilverService.plan`, `ensure_weights`, `aggregate_batch`, `publish`, `run`.
- Đầu ra: `build_weather_silver_service(root=None)`.
- Produces manual DAG ID `silver_weather_aggregation`.

- [ ] **Bước 1: Viết kiểm thử incremental và một writer**

```python
def test_existing_slice_business_keys_are_not_recomputed(service):
    plan = service.plan("run-2")
    assert plan.slice_ids == ("new-slice",)

def test_weight_table_is_reused_for_same_versions(service):
    service.ensure_weights(request)
    service.weight_builder.build.assert_not_called()

def test_weather_dag_has_one_publish_task(dag_bag):
    dag = dag_bag.get_dag("silver_weather_aggregation")
    assert dag.schedule is None
    assert "publish_weather_values" in dag.task_ids
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/weather/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: FAIL vì service/DAG are absent.

- [ ] **Bước 3: Triển khai lập kế hoạch tăng dần và TaskFlow graph**

Task graph:

```text
discover_snapshots -> plan_build -> ensure_weight_batches
  -> publish_weights -> plan_missing_slices -> aggregate_slice_batches.expand
  -> publish_weather_values -> audit_and_finalize -> cleanup_staging
```

Use keys:

```python
WEIGHT_KEY = ("source_id", "source_grid_version", "source_grid_id", "basin_id",
              "basin_version", "geometry_processing_version")
WEATHER_KEY = ("business_key_hash",)
```

Integration test tạo grid hai cell, hai basin, slice NOW/Standard, một cell thiếu và một revision mới. Kiểm tra value, coverage, các dòng nguồn được giữ, lineage chính xác, skip khi chạy lại và mỗi bảng chỉ có một Iceberg commit.

- [ ] **Bước 4: Chạy kiểm thử domain và hồi quy weather hiện có**

Chạy: `pytest tests/unit/silver/weather tests/integration/silver/test_weather_aggregation_pipeline.py tests/unit/weather tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit weather Silver pipeline**

```bash
git add src/flashflood_data/orchestration/silver/weather airflow/dags/silver_weather_aggregation.py tests/unit/silver/weather tests/integration/silver/test_weather_aggregation_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver weather aggregation DAG"
```

## Điểm kiểm tra

Dừng sau khi chạy fixture GSMaP NOW, GSMaP Standard và ERA5 nhỏ. Query hai bảng Silver bằng Trino, kiểm tra coverage, window, revision, `available_at` và hành vi chạy lại trước khi aggregate toàn bộ backfill.
