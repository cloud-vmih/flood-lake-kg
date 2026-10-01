# Kế hoạch triển khai mạng sông ở Silver


**Bản gốc:** `docs/superpowers/plans/2026-09-30-silver-river-network.md`

> **Bản tiếng Việt:** Tên file code, class, hàm, bảng, field và lệnh được giữ nguyên để khớp repository.

> **Dành cho người triển khai:** KỸ NĂNG BẮT BUỘC: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng ô đánh dấu (`- [ ]`) để theo dõi.

**Mục tiêu:** Chuẩn hóa các đoạn sông HydroRIVERS, xây topology có hướng và liên kết từng đoạn sông với lưu vực L12 có version.

**Kiến trúc:** Dùng geometry và field nguồn ở Bronze để tạo các dòng reach ổn định, suy ra cạnh hạ lưu từ ID nguồn hoặc phép snap node đã cấu hình, rồi tính phần giao với basin trong CRS mét. Publish các bảng reach, edge và river-basin thành các output Silver có version độc lập.

**Công nghệ:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, GeoPandas, Shapely, PyProj, PyArrow, PyIceberg, pytest.

**Tài liệu thiết kế:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.vi.md`

## Ràng buộc chung

- Yêu cầu nền tảng dùng chung và `silver.dim_basin` đã có.
- Giữ nguyên reach đã chọn ở landing AOI quốc gia; không âm thầm cắt geometry reach chuẩn.
- Mọi stable ID đều bao gồm identity nguồn và river version.
- Hướng topology theo hướng của provider khi có; suy luận bằng geometry phải có version rõ ràng.
- Chiều dài phần giao với basin được tính trong CRS mét.
- Chu trình có hướng, reach ID trùng và internal target không hợp lệ là lỗi nghiêm trọng.
- Mỗi bảng đích chỉ có một Iceberg writer task.

---

## Cấu trúc file

```text
config/silver/river_network.yaml
airflow/dags/silver_river_network.py
src/flashflood_data/orchestration/silver/river/
  __init__.py
  config.py
  models.py
  reader.py
  normalize.py
  topology.py
  basin_overlay.py
  quality.py
  service.py
  factory.py
tests/unit/silver/river/
  test_config.py
  test_normalize.py
  test_topology.py
  test_basin_overlay.py
  test_quality.py
  test_service.py
tests/integration/silver/test_river_pipeline.py
```

`normalize.py`, `topology.py` và `basin_overlay.py` là transform domain thuần. `service.py` sở hữu thứ tự gọi và phần publish/audit chung. Code exposure phía sau đọc bảng river Silver và không import nội bộ các module này.

### Công việc 1: Định nghĩa schema, cấu hình và model mạng sông

**Các file:**
- Tạo: `config/silver/river_network.yaml`
- Tạo: `src/flashflood_data/orchestration/silver/river/__init__.py`
- Tạo: `src/flashflood_data/orchestration/silver/river/config.py`
- Tạo: `src/flashflood_data/orchestration/silver/river/models.py`
- Sửa: `src/flashflood_data/storage/iceberg_schemas.py`
- Sửa: `config/meta/static.yaml`
- Sửa: `tests/contract/storage/test_meta_bronze_schemas.py`
- Tạo: `tests/unit/silver/river/test_config.py`

**Giao diện:**
- Đầu ra: `RiverNetworkConfig`, `RiverReachRow`, `RiverReachEdgeRow`, `RiverBasinRow`, `RiverTopologyReport`.
- Produces physical `silver.dim_river_reach`, `silver.river_reach_edge`, `silver.river_basin`.

- [ ] **Bước 1: Viết kiểm thử ban đầu cho config/schema**

```python
def test_river_config_declares_source_fields_and_tolerance():
    config = load_river_config(CONFIG)
    assert config.source_id == "hydrorivers_v10"
    assert config.field_map["river_reach_id"] == "HYRIV_ID"
    assert config.snap_tolerance_m > 0

def test_river_basin_schema_is_versioned():
    names = set(table_schema(("silver", "river_basin")).names)
    assert {"river_reach_id", "river_version", "basin_id", "basin_version",
            "geometry_processing_version", "length_inside_km"} <= names
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/river/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: FAIL vì contracts chưa tồn tại.

- [ ] **Bước 3: Triển khai config đã validate, model và schema chính xác**

Các field YAML gồm source ID, mapping field provider, quy ước hướng nguồn, snap tolerance, CRS đo chiều dài, version transform/topology/geometry-processing và batch size. Từ chối cấu hình thiếu mapping reach ID hoặc tolerance không dương.

- [ ] **Bước 4: Chạy kiểm thử contract**

Chạy: `pytest tests/unit/silver/river/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit contracts**

```bash
git add config/silver/river_network.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/river src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/river/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define Silver river contracts"
```

### Công việc 2: Chuẩn hóa reach và xây dựng topology

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/river/reader.py`
- Tạo: `src/flashflood_data/orchestration/silver/river/normalize.py`
- Tạo: `src/flashflood_data/orchestration/silver/river/topology.py`
- Tạo: `tests/unit/silver/river/test_normalize.py`
- Tạo: `tests/unit/silver/river/test_topology.py`

**Giao diện:**
- Đầu vào: exact `bronze.river_reach_raw` snapshot và config.
- Đầu ra: `normalize_reaches(rows: Sequence[Mapping[str, object]], config: RiverNetworkConfig, river_version: str) -> list[RiverReachRow]`.
- Đầu ra: `build_reach_edges(reaches, config) -> tuple[list[RiverReachEdgeRow], RiverTopologyReport]`.

- [ ] **Bước 1: Viết kiểm thử cho chuẩn hóa và topology**

```python
def test_normalize_preserves_provider_id_and_metric_length():
    reach = normalize_reaches([bronze_reach("42")], config, "rv1")[0]
    assert reach.hydrorivers_id == "42"
    assert reach.crs == "EPSG:4326"
    assert reach.length_km > 0

def test_topology_links_provider_downstream_id():
    edges, report = build_reach_edges(reaches_with_next_down(), config)
    assert edges[0].upstream_reach_id == "42"
    assert edges[0].downstream_reach_id == "43"
    assert report.cycle_reach_ids == ()
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/river/test_normalize.py tests/unit/silver/river/test_topology.py -q`  
Mong đợi: FAIL vì reader/transforms are absent.

- [ ] **Bước 3: Triển khai reach chuẩn và logic graph**

Chỉ đọc các dòng thuộc tập source object và snapshot đã cấu hình. Giải mã JSON nguồn một lần. Chuẩn hóa LineString/MultiLineString theo cách xác định, từ chối geometry rỗng, tính chiều dài mét và giữ provider ID. Ưu tiên downstream ID của provider; chỉ suy luận bằng endpoint đã snap khi nguồn không có liên kết và chỉ có đúng một candidate trong tolerance. Candidate mơ hồ được giữ dangling và báo cáo.

Tạo version từ input snapshot/config và dùng thuật toán Kahn để phát hiện cycle.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/river/test_normalize.py tests/unit/silver/river/test_topology.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit reach chuẩn hóa**

```bash
git add src/flashflood_data/orchestration/silver/river/reader.py src/flashflood_data/orchestration/silver/river/normalize.py src/flashflood_data/orchestration/silver/river/topology.py tests/unit/silver/river/test_normalize.py tests/unit/silver/river/test_topology.py
git commit -m "feat: normalize and connect river reaches"
```

### Công việc 3: Chồng lớp reach với basin và thêm cổng chất lượng

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/river/basin_overlay.py`
- Tạo: `src/flashflood_data/orchestration/silver/river/quality.py`
- Tạo: `tests/unit/silver/river/test_basin_overlay.py`
- Tạo: `tests/unit/silver/river/test_quality.py`

**Giao diện:**
- Đầu ra: `overlay_reaches_with_basins(reaches, basins, config) -> list[RiverBasinRow]`.
- Đầu ra: `check_river_outputs(reaches, edges, relations, report) -> list[QualityResult]`.

- [ ] **Bước 1: Viết kiểm thử cho chồng lớp và DQ**

```python
def test_reach_crossing_two_basins_has_two_length_rows():
    rows = overlay_reaches_with_basins(crossing_reach(), adjacent_basins(), config)
    assert len(rows) == 2
    assert sum(row.intersection_ratio for row in rows) == pytest.approx(1.0, rel=1e-3)

def test_cycle_and_ratio_over_one_are_fatal():
    failures = fatal_failures(check_river_outputs(reaches, cyclic_edges, bad_relations, report))
    assert {item.rule_id for item in failures} >= {"river_topology_acyclic", "river_basin_ratio_range"}
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/river/test_basin_overlay.py tests/unit/silver/river/test_quality.py -q`  
Mong đợi: FAIL vì overlay/DQ are absent.

- [ ] **Bước 3: Triển khai spatial index, ngữ nghĩa quan hệ và các rule có tên**

Tính `length_inside_km` sau phép giao trong CRS mét. `spatial_relation` là `within` khi toàn bộ reach được phủ, `intersects` khi phần giao có chiều dài dương và `touches` chỉ khi tiếp xúc biên với chiều dài 0 được giữ để QA. Fatal rule kiểm tra khóa, geometry, chiều dài dương, internal edge target, cycle và ratio ngoài `[0,1]`; endpoint dangling/mơ hồ là warning.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/river/test_basin_overlay.py tests/unit/silver/river/test_quality.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit chồng lớp và DQ**

```bash
git add src/flashflood_data/orchestration/silver/river/basin_overlay.py src/flashflood_data/orchestration/silver/river/quality.py tests/unit/silver/river/test_basin_overlay.py tests/unit/silver/river/test_quality.py
git commit -m "feat: relate river reaches to basins"
```

### Công việc 4: Thêm service, DAG chạy thủ công, kiểm thử tích hợp và tài liệu

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/river/service.py`
- Tạo: `src/flashflood_data/orchestration/silver/river/factory.py`
- Tạo: `airflow/dags/silver_river_network.py`
- Tạo: `tests/unit/silver/river/test_service.py`
- Tạo: `tests/integration/silver/test_river_pipeline.py`
- Sửa: `tests/contract/infra/test_silver_dags.py`
- Sửa: `README.md`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`

**Giao diện:**
- Đầu ra: `RiverSilverService.plan`, `compute_batch`, `publish`, `run`.
- Đầu ra: `build_river_silver_service(root=None)`.
- Produces manual DAG ID `silver_river_network`.

- [ ] **Bước 1: Viết kiểm thử cho publish và DAG**

```python
def test_river_publish_commits_three_tables_once(service):
    result = service.run("river-1")
    assert {item.table_name for item in result.outputs} == {
        "silver.dim_river_reach", "silver.river_reach_edge", "silver.river_basin"
    }

def test_river_dag_waits_for_all_compute_batches(dag_bag):
    dag = dag_bag.get_dag("silver_river_network")
    assert dag.schedule is None
    assert "publish_outputs" in dag.task_ids
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/river/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: FAIL vì service/DAG are absent.

- [ ] **Bước 3: Triển khai service và TaskFlow graph**

Dùng discovery chung với snapshot river Bronze và basin Silver. Tính theo batch reach, ghép topology một lần trên toàn bộ tập reach, tính overlay theo batch, validate toàn cục rồi publish đúng primary key trong schema contract. Ghi lineage từ cả hai input snapshot tới mọi output phụ thuộc.

- [ ] **Bước 4: Chạy kiểm thử domain**

Chạy: `pytest tests/unit/silver/river tests/integration/silver/test_river_pipeline.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit pipeline**

```bash
git add src/flashflood_data/orchestration/silver/river airflow/dags/silver_river_network.py tests/unit/silver/river tests/integration/silver/test_river_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver river network DAG"
```

## Điểm kiểm tra

Kiểm tra số reach, tổng chiều dài, cycle, endpoint dangling và ratio giao basin bằng Trino. Duyệt network version trước khi pipeline exposure dùng nó để tìm crossing.
