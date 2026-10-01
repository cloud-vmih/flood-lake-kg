# Kế hoạch triển khai topology lưu vực ở Silver


**Bản gốc:** `docs/superpowers/plans/2026-09-30-silver-basin-topology.md`

> **Bản tiếng Việt:** Tên file code, class, hàm, bảng, field và lệnh được giữ nguyên để khớp repository.

> **Dành cho người triển khai:** KỸ NĂNG BẮT BUỘC: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng ô đánh dấu (`- [ ]`) để theo dõi.

**Mục tiêu:** Xây dựng dimension lưu vực HydroBASINS cấp 12 chuẩn và bảng cạnh có hướng xuống hạ lưu từ các dòng Bronze.

**Kiến trúc:** Đọc một snapshot Bronze bất biến, dùng HydroBASINS làm nguồn geometry/topology, chỉ join các field BasinATLAS đã duyệt theo `HYBAS_ID`, rồi publish basin và cạnh có version. Các module chuẩn hóa/topology thuần không phụ thuộc Airflow hay storage.

**Công nghệ:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, PyArrow, Shapely, PyProj, PyIceberg, pytest.

**Tài liệu thiết kế:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.vi.md`

## Ràng buộc chung

- Yêu cầu plan `silver-common-foundation` đã hoàn tất.
- HydroBASINS là nguồn geometry và topology chính; BasinATLAS chỉ bổ sung các field thuộc allowlist đã duyệt.
- `hydrobasins_level` phải đúng bằng `12`.
- Geometry được chuẩn hóa về EPSG:4326 và tạo version theo nội dung/cấu hình.
- `NEXT_DOWN` nằm ngoài AOI vẫn được giữ để QA và tạo `edge_status=exits_AOI`.
- Chu trình và khóa basin trùng là lỗi DQ nghiêm trọng.
- Phần tính toán có thể fan-out; mỗi bảng đích chỉ có một task commit.

---

## Cấu trúc file

```text
config/silver/basin_topology.yaml
airflow/dags/silver_basin_topology.py
src/flashflood_data/orchestration/silver/basin/
  __init__.py              # public domain exports
  config.py                # YAML contract
  models.py                # basin/edge DTOs
  reader.py                # Bronze snapshot rows
  normalize.py             # geometry and field normalization
  topology.py              # directed graph construction
  quality.py               # basin/topology DQ
  service.py               # use-case orchestration
  factory.py               # production composition
tests/unit/silver/basin/
  test_config.py
  test_normalize.py
  test_topology.py
  test_quality.py
  test_service.py
tests/integration/silver/test_basin_pipeline.py
tests/contract/infra/test_silver_dags.py
```

DAG chỉ import `factory.py`. `factory.py` tạo `service.py`. Service gọi `reader.py`, `normalize.py`, `topology.py`, `quality.py`, sau đó dùng staging/publisher/audit chung. Không domain Silver nào khác import package này; pipeline phía sau đọc `silver.dim_basin` và `silver.basin_edge`.

### Công việc 1: Đăng ký schema vật lý và cấu hình basin

**Các file:**
- Tạo: `config/silver/basin_topology.yaml`
- Tạo: `src/flashflood_data/orchestration/silver/basin/__init__.py`
- Tạo: `src/flashflood_data/orchestration/silver/basin/config.py`
- Tạo: `src/flashflood_data/orchestration/silver/basin/models.py`
- Sửa: `src/flashflood_data/storage/iceberg_schemas.py`
- Sửa: `config/meta/static.yaml`
- Sửa: `tests/contract/storage/test_meta_bronze_schemas.py`
- Tạo: `tests/unit/silver/basin/test_config.py`

**Giao diện:**
- Đầu ra: `BasinTopologyConfig(level, hydro_source_id, atlas_source_id, field_map, transform_version, contract_version)`.
- Đầu ra: `BasinRow`, `BasinEdgeRow`, và `TopologyReport` Pydantic models matching `docs/schema_contract/data.md`.
- Produces physical tables `silver.dim_basin` và `silver.basin_edge`.

- [ ] **Bước 1: Viết kiểm thử ban đầu cho schema/config**

```python
def test_basin_config_is_l12_and_has_explicit_sources():
    config = load_basin_config(ROOT / "config/silver/basin_topology.yaml")
    assert config.level == 12
    assert config.hydro_source_id == "hydrobasins_v1c"
    assert config.atlas_source_id == "basinatlas_v10"

def test_silver_basin_schema_has_versioned_key():
    schema = table_schema(("silver", "dim_basin"))
    assert {"basin_id", "basin_version", "geometry_wkb", "next_down_id"} <= set(schema.names)
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/basin/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: FAIL vì config, models, và schemas chưa tồn tại.

- [ ] **Bước 3: Thêm contract chính xác và validate YAML**

YAML phải có các trường sau:

```yaml
level: 12
hydro_source_id: hydrobasins_v1c
atlas_source_id: basinatlas_v10
transform_version: basin-normalize-v1
topology_version: basin-topology-v1
contract_version: "1"
field_map:
  basin_id: HYBAS_ID
  next_down_id: NEXT_DOWN
  main_basin_id: MAIN_BAS
  pfaf_id: PFAF_ID
  topology_sort_hint: SORT
  basin_area_km2: SUB_AREA
  upstream_area_km2: UP_AREA
  distance_to_main_sink_km: DIST_MAIN
  next_sink_id: NEXT_SINK
  distance_to_sink_km: DIST_SINK
```

Thêm toàn bộ cột và nullability của hai bảng Silver đã duyệt vào `_DEFINITIONS`, đồng thời thêm mô tả hai dataset vào `config/meta/static.yaml`.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/basin/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit contract**

```bash
git add config/silver/basin_topology.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/basin src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/basin/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define Silver basin contracts"
```

### Công việc 2: Đọc và chuẩn hóa các dòng basin Bronze

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/basin/reader.py`
- Tạo: `src/flashflood_data/orchestration/silver/basin/normalize.py`
- Tạo: `tests/unit/silver/basin/test_normalize.py`

**Giao diện:**
- Đầu vào: các dòng từ `bronze.basin_polygon_raw`, `BasinTopologyConfig`.
- Đầu ra: `BasinBronzeInputs(hydro_rows, atlas_rows, snapshot_ref)`.
- Đầu ra: `normalize_basins(inputs, config, basin_version, valid_from) -> list[BasinRow]`.

- [ ] **Bước 1: Viết kiểm thử cho chuẩn hóa with one internal và one external downstream ID**

```python
def test_normalize_joins_atlas_by_hybas_id_and_preserves_string_ids():
    rows = normalize_basins(inputs_for("2123456780", next_down="2123456790"), config, "bv1", NOW)
    assert rows[0].basin_id == "2123456780"
    assert rows[0].next_down_id == "2123456790"
    assert rows[0].hydrobasins_level == 12
    assert rows[0].crs == "EPSG:4326"

def test_normalize_rejects_missing_atlas_match():
    with pytest.raises(ValueError, match="BasinATLAS.*HYBAS_ID"):
        normalize_basins(inputs_without_atlas(), config, "bv1", NOW)
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/basin/test_normalize.py -q`  
Mong đợi: FAIL vì reader/normalizer chưa tồn tại.

- [ ] **Bước 3: Triển khai đọc dữ liệu theo snapshot và chuẩn hóa**

`BasinBronzeReader.read(snapshot_id, hydro_source_id, atlas_source_id)` đọc đúng snapshot được yêu cầu, lọc theo `source_id`, giải mã `source_fields_json` và từ chối dòng thiếu source hoặc `HYBAS_ID`. `normalize_basins` parse WKB, chỉ sửa polygon có thể sửa hợp lệ, chuẩn hóa hướng polygon, ghi WKB chuẩn, tính `geometry_hash` và chỉ map các field đã cấu hình.

Generate `basin_version` with:

```python
basin_version = canonical_hash({
    "input_snapshot": snapshot_id,
    "level": config.level,
    "transform_version": config.transform_version,
})[:16]
```

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/basin/test_normalize.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit chuẩn hóa**

```bash
git add src/flashflood_data/orchestration/silver/basin/reader.py src/flashflood_data/orchestration/silver/basin/normalize.py tests/unit/silver/basin/test_normalize.py
git commit -m "feat: normalize Bronze basins for Silver"
```

### Công việc 3: Xây dựng topology và cổng chất lượng domain

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/basin/topology.py`
- Tạo: `src/flashflood_data/orchestration/silver/basin/quality.py`
- Tạo: `tests/unit/silver/basin/test_topology.py`
- Tạo: `tests/unit/silver/basin/test_quality.py`

**Giao diện:**
- Đầu vào: `Sequence[BasinRow]`, configured topology version.
- Đầu ra: `build_basin_edges(rows: Sequence[BasinRow], topology_version: str) -> tuple[list[BasinEdgeRow], TopologyReport]`.
- Đầu ra: `check_basins(rows) -> list[QualityResult]` và `check_topology(rows, edges, report) -> list[QualityResult]`.

- [ ] **Bước 1: Viết kiểm thử cho graph hành vi**

```python
def test_topology_classifies_internal_exit_and_terminal():
    edges, report = build_basin_edges(basins(), topology_version="tv1")
    assert {edge.edge_status for edge in edges} == {"internal", "exits_AOI", "terminal"}
    assert report.cycle_nodes == ()

def test_cycle_is_a_fatal_quality_failure():
    edges, report = build_basin_edges(cyclic_basins(), topology_version="tv1")
    failures = fatal_failures(check_topology(cyclic_basins(), edges, report))
    assert [item.rule_id for item in failures] == ["basin_topology_acyclic"]
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/basin/test_topology.py tests/unit/silver/basin/test_quality.py -q`  
Mong đợi: FAIL vì topology và DQ functions chưa tồn tại.

- [ ] **Bước 3: Triển khai graph và các rule rõ ràng**

Tạo một cạnh cho mỗi basin thượng nguồn. Xử lý `None`, `0` và self-sink theo contract nguồn thành `terminal`; ID khác 0 nhưng không nằm trong AOI là `exits_AOI`; ID thuộc tập đã chọn là `internal`. Dùng thuật toán Kahn để topological sort và chỉ giữ `SORT` gốc để đối chiếu QA.

Triển khai rule nghiêm trọng cho khóa duy nhất, polygon hợp lệ, diện tích dương, graph không chu trình và internal target hợp lệ. Ghi warning khi diện tích lệch, sort hint không khớp hoặc thiếu metadata sink tùy chọn.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/basin/test_topology.py tests/unit/silver/basin/test_quality.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit topology**

```bash
git add src/flashflood_data/orchestration/silver/basin/topology.py src/flashflood_data/orchestration/silver/basin/quality.py tests/unit/silver/basin/test_topology.py tests/unit/silver/basin/test_quality.py
git commit -m "feat: build and validate basin topology"
```

### Công việc 4: Điều phối, publish và sửa build basin dang dở

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/basin/service.py`
- Tạo: `src/flashflood_data/orchestration/silver/basin/factory.py`
- Sửa: `src/flashflood_data/orchestration/silver/basin/__init__.py`
- Tạo: `tests/unit/silver/basin/test_service.py`
- Tạo: `tests/integration/silver/test_basin_pipeline.py`

**Giao diện:**
- Đầu vào: common `SilverDependencies` và domain config/modules.
- Đầu ra: `BasinSilverService.plan(run_id)`, `compute(request)`, và `publish(request) -> SilverRunResult`.
- Đầu ra: `build_basin_silver_service(root=None) -> BasinSilverService`.

- [ ] **Bước 1: Viết kiểm thử cho service idempotency và repair**

```python
def test_completed_build_skips_all_compute(service):
    result = service.run("run-2")
    assert result.status == "skipped"
    service.reader.read.assert_not_called()

def test_partial_build_publishes_only_missing_edge_table(service):
    result = service.run("repair-1")
    assert [item.table_name for item in result.outputs] == ["silver.basin_edge"]
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/basin/test_service.py -q`  
Mong đợi: FAIL vì service/factory chưa tồn tại.

- [ ] **Bước 3: Triển khai luồng nghiệp vụ**

Service dùng discovery chung cho `bronze.basin_polygon_raw`, tính hai batch staging một lần, kiểm tra toàn bộ fatal rule rồi publish theo các khóa sau:

```python
DIM_BASIN_KEY = ("basin_id", "basin_version")
BASIN_EDGE_KEY = ("topology_version", "basin_version", "upstream_basin_id")
```

Ghi input/output ref và một lineage edge cho từng output Bronze → Silver. Khi có exception, ghi run thất bại/chưa hoàn tất và giữ staging.

- [ ] **Bước 4: Chạy unit và fake-catalog tích hợp**

Chạy: `pytest tests/unit/silver/basin tests/integration/silver/test_basin_pipeline.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit service**

```bash
git add src/flashflood_data/orchestration/silver/basin tests/unit/silver/basin/test_service.py tests/integration/silver/test_basin_pipeline.py
git commit -m "feat: publish Silver basin topology"
```

### Công việc 5: Thêm Airflow DAG chạy thủ công và tài liệu vận hành

**Các file:**
- Tạo: `airflow/dags/silver_basin_topology.py`
- Tạo: `tests/contract/infra/test_silver_dags.py`
- Sửa: `README.md`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`

**Giao diện:**
- Đầu vào: `build_basin_silver_service` và serializable request/result documents.
- Đầu ra: DAG ID `silver_basin_topology`, `schedule=None`, `catchup=False`, `max_active_runs=1`.

- [ ] **Bước 1: Viết kiểm thử cho contract DAG**

```python
def test_basin_dag_is_manual_and_single_run(dag_bag):
    dag = dag_bag.get_dag("silver_basin_topology")
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert {"discover_inputs", "compute_batches", "publish_outputs", "finalize_run"} <= set(dag.task_ids)
```

- [ ] **Bước 2: Chạy DAG và xác nhận đang fail**

Chạy: `pytest tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: FAIL vì DAG is absent.

- [ ] **Bước 3: Triển khai TaskFlow DAG mỏng**

Các task chỉ truyền document JSON/Pydantic qua XCom. `compute_batches` trả về path và số lượng, không truyền các dòng geometry. `publish_outputs` là writer duy nhất cho hai bảng basin nhỏ và chỉ chạy sau khi mọi compute batch hoàn tất.

Thêm các lệnh sau vào README:

```bash
docker compose exec airflow-api-server airflow dags trigger silver_basin_topology
docker compose exec trino trino --execute 'SELECT count(*) FROM lakehouse.silver.dim_basin'
```

- [ ] **Bước 4: Kiểm tra DAG và bộ kiểm thử domain**

Chạy: `pytest tests/contract/infra/test_silver_dags.py tests/unit/silver/basin tests/integration/silver/test_basin_pipeline.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit DAG và tài liệu**

```bash
git add airflow/dags/silver_basin_topology.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver basin topology DAG"
```

## Điểm kiểm tra

Dừng và kiểm tra `silver.dim_basin`, `silver.basin_edge`, Meta snapshot ref và lineage bằng Trino. Chưa bắt đầu domain Silver khác cho đến khi chấp nhận số dòng L12, kiểm tra cycle và hành vi skip khi chạy lại.
