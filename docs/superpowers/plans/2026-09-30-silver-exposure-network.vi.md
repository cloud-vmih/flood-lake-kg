# Kế hoạch triển khai mạng lưới phơi nhiễm ở Silver


**Bản gốc:** `docs/superpowers/plans/2026-09-30-silver-exposure-network.md`

> **Bản tiếng Việt:** Tên file code, class, hàm, bảng, field và lệnh được giữ nguyên để khớp repository.

> **Dành cho người triển khai:** KỸ NĂNG BẮT BUỘC: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng ô đánh dấu (`- [ ]`) để theo dõi.

**Mục tiêu:** Xây dựng cơ sở thiết yếu, cell dân số, đồ thị đường, quan hệ với basin và giao cắt đường–sông có version từ OSM và WorldPop ở Bronze.

**Kiến trúc:** Chuẩn hóa tập OSM tag đã duyệt qua các transform facility và road riêng, đăng ký grid WorldPop bằng dịch vụ identity grid dùng chung, rồi tính quan hệ không gian với version basin và river chuẩn. Tách bước tính toán theo nhóm output nhưng mỗi bảng Iceberg vẫn chỉ có một writer.

**Công nghệ:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, PyArrow, Rasterio, GeoPandas, Shapely, PyProj, NetworkX, PyIceberg, pytest.

**Tài liệu thiết kế:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.vi.md`

## Ràng buộc chung

- Yêu cầu đã có output của nền tảng dùng chung, basin topology và river network.
- Chỉ các nhóm OSM phục vụ lũ đã duyệt trong `config/bronze/osm.yaml` được đưa vào Silver.
- Capacity của facility và nhóm dân số con giữ null nếu nguồn không cung cấp trực tiếp.
- ID đồ thị đường ổn định theo OSM snapshot và graph-processing version.
- Phân loại bridge/ford dựa trên bằng chứng OSM; giao cắt chỉ suy từ geometry có loại `unknown`.
- Population dùng identity `source_grid` chung; không tạo grid registry thứ hai.
- Khoảng cách/chiều dài/diện tích dùng CRS chiếu phù hợp.
- Mỗi bảng output chỉ có một writer task.

---

## Cấu trúc file

```text
config/silver/exposure_network.yaml
airflow/dags/silver_exposure_network.py
src/flashflood_data/orchestration/spatial_grid.py
src/flashflood_data/orchestration/silver/exposure/
  __init__.py
  config.py
  models.py
  reader.py
  osm_tags.py
  facilities.py
  roads.py
  population.py
  basin_overlay.py
  crossings.py
  quality.py
  service.py
  factory.py
tests/unit/silver/exposure/
  test_config.py
  test_osm_tags.py
  test_facilities.py
  test_roads.py
  test_population.py
  test_basin_overlay.py
  test_crossings.py
  test_quality.py
  test_service.py
tests/integration/silver/test_exposure_pipeline.py
```

`reader.py` là file domain duy nhất đọc Iceberg/object storage. Transform thuần chỉ nhận model. `service.py` điều phối các transform và phần publish chung. Weather và exposure dùng chung `orchestration/spatial_grid.py`; hai domain không import lẫn nhau.

### Công việc 1: Dùng chung hóa đăng ký source grid và thêm contract exposure

**Các file:**
- Tạo: `src/flashflood_data/orchestration/spatial_grid.py`
- Sửa: `src/flashflood_data/orchestration/weather/grids.py`
- Sửa: `tests/unit/weather/test_grids.py`
- Tạo: `config/silver/exposure_network.yaml`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/__init__.py`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/config.py`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/models.py`
- Sửa: `src/flashflood_data/storage/iceberg_schemas.py`
- Sửa: `config/meta/static.yaml`
- Sửa: `tests/contract/storage/test_meta_bronze_schemas.py`
- Tạo: `tests/unit/silver/exposure/test_config.py`

**Giao diện:**
- Đầu ra: source-independent `GridDefinition`, `GridCell`, `SourceGridRegistrar` in `orchestration.spatial_grid`.
- Giữ tương thích: các import hiện có từ `orchestration.weather.grids` thông qua re-export rõ ràng.
- Đầu ra: `ExposureConfig` và row models for all eight exposure tables.
- Produces physical facility, facility-basin, population, road-node, road-edge, road-basin, và river-crossing tables.

- [ ] **Bước 1: Viết kiểm thử cho tương thích và schema**

```python
def test_weather_grid_public_api_is_preserved():
    from flashflood_data.orchestration.weather.grids import GridDefinition
    from flashflood_data.orchestration.spatial_grid import GridDefinition as Shared
    assert GridDefinition is Shared

def test_exposure_config_facilities_are_explicit():
    config = load_exposure_config(CONFIG)
    assert {"hospital", "clinic", "shelter", "fire_station", "police", "school"} <= set(config.facility_types)

def test_road_edge_schema_keeps_bridge_and_tunnel():
    names = set(table_schema(("silver", "dim_road_edge")).names)
    assert {"bridge", "tunnel", "oneway", "highway_class", "source_object_id"} <= names
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/weather/test_grids.py tests/unit/silver/exposure/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: FAIL vì shared module và exposure contracts are absent.

- [ ] **Bước 3: Chuyển grid primitive không đổi hành vi và định nghĩa contract**

Chuyển identity grid ổn định, cách đánh địa chỉ row/column, cell geometry và logic registry từ `weather/grids.py` sang `orchestration/spatial_grid.py`. Giữ re-export trong weather để caller hiện có không phải đổi trong cùng công việc.

YAML exposure khai báo mapping tag facility, road class được phép, tốc độ mặc định theo class, graph/geometry version, ngữ nghĩa source/year/value của population, CRS diện tích/khoảng cách và batch size. Thêm chính xác field và primary key đã duyệt trong `data.md` vào schema vật lý và dataset registry.

- [ ] **Bước 4: Chạy tương thích và contract**

Chạy: `pytest tests/unit/weather/test_grids.py tests/unit/silver/exposure/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit grid extraction và contracts**

```bash
git add src/flashflood_data/orchestration/spatial_grid.py src/flashflood_data/orchestration/weather/grids.py config/silver/exposure_network.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/exposure src/flashflood_data/storage/iceberg_schemas.py tests/unit/weather/test_grids.py tests/unit/silver/exposure/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "refactor: share source grid registration"
```

### Công việc 2: Chuẩn hóa facility OSM và đồ thị đường

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/exposure/reader.py`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/osm_tags.py`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/facilities.py`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/roads.py`
- Tạo: `tests/unit/silver/exposure/test_osm_tags.py`
- Tạo: `tests/unit/silver/exposure/test_facilities.py`
- Tạo: `tests/unit/silver/exposure/test_roads.py`

**Giao diện:**
- Đầu vào: exact `bronze.osm_feature_raw` snapshot.
- Đầu ra: `classify_osm_feature(tags, config) -> OSMClassification | None`.
- Đầu ra: `build_facilities(rows: Sequence[Mapping[str, object]], context: ExposureContext) -> list[FacilityRow]`.
- Đầu ra: `build_road_graph(rows: Sequence[Mapping[str, object]], context: ExposureContext) -> tuple[list[RoadNodeRow], list[RoadEdgeRow]]`.

- [ ] **Bước 1: Viết kiểm thử cho tag, facility và graph**

```python
def test_hospital_and_shelter_are_kept_but_shop_is_dropped():
    assert classify_osm_feature({"amenity": "hospital"}, config).facility_type == "hospital"
    assert classify_osm_feature({"emergency": "shelter"}, config).facility_type == "shelter"
    assert classify_osm_feature({"shop": "mall"}, config) is None

def test_oneway_road_has_one_directed_edge():
    nodes, edges = build_road_graph([osm_way(tags={"highway": "primary", "oneway": "yes"})], context)
    assert len(edges) == 1
    assert edges[0].oneway is True

def test_capacity_is_not_inferred_from_name():
    assert build_facilities([hospital_without_capacity()], context)[0].capacity is None
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/exposure/test_osm_tags.py tests/unit/silver/exposure/test_facilities.py tests/unit/silver/exposure/test_roads.py -q`  
Mong đợi: FAIL vì OSM transforms are absent.

- [ ] **Bước 3: Triển khai mapping đã duyệt và identity graph ổn định**

Parse `tags_json` một lần và chỉ giữ class đã cấu hình. Facility ID hash OSM type/ID cùng facility type; version hash OSM snapshot cùng mapping version. Tách road way tại endpoint và junction, giữ `osm_way_id`, chuẩn hóa `oneway`, `bridge`, `tunnel`, `surface`, tính chiều dài mét và chỉ suy ra base travel time từ tốc độ class/surface đã cấu hình.

- [ ] **Bước 4: Chạy kiểm thử transform OSM**

Chạy: `pytest tests/unit/silver/exposure/test_osm_tags.py tests/unit/silver/exposure/test_facilities.py tests/unit/silver/exposure/test_roads.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit OSM transforms**

```bash
git add src/flashflood_data/orchestration/silver/exposure/reader.py src/flashflood_data/orchestration/silver/exposure/osm_tags.py src/flashflood_data/orchestration/silver/exposure/facilities.py src/flashflood_data/orchestration/silver/exposure/roads.py tests/unit/silver/exposure/test_osm_tags.py tests/unit/silver/exposure/test_facilities.py tests/unit/silver/exposure/test_roads.py
git commit -m "feat: normalize flood exposure OSM features"
```

### Công việc 3: Đăng ký cell WorldPop và tạo population grid

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/exposure/population.py`
- Tạo: `tests/unit/silver/exposure/test_population.py`

**Giao diện:**
- Đầu vào: WorldPop `bronze.raster_coverage`, Raw object metadata, `SourceGridRegistrar`.
- Đầu ra: `build_population_rows(raster: PopulationRaster, context: ExposureContext) -> tuple[list[GridCell], list[PopulationGridRow]]`.

- [ ] **Bước 1: Viết kiểm thử cho identity raster và value**

```python
def test_worldpop_cell_matches_registered_source_grid():
    grid, population = build_population_rows(worldpop_2x2(), context)
    assert population[0].source_grid_id in {cell.source_grid_id for cell in grid}
    assert population[0].reference_year == 2025

def test_demographic_subgroups_remain_null_without_source_bands():
    row = build_population_rows(worldpop_total_only(), context)[1][0]
    assert row.children_population is None
    assert row.elderly_population is None
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/exposure/test_population.py -q`  
Mong đợi: FAIL vì population conversion is absent.

- [ ] **Bước 3: Triển khai identity cell raster neo toàn cục**

Dùng raster transform, kích thước, CRS và checksum để định nghĩa `source_grid_version`. Tạo row/column/cell ID ổn định từ index của raster nguồn đầy đủ, không đánh số dày lại theo AOI. Đăng ký cell qua registrar chung, giữ nodata của provider và map tổng dân số/count hoặc density đúng cấu hình. Lấy `available_at` từ metadata nguồn.

- [ ] **Bước 4: Chạy kiểm thử hồi quy population và weather grid**

Chạy: `pytest tests/unit/silver/exposure/test_population.py tests/unit/weather/test_grids.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit population conversion**

```bash
git add src/flashflood_data/orchestration/silver/exposure/population.py tests/unit/silver/exposure/test_population.py
git commit -m "feat: register WorldPop population cells"
```

### Công việc 4: Thêm quan hệ basin, giao cắt sông và cổng chất lượng

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/exposure/basin_overlay.py`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/crossings.py`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/quality.py`
- Tạo: `tests/unit/silver/exposure/test_basin_overlay.py`
- Tạo: `tests/unit/silver/exposure/test_crossings.py`
- Tạo: `tests/unit/silver/exposure/test_quality.py`

**Giao diện:**
- Đầu ra: `assign_facilities_to_basins`, `overlay_roads_with_basins`, `detect_river_crossings`.
- Đầu ra: `check_exposure_outputs(outputs: ExposureOutputs, config: ExposureConfig) -> list[QualityResult]`.

- [ ] **Bước 1: Viết kiểm thử cho quan hệ không gian và crossing**

```python
def test_facility_inside_basin_gets_stream_distance():
    row = assign_facilities_to_basins([facility()], [basin()], [nearby_stream()], config)[0]
    assert row.spatial_relation == "within"
    assert row.distance_to_stream_m >= 0

def test_bridge_tag_classifies_geometric_crossing():
    crossing = detect_river_crossings([bridge_edge()], [crossing_reach()], config)[0]
    assert crossing.crossing_type == "bridge"

def test_untagged_intersection_remains_unknown():
    assert detect_river_crossings([plain_edge()], [crossing_reach()], config)[0].crossing_type == "unknown"
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/exposure/test_basin_overlay.py tests/unit/silver/exposure/test_crossings.py tests/unit/silver/exposure/test_quality.py -q`  
Mong đợi: FAIL vì spatial modules are absent.

- [ ] **Bước 3: Triển khai phép join có index và DQ rõ ràng**

Dùng spatial index và tọa độ chiếu. Gán facility xử lý point/centroid polygon theo cấu hình và tính khoảng cách tới stream/outlet. Road overlay lưu chiều dài giao dương và ratio. Crossing ID hash road/river version cùng tọa độ giao đã snap. Fatal rule kiểm tra khóa trùng, thiếu endpoint graph, geometry lỗi, ratio ngoài miền, basin/river ID mồ côi và population không hợp lệ; facility/crossing mơ hồ ở biên là warning.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/exposure/test_basin_overlay.py tests/unit/silver/exposure/test_crossings.py tests/unit/silver/exposure/test_quality.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit relations và DQ**

```bash
git add src/flashflood_data/orchestration/silver/exposure/basin_overlay.py src/flashflood_data/orchestration/silver/exposure/crossings.py src/flashflood_data/orchestration/silver/exposure/quality.py tests/unit/silver/exposure/test_basin_overlay.py tests/unit/silver/exposure/test_crossings.py tests/unit/silver/exposure/test_quality.py
git commit -m "feat: relate exposure assets to basins and rivers"
```

### Công việc 5: Thêm service, DAG, kiểm thử tích hợp và tài liệu

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/exposure/service.py`
- Tạo: `src/flashflood_data/orchestration/silver/exposure/factory.py`
- Tạo: `airflow/dags/silver_exposure_network.py`
- Tạo: `tests/unit/silver/exposure/test_service.py`
- Tạo: `tests/integration/silver/test_exposure_pipeline.py`
- Sửa: `tests/contract/infra/test_silver_dags.py`
- Sửa: `README.md`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`

**Giao diện:**
- Đầu ra: `ExposureSilverService.plan`, family-specific compute methods, `publish`, `run`.
- Đầu ra: `build_exposure_silver_service(root=None)`.
- Produces manual DAG ID `silver_exposure_network`.

- [ ] **Bước 1: Viết kiểm thử cho service ownership và DAG**

```python
def test_exposure_service_publishes_only_owned_tables(service):
    result = service.run("exposure-1")
    assert {output.table_name for output in result.outputs} == EXPECTED_EXPOSURE_TABLES

def test_exposure_dag_declares_bounded_compute_groups(dag_bag):
    dag = dag_bag.get_dag("silver_exposure_network")
    assert dag.schedule is None
    assert {"compute_facilities", "compute_roads", "compute_population",
            "compute_crossings", "publish_outputs"} <= set(dag.task_ids)
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/exposure/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: FAIL vì service/DAG are absent.

- [ ] **Bước 3: Triển khai tính toán theo nhóm output và publish tuần tự**

Tìm đúng snapshot OSM, WorldPop, basin và river. Stage riêng từng nhóm output. Publish phần bổ sung `silver.source_grid` trước population; publish dimension facility/road trước các bảng quan hệ; publish crossing sau khi ID road/river đã hợp lệ. Một task publish duyệt các bảng theo thứ tự dependency này và ghi snapshot/lineage cho từng bảng.

- [ ] **Bước 4: Chạy kiểm thử domain và hồi quy weather**

Chạy: `pytest tests/unit/silver/exposure tests/integration/silver/test_exposure_pipeline.py tests/unit/weather/test_grids.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit pipeline**

```bash
git add src/flashflood_data/orchestration/silver/exposure src/flashflood_data/orchestration/spatial_grid.py src/flashflood_data/orchestration/weather/grids.py airflow/dags/silver_exposure_network.py tests/unit/silver/exposure tests/integration/silver/test_exposure_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver exposure network DAG"
```

## Điểm kiểm tra

Chạy fixture OSM/WorldPop nhỏ và kiểm tra từng nhóm bảng trong Trino. Xác nhận cách chọn tag facility, tính liên thông graph, identity grid WorldPop, quan hệ basin và phân loại crossing bridge/unknown trước khi xử lý toàn bộ exposure AOI.
