# Kế hoạch triển khai đặc trưng tĩnh theo lưu vực ở Silver


**Bản gốc:** `docs/superpowers/plans/2026-09-30-silver-basin-static-features.md`

> **Bản tiếng Việt:** Tên file code, class, hàm, bảng, field và lệnh được giữ nguyên để khớp repository.

> **Dành cho người triển khai:** KỸ NĂNG BẮT BUỘC: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng ô đánh dấu (`- [ ]`) để theo dõi.

**Mục tiêu:** Tạo bộ đặc trưng DEM, thủy văn, SoilGrids, lớp phủ đất và BasinATLAS có version cho từng lưu vực L12, kèm lineage đầy đủ tới asset nguồn và asset dẫn xuất.

**Kiến trúc:** Đọc geometry lưu vực và inventory raster Bronze, tính các batch đặc trưng riêng cho từng nguồn, ghép thành một dòng cho mỗi basin/build rồi publish bằng một writer cho mỗi bảng. Raster flow direction và flow accumulation được lưu thành object dẫn xuất trong MinIO và đăng ký ở Meta riêng, không ghi nhầm thành object Raw.

**Công nghệ:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, Rasterio, NumPy, SciPy, GeoPandas, PyProj, PyArrow, PyIceberg, MinIO, pytest.

**Tài liệu thiết kế:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.vi.md`

## Ràng buộc chung

- Yêu cầu plan nền tảng dùng chung và basin topology đã hoàn tất.
- DEM là nguồn chính cho địa hình; các field terrain/runoff/discharge của BasinATLAS chỉ dùng QA/tham chiếu.
- SoilGrids 0–30 cm dùng trọng số độ dày 5/10/15 cm và chuyển scale/unit riêng theo từng property.
- `awc_m3m3_0_30 = field_capacity_m3m3_0_30 - wilting_point_m3m3_0_30` và không được âm sau QA.
- Channel slope dùng cao độ đầu nguồn/cửa ra của longest flow path, không dùng mean terrain slope.
- Identity của raster dẫn xuất gồm snapshot/object ID DEM đầu vào, cấu hình xử lý và checksum.
- Tính toán fan-out theo batch basin; mỗi bảng Iceberg được commit bởi một writer task.

---

## Cấu trúc file

```text
config/silver/basin_static_features.yaml
airflow/dags/silver_basin_static_features.py
src/flashflood_data/orchestration/silver/static_features/
  __init__.py
  config.py
  models.py
  reader.py
  raster_inputs.py
  terrain.py
  hydrology.py
  soil.py
  landcover.py
  atlas.py
  assemble.py
  lineage.py
  quality.py
  service.py
  factory.py
tests/unit/silver/static_features/
  test_config.py
  test_raster_inputs.py
  test_terrain.py
  test_hydrology.py
  test_soil.py
  test_landcover.py
  test_atlas.py
  test_assemble.py
  test_lineage.py
  test_quality.py
  test_service.py
tests/integration/silver/test_static_features_pipeline.py
```

`service.py` gọi reader và calculator riêng theo nguồn, sau đó gọi `assemble.py`, `lineage.py` và `quality.py`. Calculator có thể import kernel thuần đã có test trong `src/flashflood_data/static/features/`; không được import `static/workflow/runner.py`, handler hoặc state file-first local.

### Công việc 1: Hoàn thiện contract asset dẫn xuất và đăng ký schema

**Các file:**
- Sửa: `docs/schema_contract/data.md`
- Tạo: `config/silver/basin_static_features.yaml`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/__init__.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/config.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/models.py`
- Sửa: `src/flashflood_data/storage/iceberg_schemas.py`
- Sửa: `config/meta/static.yaml`
- Sửa: `tests/contract/storage/test_meta_bronze_schemas.py`
- Tạo: `tests/unit/silver/static_features/test_config.py`

**Giao diện:**
- Đầu ra: physical `meta.derived_objects`, `silver.basin_static_feature`, và `silver.basin_feature_lineage` tables.
- Đầu ra: `StaticFeatureConfig`, `PartialFeatureRow`, `BasinStaticFeatureRow`, `FeatureLineageRow`, `DerivedObjectRow`.
- Changes lineage key to include `object_kind: Literal["source", "derived"]`.

- [ ] **Bước 1: Viết kiểm thử ban đầu cho contract**

```python
def test_derived_object_registry_is_separate_from_raw_inventory():
    schema = table_schema(("meta", "derived_objects"))
    assert {"derived_object_id", "object_uri", "checksum", "processing_version",
            "pipeline_run_id", "created_at"} <= set(schema.names)

def test_feature_lineage_disambiguates_object_kind():
    schema = table_schema(("silver", "basin_feature_lineage"))
    assert schema.field("object_kind").nullable is False
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/static_features/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: FAIL vì Silver/derived contracts are absent.

- [ ] **Bước 3: Định nghĩa registry asset dẫn xuất và cấu hình**

Thêm contract logic sau vào `data.md` và schema Arrow vật lý tương ứng:

```text
meta.derived_objects
  derived_object_id string ! PK
  object_uri string !
  media_type string !
  size_bytes long !
  checksum_algorithm string !
  checksum string !
  role string !
  processing_version string !
  pipeline_run_id string !
  created_at timestamp !
```

Thêm `object_kind` vào `basin_feature_lineage`; `object_id` trỏ tới `meta.source_objects` khi là `source`, và trỏ tới `meta.derived_objects.derived_object_id` khi là `derived`.

YAML phải khai báo chính xác trọng số độ sâu, scale factor, ngưỡng stream, sai số snap outlet, version thuật toán thủy văn, phương pháp/version Tc, beta `[0.5, 1.0, 2.0]`, cách resample raster theo property, output CRS và kích thước batch basin. Từ chối bộ trọng số có tổng độ dày khác 30 cm.

- [ ] **Bước 4: Chạy kiểm thử contract**

Chạy: `pytest tests/unit/silver/static_features/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit contracts**

```bash
git add docs/schema_contract/data.md config/silver/basin_static_features.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/static_features src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/static_features/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define static feature and derived asset contracts"
```

### Công việc 2: Xử lý raster đầu vào và tính đặc trưng ngoài thủy văn

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/static_features/reader.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/raster_inputs.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/terrain.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/soil.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/landcover.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/atlas.py`
- Tạo: `tests/unit/silver/static_features/test_raster_inputs.py`
- Tạo: `tests/unit/silver/static_features/test_terrain.py`
- Tạo: `tests/unit/silver/static_features/test_soil.py`
- Tạo: `tests/unit/silver/static_features/test_landcover.py`
- Tạo: `tests/unit/silver/static_features/test_atlas.py`

**Giao diện:**
- Đầu vào: `silver.dim_basin`, `bronze.raster_coverage`, BasinATLAS Bronze rows, Meta source objects.
- Đầu ra: `StaticFeatureInputs` grouped by basin/source object.
- Đầu ra: `compute_terrain`, `compute_soil_0_30`, `compute_landcover`, và `map_atlas_fields`, each returning `PartialFeatureRow` plus source contributions.

- [ ] **Bước 1: Viết kiểm thử cho phép tính dựa trên fixture**

```python
def test_soil_0_30_uses_thickness_weights_and_scale():
    result = compute_soil_0_30(clay_layers(values=(100, 200, 300)), config)
    assert result.values["soil_clay_pct_0_30"] == pytest.approx(
        ((100 * 5 + 200 * 10 + 300 * 15) / 30) / 10
    )

def test_terrain_uses_only_valid_pixels():
    result = compute_terrain(raster=[[100, 200], [NODATA, 300]], basin=full_extent())
    assert result.values["elevation_mean_m"] == 200
    assert result.values["relief_m"] == 200

def test_atlas_mapper_drops_unapproved_columns():
    assert "cly_pc_sav" not in map_atlas_fields(atlas_row_with_all_fields(), config).values
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/static_features/test_raster_inputs.py tests/unit/silver/static_features/test_terrain.py tests/unit/silver/static_features/test_soil.py tests/unit/silver/static_features/test_landcover.py tests/unit/silver/static_features/test_atlas.py -q`  
Mong đợi: FAIL vì modules chưa tồn tại.

- [ ] **Bước 3: Triển khai reader gắn với snapshot và các phép tính thuần**

Tìm từng raster qua `bronze.raster_coverage.object_uri`, kiểm tra URI/checksum khớp source object đã đăng ký, chỉ mở window của basin, chuyển geometry basin sang CRS raster và mask nodata trước khi tính thống kê.

Dùng mapping property → output rõ ràng:

```python
SOIL_OUTPUTS = {
    "clay": "soil_clay_pct_0_30", "sand": "soil_sand_pct_0_30",
    "silt": "soil_silt_pct_0_30", "bdod": "bulk_density_kg_dm3_0_30",
    "cfvo": "coarse_fragments_pct_0_30", "soc": "soil_organic_carbon_gkg_0_30",
    "wv0033": "field_capacity_m3m3_0_30", "wv1500": "wilting_point_m3m3_0_30",
}
```

Chỉ tính AWC sau khi chuyển đổi FC/WP. Giữ Q05/Q50/Q95 theo từng property cho đến khi tạo `soil_uncertainty_ratio` theo cấu hình. Phần trăm lớp phủ đất dùng diện tích basin hợp lệ làm mẫu số.

- [ ] **Bước 4: Chạy kiểm thử phép tính**

Chạy: `pytest tests/unit/silver/static_features/test_raster_inputs.py tests/unit/silver/static_features/test_terrain.py tests/unit/silver/static_features/test_soil.py tests/unit/silver/static_features/test_landcover.py tests/unit/silver/static_features/test_atlas.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit source calculators**

```bash
git add src/flashflood_data/orchestration/silver/static_features tests/unit/silver/static_features
git commit -m "feat: compute static raster and Atlas features"
```

### Công việc 3: Tính thủy văn và publish raster dẫn xuất an toàn

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/static_features/hydrology.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/lineage.py`
- Tạo: `tests/unit/silver/static_features/test_hydrology.py`
- Tạo: `tests/unit/silver/static_features/test_lineage.py`

**Giao diện:**
- Đầu vào: conditioned DEM window, basin polygon, hydrology config, object-store publisher.
- Đầu ra: `compute_hydrology(dem: RasterWindow, basin: BasinRow, config: StaticFeatureConfig, publisher: DerivedObjectPublisher) -> HydrologyResult`.
- Đầu ra: deterministic `register_derived_object(row) -> int | None` và lineage rows with `object_kind`.

- [ ] **Bước 1: Viết kiểm thử cho DEM tổng hợp và identity**

```python
def test_hydrology_uses_longest_path_for_channel_slope():
    result = compute_hydrology(synthetic_sloping_dem(), basin(), config)
    expected = (result.features["source_elevation_m"] -
                result.features["outlet_elevation_m"]) / result.features["longest_flow_path_m"]
    assert result.features["main_channel_slope_m_m"] == pytest.approx(expected)

def test_same_derived_payload_reuses_identity():
    assert derived_identity(input_ids, config, CHECKSUM) == derived_identity(input_ids, config, CHECKSUM)
```

Kiểm thử thêm việc snap outlet, Tc dương, Kb bằng `0.5/1/2 × Tc`, và từ chối channel phẳng hoặc dài bằng 0 thay vì âm thầm chia cho 0.

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/static_features/test_hydrology.py tests/unit/silver/static_features/test_lineage.py -q`  
Mong đợi: FAIL vì hydrology/derived lineage modules chưa tồn tại.

- [ ] **Bước 3: Triển khai kết quả thủy văn và publish object nguyên tử**

Condition DEM, tính flow direction/accumulation, tạo stream mask, snap outlet, truy vết đường thượng nguồn dài nhất, lấy cao độ hai đầu và tính channel slope/Tc/Kb. Publish GeoTIFF tới:

```text
derived/static/hydrology/<feature_build_version>/<basin_id>/flow_direction.tif
derived/static/hydrology/<feature_build_version>/<basin_id>/flow_accumulation.tif
```

Upload qua staging, kiểm tra checksum rồi copy sang key cuối. Chỉ đăng ký `meta.derived_objects` sau khi MinIO xác nhận object cuối. Dùng lineage `source` cho đầu vào DEM/soil/landcover/Atlas và lineage `derived` cho hai raster thủy văn.

- [ ] **Bước 4: Chạy kiểm thử thủy văn**

Chạy: `pytest tests/unit/silver/static_features/test_hydrology.py tests/unit/silver/static_features/test_lineage.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit hydrology và lineage**

```bash
git add src/flashflood_data/orchestration/silver/static_features/hydrology.py src/flashflood_data/orchestration/silver/static_features/lineage.py tests/unit/silver/static_features/test_hydrology.py tests/unit/silver/static_features/test_lineage.py
git commit -m "feat: derive and register basin hydrology assets"
```

### Công việc 4: Ghép các dòng và kiểm soát chất lượng đặc trưng tĩnh

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/static_features/assemble.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/quality.py`
- Tạo: `tests/unit/silver/static_features/test_assemble.py`
- Tạo: `tests/unit/silver/static_features/test_quality.py`

**Giao diện:**
- Đầu vào: toàn bộ value/contribution `PartialFeatureRow` của một basin/build.
- Đầu ra: `assemble_feature(partials: Sequence[PartialFeatureRow], context: FeatureBuildContext) -> tuple[BasinStaticFeatureRow, list[FeatureLineageRow]]`.
- Đầu ra: `check_static_features(rows: Sequence[BasinStaticFeatureRow], lineage: Sequence[FeatureLineageRow]) -> list[QualityResult]`.

- [ ] **Bước 1: Viết kiểm thử cho ghép dữ liệu và DQ**

```python
def test_assemble_rejects_two_values_for_same_field():
    with pytest.raises(ValueError, match="conflicting feature"):
        assemble_feature([partial("elevation_mean_m", 100), partial("elevation_mean_m", 120)], context)

def test_negative_awc_is_fatal():
    failures = fatal_failures(check_static_features([feature(field_capacity=0.1, wilting_point=0.2)], lineage))
    assert any(item.rule_id == "static_awc_nonnegative" for item in failures)
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/static_features/test_assemble.py tests/unit/silver/static_features/test_quality.py -q`  
Mong đợi: FAIL vì assembler và DQ chưa tồn tại.

- [ ] **Bước 3: Triển khai phép ghép xác định và các rule có tên**

Yêu cầu đúng một feature row cho mỗi `(basin_id, basin_version, feature_build_version)`. Fatal check bao gồm khóa trùng, thiếu basin, thứ tự cao độ vô lý, flow length không dương, outlet ngoài sai số snap, channel slope/AWC âm, phần trăm không hợp lệ, thiếu source role bắt buộc và thiếu object lineage. Chênh lệch với Atlas hoặc độ phủ raster thấp là warning có giá trị quan sát/kỳ vọng.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/static_features/test_assemble.py tests/unit/silver/static_features/test_quality.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit ghép dữ liệu và DQ**

```bash
git add src/flashflood_data/orchestration/silver/static_features/assemble.py src/flashflood_data/orchestration/silver/static_features/quality.py tests/unit/silver/static_features/test_assemble.py tests/unit/silver/static_features/test_quality.py
git commit -m "feat: assemble and validate basin static features"
```

### Công việc 5: Thêm service, DAG theo batch, kiểm thử tích hợp và tài liệu

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/static_features/service.py`
- Tạo: `src/flashflood_data/orchestration/silver/static_features/factory.py`
- Tạo: `airflow/dags/silver_basin_static_features.py`
- Tạo: `tests/unit/silver/static_features/test_service.py`
- Tạo: `tests/integration/silver/test_static_features_pipeline.py`
- Sửa: `tests/contract/infra/test_silver_dags.py`
- Sửa: `README.md`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`

**Giao diện:**
- Đầu ra: `StaticFeatureSilverService.plan`, `compute_batch`, `publish`, `run`.
- Đầu ra: `build_static_feature_service(root=None)`.
- Đầu ra: manual DAG ID `silver_basin_static_features`.

- [ ] **Bước 1: Viết kiểm thử cho batch, repair và DAG**

```python
def test_compute_batches_do_not_commit_iceberg(service):
    service.compute_batch(request, basin_ids=("b1", "b2"))
    service.dependencies.store.upsert_keyed_rows.assert_not_called()

def test_publish_commits_each_table_once(service):
    service.publish(request)
    assert service.dependencies.store.upsert_keyed_rows.call_count == 2

def test_static_feature_dag_is_manual(dag_bag):
    assert dag_bag.get_dag("silver_basin_static_features").schedule is None
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/static_features/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: FAIL vì service/DAG chưa tồn tại.

- [ ] **Bước 3: Triển khai compute fan-out và publish fan-in**

Task graph:

```text
discover_inputs -> plan_basin_batches -> compute_batch.expand
  -> validate_staged_batches -> publish_feature_tables
  -> audit_and_finalize -> cleanup_staging
```

Use keys:

```python
FEATURE_KEY = ("basin_id", "basin_version", "feature_build_version")
LINEAGE_KEY = ("basin_id", "basin_version", "feature_build_version", "object_kind", "object_id", "role")
```

Fixture integration dùng hai basin nhỏ và các raster DEM/soil/landcover nhỏ. Kiểm tra chính xác số dòng, dòng registry dẫn xuất, key MinIO, snapshot ref, lineage, skip khi chạy lại và repair khi thiếu một output.

- [ ] **Bước 4: Chạy kiểm thử domain**

Chạy: `pytest tests/unit/silver/static_features tests/integration/silver/test_static_features_pipeline.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Chạy kiểm thử hồi quy các kernel static**

Chạy: `pytest tests/unit/static/test_terrain_features.py tests/unit/static/test_hydrology_features.py tests/unit/static/test_soil_features.py tests/unit/static/test_landcover_features.py -q`  
Mong đợi: PASS.

- [ ] **Bước 6: Commit pipeline**

```bash
git add src/flashflood_data/orchestration/silver/static_features airflow/dags/silver_basin_static_features.py tests/unit/silver/static_features tests/integration/silver/test_static_features_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver basin static feature DAG"
```

## Điểm kiểm tra

Dừng sau run hai basin. Review từng công thức/unit, kiểm tra hai raster dẫn xuất cùng Meta registry/lineage, rồi đối chiếu field từ DEM với field QA BasinATLAS trước khi chạy toàn bộ basin.
