# Kế hoạch triển khai Silver Population

> **Dành cho người triển khai:** dùng `executing-plans`; review theo thứ tự file.
> Checkbox chưa đánh dấu là chưa triển khai, không tự commit/merge.

**Mục tiêu:** chuẩn hóa WorldPop thành cell dân số có năm, revision và provenance.

**Kiến trúc:** DAG `silver_population` đọc raster URI từ Bronze/Meta, Python/Rasterio
đọc block và ghi Parquet staging, Spark kiểm tra/DQ/publish cell registry và value.
Không kéo toàn raster thành list trên Airflow, không phụ thuộc River hoặc Static Feature.

**Công nghệ:** Rasterio/PyArrow, Spark/Sedona, Iceberg, shared Spark staging/audit.

**Thiết kế:** [Exposure tách domain](../specs/2026-11-08-silver-exposure-split-design.vi.md).

## Ràng buộc

- Input pin `bronze.raster_coverage`, Meta chọn source/revision và AOI artifact.
  Năm tham chiếu đọc metadata sản phẩm; không lấy năm execution.
- Output `silver.population_grid` và phần WorldPop của `silver.source_grid`.
  Không ghi `basin_static_feature`, không tạo `gold.basin_exposure_snapshot` ở đây.
- Cell geometry/ID theo affine và global pixel index nguồn; crop AOI không đánh
  lại số từ 0. Scope membership có thể mở rộng mà cell ID vẫn giữ nguyên. Phải có
  provider alignment hoặc parent-grid/offset metadata nếu Raw đã crop; thiếu thì
  block registration thay vì tự xem crop là một grid nguồn mới.
- Cần contract `population_version` riêng khỏi `source_grid_version`; cùng grid/năm
  nhưng payload thay đổi tạo revision population mới, không giả grid geometry mới.
- Semantics cấu hình bắt buộc: count/cell hoặc density/unit diện tích. Density
  phải nhân diện tích cell metric để ra count, không cộng trực tiếp như count.
- `nodata` không tự thành 0. Chỉ chính sách cụ thể của đúng product mới cho phép
  diễn giải nodata; giữ evidence/policy version. Nhóm tuổi thiếu nguồn giữ null.
- AOI mặc định exposure Sơn La; chỉ đăng ký cell giao AOI, không sinh toàn lưới
  WorldPop quốc gia ở độ phân giải cao. Giá trị count thuộc toàn cell gốc; chưa
  nhân tỷ lệ AOI ở Silver để tránh chia diện tích lần hai khi Gold aggregate.
- Một writer cho `source_grid` dùng chung với weather. Đây là điều kiện rollout,
  không chỉ khóa riêng trong DAG Population.

## Flow và thứ tự đọc file

```text
DAG → airflow.py → runtime.py → pipeline.py
 → reader.py (coverage + URI + revision)
 → raster.py (block → Parquet staging)
 → Spark job → shared/spatial_grid.py + transform.py → quality.py
 → source_grid commit → population_grid commit → manifest → audit Meta
```

File domain dự kiến nằm trong `src/flashflood_data/pipelines/silver_population/`.

| Thứ tự | File | Chức năng | Caller / dependency |
| ---: | --- | --- | --- |
| 1 | `config.py` | `PopulationConfig`, `load_population_config(path)`; product/year/unit/nodata/block size/AOI | Runtime, raster worker, Spark job |
| 2 | `models.py` | `PopulationRasterInput`, `PopulationGridRow`, `PopulationStagingResult` | Reader/raster/transform |
| 3 | `reader.py` | `discover_population_inputs(pinned_inputs, config)` → raster input metadata | Runtime; Meta/Bronze readers |
| 4 | `raster.py` | `stage_population_raster(input, config, staging_context)` → manifest/URI/count | Runtime hoặc worker; Rasterio đọc block |
| 5 | `transform.py` | `build_population_frames(staged, config)` → grid frame và value frame | Spark job; shared spatial primitive |
| 6 | `quality.py` | `population_rules(config)` và integrity grid–value | Spark job; shared quality |
| 7 | `pipeline.py` | Signature, version và build/repair/skip; hash AOI/input semantics | Runtime |
| 8 | `spark_job.py` | `run(request)` đọc staging, DQ, serial publish, result manifest | Shared CLI |
| 9 | `runtime.py`, `airflow.py` | Factory `build_population_runtime(root=None)`, plan/stage/submit/audit, DAG factory | DAG entrypoint |
| 10 | `__init__.py`, `README.md` | Import nhẹ và hướng dẫn vận hành | Người đọc/DAG |

Tạo `config/pipelines/silver/population.yaml`, `config/quality/silver_population.yaml`,
`airflow/dags/silver_population.py`. Chỉ phần primitive thuần grid được dùng chung.

## Công việc 1 — Contract revision và primitive grid

**File tạo:** config/model/init và hai YAML; `src/flashflood_data/shared/spatial_grid.py`;
`tests/unit/shared/test_spatial_grid.py`,
`tests/unit/pipelines/silver_population/test_config.py`,
`tests/contract/storage/test_silver_population_schema.py`.
**File sửa:** `storage/iceberg_schemas.py`, `config/shared/metadata.yaml`,
`pipelines/weather_ingest/grids.py` trong `src/flashflood_data/`.

**Giao diện:** giữ nguyên API pure `GridDefinition`, `build_grid_rows`,
`grid_cell_center` hiện có; weather re-export để caller không đổi.
Không di chuyển writer `ensure_grid` vào common. Model WorldPop cho phép affine
native resolution; từ chối raster xoay/skew chưa hỗ trợ bằng lỗi rõ ràng.

- [ ] Test trước: grid weather cũ cho cùng ID/index/geometry; crop WorldPop không
  đổi global cell ID; đổi payload giữ grid version nhưng đổi population version.

```python
def test_population_has_value_revision():
    from flashflood_data.storage.iceberg_schemas import table_schema
    names = set(table_schema(("silver", "population_grid")).names)
    assert {"population_version", "reference_year", "source_object_id"} <= names
```

- [ ] Chạy `.venv/bin/pytest tests/unit/shared/test_spatial_grid.py tests/unit/pipelines/silver_population/test_config.py tests/contract/storage/test_silver_population_schema.py tests/unit/pipelines/weather_ingest/test_grids.py -q`; ghi nhận fail phần contract mới.
- [ ] Thêm schema/registry version và primitive; giữ regression weather pass.
  Nếu population table đã tồn tại, lập migration từ snapshot cũ thay vì drop.
- [ ] Chạy lại cùng lệnh pass; review trước đụng writer source_grid dùng chung.

## Công việc 2 — Reader, block raster và staging

**File:** tạo `reader.py`, `raster.py`, `transform.py`;
`tests/unit/pipelines/silver_population/test_raster.py`,
`tests/integration/silver/test_population_transform.py`.

**Giao diện:** `PopulationRasterInput` chứa URI, object ID/checksum, reference year,
native affine/CRS và semantics. `PopulationStagingResult` chỉ URI/manifest/count;
không chứa mọi cell value. Batch ID từ input/version/window xác định.

- [ ] Viết fixture raster 2×2, count hợp lệ, nodata, giá trị âm và crop nửa cell.

```python
def test_nodata_is_not_zero(population_rows):
    cells = {row.source_grid_id: row for row in population_rows}
    assert cells["fixture-nodata"].population_count is None
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_population/test_raster.py tests/integration/silver/test_population_transform.py -q`; fixture cung cấp grid ID trên, xác nhận fail trước triển khai.
- [ ] Đọc metadata trước, enumerate Rasterio windows giao AOI, stream Parquet theo
  block; worker không commit Iceberg. Bbox prune rồi exact cell/AOI intersection.
  Không resample count raster bằng bilinear. Giá trị nodata giữ null/quality.
- [ ] Transform tạo grid/value frame, kiểm global index/alignment; density đổi
  theo diện tích cell bằng CRS diện tích. Test count còn nguyên với cell cắt AOI,
  replay staging cùng hash reuse; cùng identity khác content phải conflict.

## Công việc 3 — DQ và writer grid dùng chung

**File:** tạo `quality.py`, `spark_job.py`;
`tests/unit/pipelines/silver_population/test_quality.py`,
`tests/integration/silver/test_population_publish.py`;
**sửa khi triển khai:** pool/guard source-grid trong weather ingest nếu chưa
có khóa liên pipeline, kèm `tests/contract/infra/test_dynamic_weather_dags.py`.

- [ ] Test trước: count âm/NaN reject, thiếu CRS fatal, nhóm tuổi không nguồn null,
  value mất grid FK bị chặn; partial commit grid rồi retry không nhân cell.
- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_population/test_quality.py tests/integration/silver/test_population_publish.py tests/contract/infra/test_dynamic_weather_dags.py -q`; xác nhận fail các case mới.
- [ ] Dùng chung guard `source_grid_writer` cho mọi commit vào `source_grid` từ
  weather và population, không giữ khóa qua fetch/raster compute. Task guard phải
  tương ứng thời gian commit thật; không chỉ đặt pool trên task không ghi bảng.
- [ ] Materialize/check schema và integrity trước publish. Grid identity phải
  immutable; chỉ union scope membership, không overwrite mất scope cũ. Publish grid
  trước value, chốt bundle sau đủ hai snapshot và quarantine. Chạy test pass.

## Công việc 4 — Runtime/DAG, recovery và nghiệm thu

**File:** tạo `pipeline.py`, `runtime.py`, `airflow.py`, README, DAG entrypoint;
`tests/unit/pipelines/silver_population/test_pipeline.py`,
`tests/contract/infra/test_silver_population_dag.py`.

- [ ] Test plan/skip/repair, build signature gồm AOI và payload revision; không
  bắt buộc River/Basin snapshot; XCom không chứa cell arrays.

```python
def test_population_dag_is_manual(dag):
    assert dag.dag_id == "silver_population"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_population tests/contract/infra/test_silver_population_dag.py -q`; xác nhận fail trước wire runtime.
- [ ] DAG plan → stage blocks → Spark publish → audit; manual, paused mặc định,
  apply đúng guard `source_grid_writer` và writer output. Retry đọc manifest trước
  compute; staging complete chưa đồng nghĩa Silver publish complete.
- [ ] Test pass; smoke bảng tạm rồi một AOI nhỏ. Đối soát tổng cell count của
  fixture, source_grid FK, population_version và null policy bằng Trino. Cập nhật
  README/performance log; rollout writer chung khi weather không đang ghi bảng.
