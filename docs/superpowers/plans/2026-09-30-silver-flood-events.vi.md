# Kế hoạch triển khai sự kiện lũ ở Silver


**Bản gốc:** `docs/superpowers/plans/2026-09-30-silver-flood-events.md`

> **Bản tiếng Việt:** Tên file code, class, hàm, bảng, field và lệnh được giữ nguyên để khớp repository.

> **Dành cho người triển khai:** KỸ NĂNG BẮT BUỘC: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng ô đánh dấu (`- [ ]`) để theo dõi.

**Mục tiêu:** Chuẩn hóa các record sự kiện lũ có cấu trúc ở Bronze thành event có revision và liên kết mỗi event với một hoặc nhiều basin L12 có version.

**Kiến trúc:** Dùng identity nguồn/object để giữ event ID ổn định, chỉ tăng revision khi nội dung chuẩn hóa thay đổi và gán basin từ footprint đã xác minh, điểm được báo cáo hoặc tham chiếu hành chính đã cấu hình. Pipeline này không có parse tài liệu, chunk, embedding, bảng bridge evidence hay Qdrant.

**Công nghệ:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, PyArrow, Shapely, GeoPandas, PyProj, PyIceberg, pytest.

**Tài liệu thiết kế:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.vi.md`

## Ràng buộc chung

- Yêu cầu nền tảng dùng chung, `silver.dim_basin` và pipeline landing/Bronze flood event riêng tạo `bronze.historical_event_raw`.
- Chỉ tạo hai output `silver.observed_flood_event` và `silver.event_basin`.
- `source_document_id` và `evidence_id` giữ null trong giai đoạn này.
- Không tạo code runtime cho `source_document`, `document_chunk`, `event_evidence`, embedding hoặc Qdrant.
- Điểm hoặc tham chiếu hành chính có thể liên kết basin nhưng không được sinh diện tích ảnh hưởng.
- Chỉ tính diện tích ảnh hưởng cho `flood_footprint` đã xác minh trong CRS bảo toàn diện tích.
- Mỗi bảng đích chỉ có một writer task.

---

## Cấu trúc file

```text
config/silver/flood_events.yaml
airflow/dags/silver_flood_events.py
src/flashflood_data/orchestration/silver/events/
  __init__.py
  config.py
  models.py
  reader.py
  normalize.py
  basin_link.py
  quality.py
  service.py
  factory.py
tests/unit/silver/events/
  test_config.py
  test_normalize.py
  test_basin_link.py
  test_quality.py
  test_service.py
tests/integration/silver/test_event_pipeline.py
```

Service đọc snapshot event, object, admin tùy chọn và basin. `normalize.py` sở hữu event identity/revision. `basin_link.py` sở hữu phép gán spatial/admin. Không module nào import dependency NLP hoặc vector database.

### Công việc 1: Định nghĩa schema event, mapping nguồn và model

**Các file:**
- Tạo: `config/silver/flood_events.yaml`
- Tạo: `src/flashflood_data/orchestration/silver/events/__init__.py`
- Tạo: `src/flashflood_data/orchestration/silver/events/config.py`
- Tạo: `src/flashflood_data/orchestration/silver/events/models.py`
- Sửa: `src/flashflood_data/storage/iceberg_schemas.py`
- Sửa: `config/meta/static.yaml`
- Sửa: `tests/contract/storage/test_meta_bronze_schemas.py`
- Tạo: `tests/unit/silver/events/test_config.py`

**Giao diện:**
- Đầu ra: `FloodEventConfig`, `ObservedFloodEventRow`, `EventBasinRow`, `NormalizedEventCandidate`.
- Produces physical `silver.observed_flood_event` và `silver.event_basin`.

- [ ] **Bước 1: Viết kiểm thử cho phạm vi và schema**

```python
def test_event_config_has_no_document_or_vector_settings():
    config = load_flood_event_config(CONFIG)
    dumped = config.model_dump()
    assert not ({"chunking", "embedding", "qdrant"} & dumped.keys())

def test_event_schema_keeps_nullable_document_reference():
    schema = table_schema(("silver", "observed_flood_event"))
    assert schema.field("source_document_id").nullable is True
    assert {"flood_event_id", "event_revision", "event_geometry_kind"} <= set(schema.names)
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/events/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: FAIL vì contracts chưa tồn tại.

- [ ] **Bước 3: Triển khai mapping nguồn và chính xác schemas**

YAML map các field start/end/place/severity/longitude/latitude/footprint của từng nguồn có cấu trúc, định nghĩa geometry kind và confidence hợp lệ, CRS bảo toàn diện tích, normalization version và batch size. Không có cấu hình document/vector.

Chỉ đăng ký hai bảng output. Không đăng ký hoặc tạo schema runtime cho các bảng document đã hoãn.

- [ ] **Bước 4: Chạy kiểm thử contract**

Chạy: `pytest tests/unit/silver/events/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit contracts**

```bash
git add config/silver/flood_events.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/events src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/events/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define Silver flood event contracts"
```

### Công việc 2: Chuẩn hóa identity event ổn định và revision

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/events/reader.py`
- Tạo: `src/flashflood_data/orchestration/silver/events/normalize.py`
- Tạo: `tests/unit/silver/events/test_normalize.py`

**Giao diện:**
- Đầu vào: `bronze.historical_event_raw`, matching `meta.source_objects`, hiện có event revisions, source mapping.
- Đầu ra: `normalize_event(row, source, config) -> NormalizedEventCandidate`.
- Đầu ra: `assign_revision(candidate, existing_rows) -> ObservedFloodEventRow | None`; `None` means unchanged.

- [ ] **Bước 1: Viết kiểm thử cho identity/revision/time**

```python
def test_same_source_record_keeps_event_id_across_content_revision():
    first = normalize_event(raw_event("record-7", severity=1), source, config)
    revised = normalize_event(raw_event("record-7", severity=2), source, config)
    assert first.flood_event_id == revised.flood_event_id

def test_identical_normalized_content_creates_no_revision():
    assert assign_revision(candidate, [existing_same_content()]) is None

def test_changed_content_increments_revision_and_clears_document_id():
    row = assign_revision(changed_candidate(), [existing_revision(2)])
    assert row.event_revision == 3
    assert row.source_document_id is None
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/events/test_normalize.py -q`  
Mong đợi: FAIL vì reader/normalizer are absent.

- [ ] **Bước 3: Triển khai chuẩn hóa gắn với nguồn**

Tìm `source_id` qua `object_id -> meta.source_objects`. Event identity ổn định là:

```python
flood_event_id = canonical_hash({
    "source_id": source_id,
    "source_record_id": raw.source_record_id,
})[:24]
```

Chuẩn hóa timestamp sang UTC mà không tự bịa độ chính xác còn thiếu, giữ địa danh được báo cáo, chỉ map severity khi nguồn cung cấp và đặt geometry kind thành `flood_footprint`, `reported_location` hoặc `administrative_reference`. Hash nội dung nghiệp vụ đã chuẩn hóa, bỏ các field revision/current/run. Hash giống nhau trả về `None`; hash thay đổi tăng revision lớn nhất và đánh dấu revision cũ không còn current khi publish.

- [ ] **Bước 4: Chạy kiểm thử chuẩn hóa**

Chạy: `pytest tests/unit/silver/events/test_normalize.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit chuẩn hóa**

```bash
git add src/flashflood_data/orchestration/silver/events/reader.py src/flashflood_data/orchestration/silver/events/normalize.py tests/unit/silver/events/test_normalize.py
git commit -m "feat: normalize revisioned flood events"
```

### Công việc 3: Liên kết event với basin và kiểm soát ngữ nghĩa diện tích

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/events/basin_link.py`
- Tạo: `src/flashflood_data/orchestration/silver/events/quality.py`
- Tạo: `tests/unit/silver/events/test_basin_link.py`
- Tạo: `tests/unit/silver/events/test_quality.py`

**Giao diện:**
- Đầu vào: normalized event, `silver.dim_basin`, optional `bronze.admin_boundary_raw` lookup.
- Đầu ra: `link_event_to_basins(event: ObservedFloodEventRow, basins: Sequence[BasinRow], admin_rows: Sequence[Mapping[str, object]], config: FloodEventConfig) -> list[EventBasinRow]`.
- Đầu ra: `check_event_outputs(events: Sequence[ObservedFloodEventRow], links: Sequence[EventBasinRow]) -> list[QualityResult]`.

- [ ] **Bước 1: Viết kiểm thử cho footprint/point/admin**

```python
def test_verified_footprint_calculates_area_per_basin():
    rows = link_event_to_basins(footprint_event(), two_basins(), admin_rows=[], config=config)
    assert len(rows) == 2
    assert all(row.affected_area_km2 > 0 for row in rows)
    assert all(0 < row.affected_fraction_of_basin <= 1 for row in rows)

def test_reported_point_links_without_affected_area():
    row = link_event_to_basins(point_event(), [basin()], admin_rows=[], config=config)[0]
    assert row.affected_area_km2 is None
    assert row.affected_fraction_of_basin is None
    assert row.evidence_id is None
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/events/test_basin_link.py tests/unit/silver/events/test_quality.py -q`  
Mong đợi: FAIL vì basin linking/DQ are absent.

- [ ] **Bước 3: Triển khai thứ tự ưu tiên gán và rule DQ**

Dùng thứ tự ưu tiên sau:

```text
verified flood footprint intersection
  -> reported point containment
  -> configured exact administrative-code/name lookup
  -> unlinked event with DQ warning
```

Chỉ nhánh đầu tiên tính diện tích trong CRS bảo toàn diện tích. Nhánh admin/point đặt hai field diện tích và method thành null. Fatal check gồm khóa trùng, thời điểm kết thúc trước bắt đầu, fraction ngoài `[0,1]`, có area cho geometry không phải footprint, thiếu basin version và có hơn một current revision. Địa danh không liên kết được hoặc mơ hồ là warning.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/events/test_basin_link.py tests/unit/silver/events/test_quality.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit basin links và DQ**

```bash
git add src/flashflood_data/orchestration/silver/events/basin_link.py src/flashflood_data/orchestration/silver/events/quality.py tests/unit/silver/events/test_basin_link.py tests/unit/silver/events/test_quality.py
git commit -m "feat: link flood events to basins"
```

### Công việc 4: Thêm service, DAG chạy thủ công, kiểm thử tích hợp và tài liệu

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/events/service.py`
- Tạo: `src/flashflood_data/orchestration/silver/events/factory.py`
- Tạo: `airflow/dags/silver_flood_events.py`
- Tạo: `tests/unit/silver/events/test_service.py`
- Tạo: `tests/integration/silver/test_event_pipeline.py`
- Sửa: `tests/contract/infra/test_silver_dags.py`
- Sửa: `README.md`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`

**Giao diện:**
- Đầu ra: `FloodEventSilverService.plan`, `normalize_batch`, `publish`, `run`.
- Đầu ra: `build_flood_event_silver_service(root=None)`.
- Produces manual DAG ID `silver_flood_events`.

- [ ] **Bước 1: Viết kiểm thử cho tăng dần, phạm vi và DAG**

```python
def test_unchanged_events_produce_no_output_commit(service):
    result = service.run("events-2")
    assert result.status == "skipped"

def test_service_never_writes_deferred_tables(service):
    service.run("events-3")
    written = {call.args[0] for call in service.dependencies.store.upsert_keyed_rows.call_args_list}
    assert not written & {("silver", "source_document"), ("silver", "document_chunk"),
                          ("silver", "event_evidence")}

def test_event_dag_is_manual(dag_bag):
    assert dag_bag.get_dag("silver_flood_events").schedule is None
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/events/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: FAIL vì service/DAG are absent.

- [ ] **Bước 3: Triển khai service tăng dần và TaskFlow graph**

Tìm snapshot event và basin; chỉ thêm snapshot admin khi bật admin lookup. Stage event và event-basin theo batch event, validate revision toàn cục rồi publish event trước bảng bridge. Khi ghi revision mới, cập nhật dòng cũ thành `is_current=False` trong cùng transaction keyed table.

Task graph:

```text
discover_inputs -> plan_event_batches -> normalize_and_link.expand
  -> validate_staged_batches -> publish_events_and_links
  -> audit_and_finalize -> cleanup_staging
```

- [ ] **Bước 4: Chạy kiểm thử domain và quét dependency đã hoãn**

Chạy: `pytest tests/unit/silver/events tests/integration/silver/test_event_pipeline.py tests/contract/infra/test_silver_dags.py -q`  
Mong đợi: PASS.

Chạy: `rg -n "qdrant|embedding|document_chunk|event_evidence" src/flashflood_data/orchestration/silver/events airflow/dags/silver_flood_events.py`  
Mong đợi: không có kết quả.

- [ ] **Bước 5: Commit pipeline**

```bash
git add src/flashflood_data/orchestration/silver/events airflow/dags/silver_flood_events.py tests/unit/silver/events tests/integration/silver/test_event_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver flood event DAG"
```

## Điểm kiểm tra

Kiểm tra một tập event nhỏ trong Trino. Xác nhận stable ID, thay đổi revision, current flag, hành vi area null cho point/admin, diện tích footprint theo basin, Meta lineage và không có code runtime document/Qdrant.
