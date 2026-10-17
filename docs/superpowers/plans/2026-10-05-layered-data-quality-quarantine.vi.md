# Kế hoạch triển khai Data Quality và quarantine đa tầng

> **Dành cho người triển khai:** KỸ NĂNG BẮT BUỘC: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng ô đánh dấu (`- [ ]`) để theo dõi.

**Mục tiêu:** Xây dựng DQ framework dùng chung cho Raw/Bronze/Silver/Gold, hỗ trợ block object, reject row, warning, quarantine idempotent và migration dữ liệu hiện có không download lại mặc định.

**Kiến trúc:** Domain tạo finding và partition row; package dùng chung `shared.quality` quyết định disposition, ghi quarantine/Meta và cung cấp contract publish có thể repair. Hai bảng Iceberg trong namespace `quarantine` giữ bằng chứng object/row; bảng nghiệp vụ chỉ chứa row `passed` hoặc `warning`.

**Công nghệ:** Python 3.11, Pydantic 2, PyArrow, PyIceberg/Polaris, MinIO, Airflow 3, pytest.

**Tài liệu thiết kế:** `docs/superpowers/specs/2026-10-05-layered-data-quality-quarantine-design.vi.md`

**Trạng thái 11/10/2026:** Công việc 1–9 đã được triển khai; Công việc 10 đã có smoke script,
runbook và full unit/contract validation. Landing, static Bronze và weather Bronze đã ghi fatal/
reject/warning theo contract chung; migration audit/apply không gọi provider. Local service smoke
còn cần chạy khi MinIO/Polaris/Airflow được bật. Các bước commit để mở cho người dùng tự chốt.

## Ràng buộc chung

- Vocabulary row được publish chỉ gồm `passed` và `warning`; row reject không tồn tại trong bảng đích.
- `fatal` data failure và operational failure phải là hai nhánh khác nhau; operational failure không được xóa output tốt trước đó.
- Quarantine write, output write và Meta audit phải idempotent vì Iceberg không có transaction xuyên bảng.
- `rejection_id` không chứa run ID; retry cùng input, policy và rule phải upsert cùng key.
- Domain sở hữu rule; package dùng chung không import parser hoặc package domain.
- Weather đợt đầu phân loại theo raster slice, không tách từng grid cell thành quarantine row.
- Không drop volume, không xóa Raw và không gọi lại provider trong migration mặc định.
- Mọi thay đổi hành vi bắt đầu bằng test thất bại và kết thúc bằng test trọng tâm cùng contract suite.

---

## Cấu trúc file sau triển khai

```text
config/
  quality/
    bronze_static.yaml            # policy static Bronze
    bronze_weather.yaml           # policy weather slice
    silver_defaults.yaml          # defaults để domain Silver kế thừa
    gold_defaults.yaml            # defaults để domain Gold kế thừa

src/flashflood_data/
  shared/quality/
    __init__.py                   # public interfaces duy nhất
    models.py                     # finding/evaluation/rejection DTOs
    policy.py                     # load + hash policy có version
    evaluate.py                   # disposition và partition helpers
    quarantine.py                 # deterministic rows + Iceberg repository
    audit.py                      # convert findings sang Meta rows
    coordinator.py                # repairable publish state machine
  pipelines/static_bronze/
    quality.py                    # static Bronze rules
    pipeline.py                   # dùng DQ coordinator
  pipelines/weather_ingest/
    quality.py                    # weather slice rules
    bronze.py                     # dùng DQ coordinator
  pipelines/quality_migration/
    __init__.py
    bronze_quality.py             # audit/rewrite/reprocess planner + service
    runtime.py                    # production dependencies
    airflow.py                    # DAG implementation

airflow/dags/
  bronze_quality_backfill.py      # manual migration DAG

tests/unit/shared/quality/
tests/unit/pipelines/quality_migration/
```

`meta.quality_results` tiếp tục là bảng tổng hợp rule. `quarantine.rejected_objects` và `quarantine.rejected_rows` là evidence có thể query. Silver/Gold import framework này khi từng domain được triển khai; không tạo framework riêng theo tầng.

### Công việc 1: Chốt model DQ và disposition dùng chung

**Các file:**
- Di chuyển: `src/flashflood_data/shared/quality.py` → `src/flashflood_data/shared/quality/__init__.py`
- Tạo: `src/flashflood_data/shared/quality/models.py`
- Tạo: `src/flashflood_data/shared/quality/evaluate.py`
- Tạo: `tests/unit/shared/quality/test_evaluate.py`

**Giao diện:**
- Đầu ra: `QualityFinding(rule_id, rule_version, status, severity, scope_key, observed_value, expected_value, failed_row_count)`.
- Đầu ra: `RejectedRecord(source_record_key, record, record_uri, findings)`.
- Đầu ra: `QualityEvaluation(accepted_rows, rejected_rows, findings, disposition)`.
- Đầu ra: `evaluate_rows(rows, validators) -> QualityEvaluation`.
- Tương thích: `QualityResult` hiện tại được alias/deprecate qua `QualityFinding`; chưa xóa trong cùng commit.

- [x] **Bước 1: Viết test cho ba disposition và zero accepted row**

```python
def test_fatal_finding_blocks_object():
    result = QualityEvaluation.from_parts(
        accepted_rows=[{"id": "1"}],
        rejected_rows=[],
        findings=[finding("crs_usable", "fatal", "failed")],
    )
    assert result.disposition == "block_object"
    assert result.accepted_rows == ()

def test_reject_keeps_valid_rows_and_warning_marks_accepted_row():
    result = evaluate_rows(
        [{"id": "ok"}, {"id": "bad"}, {"id": "warn"}],
        validators=(fixture_validator,),
    )
    assert [row["id"] for row in result.accepted_rows] == ["ok", "warn"]
    assert result.accepted_rows[1]["quality_status"] == "warning"
    assert [item.source_record_key for item in result.rejected_rows] == ["bad"]

def test_zero_accepted_rows_becomes_fatal():
    result = evaluate_rows([{"id": "bad"}], validators=(reject_all,))
    assert result.disposition == "block_object"
    assert any(item.rule_id == "no_accepted_rows" for item in result.findings)
```

- [x] **Bước 2: Chạy test để xác nhận thiếu package**

Chạy: `pytest tests/unit/shared/quality/test_evaluate.py -q`
Mong đợi: FAIL do `flashflood_data.shared.quality` chưa tồn tại.

- [x] **Bước 3: Triển khai immutable models và precedence**

```python
Severity = Literal["fatal", "reject", "warning", "info"]
Disposition = Literal["pass", "accept_with_warning", "reject_rows", "block_object"]

_PRECEDENCE = {
    "pass": 0,
    "accept_with_warning": 1,
    "reject_rows": 2,
    "block_object": 3,
}

def finding_disposition(finding: QualityFinding) -> Disposition:
    if finding.status not in {"failed", "error"}:
        return "pass"
    return {
        "fatal": "block_object",
        "reject": "reject_rows",
        "warning": "accept_with_warning",
        "info": "pass",
    }[finding.severity]
```

`QualityEvaluation.from_parts()` phải bỏ accepted rows khi có fatal, tự thêm `no_accepted_rows` nếu input có row nhưng tất cả bị reject, và không cho cùng `source_record_key` vừa accepted vừa rejected.

- [x] **Bước 4: Chạy test DQ và test quality cũ**

Chạy: `pytest tests/unit/shared/quality/test_evaluate.py tests/unit/pipelines/static_bronze/test_quality.py -q`
Mong đợi: PASS.

- [ ] **Bước 5: Commit model contract**

```bash
git add src/flashflood_data/shared/quality src/flashflood_data/shared/quality.py tests/unit/shared/quality
git commit -m "feat: add shared data quality decision contract"
```

### Công việc 2: Thêm quality policy có version

**Các file:**
- Tạo: `src/flashflood_data/shared/quality/policy.py`
- Tạo: `config/quality/bronze_static.yaml`
- Tạo: `config/quality/bronze_weather.yaml`
- Tạo: `config/quality/silver_defaults.yaml`
- Tạo: `config/quality/gold_defaults.yaml`
- Tạo: `tests/unit/shared/quality/test_policy.py`

**Giao diện:**
- Đầu vào: YAML có `policy_name`, `policy_version`, `layer`, `rules`.
- Đầu ra: `QualityPolicy.policy_id` là canonical SHA-256 của name/version/config.
- Đầu ra: `load_quality_policy(path: Path) -> QualityPolicy`.

- [x] **Bước 1: Viết test policy identity và validation**

```python
def test_policy_id_is_stable_when_yaml_key_order_changes(tmp_path):
    assert load_quality_policy(left).policy_id == load_quality_policy(right).policy_id

def test_policy_rejects_duplicate_rule_ids(tmp_path):
    with pytest.raises(ValueError, match="duplicate quality rule"):
        load_quality_policy(policy_with_duplicate_rules)
```

- [x] **Bước 2: Chạy test để xác nhận lỗi**

Chạy: `pytest tests/unit/shared/quality/test_policy.py -q`
Mong đợi: FAIL do loader chưa tồn tại.

- [x] **Bước 3: Triển khai policy schema và bốn config khởi đầu**

Static policy phải khai báo tối thiểu `object_checksum`, `object_readable`, `crs_usable`, `nonempty_parse`, `business_key_unique`, `bbox_valid`, `geometry_valid`. Weather policy phải khai báo `parallel_array_lengths`, `source_grid_complete`, `unit_present`, `temporal_window_valid`, `nonnegative_flux`, `null_cell_ratio`. Silver/Gold defaults chỉ định vocabulary, default thresholds và yêu cầu domain bổ sung rule riêng; chúng không tự chạy rule giả.

- [x] **Bước 4: Chạy test policy**

Chạy: `pytest tests/unit/shared/quality/test_policy.py tests/unit/pipelines/weather_ingest/test_config.py -q`
Mong đợi: PASS.

- [ ] **Bước 5: Commit policy layer**

```bash
git add config/quality src/flashflood_data/shared/quality/policy.py tests/unit/shared/quality/test_policy.py
git commit -m "feat: version data quality policies"
```

### Công việc 3: Tạo schema Iceberg quarantine và keyed writer

**Các file:**
- Sửa: `src/flashflood_data/storage/iceberg_schemas.py`
- Sửa: `src/flashflood_data/storage/iceberg_tables.py`
- Tạo: `src/flashflood_data/shared/quality/quarantine.py`
- Sửa: `tests/contract/storage/test_meta_bronze_schemas.py`
- Sửa: `tests/unit/storage/test_iceberg_tables.py`
- Tạo: `tests/unit/shared/quality/test_quarantine.py`
- Sửa: `tests/integration/lakehouse/test_meta_bronze_smoke.py`

**Giao diện:**
- Đầu ra: schema `quarantine.rejected_objects` và `quarantine.rejected_rows` đúng spec.
- Đầu ra: `IcebergTableStore.upsert_quarantine_rows(identifier, key_fields, rows) -> int`.
- Đầu ra: `QuarantineRepository.reject_object(record: ObjectRejection) -> tuple[str, int]` trả rejection ID và snapshot ID.
- Đầu ra: `QuarantineRepository.reject_rows(records: Sequence[RowRejection]) -> tuple[tuple[str, ...], int]` trả các rejection ID và snapshot ID.

- [x] **Bước 1: Viết contract test cho schema và deterministic rejection ID**

```python
def test_quarantine_tables_are_in_physical_contract():
    assert table_schema(("quarantine", "rejected_objects")).field("rejection_id").nullable is False
    assert table_schema(("quarantine", "rejected_rows")).field("rule_ids").type == pa.list_(pa.string())

def test_rejection_id_ignores_pipeline_retry_run_id():
    assert make_rejection_id(**identity, pipeline_run_id="run-1") == make_rejection_id(
        **identity, pipeline_run_id="run-2"
    )
```

- [x] **Bước 2: Chạy test để xác nhận schema chưa tồn tại**

Chạy: `pytest tests/contract/storage/test_meta_bronze_schemas.py tests/unit/shared/quality/test_quarantine.py -q`
Mong đợi: FAIL với `table identifier outside contract`.

- [x] **Bước 3: Triển khai schema và writer giới hạn namespace**

`upsert_quarantine_rows()` chỉ nhận namespace `quarantine` hoặc smoke namespace, bắt buộc key không null và từ chối duplicate trong payload. `QuarantineRepository` kiểm tra object rejection có đúng một input identity; row rejection có ít nhất một `record_json`/`record_uri`; JSON không chứa credential fields.

- [x] **Bước 4: Kiểm tra rerun không nhân bản**

Chạy: `pytest tests/unit/storage/test_iceberg_tables.py tests/unit/shared/quality/test_quarantine.py tests/integration/lakehouse/test_meta_bronze_smoke.py -q`
Mong đợi: PASS; hai lần upsert cùng rejection ID vẫn có một row.

- [ ] **Bước 5: Commit quarantine storage**

```bash
git add src/flashflood_data/storage src/flashflood_data/shared/quality/quarantine.py tests/contract/storage tests/unit/storage tests/unit/shared/quality tests/integration/lakehouse/test_meta_bronze_smoke.py
git commit -m "feat: add Iceberg quarantine contracts"
```

### Công việc 4: Chuẩn hóa Meta audit và coordinator repairable

**Các file:**
- Tạo: `src/flashflood_data/shared/quality/audit.py`
- Tạo: `src/flashflood_data/shared/quality/coordinator.py`
- Sửa: `src/flashflood_data/shared/metadata/service.py`
- Tạo: `tests/unit/shared/quality/test_audit.py`
- Tạo: `tests/unit/shared/quality/test_coordinator.py`
- Sửa: `tests/unit/shared/metadata/test_service.py`

**Giao diện:**
- Đầu ra: `quality_rows(evaluation, context) -> list[dict[str, object]]`.
- Đầu ra: `PublishDecision(status, accepted_count, rejected_count, warning_count, output_snapshot_id, quarantine_snapshot_ids)`.
- Đầu ra: `DqPublishRequest(pipeline_run_id, layer, target_dataset_id, object_id, policy_id, parser_version, lineage_inputs)`.
- Đầu ra: `DqPublishCoordinator.publish_object(request: DqPublishRequest, evaluation: QualityEvaluation) -> PublishDecision`.
- `MetaRecorder.record_qualities()` chấp nhận severity `fatal|reject|warning|info` và giữ status hiện hữu.
- `MetaRecorder.record_run()` chấp nhận terminal status `succeeded_with_warning` ngoài các status hiện hữu.

- [x] **Bước 1: Viết test cho fatal, mixed rows và crash repair**

```python
def test_fatal_records_quality_and_object_rejection_without_target_write(deps):
    result = coordinator.publish_object(evaluation=fatal_evaluation, **request)
    assert result.status == "failed_data_quality"
    assert deps.target_writer.calls == []
    assert deps.quarantine.object_count == 1

def test_mixed_result_publishes_valid_rows_and_rejections(deps):
    result = coordinator.publish_object(evaluation=mixed_evaluation, **request)
    assert result.status == "succeeded_with_warning"
    assert result.accepted_count == 2
    assert result.rejected_count == 1

def test_retry_repairs_meta_after_output_commit(deps):
    deps.fail_once_after_output_commit = True
    with pytest.raises(RuntimeError):
        coordinator.publish_object(evaluation=passing_evaluation, **request)
    result = coordinator.publish_object(evaluation=passing_evaluation, **request)
    assert result.status == "succeeded"
    assert deps.target_writer.commit_count == 1
```

- [x] **Bước 2: Chạy test để xác nhận coordinator thiếu**

Chạy: `pytest tests/unit/shared/quality/test_audit.py tests/unit/shared/quality/test_coordinator.py -q`
Mong đợi: FAIL do module chưa tồn tại.

- [x] **Bước 3: Triển khai state machine publish**

Thứ tự nonfatal là quarantine → target output → Meta quality/snapshot/lineage → terminal run. Fatal data là quality → object quarantine → terminal failed run. Operational exception ghi run `failed` và re-raise. Metrics JSON phải chứa `accepted_count`, `rejected_count`, `warning_count`, `fatal_count`, `quality_policy_id`.

- [x] **Bước 4: Chạy test audit/coordinator/meta**

Chạy: `pytest tests/unit/shared/quality tests/unit/shared/metadata/test_service.py -q`
Mong đợi: PASS.

- [ ] **Bước 5: Commit coordinator**

```bash
git add src/flashflood_data/shared/quality src/flashflood_data/shared/metadata/service.py tests/unit/shared/quality tests/unit/shared/metadata/test_service.py
git commit -m "feat: coordinate quality gated publication"
```

### Công việc 5: Áp dụng object/row/warning DQ cho static Bronze

**Các file:**
- Sửa: `src/flashflood_data/pipelines/static_bronze/quality.py`
- Sửa: `src/flashflood_data/pipelines/static_bronze/parsers.py`
- Sửa: `src/flashflood_data/pipelines/static_bronze/osm.py`
- Sửa: `src/flashflood_data/pipelines/static_bronze/pipeline.py`
- Sửa: `src/flashflood_data/pipelines/static_bronze/runtime.py`
- Sửa: `tests/unit/pipelines/static_bronze/test_quality.py`
- Sửa: `tests/unit/pipelines/static_bronze/test_parsers.py`
- Sửa: `tests/unit/pipelines/static_bronze/test_osm.py`
- Sửa: `tests/unit/pipelines/static_bronze/test_pipeline.py`

**Giao diện:**
- Đầu ra: `evaluate_static_object(table_name, object_context, rows, policy) -> QualityEvaluation`.
- Parser không tự quyết định fatal; parser chỉ raise khi container không đọc được và trả source key/row evidence cho lỗi row có thể cô lập.
- `BronzeService` dùng contract/policy/repository chung; đường single-object dùng cùng publish
  semantics, còn batch dùng một transaction nhiều object và trả count accepted/rejected/warning.

- [x] **Bước 1: Viết test các ví dụ chốt**

```python
def test_corrupt_vector_blocks_object_without_bronze_snapshot(service):
    with pytest.raises(DataQualityBlock):
        service.process_object("hydrorivers_v10", "corrupt", run_id="r1", parser_version="v2")
    assert quarantine.object_rule("object_readable").reason_code == "container_corrupt"

def test_one_invalid_feature_is_rejected_but_valid_feature_is_published(service):
    result = service.process_object("hydrorivers_v10", "mixed", run_id="r1", parser_version="v2")
    assert result["row_count"] == 1
    assert result["rejected_count"] == 1

def test_repaired_geometry_is_published_as_warning(service):
    result = service.process_object("geofabrik_vietnam_snapshot", "repairable", run_id="r1", parser_version="v2")
    assert result["warning_count"] == 1
    assert bronze_row["quality_status"] == "warning"
```

- [x] **Bước 2: Chạy test để xác nhận hành vi cũ fail cả object hoặc dùng `flagged`**

Chạy: `pytest tests/unit/pipelines/static_bronze -q`
Mong đợi: các test mới FAIL do service chưa partition row và vocabulary còn `flagged`.

- [x] **Bước 3: Triển khai rule static và chuyển `flagged` sang finding warning/reject**

Rule object fatal: checksum/size, container readable, CRS usable, geometry family phù hợp. Rule reject row: missing business key, empty geometry, geometry không repair được, bbox không hữu hạn. Rule warning: geometry repair không đổi semantic type, optional source field thiếu. `source_record_key` dùng native feature ID; nếu source không có ID dùng deterministic layer/position key.

- [x] **Bước 4: Đổi batch semantics từ fail cả batch sang outcome theo object**

Batch soil/admin phải đánh giá từng object. Các object passed/warning được commit cùng target transaction; object fatal được ghi `rejected_objects` và không có row đích. Operational exception vẫn abort task để retry. Mở rộng writer bằng API `replace_object_partitions(identifier, accepted_by_object, blocked_object_ids)` và test việc DQ-confirmed blocked object xóa slice policy cũ, trong khi operational failure giữ slice cũ.

- [x] **Bước 5: Chạy static Bronze suite**

Chạy: `pytest tests/unit/pipelines/static_bronze tests/contract/infra/test_static_source_to_bronze_dag.py -q`
Mong đợi: PASS; không còn row mới dùng `quality_status=flagged`.

- [ ] **Bước 6: Commit static adoption**

```bash
git add src/flashflood_data/pipelines/static_bronze src/flashflood_data/storage/iceberg_tables.py tests/unit/pipelines/static_bronze tests/contract/infra/test_static_source_to_bronze_dag.py
git commit -m "feat: quarantine invalid static Bronze records"
```

### Công việc 6: Áp dụng DQ theo raster slice cho weather Bronze

**Các file:**
- Tạo: `src/flashflood_data/pipelines/weather_ingest/quality.py`
- Sửa: `src/flashflood_data/pipelines/weather_ingest/parsers.py`
- Sửa: `src/flashflood_data/pipelines/weather_ingest/bronze.py`
- Sửa: `src/flashflood_data/pipelines/weather_ingest/runtime.py`
- Sửa: `src/flashflood_data/pipelines/weather_ingest/lifecycle.py`
- Tạo: `tests/unit/pipelines/weather_ingest/test_quality.py`
- Sửa: `tests/unit/pipelines/weather_ingest/test_parsers.py`
- Sửa: `tests/unit/pipelines/weather_ingest/test_bronze.py`
- Sửa: `tests/unit/pipelines/weather_ingest/test_lifecycle.py`

**Giao diện:**
- Đầu ra: `evaluate_weather_slices(rows, allowed_indices, policy) -> QualityEvaluation`.
- Weather reject key gồm `object_id + variable + vertical_level + valid_time + source_revision`.
- Lifecycle chỉ cho Raw transient hết hạn khi mọi slice của object đã publish và object-level quality không fatal.

- [x] **Bước 1: Viết test cho structure fatal, slice reject và null warning**

```python
def test_grid_mismatch_blocks_weather_object():
    result = evaluate_weather_slices([slice_with_missing_cell], {1, 2}, policy)
    assert result.disposition == "block_object"

def test_negative_flux_rejects_only_affected_slice():
    result = evaluate_weather_slices([negative_rain, valid_soil], {1, 2}, policy)
    assert [row["variable"] for row in result.accepted_rows] == ["volumetric_soil_water_layer_1"]
    assert result.rejected_rows[0].source_record_key.endswith("total_precipitation")

def test_soft_null_ratio_keeps_slice_with_warning():
    result = evaluate_weather_slices([slice_with_one_null], {1, 2, 3}, policy)
    assert result.accepted_rows[0]["quality_status"] == "warning"
```

- [x] **Bước 2: Chạy weather tests để xác nhận validator hiện raise toàn batch**

Chạy: `pytest tests/unit/pipelines/weather_ingest/test_quality.py tests/unit/pipelines/weather_ingest/test_bronze.py -q`
Mong đợi: FAIL do `weather.quality` chưa tồn tại.

- [x] **Bước 3: Triển khai rule theo slice và tích hợp coordinator**

Giữ schema arrays thẳng hàng. Không bỏ cell khỏi `cell_indices`; soft-invalid cell được đổi thành null trước publish và warning ghi observed count/ratio. Missing/unknown grid index là object fatal. Negative accumulated flux dưới tolerance cấu hình được clamp về zero + warning; vượt tolerance reject slice. Nếu mọi slice reject thì `no_accepted_rows` block object.

- [x] **Bước 4: Bảo vệ retention**

`mark_bronze_evidence_many()` nhận `passed` hoặc `warning`, nhưng `eligible()` chỉ xóa Raw khi policy cho phép warning và không có rejected slice cần Raw để review. Trong đợt đầu, object có rejected slice không đủ điều kiện expiry; object chỉ warning được expiry sau 7 ngày.

- [x] **Bước 5: Chạy weather suite**

Chạy: `pytest tests/unit/pipelines/weather_ingest tests/contract/infra/test_dynamic_weather_dags.py -q`
Mong đợi: PASS.

- [ ] **Bước 6: Commit weather adoption**

```bash
git add src/flashflood_data/pipelines/weather_ingest tests/unit/pipelines/weather_ingest tests/contract/infra/test_dynamic_weather_dags.py
git commit -m "feat: enforce weather slice quality policy"
```

### Công việc 7: Ghi fatal Landing vào Meta/quarantine mà không đăng ký Raw hợp lệ

**Các file:**
- Sửa: `src/flashflood_data/pipelines/static_landing/acquisition.py`
- Sửa: `src/flashflood_data/pipelines/static_landing/pipeline.py`
- Sửa: `src/flashflood_data/storage/http/quarantine.py`
- Sửa: `airflow/dags/static_source_landing.py`
- Sửa: `src/flashflood_data/pipelines/weather_ingest/airflow.py`
- Sửa: `tests/unit/pipelines/static_landing/test_sources.py`
- Sửa: `tests/unit/pipelines/static_landing/test_pipeline.py`
- Sửa: `tests/unit/pipelines/weather_ingest/test_airflow.py`

**Giao diện:**
- Đầu ra: `DataQualityBlock` mang sanitized `QualityEvaluation` và optional evidence URI.
- Static/download validation failure ghi `quarantine.rejected_objects` và `meta.quality_results` pre-publish.
- Dynamic fetch/network failure vẫn là operational; scoped payload corrupt/malformed là data-quality failure.

- [x] **Bước 1: Viết test phân biệt data và operational failure**

```python
def test_checksum_mismatch_records_object_rejection_without_source_object(service):
    with pytest.raises(DataQualityBlock):
        service.publish_source("fixture", "run-1")
    assert inventory.available_objects("fixture") == ()
    assert quarantine.latest.reason_code == "checksum_mismatch"

def test_timeout_does_not_create_quality_rejection(service):
    with pytest.raises(DownloadFailed):
        service.publish_source("fixture", "run-1")
    assert quarantine.rows == []
```

- [x] **Bước 2: Chạy landing tests để xác nhận chưa có Meta quarantine**

Chạy: `pytest tests/unit/pipelines/static_landing tests/unit/pipelines/weather_ingest/test_airflow.py -q`
Mong đợi: test mới FAIL.

- [x] **Bước 3: Triển khai adapter từ ValidationResult sang QualityFinding**

Giữ local evidence hiện tại dưới `dataset/raw/_quarantine`; ghi URI nếu evidence tồn tại. Không bắt buộc upload lại payload corrupt lớn. `_retry_or_failure()` không retry `DataQualityBlock`; network, timeout và optimistic commit vẫn retry theo policy Airflow.

- [x] **Bước 4: Chạy Landing suites**

Chạy: `pytest tests/unit/pipelines/static_landing tests/contract/infra/test_static_source_landing_dag.py tests/unit/pipelines/weather_ingest/test_airflow.py -q`
Mong đợi: PASS.

- [ ] **Bước 5: Commit Landing audit**

```bash
git add src/flashflood_data/pipelines/static_landing src/flashflood_data/storage/http/quarantine.py airflow/dags/static_source_landing.py src/flashflood_data/pipelines/weather_ingest/airflow.py tests/unit/pipelines/static_landing tests/unit/pipelines/weather_ingest/test_airflow.py tests/contract/infra/test_static_source_landing_dag.py
git commit -m "feat: audit fatal landing quality failures"
```

### Công việc 8: Tạo migration audit/backfill không download lại

**Các file:**
- Tạo: `src/flashflood_data/pipelines/quality_migration/__init__.py`
- Tạo: `src/flashflood_data/pipelines/quality_migration/bronze_quality.py`
- Tạo: `src/flashflood_data/pipelines/quality_migration/runtime.py`
- Tạo: `airflow/dags/bronze_quality_backfill.py`
- Tạo: `tests/unit/pipelines/quality_migration/test_bronze_quality.py`
- Tạo: `tests/contract/infra/test_bronze_quality_backfill_dag.py`
- Sửa: `Makefile`

**Giao diện:**
- Đầu ra: `MigrationDecision(status, dataset_id, object_id, action, reason)` với action `audit_only|rewrite_bronze|reprocess_raw|mark_raw_unavailable|skip`.
- Đầu ra: `BronzeQualityMigration.plan(dataset_id: str, snapshot_id: int, policy_id: str) -> tuple[MigrationDecision, ...]`.
- Đầu ra: `BronzeQualityMigration.execute(decisions, run_id) -> MigrationSummary`.
- DAG manual params: `dataset_id`, `mode=audit|apply`, `object_limit`, `force`.

- [x] **Bước 1: Viết test planner không gọi provider**

```python
def test_existing_bronze_columns_use_audit_only(migration):
    decision = migration.plan_object(object_with_all_required_fields)
    assert decision.action == "audit_only"
    assert provider.calls == []

def test_pixel_rule_uses_existing_raw_before_refetch(migration):
    decision = migration.plan_object(raster_needing_pixel_check)
    assert decision.action == "reprocess_raw"
    assert decision.raw_uri.startswith("s3://raw/")
    assert provider.calls == []

def test_expired_transient_raw_is_marked_unavailable(migration):
    assert migration.plan_object(expired_weather).action == "mark_raw_unavailable"
```

- [x] **Bước 2: Chạy test để xác nhận migration package thiếu**

Chạy: `pytest tests/unit/pipelines/quality_migration/test_bronze_quality.py -q`
Mong đợi: FAIL do package chưa tồn tại.

- [x] **Bước 3: Triển khai checkpoint và dry-run mặc định**

Checkpoint identity là `(dataset_id, input_snapshot_id, quality_policy_id, object_id)`. `mode=audit` chỉ ghi quality/rejection evidence và summary; `mode=apply` mới rewrite Bronze. Không import provider modules trong package migration. `object_limit` giới hạn số object mỗi run để resume an toàn.

- [x] **Bước 4: Thêm DAG contract và Make target kiểm tra plan**

Thêm `make dq-audit-plan DATASET=bronze.weather_raster_slice` gọi CLI/service trong container và không mutate target Bronze. DAG dùng pool writer hiện có, `max_active_runs=1`, paused on creation và không có schedule.

- [x] **Bước 5: Chạy migration/DAG tests**

Chạy: `pytest tests/unit/pipelines/quality_migration tests/contract/infra/test_bronze_quality_backfill_dag.py -q`
Mong đợi: PASS.

- [ ] **Bước 6: Commit migration pipeline**

```bash
git add src/flashflood_data/pipelines/quality_migration airflow/dags/bronze_quality_backfill.py tests/unit/pipelines/quality_migration tests/contract/infra/test_bronze_quality_backfill_dag.py Makefile
git commit -m "feat: add resumable Bronze quality migration"
```

### Công việc 9: Tích hợp contract DQ vào các kế hoạch Silver và Gold

**Các file:**
- Sửa: `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.vi.md`
- Sửa: `docs/superpowers/plans/2026-09-30-silver-common-foundation.vi.md`
- Sửa: `docs/superpowers/plans/2026-09-30-silver-basin-topology.vi.md`
- Sửa: `docs/superpowers/plans/2026-09-30-silver-basin-static-features.vi.md`
- Sửa: `docs/superpowers/plans/2026-09-30-silver-weather-aggregation.vi.md`
- Sửa: `docs/superpowers/plans/2026-09-30-silver-river-network.vi.md`
- Sửa: `docs/superpowers/plans/2026-09-30-silver-exposure-network.vi.md`
- Sửa: `docs/superpowers/plans/2026-09-30-silver-flood-events.vi.md`
- Sửa: `docs/schema_contract/data.md`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`

**Giao diện:**
- Silver common publisher nhận `QualityEvaluation`, `QualityPolicy` và dùng `DqPublishCoordinator`.
- `quality.py` từng domain Silver chỉ tạo findings; không ghi Iceberg/Meta trực tiếp.
- Gold pipeline tương lai bắt buộc khai báo policy ID, input completeness/freshness rules và phân biệt `NO_DATA` với `LOW`.

- [x] **Bước 1: Cập nhật Silver common plan**

Thay phần publisher/audit tự định nghĩa bằng interface chung:

```python
evaluation = domain_quality.check(staged_rows, context)
decision = dq_coordinator.publish_snapshot(
    evaluation=evaluation,
    policy=domain_policy,
    input_snapshots=request.input_snapshots,
    target=output_contract,
)
```

Mỗi domain plan phải liệt kê fatal/reject/warning rules cụ thể: basin topology, static feature coverage, weather aggregation completeness, river topology, exposure geometry và flood-event evidence.

- [x] **Bước 2: Cập nhật schema contract**

Bổ sung hai quarantine tables, vocabulary `passed|warning`, quan hệ với `meta.quality_results`, và bảng rule responsibility theo Raw/Bronze/Silver/Gold. Gold section ghi rõ no-data, freshness, source priority, mass-balance và model output checks.

- [x] **Bước 3: Chạy documentation contract checks**

Chạy: `pytest tests/unit/test_readme_commands.py tests/contract/storage/test_meta_bronze_schemas.py -q`
Mong đợi: PASS và tài liệu không tham chiếu `quality_status=flagged` như trạng thái chuẩn mới.

- [ ] **Bước 4: Commit Silver/Gold contract integration**

```bash
git add docs/superpowers docs/schema_contract/data.md docs/pipeline_architecture_and_roadmap.md
git commit -m "docs: integrate layered quality contracts"
```

### Công việc 10: Chạy integration smoke và viết runbook vận hành

**Các file:**
- Tạo: `tests/integration/lakehouse/test_dq_quarantine_smoke.py`
- Tạo: `docs/runbooks/data_quality_and_quarantine.md`
- Sửa: `README.md`

**Giao diện:**
- Smoke case 1: fatal object không có output row, có object rejection và failed quality result.
- Smoke case 2: mixed object có valid Bronze row, rejected row và warning row trong cùng logical run.
- Smoke case 3: retry giữ một rejection ID và một output business key.
- Smoke case 4: migration audit không thay Bronze snapshot; apply chỉ rewrite object được chọn.

- [x] **Bước 1: Viết integration smoke trước runbook**

```python
def test_mixed_quality_publication_is_repairable(lakehouse):
    first = lakehouse.publish_fixture(fail_after="target_commit")
    assert first.run_status == "failed"
    repaired = lakehouse.publish_fixture()
    assert repaired.run_status == "succeeded_with_warning"
    assert lakehouse.count("quarantine.rejected_rows") == 1
    assert lakehouse.count_target_key("fixture-valid") == 1
```

- [ ] **Bước 2: Chạy smoke test và sửa đúng ranh giới phát hiện**

Chạy: `pytest tests/integration/lakehouse/test_dq_quarantine_smoke.py -q`
Mong đợi: PASS trên local Polaris/MinIO; nếu service chưa chạy thì test skip bằng marker hiện có, không giả PASS unit.

- [x] **Bước 3: Viết runbook với lệnh kiểm tra cụ thể**

Runbook phải có truy vấn Trino cho fatal/reject/warning counts, cách chạy `bronze_quality_backfill` ở `audit` rồi `apply`, cách tìm Raw URI, cách reprocess object, cách xử lý `raw_unavailable`, và điều kiện không được retry data-fatal như operational failure.

- [x] **Bước 4: Chạy full validation**

Chạy:

```bash
pytest tests/unit/shared/quality tests/unit/pipelines/static_bronze tests/unit/pipelines/weather_ingest tests/unit/pipelines/quality_migration -q
pytest tests/contract/storage/test_meta_bronze_schemas.py tests/contract/infra -q
pytest tests/integration/lakehouse/test_dq_quarantine_smoke.py -q
ruff check src tests airflow/dags
```

Mong đợi: tất cả test chạy đều PASS; integration chỉ SKIP khi dependency local được marker xác nhận là chưa bật.

- [ ] **Bước 5: Commit runbook và smoke coverage**

```bash
git add tests/integration/lakehouse/test_dq_quarantine_smoke.py docs/runbooks/data_quality_and_quarantine.md README.md
git commit -m "docs: add data quality operations runbook"
```

## Thứ tự rollout dữ liệu hiện có

Sau khi code của Công việc 1–8 được deploy:

1. Chạy schema bootstrap để tạo `quarantine.*`; không xóa catalog hiện tại.
2. Chạy `bronze_quality_backfill` với `mode=audit` cho từng bảng Bronze, bắt đầu từ vector nhỏ rồi đến OSM/weather.
3. Review summary và reject ratio; nếu fatal tăng đột biến, sửa policy/config trước khi apply.
4. Chạy `mode=apply` theo batch object nhỏ; không chạy tất cả bảng cùng lúc.
5. Reprocess từ Raw trong MinIO chỉ cho decision `reprocess_raw`.
6. Giữ `raw_unavailable` làm evidence; chỉ fetch lại khi dataset đó thực sự cần cho Silver.
7. Chạy reconciliation Raw → Bronze → Meta → quarantine.
8. Chỉ sau khi Bronze migration ổn định mới bật quality policy trong Silver DAG đầu tiên.

## Tiêu chí hoàn tất

- Fatal data object không có row trong output mới và có đủ Meta + object rejection evidence.
- Một object mixed-quality publish được valid/warning rows và quarantine invalid rows mà không nhân bản khi retry.
- Operational failure không tạo false rejection và không xóa snapshot tốt.
- Weather giữ array alignment; warning/reject hoạt động ở slice scope.
- Migration mặc định không gọi provider và không download lại Raw.
- Silver/Gold plans dùng cùng public DQ contract, không tự tạo framework khác.
- README, schema contract và runbook mô tả đúng câu lệnh vận hành và trạng thái dữ liệu.
