# Kế hoạch triển khai nền tảng dùng chung cho Silver


**Bản gốc:** `docs/superpowers/plans/2026-09-30-silver-common-foundation.md`

> **Bản tiếng Việt:** Tên file code, class, hàm, bảng, field và lệnh được giữ nguyên để khớp repository.

> **Dành cho người triển khai:** KỸ NĂNG BẮT BUỘC: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng ô đánh dấu (`- [ ]`) để theo dõi.

**Mục tiêu:** Xây dựng các thành phần dùng chung để tìm snapshot, tạo phiên bản xác định, lưu tạm, publish Iceberg và audit Meta cho mọi pipeline Bronze → Silver.

**Kiến trúc:** Service của từng domain gọi một thư viện dùng chung nhỏ nhưng vẫn tự sở hữu toàn bộ quy tắc nghiệp vụ. Các task tính toán ghi batch Parquet theo từng run; mỗi bảng chỉ có một publisher commit batch, còn thành phần audit riêng ghi trạng thái run, DQ, snapshot và lineage.

**Công nghệ:** Python 3.11, Pydantic 2, PyArrow, PyIceberg/Polaris, pytest.

**Tài liệu thiết kế:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.vi.md`

## Ràng buộc chung

- `common` không được import bất kỳ package domain Silver nào.
- Identity của build gồm danh sách input snapshot đã sắp xếp, config hash, transform version và output contract version.
- Nhiều task tính toán có thể chạy song song; trong một run chỉ một task được commit vào một bảng Iceberg cụ thể.
- Run chỉ trở thành `succeeded` sau khi có đủ output bắt buộc, DQ record, snapshot ref và lineage edge.
- Chạy lại build đã hoàn tất giống hệt trả về `skip`; chạy lại build chưa hoàn tất trả về `repair`.
- Hành vi và schema Raw/Bronze hiện có phải tiếp tục tương thích.

---

## Cấu trúc file

```text
src/flashflood_data/orchestration/silver/
  __init__.py                     # public Silver package
  common/
    __init__.py                   # exports stable common interfaces
    models.py                     # request/result and snapshot DTOs
    versioning.py                 # deterministic hashes
    discovery.py                  # input snapshot and prior-run planning
    staging.py                    # run-scoped Parquet batches
    publisher.py                  # one-writer Iceberg publication
    audit.py                      # Meta run/DQ/snapshot/lineage recording
    factory.py                    # production dependencies
tests/unit/silver/common/
  test_versioning.py
  test_discovery.py
  test_staging.py
  test_publisher.py
  test_audit.py
tests/contract/test_architecture.py
```

`models.py` và `versioning.py` là các module lá. `discovery.py`, `staging.py`, `publisher.py` và `audit.py` dùng các kiểu dữ liệu này. `factory.py` khởi tạo dependency nhưng không chạy nghiệp vụ pipeline. Các package domain chỉ import những tên public do `common/__init__.py` export.

### Công việc 1: Định nghĩa model dùng chung và identity build xác định

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/__init__.py`
- Tạo: `src/flashflood_data/orchestration/silver/common/__init__.py`
- Tạo: `src/flashflood_data/orchestration/silver/common/models.py`
- Tạo: `src/flashflood_data/orchestration/silver/common/versioning.py`
- Tạo: `tests/unit/silver/common/test_versioning.py`

**Giao diện:**
- Đầu ra: `InputSnapshotRef(table_name: str, snapshot_id: int)`.
- Đầu ra: `SilverBuildRequest(pipeline_run_id, pipeline_id, input_snapshots, config_hash, transform_version, contract_version, build_signature)`.
- Đầu ra: `PublishedOutput(table_name, snapshot_id, row_count)` và `SilverRunResult(status, build_signature, outputs)`.
- Đầu ra: `canonical_hash(value: object) -> str`.
- Đầu ra: `make_build_signature(inputs: Sequence[InputSnapshotRef], *, config_hash: str, transform_version: str, contract_version: str) -> str`.

- [ ] **Bước 1: Viết kiểm thử ban đầu cho identity**

```python
def test_build_signature_ignores_input_order():
    left = make_build_signature(
        [InputSnapshotRef(table_name="bronze.a", snapshot_id=2),
         InputSnapshotRef(table_name="silver.b", snapshot_id=1)],
        config_hash="cfg", transform_version="v1", contract_version="1",
    )
    right = make_build_signature(
        [InputSnapshotRef(table_name="silver.b", snapshot_id=1),
         InputSnapshotRef(table_name="bronze.a", snapshot_id=2)],
        config_hash="cfg", transform_version="v1", contract_version="1",
    )
    assert left == right

def test_build_signature_changes_with_snapshot():
    assert signature(snapshot_id=10) != signature(snapshot_id=11)
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận lỗi thiếu module**

Chạy: `pytest tests/unit/silver/common/test_versioning.py -q`  
Mong đợi: FAIL vì Silver common package does not exist.

- [ ] **Bước 3: Triển khai DTOs và canonical hash**

```python
def canonical_hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(payload.encode("utf-8")).hexdigest()

def make_build_signature(
    inputs: Sequence[InputSnapshotRef], *, config_hash: str,
    transform_version: str, contract_version: str,
) -> str:
    return canonical_hash({
        "inputs": sorted((item.table_name, item.snapshot_id) for item in inputs),
        "config_hash": config_hash,
        "transform_version": transform_version,
        "contract_version": contract_version,
    })
```

Trong model Pydantic, kiểm tra name/version không rỗng và snapshot ID phải dương. `common/__init__.py` chỉ export các interface public đã liệt kê.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/common/test_versioning.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit identity contract**

```bash
git add src/flashflood_data/orchestration/silver tests/unit/silver/common/test_versioning.py
git commit -m "feat: add Silver build identity contract"
```

### Công việc 2: Thêm API đọc và cơ chế tìm build

**Các file:**
- Sửa: `src/flashflood_data/storage/iceberg_tables.py`
- Tạo: `src/flashflood_data/orchestration/silver/common/discovery.py`
- Tạo: `tests/unit/silver/common/test_discovery.py`
- Sửa: `tests/unit/storage/test_iceberg_tables.py`

**Giao diện:**
- Đầu vào: `InputSnapshotRef`, `SilverBuildRequest`, `make_build_signature` từ Công việc 1.
- Đầu ra: `IcebergTableStore.current_snapshot_id(identifier) -> int | None`.
- Đầu ra: `IcebergTableStore.scan_rows(identifier, *, snapshot_id: int | None = None, row_filter: BooleanExpression | None = None) -> list[dict[str, Any]]`.
- Đầu ra: `BuildDecision(action: Literal["build", "skip", "repair"], request: SilverBuildRequest, completed_tables: tuple[str, ...])`.
- Đầu ra: `SilverBuildDiscovery.plan(*, pipeline_run_id: str, pipeline_id: str, required_inputs: Sequence[str], required_outputs: Sequence[str], config_document: Mapping[str, object], transform_version: str, contract_version: str) -> BuildDecision`.

- [ ] **Bước 1: Viết kiểm thử cho snapshot lookup và trạng thái quyết định**

```python
def test_plan_skips_completed_signature(fake_store, completed_run):
    decision = discovery.plan(
        pipeline_run_id="manual-1", pipeline_id="silver-basin",
        required_inputs=("bronze.basin_polygon_raw",),
        required_outputs=("silver.dim_basin", "silver.basin_edge"),
        config_document={"level": 12}, transform_version="v1", contract_version="1",
    )
    assert decision.action == "skip"

def test_plan_repairs_only_missing_output(fake_store, partial_run):
    decision = discovery.plan(
        pipeline_run_id="manual-2", pipeline_id="silver-basin",
        required_inputs=("bronze.basin_polygon_raw",),
        required_outputs=("silver.dim_basin", "silver.basin_edge"),
        config_document={"level": 12}, transform_version="v1", contract_version="1",
    )
    assert decision.action == "repair"
    assert decision.completed_tables == ("silver.dim_basin",)
```

Dùng snapshot và dòng Meta giả lập rõ ràng; không kết nối Polaris.

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/common/test_discovery.py tests/unit/storage/test_iceberg_tables.py -q`  
Mong đợi: FAIL vì snapshot scanning và discovery chưa tồn tại.

- [ ] **Bước 3: Triển khai đọc storage và tìm build**

`current_snapshot_id` refresh bảng hiện có và trả về `None` khi bảng chưa có snapshot. `scan_rows` phải từ chối identifier nằm ngoài `meta`, `bronze` hoặc `silver`; khi có PyIceberg expression, hàm dùng expression đó để trả về các dòng Arrow.

`SilverBuildDiscovery.plan` must:

```python
input_refs = tuple(
    InputSnapshotRef(table_name=name, snapshot_id=require_snapshot(name))
    for name in sorted(required_inputs)
)
signature = make_build_signature(
    input_refs, config_hash=canonical_hash(config_document),
    transform_version=transform_version, contract_version=contract_version,
)
```

Sau đó tìm các run Meta thành công hoặc chưa hoàn tất có cùng `job_name` và signature. Đủ toàn bộ output thì trả về `skip`; mới có một phần output thì trả về `repair`; chưa có output phù hợp thì trả về `build`. Lưu `build_signature` trong document `metrics_json` hiện có của `meta.pipeline_runs`; `config_hash` vẫn chỉ là hash cấu hình nên không cần migrate schema Meta.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/common/test_discovery.py tests/unit/storage/test_iceberg_tables.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit discovery**

```bash
git add src/flashflood_data/storage/iceberg_tables.py src/flashflood_data/orchestration/silver/common/discovery.py tests/unit/silver/common/test_discovery.py tests/unit/storage/test_iceberg_tables.py
git commit -m "feat: discover Silver builds from Iceberg snapshots"
```

### Công việc 3: Triển khai run-scoped staging

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/common/staging.py`
- Tạo: `tests/unit/silver/common/test_staging.py`

**Giao diện:**
- Đầu vào: `SilverBuildRequest`.
- Đầu ra: `StagedBatch(table_name, batch_id, path, row_count, schema_hash)`.
- Đầu ra: `SilverStaging.write_batch(run_id: str, table_name: str, batch_id: str, rows: Sequence[Mapping[str, object]]) -> StagedBatch`.
- Đầu ra: `SilverStaging.read_batches(run_id: str, table_name: str) -> list[dict[str, object]]`, `mark_complete(run_id: str, table_name: str, expected_batches: int) -> None`, và `cleanup(run_id: str) -> None`.

- [ ] **Bước 1: Viết kiểm thử filesystem**

```python
def test_staging_round_trip_and_marker(tmp_path):
    staging = SilverStaging(tmp_path)
    batch = staging.write_batch("run-1", "silver.dim_basin", "0001", [{"id": "a"}])
    assert batch.row_count == 1
    assert staging.read_batches("run-1", "silver.dim_basin") == [{"id": "a"}]
    staging.mark_complete("run-1", "silver.dim_basin", expected_batches=1)
    assert staging.is_complete("run-1", "silver.dim_basin")

def test_cleanup_rejects_incomplete_run(tmp_path):
    with pytest.raises(ValueError, match="not finalized"):
        SilverStaging(tmp_path).cleanup("run-1")
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/common/test_staging.py -q`  
Mong đợi: FAIL vì `SilverStaging` chưa tồn tại.

- [ ] **Bước 3: Triển khai batch Parquet và marker theo cách nguyên tử**

Dùng cấu trúc thư mục sau:

```text
<staging_root>/silver/<pipeline_run_id>/<namespace.table>/batch-<batch_id>.parquet
<staging_root>/silver/<pipeline_run_id>/<namespace.table>/_COMPLETE.json
<staging_root>/silver/<pipeline_run_id>/_FINALIZED
```

Ghi mỗi file Parquet qua một file tạm cùng thư mục rồi dùng `Path.replace`. Từ chối batch ID trùng nhưng có schema hash khác. Cleanup yêu cầu marker `_FINALIZED`; run thất bại giữ lại staging để repair.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/common/test_staging.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit staging**

```bash
git add src/flashflood_data/orchestration/silver/common/staging.py tests/unit/silver/common/test_staging.py
git commit -m "feat: add Silver batch staging"
```

### Công việc 4: Thêm cơ chế một writer và audit Meta

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/common/publisher.py`
- Tạo: `src/flashflood_data/orchestration/silver/common/audit.py`
- Tạo: `tests/unit/silver/common/test_publisher.py`
- Tạo: `tests/unit/silver/common/test_audit.py`

**Giao diện:**
- Đầu vào: `StagedBatch`, `PublishedOutput`, shared `QualityResult`, `IcebergTableStore`, `MetaRecorder`.
- Đầu ra: `SilverPublisher.publish(table_name, key_fields, rows) -> PublishedOutput`.
- Đầu ra: `SilverAudit.start(request: SilverBuildRequest) -> None`.
- Đầu ra: `SilverAudit.record_output(request: SilverBuildRequest, output: PublishedOutput, quality: Sequence[QualityResult]) -> None`.
- Đầu ra: `SilverAudit.finish_success(request: SilverBuildRequest, outputs: Sequence[PublishedOutput]) -> None` và `finish_failure(request: SilverBuildRequest, error_code: str, *, partial: bool) -> None`.

- [ ] **Bước 1: Viết kiểm thử publisher và audit**

```python
def test_publisher_uses_one_keyed_commit(store):
    output = SilverPublisher(store).publish(
        "silver.dim_basin", ("basin_id", "basin_version"), rows,
    )
    assert output.row_count == len(rows)
    store.upsert_keyed_rows.assert_called_once()

def test_audit_records_input_output_and_lineage(meta, request):
    audit = SilverAudit(meta)
    audit.start(request)
    audit.record_output(request, output, quality_results)
    audit.finish_success(request, (output,))
    assert meta.record_snapshot_ref.call_count == len(request.input_snapshots) + 1
    meta.record_lineages.assert_called_once()
```

Kiểm tra thêm rằng DQ nghiêm trọng trước commit chặn `publish`, và `finish_success` từ chối khi thiếu output bắt buộc.

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/unit/silver/common/test_publisher.py tests/unit/silver/common/test_audit.py -q`  
Mong đợi: FAIL vì classes chưa tồn tại.

- [ ] **Bước 3: Triển khai publisher và chuyển trạng thái audit**

`SilverPublisher.publish` validates a non-empty unique key set, calls `upsert_keyed_rows` once, và returns the committed snapshot. `SilverAudit` stores `build_signature` in `metrics_json`, batches quality/lineage writes, và uses these run states:

```text
running -> succeeded
running -> failed
running -> partial_failure -> succeeded on repair
```

Lineage có một cạnh cho mỗi cặp input snapshot/output snapshot. Warning không chặn publish; `fatal_failures()` sẽ chặn.

- [ ] **Bước 4: Chạy kiểm thử trọng tâm**

Chạy: `pytest tests/unit/silver/common/test_publisher.py tests/unit/silver/common/test_audit.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Commit publish và audit**

```bash
git add src/flashflood_data/orchestration/silver/common/publisher.py src/flashflood_data/orchestration/silver/common/audit.py tests/unit/silver/common/test_publisher.py tests/unit/silver/common/test_audit.py
git commit -m "feat: publish and audit Silver builds"
```

### Công việc 5: Khởi tạo dependency production và kiểm soát kiến trúc

**Các file:**
- Tạo: `src/flashflood_data/orchestration/silver/common/factory.py`
- Sửa: `src/flashflood_data/orchestration/silver/common/__init__.py`
- Sửa: `tests/contract/test_architecture.py`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`

**Giao diện:**
- Đầu vào: common components from Tasks 1–4 và hiện có `LakehouseSettings`/`ProjectPaths`.
- Đầu ra: `SilverDependencies(store, meta, discovery, staging, publisher, audit)`.
- Đầu ra: `build_silver_dependencies(root: Path | None = None) -> SilverDependencies`.

- [ ] **Bước 1: Viết kiểm thử kiến trúc và factory**

```python
def test_common_package_does_not_import_domains():
    imports = imported_modules_under("src/flashflood_data/orchestration/silver/common")
    assert not any(name.startswith("flashflood_data.orchestration.silver.basin") for name in imports)

def test_factory_composes_shared_dependencies(monkeypatch, tmp_path):
    dependencies = build_silver_dependencies(tmp_path)
    assert dependencies.publisher.store is dependencies.store
    assert dependencies.audit.meta is dependencies.meta
```

- [ ] **Bước 2: Chạy kiểm thử và xác nhận đang fail**

Chạy: `pytest tests/contract/test_architecture.py tests/unit/silver/common -q`  
Mong đợi: FAIL vì factory/export boundary is incomplete.

- [ ] **Bước 3: Triển khai dependency container và ghi tài liệu call flow**

Dùng frozen dataclass cho `SilverDependencies`. Load `.env` qua `ProjectPaths.discover(root)` và `LakehouseSettings`, đồng nhất với Bronze factory hiện có. Cập nhật roadmap bằng flow sau:

```text
DAG -> domain factory -> domain service -> transform/quality
    -> common publisher/audit -> Iceberg + Meta
```

Ghi rõ rằng các package domain trao đổi dữ liệu qua table contract, không import service của nhau.

- [ ] **Bước 4: Chạy bộ kiểm thử dùng chung**

Chạy: `pytest tests/unit/silver/common tests/unit/storage/test_iceberg_tables.py tests/contract/test_architecture.py -q`  
Mong đợi: PASS.

- [ ] **Bước 5: Chạy kiểm tra chất lượng repository**

Chạy: `ruff check src tests`  
Mong đợi: PASS.

- [ ] **Bước 6: Commit common foundation**

```bash
git add src/flashflood_data/orchestration/silver/common tests/contract/test_architecture.py docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: complete Silver common foundation"
```

## Điểm kiểm tra

Dừng sau plan này. Review các interface dùng chung và xác nhận một domain giả lập có thể tạo build xác định, stage row, publish một snapshot và ghi Meta lineage trước khi triển khai domain thật.
