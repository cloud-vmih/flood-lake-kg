# Silver Common Foundation Implementation Plan

**Vietnamese version:** `docs/superpowers/plans/2026-09-30-silver-common-foundation.vi.md`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared snapshot discovery, deterministic versioning, staging, Iceberg publication, and Meta audit primitives used by every Bronze-to-Silver pipeline.

**Architecture:** Domain services call a small common library but retain all domain rules. Compute tasks write run-scoped Parquet batches; one publisher per table commits those batches and a separate audit component records run state, DQ, snapshots, and lineage.

**Tech Stack:** Python 3.11, Pydantic 2, PyArrow, PyIceberg/Polaris, pytest.

**Spec:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.md`

## Global Constraints

- `common` must not import any Silver domain package.
- A build identity includes sorted input snapshots, config hash, transform version, and output contract version.
- Multiple compute tasks may run concurrently; only one task may commit a given Iceberg table in a run.
- A pipeline run becomes `succeeded` only after every required output, DQ record, snapshot ref, and lineage edge exists.
- Rerunning an identical completed build returns `skip`; rerunning a partial build returns `repair`.
- Existing Raw/Bronze behavior and schemas must remain compatible.

---

## File structure

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

`models.py` and `versioning.py` are leaf modules. `discovery.py`, `staging.py`, `publisher.py`, and `audit.py` consume those types. `factory.py` constructs dependencies but does not call pipeline behavior. Domain packages import only the public names exported by `common/__init__.py`.

### Task 1: Define shared models and deterministic build identity

**Files:**
- Create: `src/flashflood_data/orchestration/silver/__init__.py`
- Create: `src/flashflood_data/orchestration/silver/common/__init__.py`
- Create: `src/flashflood_data/orchestration/silver/common/models.py`
- Create: `src/flashflood_data/orchestration/silver/common/versioning.py`
- Create: `tests/unit/silver/common/test_versioning.py`

**Interfaces:**
- Produces: `InputSnapshotRef(table_name: str, snapshot_id: int)`.
- Produces: `SilverBuildRequest(pipeline_run_id, pipeline_id, input_snapshots, config_hash, transform_version, contract_version, build_signature)`.
- Produces: `PublishedOutput(table_name, snapshot_id, row_count)` and `SilverRunResult(status, build_signature, outputs)`.
- Produces: `canonical_hash(value: object) -> str`.
- Produces: `make_build_signature(inputs: Sequence[InputSnapshotRef], *, config_hash: str, transform_version: str, contract_version: str) -> str`.

- [ ] **Step 1: Write the failing identity tests**

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

- [ ] **Step 2: Run the tests and verify the missing-module failure**

Run: `pytest tests/unit/silver/common/test_versioning.py -q`  
Expected: FAIL because the Silver common package does not exist.

- [ ] **Step 3: Implement the DTOs and canonical hash**

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

Validate non-empty names/versions and positive snapshot IDs in the Pydantic models. Export only the listed public interfaces from `common/__init__.py`.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/common/test_versioning.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit the identity contract**

```bash
git add src/flashflood_data/orchestration/silver tests/unit/silver/common/test_versioning.py
git commit -m "feat: add Silver build identity contract"
```

### Task 2: Add read APIs and build discovery

**Files:**
- Modify: `src/flashflood_data/storage/iceberg_tables.py`
- Create: `src/flashflood_data/orchestration/silver/common/discovery.py`
- Create: `tests/unit/silver/common/test_discovery.py`
- Modify: `tests/unit/storage/test_iceberg_tables.py`

**Interfaces:**
- Consumes: `InputSnapshotRef`, `SilverBuildRequest`, `make_build_signature` from Task 1.
- Produces: `IcebergTableStore.current_snapshot_id(identifier) -> int | None`.
- Produces: `IcebergTableStore.scan_rows(identifier, *, snapshot_id: int | None = None, row_filter: BooleanExpression | None = None) -> list[dict[str, Any]]`.
- Produces: `BuildDecision(action: Literal["build", "skip", "repair"], request: SilverBuildRequest, completed_tables: tuple[str, ...])`.
- Produces: `SilverBuildDiscovery.plan(*, pipeline_run_id: str, pipeline_id: str, required_inputs: Sequence[str], required_outputs: Sequence[str], config_document: Mapping[str, object], transform_version: str, contract_version: str) -> BuildDecision`.

- [ ] **Step 1: Write tests for snapshot lookup and decision states**

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

Use explicit fake snapshots and Meta rows; do not connect to Polaris.

- [ ] **Step 2: Run the tests and verify failure**

Run: `pytest tests/unit/silver/common/test_discovery.py tests/unit/storage/test_iceberg_tables.py -q`  
Expected: FAIL because snapshot scanning and discovery do not exist.

- [ ] **Step 3: Implement storage reads and discovery**

`current_snapshot_id` refreshes an existing table and returns `None` when it has no snapshot. `scan_rows` must reject identifiers outside `meta`, `bronze`, or `silver`; it returns Arrow rows using the supplied PyIceberg expression when present.

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

It then finds successful/partial Meta runs with the same `job_name` and signature. A completed output set yields `skip`; a non-empty subset yields `repair`; no matching output yields `build`. Store `build_signature` under the existing `metrics_json` document of `meta.pipeline_runs`, while `config_hash` remains the hash of configuration only, so no Meta schema migration is needed.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/common/test_discovery.py tests/unit/storage/test_iceberg_tables.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit discovery**

```bash
git add src/flashflood_data/storage/iceberg_tables.py src/flashflood_data/orchestration/silver/common/discovery.py tests/unit/silver/common/test_discovery.py tests/unit/storage/test_iceberg_tables.py
git commit -m "feat: discover Silver builds from Iceberg snapshots"
```

### Task 3: Implement run-scoped staging

**Files:**
- Create: `src/flashflood_data/orchestration/silver/common/staging.py`
- Create: `tests/unit/silver/common/test_staging.py`

**Interfaces:**
- Consumes: `SilverBuildRequest`.
- Produces: `StagedBatch(table_name, batch_id, path, row_count, schema_hash)`.
- Produces: `SilverStaging.write_batch(run_id: str, table_name: str, batch_id: str, rows: Sequence[Mapping[str, object]]) -> StagedBatch`.
- Produces: `SilverStaging.read_batches(run_id: str, table_name: str) -> list[dict[str, object]]`, `mark_complete(run_id: str, table_name: str, expected_batches: int) -> None`, and `cleanup(run_id: str) -> None`.

- [ ] **Step 1: Write filesystem tests**

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

- [ ] **Step 2: Run the tests and verify failure**

Run: `pytest tests/unit/silver/common/test_staging.py -q`  
Expected: FAIL because `SilverStaging` is missing.

- [ ] **Step 3: Implement atomic Parquet batches and markers**

Use this layout:

```text
<staging_root>/silver/<pipeline_run_id>/<namespace.table>/batch-<batch_id>.parquet
<staging_root>/silver/<pipeline_run_id>/<namespace.table>/_COMPLETE.json
<staging_root>/silver/<pipeline_run_id>/_FINALIZED
```

Write each Parquet file through a temporary sibling and `Path.replace`. Reject duplicate batch IDs with a different schema hash. Cleanup requires `_FINALIZED`; failed runs retain staging for repair.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/common/test_staging.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit staging**

```bash
git add src/flashflood_data/orchestration/silver/common/staging.py tests/unit/silver/common/test_staging.py
git commit -m "feat: add Silver batch staging"
```

### Task 4: Add one-writer publication and Meta audit

**Files:**
- Create: `src/flashflood_data/orchestration/silver/common/publisher.py`
- Create: `src/flashflood_data/orchestration/silver/common/audit.py`
- Create: `tests/unit/silver/common/test_publisher.py`
- Create: `tests/unit/silver/common/test_audit.py`

**Interfaces:**
- Consumes: `StagedBatch`, `PublishedOutput`, shared `QualityResult`, `IcebergTableStore`, `MetaRecorder`.
- Produces: `SilverPublisher.publish(table_name, key_fields, rows) -> PublishedOutput`.
- Produces: `SilverAudit.start(request: SilverBuildRequest) -> None`.
- Produces: `SilverAudit.record_output(request: SilverBuildRequest, output: PublishedOutput, quality: Sequence[QualityResult]) -> None`.
- Produces: `SilverAudit.finish_success(request: SilverBuildRequest, outputs: Sequence[PublishedOutput]) -> None` and `finish_failure(request: SilverBuildRequest, error_code: str, *, partial: bool) -> None`.

- [ ] **Step 1: Write publisher and audit tests**

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

Also assert fatal pre-commit DQ prevents `publish`, and `finish_success` rejects a missing required output.

- [ ] **Step 2: Run the tests and verify failure**

Run: `pytest tests/unit/silver/common/test_publisher.py tests/unit/silver/common/test_audit.py -q`  
Expected: FAIL because the classes are missing.

- [ ] **Step 3: Implement publisher and audit state transitions**

`SilverPublisher.publish` validates a non-empty unique key set, calls `upsert_keyed_rows` once, and returns the committed snapshot. `SilverAudit` stores `build_signature` in `metrics_json`, batches quality/lineage writes, and uses these run states:

```text
running -> succeeded
running -> failed
running -> partial_failure -> succeeded on repair
```

Lineage is one edge per input snapshot/output snapshot pair. Warnings do not block publication; `fatal_failures()` does.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/common/test_publisher.py tests/unit/silver/common/test_audit.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit publication and audit**

```bash
git add src/flashflood_data/orchestration/silver/common/publisher.py src/flashflood_data/orchestration/silver/common/audit.py tests/unit/silver/common/test_publisher.py tests/unit/silver/common/test_audit.py
git commit -m "feat: publish and audit Silver builds"
```

### Task 5: Compose production dependencies and enforce architecture

**Files:**
- Create: `src/flashflood_data/orchestration/silver/common/factory.py`
- Modify: `src/flashflood_data/orchestration/silver/common/__init__.py`
- Modify: `tests/contract/test_architecture.py`
- Modify: `docs/pipeline_architecture_and_roadmap.md`

**Interfaces:**
- Consumes: common components from Tasks 1–4 and existing `LakehouseSettings`/`ProjectPaths`.
- Produces: `SilverDependencies(store, meta, discovery, staging, publisher, audit)`.
- Produces: `build_silver_dependencies(root: Path | None = None) -> SilverDependencies`.

- [ ] **Step 1: Write architecture and factory tests**

```python
def test_common_package_does_not_import_domains():
    imports = imported_modules_under("src/flashflood_data/orchestration/silver/common")
    assert not any(name.startswith("flashflood_data.orchestration.silver.basin") for name in imports)

def test_factory_composes_shared_dependencies(monkeypatch, tmp_path):
    dependencies = build_silver_dependencies(tmp_path)
    assert dependencies.publisher.store is dependencies.store
    assert dependencies.audit.meta is dependencies.meta
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/contract/test_architecture.py tests/unit/silver/common -q`  
Expected: FAIL because the factory/export boundary is incomplete.

- [ ] **Step 3: Implement the dependency container and document the call flow**

Use a frozen dataclass for `SilverDependencies`. Load `.env` through `ProjectPaths.discover(root)` and `LakehouseSettings`, matching the existing Bronze factory. Update the roadmap with:

```text
DAG -> domain factory -> domain service -> transform/quality
    -> common publisher/audit -> Iceberg + Meta
```

Document that domain packages exchange data through table contracts, not service imports.

- [ ] **Step 4: Run the common suite**

Run: `pytest tests/unit/silver/common tests/unit/storage/test_iceberg_tables.py tests/contract/test_architecture.py -q`  
Expected: PASS.

- [ ] **Step 5: Run repository quality checks**

Run: `ruff check src tests`  
Expected: PASS.

- [ ] **Step 6: Commit the common foundation**

```bash
git add src/flashflood_data/orchestration/silver/common tests/contract/test_architecture.py docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: complete Silver common foundation"
```

## Checkpoint

Stop after this plan. Review the common interfaces and confirm a fake domain can produce a deterministic build, stage rows, publish one snapshot, and record Meta lineage before implementing any real domain pipeline.
