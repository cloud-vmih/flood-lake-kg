# Silver Flood Events Implementation Plan

**Vietnamese version:** `docs/superpowers/plans/2026-09-30-silver-flood-events.vi.md`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Normalize structured Bronze flood-event records into revisioned events and link each event to one or more versioned L12 basins.

**Architecture:** Use source/object identity to keep a stable event ID, increment revisions only when normalized event content changes, and assign basins from verified footprints, reported points, or configured administrative references. This pipeline contains no document parsing, chunks, embeddings, evidence bridge, or Qdrant integration.

**Tech Stack:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, PyArrow, Shapely, GeoPandas, PyProj, PyIceberg, pytest.

**Spec:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.md`

## Global Constraints

- Requires common foundation, `silver.dim_basin`, and a separate flood-event landing/Bronze pipeline that produces `bronze.historical_event_raw`.
- Outputs are only `silver.observed_flood_event` and `silver.event_basin`.
- `source_document_id` and `evidence_id` remain null in this phase.
- No `source_document`, `document_chunk`, `event_evidence`, embedding, or Qdrant runtime code is created.
- A point or administrative reference may link a basin but must not produce affected area.
- Affected area is calculated only for a verified `flood_footprint` in an equal-area CRS.
- Each target table has one writer task.

---

## File structure

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

The service reads event, object, optional admin, and basin snapshots. `normalize.py` owns event identity/revision. `basin_link.py` owns spatial/admin assignment. No module imports an NLP or vector database dependency.

### Task 1: Define event schemas, source mappings, and models

**Files:**
- Create: `config/silver/flood_events.yaml`
- Create: `src/flashflood_data/orchestration/silver/events/__init__.py`
- Create: `src/flashflood_data/orchestration/silver/events/config.py`
- Create: `src/flashflood_data/orchestration/silver/events/models.py`
- Modify: `src/flashflood_data/storage/iceberg_schemas.py`
- Modify: `config/meta/static.yaml`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`
- Create: `tests/unit/silver/events/test_config.py`

**Interfaces:**
- Produces: `FloodEventConfig`, `ObservedFloodEventRow`, `EventBasinRow`, `NormalizedEventCandidate`.
- Produces physical `silver.observed_flood_event` and `silver.event_basin`.

- [ ] **Step 1: Write scope and schema tests**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/events/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: FAIL because contracts are missing.

- [ ] **Step 3: Implement source mapping and exact schemas**

The YAML maps each structured source's start/end/place/severity/longitude/latitude/footprint fields, defines allowed geometry kinds and confidence values, equal-area CRS, normalization version, and batch size. It contains no document/vector settings.

Register only the two output tables. Do not register or create schemas for deferred document tables in runtime code.

- [ ] **Step 4: Run contract tests**

Run: `pytest tests/unit/silver/events/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit contracts**

```bash
git add config/silver/flood_events.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/events src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/events/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define Silver flood event contracts"
```

### Task 2: Normalize stable event identities and revisions

**Files:**
- Create: `src/flashflood_data/orchestration/silver/events/reader.py`
- Create: `src/flashflood_data/orchestration/silver/events/normalize.py`
- Create: `tests/unit/silver/events/test_normalize.py`

**Interfaces:**
- Consumes: `bronze.historical_event_raw`, matching `meta.source_objects`, existing event revisions, source mapping.
- Produces: `normalize_event(row, source, config) -> NormalizedEventCandidate`.
- Produces: `assign_revision(candidate, existing_rows) -> ObservedFloodEventRow | None`; `None` means unchanged.

- [ ] **Step 1: Write identity/revision/time tests**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/events/test_normalize.py -q`  
Expected: FAIL because reader/normalizer are absent.

- [ ] **Step 3: Implement source-bound normalization**

Resolve `source_id` through `object_id -> meta.source_objects`. Stable event identity is:

```python
flood_event_id = canonical_hash({
    "source_id": source_id,
    "source_record_id": raw.source_record_id,
})[:24]
```

Normalize timestamps to UTC without inventing missing precision, preserve reported place, map severity only when the source supplies it, and set geometry kind to `flood_footprint`, `reported_location`, or `administrative_reference`. Hash the normalized business content excluding revision/current/run fields. Same hash returns `None`; changed hash increments the maximum revision and marks the previous revision non-current during publication.

- [ ] **Step 4: Run normalization tests**

Run: `pytest tests/unit/silver/events/test_normalize.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit normalization**

```bash
git add src/flashflood_data/orchestration/silver/events/reader.py src/flashflood_data/orchestration/silver/events/normalize.py tests/unit/silver/events/test_normalize.py
git commit -m "feat: normalize revisioned flood events"
```

### Task 3: Link events to basins and enforce area semantics

**Files:**
- Create: `src/flashflood_data/orchestration/silver/events/basin_link.py`
- Create: `src/flashflood_data/orchestration/silver/events/quality.py`
- Create: `tests/unit/silver/events/test_basin_link.py`
- Create: `tests/unit/silver/events/test_quality.py`

**Interfaces:**
- Consumes: normalized event, `silver.dim_basin`, optional `bronze.admin_boundary_raw` lookup.
- Produces: `link_event_to_basins(event: ObservedFloodEventRow, basins: Sequence[BasinRow], admin_rows: Sequence[Mapping[str, object]], config: FloodEventConfig) -> list[EventBasinRow]`.
- Produces: `check_event_outputs(events: Sequence[ObservedFloodEventRow], links: Sequence[EventBasinRow]) -> list[QualityResult]`.

- [ ] **Step 1: Write footprint/point/admin tests**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/events/test_basin_link.py tests/unit/silver/events/test_quality.py -q`  
Expected: FAIL because basin linking/DQ are absent.

- [ ] **Step 3: Implement assignment precedence and DQ rules**

Use this precedence:

```text
verified flood footprint intersection
  -> reported point containment
  -> configured exact administrative-code/name lookup
  -> unlinked event with DQ warning
```

Only the first branch computes area in the configured equal-area CRS. Admin/point branches set both area fields and method to null. Fatal checks cover duplicate keys, end before start, fraction outside `[0,1]`, area present for a non-footprint, missing basin version, and more than one current revision. Unlinked/ambiguous place is a warning.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/events/test_basin_link.py tests/unit/silver/events/test_quality.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit basin links and DQ**

```bash
git add src/flashflood_data/orchestration/silver/events/basin_link.py src/flashflood_data/orchestration/silver/events/quality.py tests/unit/silver/events/test_basin_link.py tests/unit/silver/events/test_quality.py
git commit -m "feat: link flood events to basins"
```

### Task 4: Add service, manual DAG, integration test, and docs

**Files:**
- Create: `src/flashflood_data/orchestration/silver/events/service.py`
- Create: `src/flashflood_data/orchestration/silver/events/factory.py`
- Create: `airflow/dags/silver_flood_events.py`
- Create: `tests/unit/silver/events/test_service.py`
- Create: `tests/integration/silver/test_event_pipeline.py`
- Modify: `tests/contract/infra/test_silver_dags.py`
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`

**Interfaces:**
- Produces: `FloodEventSilverService.plan`, `normalize_batch`, `publish`, `run`.
- Produces: `build_flood_event_silver_service(root=None)`.
- Produces manual DAG ID `silver_flood_events`.

- [ ] **Step 1: Write incremental, scope, and DAG tests**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/events/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Expected: FAIL because service/DAG are absent.

- [ ] **Step 3: Implement incremental service and TaskFlow graph**

Discover event and basin snapshots; include admin snapshot only when admin lookup is enabled. Stage event and event-basin rows by event batch, globally validate revisions, then publish event rows before bridge rows. When a new revision is written, update the former row's `is_current=False` in the same keyed table transaction.

Task graph:

```text
discover_inputs -> plan_event_batches -> normalize_and_link.expand
  -> validate_staged_batches -> publish_events_and_links
  -> audit_and_finalize -> cleanup_staging
```

- [ ] **Step 4: Run domain tests and scan for deferred dependencies**

Run: `pytest tests/unit/silver/events tests/integration/silver/test_event_pipeline.py tests/contract/infra/test_silver_dags.py -q`  
Expected: PASS.

Run: `rg -n "qdrant|embedding|document_chunk|event_evidence" src/flashflood_data/orchestration/silver/events airflow/dags/silver_flood_events.py`  
Expected: no matches.

- [ ] **Step 5: Commit pipeline**

```bash
git add src/flashflood_data/orchestration/silver/events airflow/dags/silver_flood_events.py tests/unit/silver/events tests/integration/silver/test_event_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver flood event DAG"
```

## Checkpoint

Inspect a small event set in Trino. Verify stable IDs, revision changes, current flags, point/admin null-area behavior, footprint area per basin, Meta lineage, and complete absence of document/Qdrant runtime code.
