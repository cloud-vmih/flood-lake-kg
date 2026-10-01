# Silver Basin Topology Implementation Plan

**Vietnamese version:** `docs/superpowers/plans/2026-09-30-silver-basin-topology.vi.md`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the canonical HydroBASINS level-12 basin dimension and directed downstream edge table from Bronze basin rows.

**Architecture:** Read one immutable Bronze basin snapshot, use HydroBASINS for geometry/topology, join only approved BasinATLAS fields by `HYBAS_ID`, and publish versioned basin and edge rows. Pure normalization/topology modules remain independent of Airflow and storage.

**Tech Stack:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, PyArrow, Shapely, PyProj, PyIceberg, pytest.

**Spec:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.md`

## Global Constraints

- Requires the completed `silver-common-foundation` plan.
- HydroBASINS is the primary geometry and topology source; BasinATLAS only enriches the approved field allowlist.
- `hydrobasins_level` is exactly `12`.
- Geometry is normalized to EPSG:4326 and versioned by content/config.
- `NEXT_DOWN` outside the selected AOI remains available for QA and produces `edge_status=exits_AOI`.
- Cycles and duplicate basin keys are fatal DQ failures.
- Compute may fan out; each target table has one commit task.

---

## File structure

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

The DAG imports only `factory.py`. `factory.py` builds `service.py`. The service calls `reader.py`, `normalize.py`, `topology.py`, and `quality.py`, then shared staging/publisher/audit. No other Silver domain imports this package; downstream pipelines consume `silver.dim_basin` and `silver.basin_edge`.

### Task 1: Register physical schemas and basin configuration

**Files:**
- Create: `config/silver/basin_topology.yaml`
- Create: `src/flashflood_data/orchestration/silver/basin/__init__.py`
- Create: `src/flashflood_data/orchestration/silver/basin/config.py`
- Create: `src/flashflood_data/orchestration/silver/basin/models.py`
- Modify: `src/flashflood_data/storage/iceberg_schemas.py`
- Modify: `config/meta/static.yaml`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`
- Create: `tests/unit/silver/basin/test_config.py`

**Interfaces:**
- Produces: `BasinTopologyConfig(level, hydro_source_id, atlas_source_id, field_map, transform_version, contract_version)`.
- Produces: `BasinRow`, `BasinEdgeRow`, and `TopologyReport` Pydantic models matching `docs/schema_contract/data.md`.
- Produces physical tables `silver.dim_basin` and `silver.basin_edge`.

- [ ] **Step 1: Write failing schema/config tests**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/basin/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: FAIL because config, models, and schemas are missing.

- [ ] **Step 3: Add the exact contract and validated YAML**

The YAML must contain:

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

Add every column and nullability from the two approved Silver tables to `_DEFINITIONS`, and add both dataset descriptions to `config/meta/static.yaml`.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/basin/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit contract**

```bash
git add config/silver/basin_topology.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/basin src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/basin/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define Silver basin contracts"
```

### Task 2: Read and normalize Bronze basin rows

**Files:**
- Create: `src/flashflood_data/orchestration/silver/basin/reader.py`
- Create: `src/flashflood_data/orchestration/silver/basin/normalize.py`
- Create: `tests/unit/silver/basin/test_normalize.py`

**Interfaces:**
- Consumes: rows from `bronze.basin_polygon_raw`, `BasinTopologyConfig`.
- Produces: `BasinBronzeInputs(hydro_rows, atlas_rows, snapshot_ref)`.
- Produces: `normalize_basins(inputs, config, basin_version, valid_from) -> list[BasinRow]`.

- [ ] **Step 1: Write normalization tests with one internal and one external downstream ID**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/basin/test_normalize.py -q`  
Expected: FAIL because reader/normalizer do not exist.

- [ ] **Step 3: Implement snapshot-bound reading and normalization**

`BasinBronzeReader.read(snapshot_id, hydro_source_id, atlas_source_id)` scans the requested snapshot, filters by `source_id`, decodes `source_fields_json`, and rejects rows whose source or `HYBAS_ID` is missing. `normalize_basins` parses WKB, repairs only validly repairable polygons, orients them consistently, writes canonical WKB, calculates `geometry_hash`, and maps only configured fields.

Generate `basin_version` with:

```python
basin_version = canonical_hash({
    "input_snapshot": snapshot_id,
    "level": config.level,
    "transform_version": config.transform_version,
})[:16]
```

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/basin/test_normalize.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit normalization**

```bash
git add src/flashflood_data/orchestration/silver/basin/reader.py src/flashflood_data/orchestration/silver/basin/normalize.py tests/unit/silver/basin/test_normalize.py
git commit -m "feat: normalize Bronze basins for Silver"
```

### Task 3: Build topology and domain quality gates

**Files:**
- Create: `src/flashflood_data/orchestration/silver/basin/topology.py`
- Create: `src/flashflood_data/orchestration/silver/basin/quality.py`
- Create: `tests/unit/silver/basin/test_topology.py`
- Create: `tests/unit/silver/basin/test_quality.py`

**Interfaces:**
- Consumes: `Sequence[BasinRow]`, configured topology version.
- Produces: `build_basin_edges(rows: Sequence[BasinRow], topology_version: str) -> tuple[list[BasinEdgeRow], TopologyReport]`.
- Produces: `check_basins(rows) -> list[QualityResult]` and `check_topology(rows, edges, report) -> list[QualityResult]`.

- [ ] **Step 1: Write graph behavior tests**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/basin/test_topology.py tests/unit/silver/basin/test_quality.py -q`  
Expected: FAIL because topology and DQ functions are missing.

- [ ] **Step 3: Implement graph construction and explicit rules**

Build one edge per upstream basin. Treat `None`, `0`, and self-sink according to the source contract as `terminal`; a nonzero ID absent from AOI is `exits_AOI`; an ID in the selected set is `internal`. Use Kahn topological sorting and retain the original `SORT` only as a QA comparison.

Implement fatal rules for unique keys, valid polygon, positive area, acyclic graph, and valid internal target. Implement warnings for area disagreement, sort-hint mismatch, and missing optional sink metadata.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/basin/test_topology.py tests/unit/silver/basin/test_quality.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit topology**

```bash
git add src/flashflood_data/orchestration/silver/basin/topology.py src/flashflood_data/orchestration/silver/basin/quality.py tests/unit/silver/basin/test_topology.py tests/unit/silver/basin/test_quality.py
git commit -m "feat: build and validate basin topology"
```

### Task 4: Orchestrate, publish, and repair basin builds

**Files:**
- Create: `src/flashflood_data/orchestration/silver/basin/service.py`
- Create: `src/flashflood_data/orchestration/silver/basin/factory.py`
- Modify: `src/flashflood_data/orchestration/silver/basin/__init__.py`
- Create: `tests/unit/silver/basin/test_service.py`
- Create: `tests/integration/silver/test_basin_pipeline.py`

**Interfaces:**
- Consumes: common `SilverDependencies` and domain config/modules.
- Produces: `BasinSilverService.plan(run_id)`, `compute(request)`, and `publish(request) -> SilverRunResult`.
- Produces: `build_basin_silver_service(root=None) -> BasinSilverService`.

- [ ] **Step 1: Write service idempotency and repair tests**

```python
def test_completed_build_skips_all_compute(service):
    result = service.run("run-2")
    assert result.status == "skipped"
    service.reader.read.assert_not_called()

def test_partial_build_publishes_only_missing_edge_table(service):
    result = service.run("repair-1")
    assert [item.table_name for item in result.outputs] == ["silver.basin_edge"]
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/basin/test_service.py -q`  
Expected: FAIL because the service/factory are missing.

- [ ] **Step 3: Implement the use case**

The service calls common discovery for `bronze.basin_polygon_raw`, computes both staged batches once, validates all fatal rules, and publishes with these keys:

```python
DIM_BASIN_KEY = ("basin_id", "basin_version")
BASIN_EDGE_KEY = ("topology_version", "basin_version", "upstream_basin_id")
```

Record input/output refs and one lineage edge per Bronze-to-Silver output. On exception, write a failed/partial run and retain staging.

- [ ] **Step 4: Run unit and fake-catalog integration tests**

Run: `pytest tests/unit/silver/basin tests/integration/silver/test_basin_pipeline.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit service**

```bash
git add src/flashflood_data/orchestration/silver/basin tests/unit/silver/basin/test_service.py tests/integration/silver/test_basin_pipeline.py
git commit -m "feat: publish Silver basin topology"
```

### Task 5: Add the manual Airflow DAG and operator documentation

**Files:**
- Create: `airflow/dags/silver_basin_topology.py`
- Create: `tests/contract/infra/test_silver_dags.py`
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`

**Interfaces:**
- Consumes: `build_basin_silver_service` and serializable request/result documents.
- Produces: DAG ID `silver_basin_topology`, `schedule=None`, `catchup=False`, `max_active_runs=1`.

- [ ] **Step 1: Write DAG contract tests**

```python
def test_basin_dag_is_manual_and_single_run(dag_bag):
    dag = dag_bag.get_dag("silver_basin_topology")
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert {"discover_inputs", "compute_batches", "publish_outputs", "finalize_run"} <= set(dag.task_ids)
```

- [ ] **Step 2: Run the DAG test and verify failure**

Run: `pytest tests/contract/infra/test_silver_dags.py -q`  
Expected: FAIL because the DAG is absent.

- [ ] **Step 3: Implement the thin TaskFlow DAG**

Tasks pass only JSON/Pydantic documents through XCom. `compute_batches` returns paths and counts, never geometry rows. `publish_outputs` is the sole writer for both small basin tables and runs after every compute batch.

Add README commands:

```bash
docker compose exec airflow-api-server airflow dags trigger silver_basin_topology
docker compose exec trino trino --execute 'SELECT count(*) FROM lakehouse.silver.dim_basin'
```

- [ ] **Step 4: Validate DAG and domain suite**

Run: `pytest tests/contract/infra/test_silver_dags.py tests/unit/silver/basin tests/integration/silver/test_basin_pipeline.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit DAG and docs**

```bash
git add airflow/dags/silver_basin_topology.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver basin topology DAG"
```

## Checkpoint

Stop and inspect `silver.dim_basin`, `silver.basin_edge`, Meta snapshot refs, and lineage in Trino. Do not start another Silver domain until L12 row counts, cycle checks, and rerun skip behavior are accepted.
