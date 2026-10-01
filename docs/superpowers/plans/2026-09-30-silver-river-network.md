# Silver River Network Implementation Plan

**Vietnamese version:** `docs/superpowers/plans/2026-09-30-silver-river-network.vi.md`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Normalize HydroRIVERS reaches, construct directed reach topology, and relate every reach to versioned L12 basins.

**Architecture:** Use Bronze river geometry and source fields to build stable reach rows, derive downstream edges from configured source IDs/node snapping, and calculate basin intersections in a metric CRS. Publish the reach, edge, and bridge tables as independent versioned Silver outputs.

**Tech Stack:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, GeoPandas, Shapely, PyProj, PyArrow, PyIceberg, pytest.

**Spec:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.md`

## Global Constraints

- Requires common foundation and `silver.dim_basin`.
- Preserve complete reaches selected at national AOI landing; do not silently clip the canonical reach geometry.
- All stable IDs include source identity and river version.
- Topology direction follows provider direction when available; geometric inference is explicitly versioned.
- Basin intersection length is calculated in a metric CRS.
- Directed cycles, duplicate reach IDs, and invalid internal targets are fatal.
- Each target table has one Iceberg writer task.

---

## File structure

```text
config/silver/river_network.yaml
airflow/dags/silver_river_network.py
src/flashflood_data/orchestration/silver/river/
  __init__.py
  config.py
  models.py
  reader.py
  normalize.py
  topology.py
  basin_overlay.py
  quality.py
  service.py
  factory.py
tests/unit/silver/river/
  test_config.py
  test_normalize.py
  test_topology.py
  test_basin_overlay.py
  test_quality.py
  test_service.py
tests/integration/silver/test_river_pipeline.py
```

`normalize.py`, `topology.py`, and `basin_overlay.py` are pure domain transforms. `service.py` owns their sequence and common publication/audit. Downstream exposure code reads Silver river tables and never imports these internals.

### Task 1: Define river schemas, config, and models

**Files:**
- Create: `config/silver/river_network.yaml`
- Create: `src/flashflood_data/orchestration/silver/river/__init__.py`
- Create: `src/flashflood_data/orchestration/silver/river/config.py`
- Create: `src/flashflood_data/orchestration/silver/river/models.py`
- Modify: `src/flashflood_data/storage/iceberg_schemas.py`
- Modify: `config/meta/static.yaml`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`
- Create: `tests/unit/silver/river/test_config.py`

**Interfaces:**
- Produces: `RiverNetworkConfig`, `RiverReachRow`, `RiverReachEdgeRow`, `RiverBasinRow`, `RiverTopologyReport`.
- Produces physical `silver.dim_river_reach`, `silver.river_reach_edge`, `silver.river_basin`.

- [ ] **Step 1: Write failing config/schema tests**

```python
def test_river_config_declares_source_fields_and_tolerance():
    config = load_river_config(CONFIG)
    assert config.source_id == "hydrorivers_v10"
    assert config.field_map["river_reach_id"] == "HYRIV_ID"
    assert config.snap_tolerance_m > 0

def test_river_basin_schema_is_versioned():
    names = set(table_schema(("silver", "river_basin")).names)
    assert {"river_reach_id", "river_version", "basin_id", "basin_version",
            "geometry_processing_version", "length_inside_km"} <= names
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/river/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: FAIL because contracts are missing.

- [ ] **Step 3: Implement validated config, models, and exact schemas**

YAML fields include source ID, provider field map, source direction convention, snap tolerance, length CRS, transform/topology/geometry-processing versions, and batch size. Reject an absent reach-ID mapping or nonpositive tolerance.

- [ ] **Step 4: Run contract tests**

Run: `pytest tests/unit/silver/river/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit contracts**

```bash
git add config/silver/river_network.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/river src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/river/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define Silver river contracts"
```

### Task 2: Normalize reaches and construct topology

**Files:**
- Create: `src/flashflood_data/orchestration/silver/river/reader.py`
- Create: `src/flashflood_data/orchestration/silver/river/normalize.py`
- Create: `src/flashflood_data/orchestration/silver/river/topology.py`
- Create: `tests/unit/silver/river/test_normalize.py`
- Create: `tests/unit/silver/river/test_topology.py`

**Interfaces:**
- Consumes: exact `bronze.river_reach_raw` snapshot and config.
- Produces: `normalize_reaches(rows: Sequence[Mapping[str, object]], config: RiverNetworkConfig, river_version: str) -> list[RiverReachRow]`.
- Produces: `build_reach_edges(reaches, config) -> tuple[list[RiverReachEdgeRow], RiverTopologyReport]`.

- [ ] **Step 1: Write normalization and topology tests**

```python
def test_normalize_preserves_provider_id_and_metric_length():
    reach = normalize_reaches([bronze_reach("42")], config, "rv1")[0]
    assert reach.hydrorivers_id == "42"
    assert reach.crs == "EPSG:4326"
    assert reach.length_km > 0

def test_topology_links_provider_downstream_id():
    edges, report = build_reach_edges(reaches_with_next_down(), config)
    assert edges[0].upstream_reach_id == "42"
    assert edges[0].downstream_reach_id == "43"
    assert report.cycle_reach_ids == ()
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/river/test_normalize.py tests/unit/silver/river/test_topology.py -q`  
Expected: FAIL because reader/transforms are absent.

- [ ] **Step 3: Implement canonical reach and graph logic**

Read only rows belonging to the configured source object set and snapshot. Decode source JSON once. Normalize LineString/MultiLineString deterministically, reject empty geometry, calculate metric length, and preserve provider ID. Use provider downstream ID first; use snapped endpoint inference only when provider linkage is absent and exactly one candidate lies within tolerance. Ambiguous candidates remain dangling and are reported.

Generate versions from input snapshot/config and use Kahn sorting to identify cycles.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/river/test_normalize.py tests/unit/silver/river/test_topology.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit reach normalization**

```bash
git add src/flashflood_data/orchestration/silver/river/reader.py src/flashflood_data/orchestration/silver/river/normalize.py src/flashflood_data/orchestration/silver/river/topology.py tests/unit/silver/river/test_normalize.py tests/unit/silver/river/test_topology.py
git commit -m "feat: normalize and connect river reaches"
```

### Task 3: Overlay reaches with basins and add quality gates

**Files:**
- Create: `src/flashflood_data/orchestration/silver/river/basin_overlay.py`
- Create: `src/flashflood_data/orchestration/silver/river/quality.py`
- Create: `tests/unit/silver/river/test_basin_overlay.py`
- Create: `tests/unit/silver/river/test_quality.py`

**Interfaces:**
- Produces: `overlay_reaches_with_basins(reaches, basins, config) -> list[RiverBasinRow]`.
- Produces: `check_river_outputs(reaches, edges, relations, report) -> list[QualityResult]`.

- [ ] **Step 1: Write overlay and DQ tests**

```python
def test_reach_crossing_two_basins_has_two_length_rows():
    rows = overlay_reaches_with_basins(crossing_reach(), adjacent_basins(), config)
    assert len(rows) == 2
    assert sum(row.intersection_ratio for row in rows) == pytest.approx(1.0, rel=1e-3)

def test_cycle_and_ratio_over_one_are_fatal():
    failures = fatal_failures(check_river_outputs(reaches, cyclic_edges, bad_relations, report))
    assert {item.rule_id for item in failures} >= {"river_topology_acyclic", "river_basin_ratio_range"}
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/river/test_basin_overlay.py tests/unit/silver/river/test_quality.py -q`  
Expected: FAIL because overlay/DQ are absent.

- [ ] **Step 3: Implement spatial index, relation semantics, and named rules**

Calculate `length_inside_km` after intersection in metric CRS. `spatial_relation` is `within` when the entire reach is covered, `intersects` for positive partial length, and `touches` only for a zero-length boundary contact retained for QA. Fatal rules cover keys, geometry, positive length, internal edge target, cycles, and ratios outside `[0,1]`; dangling/ambiguous endpoints are warnings.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/river/test_basin_overlay.py tests/unit/silver/river/test_quality.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit overlay and DQ**

```bash
git add src/flashflood_data/orchestration/silver/river/basin_overlay.py src/flashflood_data/orchestration/silver/river/quality.py tests/unit/silver/river/test_basin_overlay.py tests/unit/silver/river/test_quality.py
git commit -m "feat: relate river reaches to basins"
```

### Task 4: Add service, manual DAG, integration test, and docs

**Files:**
- Create: `src/flashflood_data/orchestration/silver/river/service.py`
- Create: `src/flashflood_data/orchestration/silver/river/factory.py`
- Create: `airflow/dags/silver_river_network.py`
- Create: `tests/unit/silver/river/test_service.py`
- Create: `tests/integration/silver/test_river_pipeline.py`
- Modify: `tests/contract/infra/test_silver_dags.py`
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`

**Interfaces:**
- Produces: `RiverSilverService.plan`, `compute_batch`, `publish`, `run`.
- Produces: `build_river_silver_service(root=None)`.
- Produces manual DAG ID `silver_river_network`.

- [ ] **Step 1: Write publication and DAG tests**

```python
def test_river_publish_commits_three_tables_once(service):
    result = service.run("river-1")
    assert {item.table_name for item in result.outputs} == {
        "silver.dim_river_reach", "silver.river_reach_edge", "silver.river_basin"
    }

def test_river_dag_waits_for_all_compute_batches(dag_bag):
    dag = dag_bag.get_dag("silver_river_network")
    assert dag.schedule is None
    assert "publish_outputs" in dag.task_ids
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/river/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Expected: FAIL because service/DAG are absent.

- [ ] **Step 3: Implement service and TaskFlow graph**

Use common discovery with Bronze river and Silver basin snapshots. Compute reach batches, merge topology once across the complete reach set, calculate overlays by batch, validate globally, then publish with the exact table primary keys from the schema contract. Record lineage from both input snapshots to every dependent output.

- [ ] **Step 4: Run domain tests**

Run: `pytest tests/unit/silver/river tests/integration/silver/test_river_pipeline.py tests/contract/infra/test_silver_dags.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit pipeline**

```bash
git add src/flashflood_data/orchestration/silver/river airflow/dags/silver_river_network.py tests/unit/silver/river tests/integration/silver/test_river_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver river network DAG"
```

## Checkpoint

Inspect reach counts, length totals, cycles, dangling endpoints, and basin intersection ratios in Trino. Accept the network version before the exposure pipeline uses it for crossings.
