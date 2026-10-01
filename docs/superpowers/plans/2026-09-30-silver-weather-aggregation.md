# Silver Weather Basin Aggregation Implementation Plan

**Vietnamese version:** `docs/superpowers/plans/2026-09-30-silver-weather-aggregation.vi.md`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert Bronze raster slices into versioned L12 basin weather values using reusable source-grid-to-basin intersection weights.

**Architecture:** Build weights only for unseen grid/basin/geometry versions, map each raster-slice array through stable `cell_index`, aggregate valid cells by intersection area, and append idempotent basin values by a deterministic business-key hash. NOW, Standard, ERA5-Land, and IFS remain separate observations in Silver.

**Tech Stack:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, NumPy, PyArrow, Shapely, PyProj, PyIceberg, pytest.

**Spec:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.md`

## Global Constraints

- Requires common foundation and `silver.dim_basin`.
- `silver.source_grid` is an existing weather-ingest output and must not be rebuilt here.
- Basin aggregation never treats missing/nodata grid cells as zero.
- `valid_coverage_fraction` is the valid intersection area divided by basin area.
- NOW half-hour-shifted windows remain distinct; Standard does not delete or overwrite NOW.
- IFS and ERA5-Land remain distinct sources even when variables overlap.
- Weight rebuild identity includes source grid version, basin version, and geometry processing version.
- Compute fans out by slice batch; each output table has one writer task.

---

## File structure

```text
config/silver/weather_aggregation.yaml
airflow/dags/silver_weather_aggregation.py
src/flashflood_data/orchestration/silver/weather/
  __init__.py
  config.py
  models.py
  reader.py
  weights.py
  aggregate.py
  revisions.py
  quality.py
  service.py
  factory.py
tests/unit/silver/weather/
  test_config.py
  test_weights.py
  test_aggregate.py
  test_revisions.py
  test_quality.py
  test_service.py
tests/integration/silver/test_weather_aggregation_pipeline.py
```

The service reads three table contracts, calls `weights.py` only for missing combinations, calls `aggregate.py` for missing slice keys, then publishes through the common writer/audit layer. No provider API or Raw object fetch is allowed in this package.

### Task 1: Add schemas, config, and row contracts

**Files:**
- Create: `config/silver/weather_aggregation.yaml`
- Create: `src/flashflood_data/orchestration/silver/weather/__init__.py`
- Create: `src/flashflood_data/orchestration/silver/weather/config.py`
- Create: `src/flashflood_data/orchestration/silver/weather/models.py`
- Modify: `src/flashflood_data/storage/iceberg_schemas.py`
- Modify: `config/meta/static.yaml`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`
- Create: `tests/unit/silver/weather/test_config.py`

**Interfaces:**
- Produces: physical `silver.grid_basin_weight` and `silver.basin_weather_value`.
- Produces: `WeatherAggregationConfig`, `GridBasinWeightRow`, `BasinWeatherValueRow`, `WeatherAggregationReport`.

- [ ] **Step 1: Write failing schema and variable-policy tests**

```python
def test_weather_config_declares_every_bronze_variable():
    config = load_weather_aggregation_config(CONFIG)
    assert config.variables["precipitation"].unit == "mm"
    assert config.variables["surface_runoff"].aggregation == "area_mean"
    assert config.variables["subsurface_runoff"].aggregation == "area_mean"
    assert config.variables["soil_moisture"].aggregation == "area_mean"

def test_basin_weather_schema_keeps_source_and_revision():
    names = set(table_schema(("silver", "basin_weather_value")).names)
    assert {"source_id", "source_product", "source_revision", "available_at",
            "valid_coverage_fraction", "business_key_hash"} <= names
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/weather/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: FAIL because config/models/schemas are absent.

- [ ] **Step 3: Implement validated policy and physical contracts**

The YAML defines `geometry_processing_version`, equal-area CRS, minimum publish coverage, slice batch size, canonical units, and aggregation method for precipitation, soil-moisture layers, surface runoff, and subsurface runoff. Reject a variable without a unit or with an aggregation other than `area_mean`.

Add every approved column from `docs/schema_contract/data.md`. Register both datasets with `quality_policy_id: silver-weather-v1`.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/weather/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit contracts**

```bash
git add config/silver/weather_aggregation.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/weather src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/weather/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define Silver weather contracts"
```

### Task 2: Build reusable grid-basin weights

**Files:**
- Create: `src/flashflood_data/orchestration/silver/weather/reader.py`
- Create: `src/flashflood_data/orchestration/silver/weather/weights.py`
- Create: `tests/unit/silver/weather/test_weights.py`

**Interfaces:**
- Consumes: rows from `silver.source_grid`, `silver.dim_basin`, and existing weights.
- Produces: `WeatherSilverInputs` bound to exact snapshots.
- Produces: `build_grid_basin_weights(grid_rows, basin_rows, config) -> list[GridBasinWeightRow]`.

- [ ] **Step 1: Write exact-area weight tests**

```python
def test_two_equal_cells_cover_one_basin():
    rows = build_grid_basin_weights(two_equal_cells(), one_basin(), config)
    assert sum(row.weight_by_basin for row in rows) == pytest.approx(1.0)
    assert all(row.weight_by_grid == pytest.approx(1.0) for row in rows)

def test_partial_cell_uses_intersection_area():
    row = build_grid_basin_weights(one_half_intersection(), one_basin(), config)[0]
    assert row.weight_by_grid == pytest.approx(0.5, rel=1e-3)
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/weather/test_weights.py -q`  
Expected: FAIL because reader/weights do not exist.

- [ ] **Step 3: Implement indexed overlay in equal-area CRS**

Decode WKB, repair/reject invalid geometry according to config, reproject both layers to the configured equal-area CRS, use a spatial index to avoid a full Cartesian join, and calculate:

```python
intersection_area_m2 = intersection.area
weight_by_basin = intersection_area_m2 / basin.area
weight_by_grid = intersection_area_m2 / grid_cell.area
```

Skip zero-area touches. Generate `geometry_processing_version` from the configured algorithm and CRS, and preserve source/basin versions in every row.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/weather/test_weights.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit weights**

```bash
git add src/flashflood_data/orchestration/silver/weather/reader.py src/flashflood_data/orchestration/silver/weather/weights.py tests/unit/silver/weather/test_weights.py
git commit -m "feat: build source-grid basin weights"
```

### Task 3: Aggregate slices and retain revision semantics

**Files:**
- Create: `src/flashflood_data/orchestration/silver/weather/aggregate.py`
- Create: `src/flashflood_data/orchestration/silver/weather/revisions.py`
- Create: `src/flashflood_data/orchestration/silver/weather/quality.py`
- Create: `tests/unit/silver/weather/test_aggregate.py`
- Create: `tests/unit/silver/weather/test_revisions.py`
- Create: `tests/unit/silver/weather/test_quality.py`

**Interfaces:**
- Consumes: one `weather_raster_slice`, cell-index lookup, relevant weights, basin/version, variable policy.
- Produces: `aggregate_slice(slice_row: Mapping[str, object], weights: Sequence[GridBasinWeightRow], context: AggregationContext) -> tuple[list[BasinWeatherValueRow], WeatherAggregationReport]`.
- Produces: `weather_business_key(row_without_hash) -> str`.
- Produces: `check_weather_values(rows: Sequence[BasinWeatherValueRow], report: WeatherAggregationReport, config: WeatherAggregationConfig) -> list[QualityResult]`.

- [ ] **Step 1: Write aggregation, missing-data, and window tests**

```python
def test_area_mean_renormalizes_over_valid_coverage():
    result, report = aggregate_slice(slice_values([10.0, float("nan")]), weights([0.5, 0.5]), context)
    assert result[0].value == pytest.approx(10.0)
    assert result[0].valid_coverage_fraction == pytest.approx(0.5)

def test_now_and_standard_same_window_have_different_business_keys():
    assert weather_business_key(now_row()) != weather_business_key(standard_row())

def test_shifted_now_windows_are_both_retained():
    assert weather_business_key(now_at("10:00")) != weather_business_key(now_at("10:30"))
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/weather/test_aggregate.py tests/unit/silver/weather/test_revisions.py tests/unit/silver/weather/test_quality.py -q`  
Expected: FAIL because aggregation/revision/DQ code is absent.

- [ ] **Step 3: Implement aligned array aggregation and full business identity**

Build a `cell_index -> value` lookup from positionally aligned arrays and reject length mismatch or duplicate indices. For each basin:

```python
valid_weight = sum(weight_by_basin for valid cells)
value = sum(cell_value * weight_by_basin for valid cells) / valid_weight
coverage = valid_weight
```

The business-key hash includes basin/version, source/product/grid version, variable/level/cycle, model run, valid time, window start/end, and revision. It excludes mutable QA flags. Resolve missing `available_at` from the source-object Meta record; reject the row if no defensible availability time exists.

Fatal DQ: array misalignment, unknown cell, duplicate business key, unit/value-kind mismatch, invalid time window, coverage outside 0–1. Low coverage is a warning and sets `value=None` when below the configured publish threshold.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/weather/test_aggregate.py tests/unit/silver/weather/test_revisions.py tests/unit/silver/weather/test_quality.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit aggregation**

```bash
git add src/flashflood_data/orchestration/silver/weather/aggregate.py src/flashflood_data/orchestration/silver/weather/revisions.py src/flashflood_data/orchestration/silver/weather/quality.py tests/unit/silver/weather/test_aggregate.py tests/unit/silver/weather/test_revisions.py tests/unit/silver/weather/test_quality.py
git commit -m "feat: aggregate weather slices by basin"
```

### Task 4: Add incremental service, DAG, and integration coverage

**Files:**
- Create: `src/flashflood_data/orchestration/silver/weather/service.py`
- Create: `src/flashflood_data/orchestration/silver/weather/factory.py`
- Create: `airflow/dags/silver_weather_aggregation.py`
- Create: `tests/unit/silver/weather/test_service.py`
- Create: `tests/integration/silver/test_weather_aggregation_pipeline.py`
- Modify: `tests/contract/infra/test_silver_dags.py`
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`

**Interfaces:**
- Produces: `WeatherSilverService.plan`, `ensure_weights`, `aggregate_batch`, `publish`, `run`.
- Produces: `build_weather_silver_service(root=None)`.
- Produces manual DAG ID `silver_weather_aggregation`.

- [ ] **Step 1: Write incremental and one-writer tests**

```python
def test_existing_slice_business_keys_are_not_recomputed(service):
    plan = service.plan("run-2")
    assert plan.slice_ids == ("new-slice",)

def test_weight_table_is_reused_for_same_versions(service):
    service.ensure_weights(request)
    service.weight_builder.build.assert_not_called()

def test_weather_dag_has_one_publish_task(dag_bag):
    dag = dag_bag.get_dag("silver_weather_aggregation")
    assert dag.schedule is None
    assert "publish_weather_values" in dag.task_ids
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/weather/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Expected: FAIL because service/DAG are absent.

- [ ] **Step 3: Implement incremental planning and TaskFlow graph**

Task graph:

```text
discover_snapshots -> plan_build -> ensure_weight_batches
  -> publish_weights -> plan_missing_slices -> aggregate_slice_batches.expand
  -> publish_weather_values -> audit_and_finalize -> cleanup_staging
```

Use keys:

```python
WEIGHT_KEY = ("source_id", "source_grid_version", "source_grid_id", "basin_id",
              "basin_version", "geometry_processing_version")
WEATHER_KEY = ("business_key_hash",)
```

The integration test creates a two-cell grid, two basins, NOW/Standard slices, one missing cell, and a later revision. Assert values, coverage, retained source rows, exact lineage, rerun skip, and one Iceberg commit per published table.

- [ ] **Step 4: Run domain and existing weather regression tests**

Run: `pytest tests/unit/silver/weather tests/integration/silver/test_weather_aggregation_pipeline.py tests/unit/weather tests/contract/infra/test_silver_dags.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit the weather Silver pipeline**

```bash
git add src/flashflood_data/orchestration/silver/weather airflow/dags/silver_weather_aggregation.py tests/unit/silver/weather tests/integration/silver/test_weather_aggregation_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver weather aggregation DAG"
```

## Checkpoint

Stop after running a small GSMaP NOW, GSMaP Standard, and ERA5 fixture. Query both Silver tables in Trino and verify coverage, windows, revisions, `available_at`, and rerun behavior before aggregating the full backfill.
