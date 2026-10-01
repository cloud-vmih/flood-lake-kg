# Silver Basin Static Features Implementation Plan

**Vietnamese version:** `docs/superpowers/plans/2026-09-30-silver-basin-static-features.vi.md`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce versioned DEM, hydrology, SoilGrids, land-cover, and BasinATLAS features for each L12 basin with complete raw/derived asset lineage.

**Architecture:** Read basin geometry and Bronze raster inventories, compute source-specific partial feature batches, assemble one row per basin/build, and publish through one writer per table. Persist flow-direction and flow-accumulation rasters as derived MinIO objects in a dedicated Meta registry rather than misclassifying them as Raw source objects.

**Tech Stack:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, Rasterio, NumPy, SciPy, GeoPandas, PyProj, PyArrow, PyIceberg, MinIO, pytest.

**Spec:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.md`

## Global Constraints

- Requires completed common foundation and basin topology plans.
- DEM is authoritative for terrain; BasinATLAS terrain/runoff/discharge fields are QA/reference only.
- SoilGrids 0–30 cm uses thickness weights 5/10/15 cm and property-specific scale/unit conversion.
- `awc_m3m3_0_30 = field_capacity_m3m3_0_30 - wilting_point_m3m3_0_30` and must not be negative after QA.
- Channel slope uses longest flow path source/outlet elevation, never mean terrain slope.
- Derived raster identity includes input DEM snapshot/object IDs, processing config, and checksum.
- Compute fans out by basin batch; each Iceberg table is committed by one writer task.

---

## File structure

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

`service.py` calls the reader and source-specific calculators, then `assemble.py`, `lineage.py`, and `quality.py`. The calculators may import already-tested pure kernels in `src/flashflood_data/static/features/`; they must not import `static/workflow/runner.py`, handlers, or local file-first state.

### Task 1: Close the derived-asset contract and register schemas

**Files:**
- Modify: `docs/schema_contract/data.md`
- Create: `config/silver/basin_static_features.yaml`
- Create: `src/flashflood_data/orchestration/silver/static_features/__init__.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/config.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/models.py`
- Modify: `src/flashflood_data/storage/iceberg_schemas.py`
- Modify: `config/meta/static.yaml`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`
- Create: `tests/unit/silver/static_features/test_config.py`

**Interfaces:**
- Produces: physical `meta.derived_objects`, `silver.basin_static_feature`, and `silver.basin_feature_lineage` tables.
- Produces: `StaticFeatureConfig`, `PartialFeatureRow`, `BasinStaticFeatureRow`, `FeatureLineageRow`, `DerivedObjectRow`.
- Changes lineage key to include `object_kind: Literal["source", "derived"]`.

- [ ] **Step 1: Write failing contract tests**

```python
def test_derived_object_registry_is_separate_from_raw_inventory():
    schema = table_schema(("meta", "derived_objects"))
    assert {"derived_object_id", "object_uri", "checksum", "processing_version",
            "pipeline_run_id", "created_at"} <= set(schema.names)

def test_feature_lineage_disambiguates_object_kind():
    schema = table_schema(("silver", "basin_feature_lineage"))
    assert schema.field("object_kind").nullable is False
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/static_features/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: FAIL because the Silver/derived contracts are absent.

- [ ] **Step 3: Define the explicit derived registry and configuration**

Add this logical contract to `data.md` and its physical Arrow schema:

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

Add `object_kind` to `basin_feature_lineage`; `object_id` points to `meta.source_objects` when `source`, and `meta.derived_objects.derived_object_id` when `derived`.

The YAML must declare exact depth weights, scale factors, stream threshold, outlet snapping tolerance, hydrology algorithm version, Tc method/version, beta values `[0.5, 1.0, 2.0]`, raster resampling per property, output CRS, and basin batch size. Reject weights whose thickness sum is not 30 cm.

- [ ] **Step 4: Run contract tests**

Run: `pytest tests/unit/silver/static_features/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit contracts**

```bash
git add docs/schema_contract/data.md config/silver/basin_static_features.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/static_features src/flashflood_data/storage/iceberg_schemas.py tests/unit/silver/static_features/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: define static feature and derived asset contracts"
```

### Task 2: Resolve raster inputs and compute non-hydrology features

**Files:**
- Create: `src/flashflood_data/orchestration/silver/static_features/reader.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/raster_inputs.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/terrain.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/soil.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/landcover.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/atlas.py`
- Create: `tests/unit/silver/static_features/test_raster_inputs.py`
- Create: `tests/unit/silver/static_features/test_terrain.py`
- Create: `tests/unit/silver/static_features/test_soil.py`
- Create: `tests/unit/silver/static_features/test_landcover.py`
- Create: `tests/unit/silver/static_features/test_atlas.py`

**Interfaces:**
- Consumes: `silver.dim_basin`, `bronze.raster_coverage`, BasinATLAS Bronze rows, Meta source objects.
- Produces: `StaticFeatureInputs` grouped by basin/source object.
- Produces: `compute_terrain`, `compute_soil_0_30`, `compute_landcover`, and `map_atlas_fields`, each returning `PartialFeatureRow` plus source contributions.

- [ ] **Step 1: Write fixture-based calculation tests**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/static_features/test_raster_inputs.py tests/unit/silver/static_features/test_terrain.py tests/unit/silver/static_features/test_soil.py tests/unit/silver/static_features/test_landcover.py tests/unit/silver/static_features/test_atlas.py -q`  
Expected: FAIL because the modules are missing.

- [ ] **Step 3: Implement snapshot-bound readers and pure calculations**

Resolve each raster through `bronze.raster_coverage.object_uri`, verify it matches the registered source object URI/checksum, open only the basin window, transform basin geometry into the raster CRS, and mask nodata before statistics.

Use explicit property output mapping:

```python
SOIL_OUTPUTS = {
    "clay": "soil_clay_pct_0_30", "sand": "soil_sand_pct_0_30",
    "silt": "soil_silt_pct_0_30", "bdod": "bulk_density_kg_dm3_0_30",
    "cfvo": "coarse_fragments_pct_0_30", "soc": "soil_organic_carbon_gkg_0_30",
    "wv0033": "field_capacity_m3m3_0_30", "wv1500": "wilting_point_m3m3_0_30",
}
```

Calculate AWC only after FC/WP conversion. Keep Q05/Q50/Q95 by property until `soil_uncertainty_ratio` is built according to config. Land-cover percentages use valid basin area as denominator.

- [ ] **Step 4: Run calculation tests**

Run: `pytest tests/unit/silver/static_features/test_raster_inputs.py tests/unit/silver/static_features/test_terrain.py tests/unit/silver/static_features/test_soil.py tests/unit/silver/static_features/test_landcover.py tests/unit/silver/static_features/test_atlas.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit source calculators**

```bash
git add src/flashflood_data/orchestration/silver/static_features tests/unit/silver/static_features
git commit -m "feat: compute static raster and Atlas features"
```

### Task 3: Compute hydrology and publish derived rasters safely

**Files:**
- Create: `src/flashflood_data/orchestration/silver/static_features/hydrology.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/lineage.py`
- Create: `tests/unit/silver/static_features/test_hydrology.py`
- Create: `tests/unit/silver/static_features/test_lineage.py`

**Interfaces:**
- Consumes: conditioned DEM window, basin polygon, hydrology config, object-store publisher.
- Produces: `compute_hydrology(dem: RasterWindow, basin: BasinRow, config: StaticFeatureConfig, publisher: DerivedObjectPublisher) -> HydrologyResult`.
- Produces: deterministic `register_derived_object(row) -> int | None` and lineage rows with `object_kind`.

- [ ] **Step 1: Write synthetic DEM and identity tests**

```python
def test_hydrology_uses_longest_path_for_channel_slope():
    result = compute_hydrology(synthetic_sloping_dem(), basin(), config)
    expected = (result.features["source_elevation_m"] -
                result.features["outlet_elevation_m"]) / result.features["longest_flow_path_m"]
    assert result.features["main_channel_slope_m_m"] == pytest.approx(expected)

def test_same_derived_payload_reuses_identity():
    assert derived_identity(input_ids, config, CHECKSUM) == derived_identity(input_ids, config, CHECKSUM)
```

Also test outlet snapping, positive Tc, Kb values `0.5/1/2 × Tc`, and rejection of a flat/zero-length channel without silently dividing by zero.

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/static_features/test_hydrology.py tests/unit/silver/static_features/test_lineage.py -q`  
Expected: FAIL because hydrology/derived lineage modules are missing.

- [ ] **Step 3: Implement the hydrology result and atomic object publication**

Condition the DEM, compute flow direction/accumulation, derive stream mask, snap outlet, trace the longest upstream path, sample endpoint elevations, and calculate channel slope/Tc/Kb. Publish GeoTIFFs to:

```text
derived/static/hydrology/<feature_build_version>/<basin_id>/flow_direction.tif
derived/static/hydrology/<feature_build_version>/<basin_id>/flow_accumulation.tif
```

Upload through staging, verify checksum, then copy to the final key. Register `meta.derived_objects` only after MinIO confirms the final object. Use `source` lineage for DEM/soil/landcover/Atlas inputs and `derived` lineage for the two hydrology rasters.

- [ ] **Step 4: Run hydrology tests**

Run: `pytest tests/unit/silver/static_features/test_hydrology.py tests/unit/silver/static_features/test_lineage.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit hydrology and lineage**

```bash
git add src/flashflood_data/orchestration/silver/static_features/hydrology.py src/flashflood_data/orchestration/silver/static_features/lineage.py tests/unit/silver/static_features/test_hydrology.py tests/unit/silver/static_features/test_lineage.py
git commit -m "feat: derive and register basin hydrology assets"
```

### Task 4: Assemble rows and enforce static-feature quality

**Files:**
- Create: `src/flashflood_data/orchestration/silver/static_features/assemble.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/quality.py`
- Create: `tests/unit/silver/static_features/test_assemble.py`
- Create: `tests/unit/silver/static_features/test_quality.py`

**Interfaces:**
- Consumes: all `PartialFeatureRow` values/contributions for one basin/build.
- Produces: `assemble_feature(partials: Sequence[PartialFeatureRow], context: FeatureBuildContext) -> tuple[BasinStaticFeatureRow, list[FeatureLineageRow]]`.
- Produces: `check_static_features(rows: Sequence[BasinStaticFeatureRow], lineage: Sequence[FeatureLineageRow]) -> list[QualityResult]`.

- [ ] **Step 1: Write assembly and DQ tests**

```python
def test_assemble_rejects_two_values_for_same_field():
    with pytest.raises(ValueError, match="conflicting feature"):
        assemble_feature([partial("elevation_mean_m", 100), partial("elevation_mean_m", 120)], context)

def test_negative_awc_is_fatal():
    failures = fatal_failures(check_static_features([feature(field_capacity=0.1, wilting_point=0.2)], lineage))
    assert any(item.rule_id == "static_awc_nonnegative" for item in failures)
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/static_features/test_assemble.py tests/unit/silver/static_features/test_quality.py -q`  
Expected: FAIL because assembler and DQ are missing.

- [ ] **Step 3: Implement deterministic assembly and named rules**

Require exactly one feature row per `(basin_id, basin_version, feature_build_version)`. Fatal checks cover duplicate keys, missing basin, impossible elevation order, nonpositive flow length, outlet outside snap tolerance, negative channel slope/AWC, invalid percentages, missing required source role, and lineage object absence. Atlas disagreement and low raster coverage are warnings with observed/expected values.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/static_features/test_assemble.py tests/unit/silver/static_features/test_quality.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit assembly and DQ**

```bash
git add src/flashflood_data/orchestration/silver/static_features/assemble.py src/flashflood_data/orchestration/silver/static_features/quality.py tests/unit/silver/static_features/test_assemble.py tests/unit/silver/static_features/test_quality.py
git commit -m "feat: assemble and validate basin static features"
```

### Task 5: Add service, batch DAG, integration test, and docs

**Files:**
- Create: `src/flashflood_data/orchestration/silver/static_features/service.py`
- Create: `src/flashflood_data/orchestration/silver/static_features/factory.py`
- Create: `airflow/dags/silver_basin_static_features.py`
- Create: `tests/unit/silver/static_features/test_service.py`
- Create: `tests/integration/silver/test_static_features_pipeline.py`
- Modify: `tests/contract/infra/test_silver_dags.py`
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`

**Interfaces:**
- Produces: `StaticFeatureSilverService.plan`, `compute_batch`, `publish`, `run`.
- Produces: `build_static_feature_service(root=None)`.
- Produces: manual DAG ID `silver_basin_static_features`.

- [ ] **Step 1: Write batch, repair, and DAG tests**

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

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/static_features/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Expected: FAIL because service/DAG are missing.

- [ ] **Step 3: Implement compute fan-out and publish fan-in**

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

The integration fixture uses two tiny basins and tiny DEM/soil/landcover rasters. Assert exact row counts, derived registry rows, MinIO keys, snapshot refs, lineage, rerun skip, and repair after one output is absent.

- [ ] **Step 4: Run domain tests**

Run: `pytest tests/unit/silver/static_features tests/integration/silver/test_static_features_pipeline.py tests/contract/infra/test_silver_dags.py -q`  
Expected: PASS.

- [ ] **Step 5: Run static kernel regression tests**

Run: `pytest tests/unit/static/test_terrain_features.py tests/unit/static/test_hydrology_features.py tests/unit/static/test_soil_features.py tests/unit/static/test_landcover_features.py -q`  
Expected: PASS.

- [ ] **Step 6: Commit pipeline**

```bash
git add src/flashflood_data/orchestration/silver/static_features airflow/dags/silver_basin_static_features.py tests/unit/silver/static_features tests/integration/silver/test_static_features_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver basin static feature DAG"
```

## Checkpoint

Stop after a two-basin run. Review every formula/unit, inspect both derived rasters, verify their Meta registry and lineage, then compare DEM-derived fields against BasinATLAS QA fields before scaling to all basins.
