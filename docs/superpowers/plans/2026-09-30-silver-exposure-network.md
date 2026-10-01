# Silver Exposure Network Implementation Plan

**Vietnamese version:** `docs/superpowers/plans/2026-09-30-silver-exposure-network.vi.md`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build versioned flood-relevant facilities, population cells, road graph, basin relations, and road-river crossings from Bronze OSM and WorldPop inputs.

**Architecture:** Normalize the approved OSM tag subset into separate facility and road transforms, register the WorldPop grid with the shared grid identity service, and calculate spatial relations against canonical basin and river versions. Compute stages are separated by output family while publication remains one writer per Iceberg table.

**Tech Stack:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, PyArrow, Rasterio, GeoPandas, Shapely, PyProj, NetworkX, PyIceberg, pytest.

**Spec:** `docs/superpowers/specs/2026-09-30-bronze-to-silver-pipelines-design.md`

## Global Constraints

- Requires common foundation, basin topology, and river network outputs.
- Only the flood-relevant OSM groups approved in `config/bronze/osm.yaml` enter Silver.
- Facility capacity and demographic subgroups remain null unless directly supplied by a source.
- Road graph IDs are stable for the OSM snapshot and graph-processing version.
- Bridge/ford classification uses OSM evidence; a pure geometric intersection is `unknown`.
- Population uses a shared `source_grid` identity; it does not create a second grid registry.
- Distances/lengths/areas use an appropriate projected CRS.
- Each output table has one writer task.

---

## File structure

```text
config/silver/exposure_network.yaml
airflow/dags/silver_exposure_network.py
src/flashflood_data/orchestration/spatial_grid.py
src/flashflood_data/orchestration/silver/exposure/
  __init__.py
  config.py
  models.py
  reader.py
  osm_tags.py
  facilities.py
  roads.py
  population.py
  basin_overlay.py
  crossings.py
  quality.py
  service.py
  factory.py
tests/unit/silver/exposure/
  test_config.py
  test_osm_tags.py
  test_facilities.py
  test_roads.py
  test_population.py
  test_basin_overlay.py
  test_crossings.py
  test_quality.py
  test_service.py
tests/integration/silver/test_exposure_pipeline.py
```

`reader.py` is the only domain file that reads Iceberg/object storage. Pure transforms consume models. `service.py` orchestrates them and common publication. Weather and exposure share `orchestration/spatial_grid.py`; neither imports the other domain.

### Task 1: Generalize source-grid registration and add exposure contracts

**Files:**
- Create: `src/flashflood_data/orchestration/spatial_grid.py`
- Modify: `src/flashflood_data/orchestration/weather/grids.py`
- Modify: `tests/unit/weather/test_grids.py`
- Create: `config/silver/exposure_network.yaml`
- Create: `src/flashflood_data/orchestration/silver/exposure/__init__.py`
- Create: `src/flashflood_data/orchestration/silver/exposure/config.py`
- Create: `src/flashflood_data/orchestration/silver/exposure/models.py`
- Modify: `src/flashflood_data/storage/iceberg_schemas.py`
- Modify: `config/meta/static.yaml`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`
- Create: `tests/unit/silver/exposure/test_config.py`

**Interfaces:**
- Produces: source-independent `GridDefinition`, `GridCell`, `SourceGridRegistrar` in `orchestration.spatial_grid`.
- Keeps: existing imports from `orchestration.weather.grids` through explicit re-exports.
- Produces: `ExposureConfig` and row models for all eight exposure tables.
- Produces physical facility, facility-basin, population, road-node, road-edge, road-basin, and river-crossing tables.

- [ ] **Step 1: Write compatibility and schema tests**

```python
def test_weather_grid_public_api_is_preserved():
    from flashflood_data.orchestration.weather.grids import GridDefinition
    from flashflood_data.orchestration.spatial_grid import GridDefinition as Shared
    assert GridDefinition is Shared

def test_exposure_config_facilities_are_explicit():
    config = load_exposure_config(CONFIG)
    assert {"hospital", "clinic", "shelter", "fire_station", "police", "school"} <= set(config.facility_types)

def test_road_edge_schema_keeps_bridge_and_tunnel():
    names = set(table_schema(("silver", "dim_road_edge")).names)
    assert {"bridge", "tunnel", "oneway", "highway_class", "source_object_id"} <= names
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/weather/test_grids.py tests/unit/silver/exposure/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: FAIL because the shared module and exposure contracts are absent.

- [ ] **Step 3: Move grid primitives without behavior changes and define contracts**

Move stable grid identity, row/column addressing, cell geometry, and registry logic from `weather/grids.py` into `orchestration/spatial_grid.py`. Keep weather re-exports so existing callers do not change in the same task.

The exposure YAML declares facility tag mappings, allowed road classes, speed defaults by class, graph/geometry versions, population source/year/value semantics, equal-area/distance CRS, and batch sizes. Add the exact approved table fields and primary keys from `data.md` to physical schemas and dataset registry.

- [ ] **Step 4: Run compatibility and contract tests**

Run: `pytest tests/unit/weather/test_grids.py tests/unit/silver/exposure/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit grid extraction and contracts**

```bash
git add src/flashflood_data/orchestration/spatial_grid.py src/flashflood_data/orchestration/weather/grids.py config/silver/exposure_network.yaml config/meta/static.yaml src/flashflood_data/orchestration/silver/exposure src/flashflood_data/storage/iceberg_schemas.py tests/unit/weather/test_grids.py tests/unit/silver/exposure/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "refactor: share source grid registration"
```

### Task 2: Normalize OSM facilities and road graph

**Files:**
- Create: `src/flashflood_data/orchestration/silver/exposure/reader.py`
- Create: `src/flashflood_data/orchestration/silver/exposure/osm_tags.py`
- Create: `src/flashflood_data/orchestration/silver/exposure/facilities.py`
- Create: `src/flashflood_data/orchestration/silver/exposure/roads.py`
- Create: `tests/unit/silver/exposure/test_osm_tags.py`
- Create: `tests/unit/silver/exposure/test_facilities.py`
- Create: `tests/unit/silver/exposure/test_roads.py`

**Interfaces:**
- Consumes: exact `bronze.osm_feature_raw` snapshot.
- Produces: `classify_osm_feature(tags, config) -> OSMClassification | None`.
- Produces: `build_facilities(rows: Sequence[Mapping[str, object]], context: ExposureContext) -> list[FacilityRow]`.
- Produces: `build_road_graph(rows: Sequence[Mapping[str, object]], context: ExposureContext) -> tuple[list[RoadNodeRow], list[RoadEdgeRow]]`.

- [ ] **Step 1: Write tag, facility, and graph tests**

```python
def test_hospital_and_shelter_are_kept_but_shop_is_dropped():
    assert classify_osm_feature({"amenity": "hospital"}, config).facility_type == "hospital"
    assert classify_osm_feature({"emergency": "shelter"}, config).facility_type == "shelter"
    assert classify_osm_feature({"shop": "mall"}, config) is None

def test_oneway_road_has_one_directed_edge():
    nodes, edges = build_road_graph([osm_way(tags={"highway": "primary", "oneway": "yes"})], context)
    assert len(edges) == 1
    assert edges[0].oneway is True

def test_capacity_is_not_inferred_from_name():
    assert build_facilities([hospital_without_capacity()], context)[0].capacity is None
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/exposure/test_osm_tags.py tests/unit/silver/exposure/test_facilities.py tests/unit/silver/exposure/test_roads.py -q`  
Expected: FAIL because the OSM transforms are absent.

- [ ] **Step 3: Implement approved mapping and stable graph identities**

Parse `tags_json` once and keep only configured classes. Facility ID hashes OSM type/ID plus facility type; version hashes OSM snapshot plus mapping version. Split road ways at endpoints and junctions, preserve `osm_way_id`, normalize `oneway`, `bridge`, `tunnel`, `surface`, calculate metric length, and derive base travel time only from configured class/surface speed.

- [ ] **Step 4: Run OSM transform tests**

Run: `pytest tests/unit/silver/exposure/test_osm_tags.py tests/unit/silver/exposure/test_facilities.py tests/unit/silver/exposure/test_roads.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit OSM transforms**

```bash
git add src/flashflood_data/orchestration/silver/exposure/reader.py src/flashflood_data/orchestration/silver/exposure/osm_tags.py src/flashflood_data/orchestration/silver/exposure/facilities.py src/flashflood_data/orchestration/silver/exposure/roads.py tests/unit/silver/exposure/test_osm_tags.py tests/unit/silver/exposure/test_facilities.py tests/unit/silver/exposure/test_roads.py
git commit -m "feat: normalize flood exposure OSM features"
```

### Task 3: Register WorldPop cells and populate the population grid

**Files:**
- Create: `src/flashflood_data/orchestration/silver/exposure/population.py`
- Create: `tests/unit/silver/exposure/test_population.py`

**Interfaces:**
- Consumes: WorldPop `bronze.raster_coverage`, Raw object metadata, `SourceGridRegistrar`.
- Produces: `build_population_rows(raster: PopulationRaster, context: ExposureContext) -> tuple[list[GridCell], list[PopulationGridRow]]`.

- [ ] **Step 1: Write raster identity and value tests**

```python
def test_worldpop_cell_matches_registered_source_grid():
    grid, population = build_population_rows(worldpop_2x2(), context)
    assert population[0].source_grid_id in {cell.source_grid_id for cell in grid}
    assert population[0].reference_year == 2025

def test_demographic_subgroups_remain_null_without_source_bands():
    row = build_population_rows(worldpop_total_only(), context)[1][0]
    assert row.children_population is None
    assert row.elderly_population is None
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/exposure/test_population.py -q`  
Expected: FAIL because population conversion is absent.

- [ ] **Step 3: Implement globally anchored raster cell identity**

Use raster transform, dimensions, CRS, and checksum to define `source_grid_version`. Generate stable row/column/cell IDs from the full source raster indices, not dense AOI enumeration. Register cells through the shared registrar, preserve provider nodata, and map total population/count or density exactly as declared in config. Resolve `available_at` from source metadata.

- [ ] **Step 4: Run population and weather-grid regression tests**

Run: `pytest tests/unit/silver/exposure/test_population.py tests/unit/weather/test_grids.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit population conversion**

```bash
git add src/flashflood_data/orchestration/silver/exposure/population.py tests/unit/silver/exposure/test_population.py
git commit -m "feat: register WorldPop population cells"
```

### Task 4: Add basin relations, river crossings, and quality gates

**Files:**
- Create: `src/flashflood_data/orchestration/silver/exposure/basin_overlay.py`
- Create: `src/flashflood_data/orchestration/silver/exposure/crossings.py`
- Create: `src/flashflood_data/orchestration/silver/exposure/quality.py`
- Create: `tests/unit/silver/exposure/test_basin_overlay.py`
- Create: `tests/unit/silver/exposure/test_crossings.py`
- Create: `tests/unit/silver/exposure/test_quality.py`

**Interfaces:**
- Produces: `assign_facilities_to_basins`, `overlay_roads_with_basins`, `detect_river_crossings`.
- Produces: `check_exposure_outputs(outputs: ExposureOutputs, config: ExposureConfig) -> list[QualityResult]`.

- [ ] **Step 1: Write spatial relation and crossing tests**

```python
def test_facility_inside_basin_gets_stream_distance():
    row = assign_facilities_to_basins([facility()], [basin()], [nearby_stream()], config)[0]
    assert row.spatial_relation == "within"
    assert row.distance_to_stream_m >= 0

def test_bridge_tag_classifies_geometric_crossing():
    crossing = detect_river_crossings([bridge_edge()], [crossing_reach()], config)[0]
    assert crossing.crossing_type == "bridge"

def test_untagged_intersection_remains_unknown():
    assert detect_river_crossings([plain_edge()], [crossing_reach()], config)[0].crossing_type == "unknown"
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/exposure/test_basin_overlay.py tests/unit/silver/exposure/test_crossings.py tests/unit/silver/exposure/test_quality.py -q`  
Expected: FAIL because spatial modules are absent.

- [ ] **Step 3: Implement indexed joins and explicit DQ**

Use spatial indexes and projected coordinates. Facility assignment handles point/polygon centroids according to config and calculates stream/outlet distance. Road overlay stores positive intersection length and ratio. Crossing IDs hash road/river versions plus snapped intersection coordinate. Fatal rules cover duplicate keys, missing graph endpoints, invalid geometry, ratio range, orphan basin/river IDs, and invalid population values; ambiguous boundary facilities/crossings are warnings.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/silver/exposure/test_basin_overlay.py tests/unit/silver/exposure/test_crossings.py tests/unit/silver/exposure/test_quality.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit relations and DQ**

```bash
git add src/flashflood_data/orchestration/silver/exposure/basin_overlay.py src/flashflood_data/orchestration/silver/exposure/crossings.py src/flashflood_data/orchestration/silver/exposure/quality.py tests/unit/silver/exposure/test_basin_overlay.py tests/unit/silver/exposure/test_crossings.py tests/unit/silver/exposure/test_quality.py
git commit -m "feat: relate exposure assets to basins and rivers"
```

### Task 5: Add service, DAG, integration coverage, and docs

**Files:**
- Create: `src/flashflood_data/orchestration/silver/exposure/service.py`
- Create: `src/flashflood_data/orchestration/silver/exposure/factory.py`
- Create: `airflow/dags/silver_exposure_network.py`
- Create: `tests/unit/silver/exposure/test_service.py`
- Create: `tests/integration/silver/test_exposure_pipeline.py`
- Modify: `tests/contract/infra/test_silver_dags.py`
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`

**Interfaces:**
- Produces: `ExposureSilverService.plan`, family-specific compute methods, `publish`, `run`.
- Produces: `build_exposure_silver_service(root=None)`.
- Produces manual DAG ID `silver_exposure_network`.

- [ ] **Step 1: Write service ownership and DAG tests**

```python
def test_exposure_service_publishes_only_owned_tables(service):
    result = service.run("exposure-1")
    assert {output.table_name for output in result.outputs} == EXPECTED_EXPOSURE_TABLES

def test_exposure_dag_declares_bounded_compute_groups(dag_bag):
    dag = dag_bag.get_dag("silver_exposure_network")
    assert dag.schedule is None
    assert {"compute_facilities", "compute_roads", "compute_population",
            "compute_crossings", "publish_outputs"} <= set(dag.task_ids)
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/unit/silver/exposure/test_service.py tests/contract/infra/test_silver_dags.py -q`  
Expected: FAIL because service/DAG are absent.

- [ ] **Step 3: Implement family compute and serialized publication**

Discover exact OSM, WorldPop, basin, and river snapshots. Stage each output family separately. Publish `silver.source_grid` additions first, then population; publish facility/road dimensions before bridge relations; publish crossings after road and river identifiers are validated. One publish task loops tables in this dependency order and records one snapshot/lineage set per table.

- [ ] **Step 4: Run domain and weather regression tests**

Run: `pytest tests/unit/silver/exposure tests/integration/silver/test_exposure_pipeline.py tests/unit/weather/test_grids.py tests/contract/infra/test_silver_dags.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit pipeline**

```bash
git add src/flashflood_data/orchestration/silver/exposure src/flashflood_data/orchestration/spatial_grid.py src/flashflood_data/orchestration/weather/grids.py airflow/dags/silver_exposure_network.py tests/unit/silver/exposure tests/integration/silver/test_exposure_pipeline.py tests/contract/infra/test_silver_dags.py README.md docs/pipeline_architecture_and_roadmap.md
git commit -m "feat: add Silver exposure network DAG"
```

## Checkpoint

Run a small OSM/WorldPop fixture and inspect every table family in Trino. Verify facility tag selection, graph connectivity, WorldPop grid identity, basin relations, and bridge/unknown crossing classifications before processing the complete exposure AOI.
