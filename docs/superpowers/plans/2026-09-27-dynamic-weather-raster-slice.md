# Dynamic Weather Raster Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Crop dynamic weather to the Son La hydrological scope before Raw publication, store Bronze as raster slices, register stable nationwide provider grids, split GSMaP NOW and Standard DAGs, and expire NOW/IFS payloads safely after seven days.

**Architecture:** Keep the existing restart-safe expected-object planner and immutable landing flow, adding `spatial_scope_id` to its identity. Register a stable provider grid once, subset fetched payloads in staging, publish scoped Raw objects, and parse each object into one or more array-valued Bronze slices. Track transient payload state in a separate Meta lifecycle table so immutable provenance survives MinIO cleanup.

**Tech Stack:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, NumPy, Xarray, GeoPandas, PyArrow, PyIceberg/Polaris, MinIO, pytest.

**Spec:** `docs/superpowers/specs/2026-09-27-weather-raster-slice-and-aoi-design.md`

## Global Constraints

- Dynamic Raw and Bronze contain only `hydrological_aoi.geoparquet` cells.
- `silver.source_grid` covers `vietnam_hydrological_aoi.geoparquet` and uses globally anchored stable indices.
- GSMaP NOW and IFS Raw payloads have `transient_7d` retention; GSMaP Standard and ERA5-Land are durable.
- Transient deletion requires age, Bronze snapshot, fatal DQ pass, lineage, and confirmed MinIO deletion.
- GSMaP NOW values are rolling one-hour windows released every 30 minutes; never sum adjacent NOW values.
- The watermark key includes `spatial_scope_id` and advances on contiguous Raw acquisition coverage.
- No basin aggregation, source reconciliation, Silver weather values, or Gold outputs are implemented.
- Tests use fixtures/fake clients and never call live weather APIs.

---

### Task 1: Split GSMaP configuration and add scope/retention contracts

**Files:**
- Create: `config/dynamic/gsmap_now.yaml`
- Create: `config/dynamic/gsmap_standard.yaml`
- Delete: `config/dynamic/gsmap.yaml`
- Modify: `config/dynamic/era5_land.yaml`
- Modify: `config/dynamic/ifs.yaml`
- Modify: `src/flashflood_data/orchestration/weather/models.py`
- Modify: `src/flashflood_data/orchestration/weather/config.py`
- Modify: `tests/unit/weather/test_config.py`

**Interfaces:**
- Produces: `RetentionClass = Literal["durable", "transient_7d"]`.
- Produces: `WeatherSourceConfig.retention_class`, `WeatherSourceConfig.spatial_scope_name`, and `WeatherSourceConfig.grid_scope_path`.
- Produces one stream per GSMaP config.

- [ ] **Step 1: Write failing configuration tests**

```python
def test_gsmap_products_have_independent_schedules_and_retention():
    now = load_weather_config(ROOT / "config/dynamic/gsmap_now.yaml")
    standard = load_weather_config(ROOT / "config/dynamic/gsmap_standard.yaml")
    assert now.schedule == "7,37 * * * *"
    assert now.retention_class == "transient_7d"
    assert standard.schedule == "27 */6 * * *"
    assert standard.retention_class == "durable"
    assert len(now.streams) == len(standard.streams) == 1

def test_every_weather_source_declares_current_and_grid_scope():
    assert config.aoi_path.name == "hydrological_aoi.geoparquet"
    assert config.grid_scope_path.name == "vietnam_hydrological_aoi.geoparquet"
    assert config.spatial_scope_name == "sonla-l12-h1"
```

Also assert ERA contains provider variable `sub_surface_runoff` in addition to `surface_runoff`, mapped to canonical `subsurface_runoff` and `surface_runoff`. Assert IFS requests every Open-Meteo runoff component exposed by the selected endpoint and maps them to those same canonical names; reject configuration if a required component is unsupported instead of silently treating total runoff as subsurface runoff.

- [ ] **Step 2: Run configuration tests**

Run: `pytest tests/unit/weather/test_config.py -q`

Expected: FAIL because GSMaP is combined and retention/scope fields do not exist.

- [ ] **Step 3: Add validated fields and split YAML**

Require `spatial_scope_name`, `grid_scope_path`, and `retention_class` at source level. Reject absolute project paths and reject `transient_7d` if a source has no positive expiry duration. Keep credentials as environment-variable names only.

- [ ] **Step 4: Run configuration tests**

Run: `pytest tests/unit/weather/test_config.py -q`

Expected: PASS.

- [ ] **Step 5: Commit source contracts**

```bash
git add config/dynamic src/flashflood_data/orchestration/weather/models.py src/flashflood_data/orchestration/weather/config.py tests/unit/weather/test_config.py
git commit -m "feat: split GSMaP ingest contracts"
```

---

### Task 2: Add Iceberg schemas for raster slices, grids, lifecycle, and scoped watermarks

**Files:**
- Modify: `src/flashflood_data/storage/iceberg_schemas.py`
- Modify: `src/flashflood_data/storage/iceberg_tables.py`
- Modify: `src/flashflood_data/orchestration/weather/models.py`
- Modify: `src/flashflood_data/orchestration/weather/watermarks.py`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`
- Modify: `tests/unit/storage/test_iceberg_tables.py`
- Modify: `tests/unit/weather/test_watermarks.py`

**Interfaces:**
- Produces physical tables `bronze.weather_raster_slice`, `silver.source_grid`, and `meta.object_lifecycle`.
- Produces scoped watermark methods `load(source_id, product, stream_id, spatial_scope_id)` and `save(IngestWatermark)`.
- Produces `ObjectLifecycleRow` and `WeatherRasterSlice` validated models.

- [ ] **Step 1: Write failing schema and watermark tests**

```python
def test_weather_slice_uses_parallel_arrays():
    schema = table_schema(("bronze", "weather_raster_slice"))
    assert schema.field("cell_indices").type == pa.list_(pa.int64())
    assert schema.field("values").type == pa.list_(pa.float32())
    assert "weather_grid_value" not in BRONZE_KEYS

def test_watermark_isolated_by_spatial_scope():
    store.save(watermark.model_copy(update={"spatial_scope_id": "sonla-a"}))
    assert store.load("gsmap", "now", "gauge_now", "sonla-b") is None
```

Assert `source_grid.scope_ids` is `list<string>`, `cell_index` is `int64`, lifecycle key is `object_id`, and the slice business key exactly matches the spec.

- [ ] **Step 2: Run focused schema tests**

Run: `pytest tests/contract/storage/test_meta_bronze_schemas.py tests/unit/storage/test_iceberg_tables.py tests/unit/weather/test_watermarks.py -q`

Expected: FAIL because the three tables and scoped watermark field are absent.

- [ ] **Step 3: Extend the schema type parser and table definitions**

Add named types for `float`, `list_long`, `list_float`, and `list_string`. Define the exact fields from sections 5–7 of the spec. Replace the active Bronze key entry with:

```python
"weather_raster_slice": (
    "source_grid_version", "spatial_scope_id", "variable", "vertical_level",
    "source_cycle_id", "valid_time", "window_start", "window_end", "source_revision",
),
```

Keep `object_id` prepended by the object-scoped writer.

- [ ] **Step 4: Update the watermark model and repository**

Add nonempty `spatial_scope_id: str`; include it in the Iceberg key fields and every `load` call. No fallback to an unscoped legacy row is allowed.

- [ ] **Step 5: Run the focused schema tests**

Run: `pytest tests/contract/storage/test_meta_bronze_schemas.py tests/unit/storage/test_iceberg_tables.py tests/unit/weather/test_watermarks.py -q`

Expected: PASS.

- [ ] **Step 6: Commit schemas**

```bash
git add src/flashflood_data/storage/iceberg_schemas.py src/flashflood_data/storage/iceberg_tables.py src/flashflood_data/orchestration/weather/models.py src/flashflood_data/orchestration/weather/watermarks.py tests/contract/storage/test_meta_bronze_schemas.py tests/unit/storage/test_iceberg_tables.py tests/unit/weather/test_watermarks.py
git commit -m "feat: add weather slice and lifecycle schemas"
```

---

### Task 3: Register stable nationwide provider grids

**Files:**
- Create: `src/flashflood_data/orchestration/weather/grids.py`
- Create: `tests/unit/weather/test_grids.py`
- Modify: `src/flashflood_data/orchestration/weather/factory.py`
- Modify: `tests/unit/weather/test_factory.py`

**Interfaces:**
- Produces: `GridDefinition(source_id, source_grid_version, resolution_x, resolution_y, north, west, width, height, crs)`.
- Produces: `WeatherGridRegistry.ensure_grid(config: WeatherSourceConfig) -> GridRegistration`.
- Produces: `GridRegistration.source_grid_version`, `.scope_id`, and `.cell_index_by_grid_id`.
- Stable index formula: `cell_index = global_row * width + global_column`.

- [ ] **Step 1: Write failing stability and scope tests**

```python
def test_cell_index_does_not_change_when_scope_expands():
    small = build_grid_rows(definition, sonla_geometry, scope_id="sonla")
    large = build_grid_rows(definition, vietnam_geometry, scope_id="vietnam")
    small_by_id = {row["source_grid_id"]: row["cell_index"] for row in small}
    large_by_id = {row["source_grid_id"]: row["cell_index"] for row in large}
    assert all(large_by_id[key] == value for key, value in small_by_id.items())

def test_grid_registration_merges_scope_ids_without_duplicate_cells():
    registry.ensure_grid(config)
    registry.ensure_grid(config)
    assert writer.rows_have_unique_key(("source_id", "source_grid_version", "source_grid_id"))
```

Test GSMaP centers at `0.05` offsets, ERA regular 0.1 alignment, and Open-Meteo project lattice anchored globally rather than at AOI bounds.

- [ ] **Step 2: Run grid tests**

Run: `pytest tests/unit/weather/test_grids.py tests/unit/weather/test_factory.py -q`

Expected: FAIL because the registry does not exist.

- [ ] **Step 3: Implement grid definitions and spatial selection**

Build cell polygons/centroids only for the national AOI bounding rows and columns, retain cells whose polygon intersects the national geometry, sort by `cell_index`, and serialize WKB in EPSG:4326. Calculate `source_grid_version` from canonical grid-definition JSON, independent of AOI.

- [ ] **Step 4: Implement idempotent Iceberg registration**

Use keyed Meta-style upsert semantics for `silver.source_grid`. On an existing key, reject changed geometry/index/definition; only merge and sort `scope_ids`. Return the mapping needed by subsetting and parsing.

- [ ] **Step 5: Run grid tests**

Run: `pytest tests/unit/weather/test_grids.py tests/unit/weather/test_factory.py -q`

Expected: PASS.

- [ ] **Step 6: Commit grid registration**

```bash
git add src/flashflood_data/orchestration/weather/grids.py src/flashflood_data/orchestration/weather/factory.py tests/unit/weather/test_grids.py tests/unit/weather/test_factory.py
git commit -m "feat: register stable weather source grids"
```

---

### Task 4: Crop fetched weather payloads before Raw publication

**Files:**
- Create: `src/flashflood_data/orchestration/weather/subset.py`
- Create: `tests/unit/weather/test_subset.py`
- Modify: `src/flashflood_data/orchestration/weather/models.py`
- Modify: `src/flashflood_data/orchestration/weather/providers/gsmap.py`
- Modify: `src/flashflood_data/orchestration/weather/providers/era5_land.py`
- Modify: `src/flashflood_data/orchestration/weather/providers/ifs_openmeteo.py`
- Modify: `src/flashflood_data/orchestration/weather/factory.py`
- Modify: `tests/unit/weather/test_providers.py`

**Interfaces:**
- Consumes: `FetchedWeatherObject`, current AOI geometry, and `GridRegistration`.
- Produces: `ScopedWeatherObject(path, filename, media_type, spatial_scope_id, source_grid_version, cell_indices, provider_payload_checksum, provider_payload_size_bytes, retrieved_at)`.
- Produces: `scope_fetched_object(fetched, config, grid) -> ScopedWeatherObject`.

- [ ] **Step 1: Write failing provider-subset tests**

```python
def test_gsmap_subset_contains_only_scope_cells_and_original_checksum(
    fetched_global_gzip, gsmap_config, gsmap_grid
):
    scoped = scope_fetched_object(fetched_global_gzip, gsmap_config, gsmap_grid)
    with np.load(scoped.path) as payload:
        assert payload["cell_indices"].tolist() == expected_indices
        assert payload["values"].shape == (len(expected_indices),)
    assert scoped.provider_payload_checksum == sha256_file(fetched_global_gzip.path)

def test_era_subset_masks_cells_outside_polygon(fetched_netcdf, era_config, era_grid):
    scoped = scope_fetched_object(fetched_netcdf, era_config, era_grid)
    with xr.open_dataset(scoped.path) as ds:
        assert ds.cell_index.values.tolist() == expected_indices
        assert set(ds.data_vars) == set(config.streams[0].variables)
```

Test that IFS request coordinates already equal the scope grid cells and its scoped JSON contains no response outside that set.

- [ ] **Step 2: Run subset tests**

Run: `pytest tests/unit/weather/test_subset.py tests/unit/weather/test_providers.py -q`

Expected: FAIL because fetched objects are published directly.

- [ ] **Step 3: Implement source-specific lossless subsetting**

- GSMaP: decode the global gzip once, gather approved indices, and write deterministic compressed NumPy payload arrays `cell_indices` and `values`.
- ERA5-Land: select approved coordinates and write NetCDF with a `cell_index` dimension, preserving time, variable attributes, and values.
- IFS/Open-Meteo: request only approved stable lattice centers and validate returned coordinates before writing the scoped JSON.

Use atomic local targets. Delete the larger provider staging file only after the scoped payload is validated.

- [ ] **Step 4: Attach source and subset evidence**

Carry both checksums and sizes into the scoped model. `spatial_scope_id` is a hash of scope name, AOI bytes/checksum, basin level, and upstream-hop configuration. Never include secrets or authorization headers.

- [ ] **Step 5: Run subset/provider tests**

Run: `pytest tests/unit/weather/test_subset.py tests/unit/weather/test_providers.py -q`

Expected: PASS.

- [ ] **Step 6: Commit weather subsetting**

```bash
git add src/flashflood_data/orchestration/weather/subset.py src/flashflood_data/orchestration/weather/models.py src/flashflood_data/orchestration/weather/providers src/flashflood_data/orchestration/weather/factory.py tests/unit/weather/test_subset.py tests/unit/weather/test_providers.py
git commit -m "feat: crop weather payloads before Raw"
```

---

### Task 5: Publish scoped Raw and register lifecycle state

**Files:**
- Modify: `src/flashflood_data/orchestration/weather/landing.py`
- Create: `src/flashflood_data/orchestration/weather/lifecycle.py`
- Modify: `src/flashflood_data/orchestration/weather/factory.py`
- Modify: `tests/unit/weather/test_landing.py`
- Create: `tests/unit/weather/test_lifecycle.py`

**Interfaces:**
- Consumes: `ScopedWeatherObject` from Task 4.
- Produces: `ObjectLifecycleStore.register(object_id, retention_class, published_at, run_id)`.
- Produces: manifest selection fields `spatial_scope_id`, `source_grid_version`, and `cell_count`; provider metadata fields `provider_payload_checksum` and `provider_payload_size_bytes`.

- [ ] **Step 1: Write failing publication/lifecycle tests**

```python
def test_now_publication_registers_seven_day_lifecycle():
    published = service.publish_and_register(scoped_now, run_id="run-1")
    row = lifecycle.rows[published.object_id]
    assert row["retention_class"] == "transient_7d"
    assert row["expires_at"] == scoped_now.retrieved_at + timedelta(days=7)
    assert row["storage_status"] == "available"

def test_standard_and_era_are_durable():
    assert lifecycle.rows[object_id]["expires_at"] is None
```

Also assert an identical scoped object reuses Raw/manifest/registry and does not create conflicting lifecycle state.

- [ ] **Step 2: Run landing/lifecycle tests**

Run: `pytest tests/unit/weather/test_landing.py tests/unit/weather/test_lifecycle.py -q`

Expected: FAIL because landing accepts `FetchedWeatherObject` and no lifecycle store exists.

- [ ] **Step 3: Publish `ScopedWeatherObject` and its provenance**

Change `publish_and_register` to accept the scoped model. Hash the scoped payload for immutable object identity. Store the provider checksum as provenance, not as the published object checksum. Register `meta.source_objects` first, then upsert lifecycle state; on retry, both writes must converge to the same rows.

- [ ] **Step 4: Implement guarded lifecycle repository methods**

Provide these exact public method signatures: `register(object_id: str, retention_class: RetentionClass, published_at: datetime, run_id: str) -> int`; `mark_bronze_evidence(object_id: str, snapshot_id: int, quality_status: str, lineage_edge_id: str, checked_at: datetime) -> int`; `eligible(now: datetime) -> tuple[ObjectLifecycleRow, ...]`; `mark_expired(object_id: str, deleted_at: datetime) -> int`; and `mark_delete_failed(object_id: str, checked_at: datetime, reason: str) -> int`.

`eligible` returns only expired-time transient rows with snapshot, `passed` quality, and lineage evidence.

- [ ] **Step 5: Run landing/lifecycle tests**

Run: `pytest tests/unit/weather/test_landing.py tests/unit/weather/test_lifecycle.py -q`

Expected: PASS.

- [ ] **Step 6: Commit scoped landing**

```bash
git add src/flashflood_data/orchestration/weather/landing.py src/flashflood_data/orchestration/weather/lifecycle.py src/flashflood_data/orchestration/weather/factory.py tests/unit/weather/test_landing.py tests/unit/weather/test_lifecycle.py
git commit -m "feat: track weather Raw lifecycle"
```

---

### Task 6: Parse and atomically write raster slices

**Files:**
- Modify: `src/flashflood_data/orchestration/weather/parsers.py`
- Modify: `src/flashflood_data/orchestration/weather/bronze.py`
- Modify: `tests/unit/weather/test_parsers.py`
- Modify: `tests/unit/weather/test_bronze.py`

**Interfaces:**
- Produces: `parse_weather_object(row: SourceObjectRow, path: Path, *, run_id: str, parser_version: str) -> Iterator[dict[str, object]]` where each item is a complete raster slice.
- Consumes: `WeatherGridRegistry` lookup and `ObjectLifecycleStore.mark_bronze_evidence`.
- Writes: `bronze.weather_raster_slice` by complete object batch.

- [ ] **Step 1: Replace row-per-cell expectations with failing slice tests**

```python
def test_parser_emits_one_aligned_slice_per_variable_and_time(source_row, scoped_path):
    slices = list(
        parse_weather_object(
            source_row, scoped_path, run_id="run", parser_version="v2"
        )
    )
    assert len(slices) == 1
    assert slices[0]["cell_indices"] == [121, 122, 481]
    assert slices[0]["values"] == [1.0, None, 3.5]
    assert len(slices[0]["cell_indices"]) == len(slices[0]["values"])
```

Add failures for unsorted/duplicate/unknown indices, negative precipitation, inconsistent windows, array length mismatch, and an object whose cells reference the wrong grid version.

- [ ] **Step 2: Run parser/Bronze tests**

Run: `pytest tests/unit/weather/test_parsers.py tests/unit/weather/test_bronze.py -q`

Expected: FAIL because parsers emit one row per grid value and Bronze writes `weather_grid_value`.

- [ ] **Step 3: Aggregate provider payloads into slices**

Group by variable, vertical level, cycle, valid time, exact window, and revision. Sort pairs by `cell_index` once and unzip them into parallel arrays. Compute:

```python
slice_id = sha256(canonical_json({name: row[name] for name in SLICE_KEY_FIELDS})).hexdigest()
```

Use exact provider window semantics. NOW releases 30 minutes apart retain distinct one-hour windows.

- [ ] **Step 4: Write complete objects and evidence atomically**

Change dataset/table references to `weather_raster_slice`. Validate every slice before `replace_object_batches`. After snapshot, fatal DQ, and lineage commits succeed, call `mark_bronze_evidence` with the returned snapshot and lineage edge ID. Discovery excludes lifecycle state `expired` but continues to discover available Raw objects lacking current-parser lineage.

- [ ] **Step 5: Prove idempotency**

Add a test that runs the same object twice, verifies the second discovery is empty, and verifies no second table commit occurs. Add a revision test proving a different object ID/revision remains queryable beside the first.

- [ ] **Step 6: Run parser/Bronze tests**

Run: `pytest tests/unit/weather/test_parsers.py tests/unit/weather/test_bronze.py -q`

Expected: PASS.

- [ ] **Step 7: Commit raster-slice Bronze**

```bash
git add src/flashflood_data/orchestration/weather/parsers.py src/flashflood_data/orchestration/weather/bronze.py tests/unit/weather/test_parsers.py tests/unit/weather/test_bronze.py
git commit -m "feat: store weather Bronze as raster slices"
```

---

### Task 7: Split DAGs and add safe transient cleanup

**Files:**
- Create: `airflow/dags/gsmap_now_ingest.py`
- Create: `airflow/dags/gsmap_standard_ingest.py`
- Delete: `airflow/dags/gsmap_ingest.py`
- Modify: `src/flashflood_data/orchestration/weather/airflow_factory.py`
- Modify: `src/flashflood_data/orchestration/weather/factory.py`
- Modify: `tests/contract/infra/test_dynamic_weather_dags.py`
- Modify: `tests/unit/weather/test_airflow_factory.py`
- Modify: `tests/unit/weather/test_planner.py`

**Interfaces:**
- DAG IDs: `gsmap_now_ingest`, `gsmap_standard_ingest`, `era5_land_ingest`, `ifs_ingest`.
- New common tasks: `ensure_source_grid`, `scope_fetched_payload`, and `expire_transient_raw`.
- Cleanup consumes only `ObjectLifecycleStore.eligible(now)` rows.

- [ ] **Step 1: Write failing DAG contract tests**

```python
DAGS = {
    "gsmap_now_ingest.py": ("gsmap_now_ingest", "gsmap_now.yaml"),
    "gsmap_standard_ingest.py": ("gsmap_standard_ingest", "gsmap_standard.yaml"),
    "era5_land_ingest.py": ("era5_land_ingest", "era5_land.yaml"),
    "ifs_ingest.py": ("ifs_ingest", "ifs.yaml"),
}
```

Assert the graph orders `ensure_source_grid` before subsetting/Bronze, `scope_fetched_payload` before `register_raw_and_meta`, and cleanup after Bronze without making cleanup failure roll back a successful Bronze run.

- [ ] **Step 2: Run DAG/factory tests**

Run: `pytest tests/contract/infra/test_dynamic_weather_dags.py tests/unit/weather/test_airflow_factory.py tests/unit/weather/test_planner.py -q`

Expected: FAIL because one combined GSMaP DAG remains and scope is absent from cursor planning.

- [ ] **Step 3: Thread `spatial_scope_id` through planning**

Include it in expected asset identity, request fingerprint, inventory subtraction, watermark reads/writes, and plan documents. A new AOI hash must plan its historical start even if another scope has a current cursor.

- [ ] **Step 4: Split the thin DAG modules and extend the shared graph**

Each DAG imports only `ProjectPaths` and `build_weather_dag`. The shared graph performs grid ensure, fetch, subset, Raw registration, contiguous acquisition verification, cursor advancement, Bronze discovery/parse, then best-effort eligible cleanup.

- [ ] **Step 5: Implement confirmed deletion semantics**

For every eligible object, parse its `s3://raw/...` key, call the object-store delete method, confirm `exists(key) is False`, then mark `expired`. On exception or remaining object, mark `delete_failed` with the exception class only; never store credentials or full signed URLs.

- [ ] **Step 6: Run DAG/factory tests**

Run: `pytest tests/contract/infra/test_dynamic_weather_dags.py tests/unit/weather/test_airflow_factory.py tests/unit/weather/test_planner.py tests/unit/weather/test_lifecycle.py -q`

Expected: PASS.

- [ ] **Step 7: Commit orchestration**

```bash
git add airflow/dags config/dynamic src/flashflood_data/orchestration/weather/airflow_factory.py src/flashflood_data/orchestration/weather/factory.py tests/contract/infra/test_dynamic_weather_dags.py tests/unit/weather/test_airflow_factory.py tests/unit/weather/test_planner.py tests/unit/weather/test_lifecycle.py
git commit -m "feat: split GSMaP DAGs and expire transient Raw"
```

---

### Task 8: Update data contracts, operations docs, and complete verification

**Files:**
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`
- Modify: `docs/schema_contract/data.md`
- Modify: `docs/son_la_flood_schema_contract.md`
- Modify: `docs/son_la_flood_class_diagram.drawio`
- Modify: `tests/unit/test_readme_commands.py`
- Modify: `tests/contract/test_architecture.py`

**Interfaces:**
- Consumes: completed static plan and Tasks 1–7.
- Produces: executable operator instructions and diagrams consistent with physical schemas.

- [ ] **Step 1: Write failing documentation/architecture checks**

Assert docs contain all four DAG IDs, `bronze.weather_raster_slice`, `meta.object_lifecycle`, `silver.source_grid`, seven-day NOW/IFS retention, and explicitly mark `grid_basin_weight`/`basin_weather_value` as future work. Assert active docs no longer describe `weather_grid_value` as current.

- [ ] **Step 2: Run docs checks**

Run: `pytest tests/unit/test_readme_commands.py tests/contract/test_architecture.py -q`

Expected: FAIL on old table and DAG names.

- [ ] **Step 3: Update documentation and draw.io XML**

Document the data flow:

```text
provider -> run staging -> AOI subset -> scoped Raw -> weather_raster_slice
         -> source_grid reference
         -> DQ/snapshot/lineage -> optional 7-day Raw expiry
```

Include Trino examples using `cardinality(cell_indices)`, `cardinality(values)`, `UNNEST`, and lifecycle status. Include the explicit one-time development reset command; do not run it automatically.

- [ ] **Step 4: Run focused weather verification**

Run:

```bash
pytest tests/unit/weather tests/contract/infra/test_dynamic_weather_dags.py tests/contract/storage/test_meta_bronze_schemas.py tests/contract/test_architecture.py -q
ruff check src/flashflood_data/orchestration/weather airflow/dags tests/unit/weather tests/contract/infra/test_dynamic_weather_dags.py
python -m compileall -q airflow/dags src/flashflood_data/orchestration/weather
docker compose config --quiet
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 5: Run the full regression suite**

Run: `pytest -q`

Expected: PASS. If an environment-backed integration test is explicitly skipped because Docker services are unavailable, record the exact skip; do not report it as executed.

- [ ] **Step 6: Commit docs and final contracts**

```bash
git add README.md docs/pipeline_architecture_and_roadmap.md docs/schema_contract/data.md docs/son_la_flood_schema_contract.md docs/son_la_flood_class_diagram.drawio tests/unit/test_readme_commands.py tests/contract/test_architecture.py
git commit -m "docs: describe scoped weather raster pipelines"
```
