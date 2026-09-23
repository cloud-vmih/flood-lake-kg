# Dynamic Weather Ingest Implementation Plan

> **For Codex:** Execute this plan with `executing-plans`; keep network calls outside tests and leave production DAGs paused on creation.

**Goal:** Add restart-safe Raw-to-Bronze Airflow pipelines for GSMaP, ERA5-Land, and IFS/Open-Meteo that recover missing time windows after Docker downtime.

**Architecture:** Three thin DAGs call one shared weather orchestration package. Each source computes provider-safe expected objects from an Iceberg watermark, subtracts immutable objects already recorded in `meta.source_objects`, publishes only missing/revised provider responses to MinIO, advances its watermark only across contiguous coverage, and parses every unprocessed Raw object into `bronze.weather_grid_value`. Provider adapters own request and file-format details; common planning, publication, metadata, idempotency, and Bronze discovery stay source-independent.

**Tech Stack:** Python 3.11, Pydantic 2, Airflow 3 TaskFlow, PyArrow/MinIO, PyIceberg/Polaris, xarray/cfgrib/eccodes, cdsapi, httpx, pytest.

---

### Task 1: Freeze dynamic contracts and configuration

**Files:**
- Create: `config/dynamic/gsmap.yaml`
- Create: `config/dynamic/era5_land.yaml`
- Create: `config/dynamic/ifs.yaml`
- Create: `src/flashflood_data/orchestration/weather/models.py`
- Create: `src/flashflood_data/orchestration/weather/config.py`
- Test: `tests/unit/weather/test_config.py`

1. Write failing tests for UTC-aware windows, supported modes, exact variables, overlap, schedule, AOI path, and credential-free YAML.
2. Add immutable Pydantic models for source config, planned objects, fetch results, publication results, and watermarks.
3. Load and validate the three YAML files; reject secrets and invalid time steps.
4. Run `pytest tests/unit/weather/test_config.py -q`.

### Task 2: Add durable watermark and gap planning

**Files:**
- Modify: `src/flashflood_data/storage/iceberg_schemas.py`
- Create: `src/flashflood_data/orchestration/weather/watermarks.py`
- Create: `src/flashflood_data/orchestration/weather/planner.py`
- Test: `tests/unit/weather/test_watermarks.py`
- Test: `tests/unit/weather/test_planner.py`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`

1. Write failing tests for `meta.ingest_watermarks`, missing-window subtraction, overlap replay, explicit no-data, and contiguous cursor advancement over a gap.
2. Add the physical Iceberg schema keyed by source/product/stream and a repository using keyed Meta upserts.
3. Implement deterministic expected-object identity and planner functions; never infer completeness from only `max(valid_time)`.
4. Run focused tests.

### Task 3: Implement provider adapters

**Files:**
- Create: `src/flashflood_data/orchestration/weather/providers/base.py`
- Create: `src/flashflood_data/orchestration/weather/providers/gsmap.py`
- Create: `src/flashflood_data/orchestration/weather/providers/era5_land.py`
- Create: `src/flashflood_data/orchestration/weather/providers/ifs_openmeteo.py`
- Modify: `requirements/lakehouse.txt`
- Test: `tests/unit/weather/test_providers.py`

1. Write fake-client tests for source-specific availability lag, request construction, product/cycle identity, and downloaded output metadata.
2. Implement GSMaP template downloads with credentials read only from environment, ERA5-Land monthly CDS requests via lazy `cdsapi`, and IFS Single Runs calls grouped by cycle and AOI grid points.
3. Ensure each adapter returns provider revision/cycle/validity metadata and streams bytes to run-scoped staging.
4. Run provider tests without internet.

### Task 4: Publish immutable dynamic Raw objects

**Files:**
- Create: `src/flashflood_data/orchestration/weather/landing.py`
- Create: `src/flashflood_data/orchestration/weather/factory.py`
- Test: `tests/unit/weather/test_landing.py`

1. Write tests for content-addressed IDs, object keys, manifest reuse/conflicts, source-object registration, failed-fetch audit, and rerun reuse.
2. Build the common service with `ObjectPublisher`, `SourceObjectInventory`, `MetaRecorder`, and the selected provider.
3. Store payload and credential-free manifest under `weather/<source>/<product>/...`; register `source_type=dynamic` in Iceberg.
4. Keep Raw commits independent of later Bronze failures.
5. Run focused tests.

### Task 5: Parse dynamic Raw into Bronze

**Files:**
- Create: `src/flashflood_data/orchestration/weather/bronze.py`
- Create: `src/flashflood_data/orchestration/weather/parsers.py`
- Test: `tests/unit/weather/test_parsers.py`
- Test: `tests/unit/weather/test_bronze.py`

1. Write fixture-based tests for GSMaP grids, ERA5 NetCDF/GRIB-compatible xarray datasets, and Open-Meteo JSON.
2. Normalize time, units, grid IDs, cycle/revision, value kind, and window bounds into `bronze.weather_grid_value`.
3. Discover all registered Raw objects that lack the current parser version, replace one object slice atomically, and record quality, snapshot, and lineage Meta rows.
4. Run focused tests.

### Task 6: Add the three Airflow DAGs

**Files:**
- Create: `src/flashflood_data/orchestration/weather/airflow_factory.py`
- Create: `airflow/dags/gsmap_ingest.py`
- Create: `airflow/dags/era5_land_ingest.py`
- Create: `airflow/dags/ifs_ingest.py`
- Test: `tests/contract/infra/test_dynamic_weather_dags.py`

1. Write contract tests for DAG IDs, schedules, `catchup=False`, `max_active_runs=1`, paused creation, source-specific files, Raw/Bronze task groups, pools, and backfill config.
2. Build shared TaskFlow tasks for cursor load, safe-end calculation, expected/missing planning, mapped fetch/register, coverage verification, cursor advancement, Bronze discovery, and mapped parsing.
3. Make operational runs use the cursor plus overlap; make explicit backfills use supplied start/end without moving the operational cursor.
4. Run DAG contract/import tests.

### Task 7: Wire runtime and operator documentation

**Files:**
- Modify: `compose.yaml`
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`
- Test: `tests/unit/test_lakehouse_compose.py`
- Test: `tests/unit/test_lakehouse_env.py`
- Test: `tests/unit/test_python_lakehouse_runtime.py`

1. Add only credential names and source endpoint settings to `.env.example`; pass them to Airflow without committing values.
2. Install/pin CDS runtime support and add any required Airflow pools/init checks.
3. Document setup, backfill trigger examples, operational schedules, expected Raw/Meta/Bronze tables, recovery semantics, and provider limitations with official source links.
4. Fix the two existing README environment contract omissions (`Windows + WSL2`, `usermod -aG docker`).
5. Run focused infra/docs tests.

### Task 8: Verify end to end without downloading production data

**Files:**
- Add or modify tests only where failures reveal a real contract gap.

1. Run Ruff on changed Python files.
2. Run all weather, schema, DAG, compose, environment, and README tests.
3. Run `pytest -q` and confirm no regression from the known baseline.
4. Compile/import all three DAG files in the Airflow runtime if the compose stack is available; otherwise validate their AST/import contract and clearly report that limitation.
5. Review `git diff --check`, secrets, and final changed-file list.
