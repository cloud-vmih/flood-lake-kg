# Meta and Bronze Implementation Plan

> **For agentic workers:** Execute the tasks in order, check each box after verification, and review each independently testable result before starting the next. Use the repository's execution workflow when implementation begins.

**Goal:** Turn the approved Meta and Bronze data contract into queryable Iceberg tables over the raw objects already in MinIO, with repeatable parsing, quality evidence, and snapshot-level lineage.

**Architecture:** Extend the **existing `static_source_landing` DAG** to register raw-source Meta information alongside its existing `meta.source_objects` registration. Add a small, shared Iceberg repository for contract-driven table creation and keyed writes; put parser implementations and source routing in `orchestration/bronze`. A **separate Airflow Bronze DAG** discovers eligible `source_objects` → parses and commits Bronze rows → checks QA → registers snapshot/lineage → publishes the run. Payloads stay in MinIO and XCom carries IDs/counts only. CLI access is for local tests and manual backfill using the same service.

**Tech Stack:** Python 3.11, PyIceberg 0.11.1, PyArrow, GeoPandas/Pyogrio, Rasterio, Airflow 3.3.1, Polaris 1.7.0, MinIO, Pytest. Use the existing Spark runtime only if the PBF or dynamic-grid benchmark shows Python cannot handle the required scale.

**Spec:** `docs/schema_contract/data.md` (sections 1–2), `docs/son_la_flood_class_diagram.drawio`, and `docs/superpowers/specs/2026-09-16-l12-source-landing-and-static-feature-design.md`.

## Global constraints

- The existing `meta.source_objects` physical schema and immutable MinIO objects remain compatible; this plan does not require redownloading raw data.
- Extend `static_source_landing` in place for raw Meta registration. Do not create a second raw-registration DAG or make the Bronze DAG register raw objects.
- `basin_id` is HydroBASINS/BasinATLAS level 12. Bronze retains source field names, geometry, CRS and units; field mapping and feature derivation belong to Silver.
- Iceberg does not enforce logical PK/FK. Validate batch keys, existing keys, source references, and snapshot references in the writer and QA jobs.
- A retry for the same object/parser version must produce the same rows. Parser changes replace that object's rows atomically or produce a deliberately versioned output; never use delete-then-append as two visible commits.
- One writer per target table during the first release; Airflow pool and `max_active_runs=1` are required, but writer checks must still detect conflicts. Failed QA leaves `published_at` null; downstream consumers select only published snapshots/runs. A committed rejected snapshot can still be inspected directly for diagnosis.
- `source_objects.status=available` is the only raw-input eligibility state. Verify object existence, size/checksum or trusted manifest before parsing; a missing/corrupt object is an error, not an empty Bronze result.
- Do not populate `bronze.weather_grid_value` until a representative GRIB/NetCDF benchmark establishes row count, bytes, write time, and query cost. Do not claim it exists before then.
- Preserve all existing user changes. The current checkout is dirty; create an isolated worktree at execution time or use carefully scoped commits, and never reset/delete unrelated files.

## Source-to-table coverage

| Landing source | Bronze target | Initial handling |
| --- | --- | --- |
| `hydrobasins_v1c`, `basinatlas_v10` | `basin_polygon_raw` | Parse only L12 shapefile members from each immutable ZIP; retain all source attributes. |
| `hydrorivers_v10` | `river_reach_raw` | Parse source reaches and attributes without topology remapping. |
| `geofabrik_vietnam_snapshot` | `osm_feature_raw` | Parse PBF node/way/relation IDs, tags and available geometry; size/semantics gate below. |
| `cop_dem_glo30_2024_1`, `soilgrids_2_0`, `esa_worldcover_2021_v200`, `worldpop_vnm_2025` | `raster_coverage` | One row per data band/layer; do not create rows for XML/license/MD5 sidecars. |
| `historical_flood_evidence_2020_2026` | `historical_event_raw` | Preserve original row/text and source record identity. |
| `sonla_admin_2025`, `gadm_vnm_4_1` | Contract gap | Add `bronze.admin_boundary_raw` to the contract before parsing; retain original administrative IDs, attributes and geometry. |
| Future ERA5-Land/IFS | `weather_grid_value` conditionally | Separate dynamic landing and benchmark first; no static DAG dependency. |

## Files and boundaries

Organize by **pipeline first**, then by responsibility within each pipeline:

```text
airflow/dags/
  static_source_landing.py          # existing raw → MinIO/Meta DAG; extend in place
  static_source_to_bronze.py        # separate raw → Bronze parse DAG
config/
  landing/static.yaml               # raw acquisition/registration policy
  bronze/static.yaml                # object routing, parser versions, QA policy
src/flashflood_data/
  orchestration/
    landing/                       # raw pipeline: acquire, publish, register
    bronze/                        # Bronze pipeline: discover, parse, validate, commit
    meta/                          # shared Meta repositories/services, no pipeline scheduling
  storage/                           # shared MinIO/Polaris/Iceberg primitives
tests/
  unit/orchestration/landing/       # raw pipeline behavior
  unit/orchestration/bronze/        # Bronze pipeline behavior
  unit/orchestration/meta/          # cross-pipeline Meta behavior
  contract/infra/                  # DAG wiring and task boundaries
```

The DAG files define orchestration only. `landing` must not import Bronze parsers; `bronze` reads the `source_objects` inventory and raw MinIO objects, not landing implementation internals. The shared `meta` and `storage` packages expose persistence APIs consumed by either pipeline. Future dynamic-source pipelines get their own package and DAG rather than being added to `bronze/static` by default.

| Unit | Proposed files | Responsibility |
| --- | --- | --- |
| Contract and schema | `docs/schema_contract/data.md`, `src/flashflood_data/storage/iceberg_schemas.py`, `tests/contract/storage/test_meta_bronze_schemas.py` | Frozen column names, nullability, grain, table properties. |
| Iceberg writing | `src/flashflood_data/storage/iceberg_tables.py`, `tests/unit/storage/test_iceberg_tables.py` | Catalog/table creation, atomic object replacement, key/conflict checks, snapshot IDs. Keep `storage/iceberg.py` as the existing source-object adapter. |
| Meta services | `src/flashflood_data/orchestration/meta/{registry,runs,quality,lineage}.py`, `airflow/dags/static_source_landing.py`, corresponding unit tests | Extend the existing landing DAG with raw-source registries, attempts and run audit; serve Bronze run/QA/snapshot/lineage recording. |
| Bronze parsers | `config/bronze/static.yaml`, `src/flashflood_data/orchestration/bronze/{registry,vector,raster,events,osm,service}.py`, corresponding unit tests | Route by source/media type, use pinned parser/QA policy and produce Arrow rows without Silver mapping. |
| Runtime | `airflow/dags/static_source_to_bronze.py`, `src/flashflood_data/cli/commands/bronze.py`, `tests/contract/infra/test_static_source_to_bronze_dag.py` | Airflow owns production parse scheduling and status; CLI calls the same service for tests/manual backfill. |
| End-to-end | `tools/smoke/meta_bronze.sh`, `tests/integration/lakehouse/test_meta_bronze_smoke.py`, `README.md` | One fixture through MinIO → Iceberg → Meta audit, including retry. |

Do not call the existing local `dataset/harmonized` products Bronze. Reuse its readers only where they preserve the original source records. The current `static/harmonize/hydro.py` is L10-oriented and is not the L12 Bronze parser.

## Task 1 — Freeze missing contract details and write feasibility probes

**Files:** `docs/schema_contract/data.md`, `docs/son_la_flood_class_diagram.drawio`, `config/bronze/static.yaml`, `tests/contract/storage/test_meta_bronze_schemas.py`, `tests/fixtures/static_pipeline/`.

- [x] Add `bronze.admin_boundary_raw` to the contract/diagram with grain `(object_id, source_feature_id)` and columns `object_id`, `source_feature_id`, `source_id`, `source_fields_json`, `geometry_wkb`, `crs`, `bbox_wgs84`, `ingest_run_id`, `parser_version`, `quality_status`. State which admin assets are input and why sidecars are excluded.
- [x] Pin rule identifiers/severities for raw object availability, ZIP membership, CRS, WGS84 bbox, geometry type, duplicate source key, raster band metadata, and expected nonempty output. Put source-to-parser routing, `contract_version` and `parser_version` in `config/bronze/static.yaml`; never infer them from run time.
- [x] Build one tiny L12 ZIP, one raster, one historical row, and one PBF fixture or use the existing fixture if it retains OSM IDs and tags. Run read probes against immutable local/MinIO-backed bytes and record resource use for PBF. Confirm source ZIP member naming rather than relying on filenames alone.
- [x] Write a repository-level contract test that compares all actual Arrow schemas to the column sets and nullability in this document; the test should fail until Task 2 supplies schemas. Review the contract delta before creating physical tables.

**Exit:** every landed static source has an explicit Bronze mapping or documented excluded sidecar; no source is silently skipped.

## Task 2 — Shared Iceberg table contracts and atomic keyed writer

**Files:** `src/flashflood_data/storage/iceberg_schemas.py`, `src/flashflood_data/storage/iceberg_tables.py`, `tests/unit/storage/test_iceberg_tables.py`.

- [x] Write failing tests for namespace/table creation, exact Arrow field types/nullability, canonical JSON/WKB/list fields, duplicate keys within a batch, same-key conflicting payload, empty valid batch, and retrying a committed batch.
- [x] Implement `table_schema(identifier) -> pa.Schema` for all nine Meta tables and all Bronze tables in scope. Use `timestamp[us, UTC]`, `int64` for snapshot IDs, `binary` WKB, `list<float64>` bbox, and JSON `string` exactly as the contract says; explicitly test `source_objects` compatibility.
- [x] Implement `ensure_table(catalog, identifier, schema)`, `replace_object_rows(catalog, table_id, object_id, rows, parser_version) -> snapshot_id`, and `upsert_meta_row(catalog, table_id, key, row) -> snapshot_id`. Both writes must validate batch keys, read the prior key slice, skip byte-equivalent retry, and perform one atomic Iceberg replacement scoped to the key. Test the selected PyIceberg overwrite behavior on a real temporary Iceberg table; if it cannot meet atomic/filter semantics, use the existing Spark Iceberg runtime for this writer and record the choice here before production data.
- [x] Add a serialization/concurrency test: two attempts at the same object/key cannot both report a clean publish if they produce different rows. Airflow writer pool serializes normal runs; a conflicting external writer must be detected by post-commit uniqueness/content QA.
- [x] Run `.venv/bin/pytest -q tests/contract/storage/test_meta_bronze_schemas.py tests/unit/storage/test_iceberg_tables.py` and Ruff on touched Python files.

**Exit:** physical schemas match the contract; object-level retry/reparse is atomic and testable.

## Task 3 — Extend the existing landing DAG with raw Meta registration

**Files:** `src/flashflood_data/orchestration/meta/registry.py`, `src/flashflood_data/orchestration/meta/attempts.py`, `src/flashflood_data/orchestration/landing/service.py`, `airflow/dags/static_source_landing.py`, tests under `tests/unit/orchestration/meta/` and `tests/contract/infra/`.

- [x] Add tests proving a repeated registry seed does not duplicate `(source_id, source_version)` or `(dataset_id, contract_version)` and that a changed contract creates a new version rather than mutating history. Seed source facts from `config/landing/static.yaml` plus a curated registry manifest for provider/license/owner/policy fields; never invent missing license or access policy values.
- [x] Implement `source_registry` and `dataset_registry` writers. Include `meta` and `bronze` dataset entries, schema refs, owner and quality/access/retention policy refs; reject incomplete rows instead of using placeholder strings.
- [x] Add a registry preflight to `static_source_landing` before its source groups. It seeds/checks `meta.source_registry` and `meta.dataset_registry`; the existing `register_batch` still writes `meta.source_objects` after MinIO publication. A registry failure stops that landing run before raw publication.
- [x] Add landing instrumentation that records `ingest_attempts` for every asset attempt, including HTTP status/error code if observed. Stable `(ingest_run_id, source_id, asset_id, attempt_no)` must survive Airflow task retry; use actual request fingerprint from the landing service, not a new random ID.
- [x] Test the existing DAG's sequence `registry preflight → publish_source → register_batch → cleanup_batch → publish_run_summary`. Test one successful source, one skipped/reused object and one failed fetch; verify attempts point at the correct run and no credentials appear in stored metadata.
- [x] For objects landed before this instrumentation exists, retain their existing `source_objects` rows and mark attempt history as unavailable in the backfill report; do not synthesize successful HTTP attempts from manifests.

**Exit:** the existing landing DAG registers raw objects and their source/dataset/attempt metadata in Meta; previously landed raw objects remain untouched.

## Task 4 — Pipeline-run, QA, snapshot and lineage Meta services

**Files:** `src/flashflood_data/orchestration/meta/{runs,quality,lineage}.py`, tests under `tests/unit/orchestration/meta/`.

- [x] Test run state transitions `running → succeeded/failed`, retry count, terminal timestamp, and `published_at` only after required QA passes. A rerun of the same event must not append another row for the same `pipeline_run_id`.
- [x] Implement `pipeline_runs`, `quality_results`, `table_snapshot_ref`, and `lineage_edges` writers with the exact grains from `data.md`. `quality_results` must identify pre/post-commit phase; post-commit results carry `(snapshot_table, snapshot_id)`.
- [x] Use a stable lineage edge hash over run ID, one input reference, output table/snapshot and transform role. Enforce exactly one input reference form: raw object ID **or** `(input_table, input_snapshot_id)`. Reject an output snapshot not registered by the same run.
- [x] Make Meta reporting recoverable: if Bronze commit succeeds but a Meta write fails, replay recording from the persisted `pipeline_run_id`, input object IDs and committed output snapshot. Never rerun a destructive data rewrite merely to repair Meta.
- [x] Keep `parameter_sets` schema available now; populate it only when a routing/model parameter set exists. Do not generate fake beta/alpha data for Bronze.
- [x] Wire `pipeline_runs`, raw availability/checksum QA, and the `meta.source_objects` output snapshot reference into the existing landing DAG's summary path. Reuse its current source-level partial-failure summary; register successful source groups even if another group fails. Reserve `lineage_edges` for actual raw-object/input-snapshot → derived-output-snapshot transforms in Bronze and later layers; raw landing already has direct `source_objects`/manifest provenance.

**Exit:** the landing DAG records raw registration and run/QA audit in Meta; each later Bronze commit can be traced back to raw object(s), parser version, QA evidence and exact Iceberg snapshot.

## Task 5 — First Bronze vertical slice: L12 basin polygon

**Files:** `src/flashflood_data/orchestration/bronze/{registry,vector,service}.py`, `airflow/dags/static_source_to_bronze.py`, `tests/unit/orchestration/bronze/test_vector.py`, `tests/contract/infra/test_static_source_to_bronze_dag.py`, `tests/integration/lakehouse/test_meta_bronze_smoke.py`.

- [x] Test a ZIP fixture with HydroBASINS L12 records: preserve raw field spellings/values, stable `source_feature_id`, original CRS/WKB, valid WGS84 bbox and source provenance. Reject a missing `.shp/.shx/.dbf/.prj` member, duplicate feature ID or wrong level.
- [x] Implement parser dispatch keyed by `source_id` and media type. Read the published object from MinIO into bounded staging, validate the manifest, parse the chosen member, and write `bronze.basin_polygon_raw` with `object_id` provenance. Do not derive `basin_id`, `NEXT_DOWN`, slope or soil fields here.
- [x] Create the first Airflow DAG path for HydroBASINS: `discover_objects` returns object IDs, mapped `parse_and_commit` invokes the Bronze service for each object, `check_quality` evaluates committed rows/snapshot, and `finalize_run` records lineage and sets `published_at` only on pass. Keep raw bytes and parsed rows out of XCom; propagate only IDs, status, counts and snapshot IDs. Put Iceberg write tasks in one writer pool.
- [x] Run the same object twice and assert identical Bronze row count, unchanged business keys, no extra data rows, a successful QA result and a valid input-object → output-snapshot lineage edge. Change `parser_version` in a fixture and verify object-slice replacement yields one visible version.

**Exit:** one source can travel raw → Bronze → Meta with retry and provenance verified end to end.

## Task 6 — Remaining static parsers

**Files:** `src/flashflood_data/orchestration/bronze/{vector,raster,events,osm}.py`, focused tests under `tests/unit/orchestration/bronze/`.

- [x] Add BasinATLAS L12 and HydroRIVERS ZIP parsing to `basin_polygon_raw` and `river_reach_raw`. Test that same feature IDs in different `object_id`s do not collide and raw names remain unchanged.
- [x] Add `admin_boundary_raw` for current Sơn La and historical GADM sources after Task 1's contract update. Test source ID/code preservation and null-safe source attributes.
- [x] Add `raster_coverage` by reading each raster header and writing per-band rows for DEM, SoilGrids, WorldCover and WorldPop. Derive bbox from CRS only, record original resolution/dtype/nodata and `object_uri`; verify no pixel-array materialization. Test multi-band and NoData cases.
- [x] Add `historical_event_raw` for workbook/CSV/document records. Keep unparsed original text/date and original column names; use stable row identity independent of workbook iteration order. If PDF/OCR cannot yield structured records, record a QA skip for that object and leave it raw pending a versioned extractor.
- [x] Benchmark OSM PBF on representative data for ID/tag/geometry fidelity and peak memory. Only then implement `osm_feature_raw` in bounded batches with `node`, `way`, `relation` keys. If the current Pyogrio flow loses required IDs or raw tags, add a dedicated streaming PBF reader as a separate dependency decision rather than writing incomplete Bronze rows.
- [x] Run focused unit/contract tests for each parser and assert all 11 static source IDs route to a target or an explicit non-data/unsupported outcome.

**Exit:** all eligible static sources have a verifiable parsed representation, with exact source provenance and no implicit Silver transformations.

## Task 7 — Expand the Bronze DAG, backfill and recovery

**Files:** `airflow/dags/static_source_to_bronze.py`, `src/flashflood_data/cli/commands/bronze.py`, `tests/contract/infra/test_static_source_to_bronze_dag.py`, `README.md`.

- [x] Expand the Task 5 Bronze DAG's source routing to every supported static parser. Use per-object dynamic task mapping or bounded batches, one writer pool, `max_active_runs=1`, metadata-only XCom and per-object failure isolation. It is separately triggerable after the existing `static_source_landing` DAG; historical backfill must not rerun landing or call source download adapters.
- [x] Add a CLI command such as `flashflood-data bronze backfill --source-id hydrobasins_v1c` with optional `--object-id` and `--dry-run` arguments for local tests/manual recovery. It must invoke the same Bronze service as the DAG and must not become a second parsing implementation.
- [x] Verify DAG import, explicit dependency ordering, run IDs and trigger behavior. Simulate one parser failure and one QA failure: both must leave visible Meta failure evidence, prevent `published_at`, and let unrelated objects complete.
- [x] Provide read-only reconciliation command/report: available source objects without Bronze rows; Bronze rows lacking available raw object; duplicate business keys; published runs missing snapshot refs or lineage edges.

**Exit:** existing MinIO raw assets can be backfilled into Bronze without refetching, and an operator can see missing/failed objects.

## Task 8 — End-to-end verification and rollout

**Files:** `tools/smoke/meta_bronze.sh`, `tests/integration/lakehouse/test_meta_bronze_smoke.py`, `README.md`.

- [x] Build an isolated MinIO prefix and temporary Polaris namespace. Land fixture objects, run Bronze, read Iceberg tables, compare source/object IDs, count rows and check QA/lineage/snapshot references. Repeat the run and verify unchanged business row counts. Clean up only the test prefix/namespace.
- [x] Run `.venv/bin/pytest -q tests/unit/orchestration/meta tests/unit/orchestration/bronze tests/unit/storage/test_iceberg_tables.py tests/contract/storage/test_meta_bronze_schemas.py tests/contract/infra/test_static_source_to_bronze_dag.py`; run repository Ruff and `docker compose config --quiet`; run the isolated lakehouse smoke when infrastructure is available.
- [x] Roll out in order: create Meta/contract tables; seed registries; backfill one HydroBASINS object; compare source and Bronze geometry/counts; then expand by source family. Record a checkpoint after every family with attempted/succeeded/failed/skipped counts. Keep `published_at` null until QA passes.
- [x] Update README with actual table status and commands only after verification. Never label all Meta/Bronze tables populated just because the schemas were created.


**Exit:** tables are queryable, retries do not duplicate rows, failures are auditable, and rollout status is honest.

## Separate dynamic-data decision

After static Meta/Bronze is stable, implement ERA5-Land/IFS landing with immutable time-window manifests and a source-specific freshness/revision policy. Benchmark one representative period for `bronze.weather_grid_value`; choose row-level Iceberg only if storage/write/query costs are acceptable. Otherwise keep GRIB/NetCDF chunks as Bronze payloads in MinIO, add indexed coverage/time metadata to the contract, and parse selectively for Silver grid values. This decision does not block the static tasks above.
