# Repository Organization and Source-Landing Design

**Date:** 2026-09-15

**Status:** Approved for implementation planning

**Scope:** Repository refactor plus the boundary for the next static and dynamic source-landing work

## 1. Goal

Reorganize the existing repository into explicit modules for static data, storage, dynamic
weather ingestion, hydrology, threat, impact, graph, road routing, serving, and orchestration.
The refactor must preserve the current CLI, static-pipeline behavior, data products, and test
coverage.

The structure must also support the next delivery sequence:

1. land canonical static source objects in MinIO;
2. land dynamic provider responses in MinIO without prematurely defining canonical tables;
3. approve the domain and Iceberg schemas;
4. build canonical tables, features, B0-B3, impact, graph, and routing on those schemas.

## 2. Context

Section 9 of `docs/Ke_hoach_trien_khai_de_tai_runoff_routing_Son_La.docx` proposes a
layered `src/sonla_flood/` package with ingestion, contracts, geospatial, features,
hydrology, threat, impact, graph, routing, serving, and common directories. Its separation of
responsibilities is useful, but the proposal assumes a new project. The current repository
already has a working `flashflood_data` package, a `flashflood-data` CLI, 405 tests, fixed
Make targets, a Compose stack, and completed static artifacts.

The current concentration points are:

| Module | Approximate size | Mixed responsibilities |
|---|---:|---|
| `http.py` | 1,142 lines | client, retry, resume, bounds, payload validation, redaction |
| `qa/checks.py` | 1,266 lines | admin, hydro, mapping, raster, population, event, provenance checks |
| `sources/admin.py` | 703 lines | current admin, historical admin, normalization, validation, adapters |
| `pipeline.py` | 698 lines | stages, run lifecycle, source execution, reuse and failure handling |
| `composition.py` | 417 lines | dependencies, fingerprints, handlers, output registration |

The refactor addresses those boundaries without changing scientific formulas or data semantics.

## 3. Decisions

### 3.1. Preserve stable external interfaces

- Keep the Python package name `flashflood_data`.
- Keep the executable name `flashflood-data`.
- Keep `config/`, `compose.yaml`, `airflow/dags/`, `spark/jobs/`, and existing Make target names.
- Add temporary re-export modules when a move would otherwise break imports across multiple
  migration commits. Remove the shims after all repository consumers move.
- Do not combine file moves with behavior, schema, algorithm, or data-output changes.

Renaming the package to `sonla_flood`, `config/` to `configs/`, or `compose.yaml` to
`docker-compose.yml` would create broad churn without improving module boundaries.

### 3.2. Organize application code by bounded domain

The eventual package tree is:

```text
src/flashflood_data/
├── cli/
│   ├── app.py
│   └── commands/
├── core/
│   ├── config.py
│   ├── paths.py
│   ├── errors.py
│   └── units.py
├── catalog/
│   ├── models.py
│   ├── repository.py
│   └── fingerprint.py
├── storage/
│   ├── filesystem.py
│   ├── atomic.py
│   ├── object_store.py
│   └── iceberg.py
├── static/
│   ├── sources/
│   ├── harmonize/
│   ├── features/
│   ├── mappings/
│   ├── qa/
│   └── workflow/
├── weather/
│   ├── sources/
│   ├── contracts.py
│   ├── normalization.py
│   └── workflow.py
├── hydrology/
├── threat/
│   └── models/
├── impact/
├── graph/
├── routing/
├── serving/
└── orchestration/
```

Directories for future domains are created only when their first working module is added. No
empty architecture placeholders are committed.

### 3.3. Dependency direction

Dependencies flow in this direction:

```text
core
  ↑
catalog / storage
  ↑
static / weather / hydrology / threat / impact / graph / routing
  ↑
orchestration
  ↑
CLI / Airflow DAGs / Spark job entrypoints / serving
```

- `core` contains configuration, paths, errors, and units only. It does not import a domain.
- `catalog` owns asset, source, and run records plus fingerprinting and persistence.
- `storage` owns filesystem, object-store, and Iceberg adapters. It does not own provider or
  scientific logic.
- A domain may use `core`, `catalog`, and `storage`; domains do not import orchestration or CLI.
- Airflow DAGs and Spark jobs call application services. They do not contain transformations,
  formulas, provider rules, or catalog transitions.
- Avoid a generic `common/` or `utils.py`; shared code must have a specific responsibility.

### 3.4. Resolve the two meanings of routing

- `hydrology/basin_routing.py` means propagation of modelled discharge through the directed
  HydroBASINS network.
- `routing/` means road-network route selection for response or accessibility.
- Neither module may use the unqualified name `network_routing.py` at a shared package level.

The B0-B3 names follow the consolidated runoff-routing document:

- B0: rainfall baseline;
- B1: basin runoff;
- B2: local basin routing;
- B3: upstream-to-downstream basin-network routing.

Susceptibility and soil moisture remain diagnostic, localization, sensitivity, or fallback
inputs as specified by the consolidated plan; they do not redefine B1-B3.

## 4. Static Package Boundaries

`static/` retains the complete working static pipeline as one bounded domain:

```text
static/
├── sources/       # source-specific discovery, download contracts and validation
├── harmonize/     # AOI clipping and schema/CRS normalization
├── features/      # terrain, soil, land cover, hydrology, population and profile
├── mappings/      # basin-to-admin/river/road/bridge/facility/settlement relationships
├── qa/            # domain-specific quality checks plus report/map publication
└── workflow/      # static stages, runner, dependency resolver and handlers
```

Provider adapters stay separate. A broad `static_sources.py` module is prohibited because it
would recreate the current concentration problem.

The largest modules split as follows:

| Current module | Target modules |
|---|---|
| `http.py` | shared HTTP client, retry/resume, payload verification, redaction, errors |
| `sources/admin.py` | current admin adapter, historical adapter, normalization, validation |
| `qa/checks.py` | admin, hydro, raster, mapping, population, event, provenance checks |
| `pipeline.py` | stages, summary, runner, source execution, reuse/recovery |
| `composition.py` | dependency resolver, stage handlers, output registration |

## 5. Repository, Docker, and Tool Layout

```text
.
├── compose.yaml
├── .dockerignore
├── Makefile
├── config/
├── src/flashflood_data/
├── infra/
│   ├── docker/
│   │   ├── airflow/Dockerfile
│   │   └── spark/Dockerfile
│   └── services/
│       ├── postgres/init-multiple-databases.sh
│       └── polaris/bootstrap.sh
├── tools/
│   ├── bootstrap/
│   ├── smoke/
│   ├── migration/
│   ├── maintenance/
│   └── benchmark/
├── airflow/
│   ├── dags/
│   └── plugins/
├── spark/jobs/
├── sql/
│   ├── iceberg/
│   └── postgis/
├── cypher/neo4j/
├── tests/
├── notebooks/
├── docs/
└── dataset/
```

### 5.1. Docker rules

- Keep one root `compose.yaml`; add optional profiles for Spark and later graph, serving, or
  monitoring services instead of creating unrelated Compose stacks.
- Pin image and runtime versions. Do not use mutable `latest` tags.
- Build images from code and dependency manifests only. Data and secrets enter at runtime
  through object storage, bind mounts, or environment variables.
- Persistent service state remains under ignored `dataset/lakehouse/`.
- Base Lakehouse services remain usable without starting Spark or future optional services.
- Add `.dockerignore` before further image builds. It excludes at least `dataset/`, `.venv/`,
  `.git/`, `.worktrees/`, caches, and large office/PDF documents.
- Narrow the Spark build context to its Docker directory because it does not need repository
  data. Airflow may retain the root context to read `requirements/lakehouse.txt`, protected by
  `.dockerignore`.

### 5.2. Tool rules

- `tools/` contains operator-facing bootstrap, smoke, migration, maintenance, and benchmark
  entrypoints.
- Reusable logic belongs in `src/flashflood_data`; a tool only parses arguments and calls that
  logic.
- `src/flashflood_data` never imports from `tools/`.
- Make targets remain the stable interface and delegate to the relocated tools.
- A destructive maintenance operation must support a non-destructive dry run, print exact
  resolved targets and reclaimable bytes, validate its recovery source, and require an explicit
  execution flag.
- Tools do not print credentials or complete environment files.

Service bootstrap scripts invoked inside containers stay under `infra/services/`, not
`tools/`. Host commands stay under `tools/`.

## 6. Test Layout

Keep test type as the first boundary and add domain subdirectories:

```text
tests/
├── unit/
│   ├── core/
│   ├── catalog/
│   ├── storage/
│   ├── static/
│   ├── weather/
│   ├── hydrology/
│   └── threat/
├── contract/
│   ├── sources/
│   ├── storage/
│   └── infra/
├── integration/
│   ├── static/
│   ├── lakehouse/
│   └── model/
├── replay/
└── fixtures/
```

Do not move a test merely to fill this tree. Move it with the production slice it verifies.
Keep provider contract tests distinct from broader integration tests.

## 7. Refactor Sequence

The implementation uses small, behavior-preserving slices:

1. Add `.dockerignore` and architecture/CLI/Make contract checks.
2. Extract `core`, catalog models/repository/fingerprints, and filesystem storage.
3. Split the shared HTTP implementation without changing retry, resume, or redaction behavior.
4. Move static source adapters and split the admin module.
5. Move harmonization, feature, mapping, and profile modules.
6. Split static QA checks by subject while retaining one report aggregator.
7. Split pipeline/composition into static workflow stages, runner, dependency resolver, and
   handlers.
8. Split CLI commands while retaining the executable and command names.
9. Reorganize Dockerfiles, service bootstrap scripts, host tools, tests, and documentation.
10. Remove temporary import shims after repository consumers use the target paths.

Each slice must keep the full test suite and lint clean. Static smoke, recovery, and idempotence
tests run after any workflow boundary changes. Docker/infra slices additionally validate the
Compose configuration and run the applicable Lakehouse or Spark smoke check when the local
services are available.

## 8. Source-First MinIO Landing After the Refactor

The next feature after directory reorganization is source landing. It deliberately precedes
canonical and Iceberg domain-table design.

### 8.1. Object layout

Canonical static source objects use:

```text
s3://raw/static/<source_id>/<source_version>/<asset_id>/<filename>
```

Dynamic provider responses use:

```text
s3://raw/dynamic/<source_id>/<product>/<yyyy>/<mm>/<dd>/<retrieval_id>/<filename>
```

Times absent or unverified in a provider response are not invented for an object path or
manifest. Provider-issued, model-run, valid, available, first-seen, and retrieval times remain
separate metadata fields whenever they are known.

### 8.2. Raw immutability and idempotence

- Raw objects are immutable and content-addressed by the existing `asset_id` and SHA-256.
- A correction or provider revision creates a new object; it never overwrites an accepted
  object.
- Upload uses a run-scoped staging key. The writer verifies size and SHA-256 before publishing
  the immutable final key and removing only its own staging object.
- If the final key already has the expected checksum and size, the run records reuse and does
  not upload a duplicate.
- Failures retain source/run metadata and a sanitized error. Invalid payloads go to a quarantine
  prefix and are excluded from downstream publication.

### 8.3. Minimal landing manifest

Before the canonical schema is approved, every object receives a small operational manifest:

- `asset_id`, `source_id`, source product/version, and media type;
- original source URI with credentials removed;
- MinIO object URI, byte size, checksum algorithm, and checksum;
- provider times only when evidenced, plus `retrieved_at` and `first_seen_at`;
- request fingerprint, retrieval run ID, license identifier, status, and sanitized error;
- free-form provider metadata that does not contain credentials.

The manifest is stored beside the raw object and registered in the current file catalog. It is
an operational landing contract, not the final weather, hydrology, threat, or Iceberg schema.
After schema approval, manifests become the input for a versioned `meta.source_object` table.

### 8.4. Static backfill policy

- Import only canonical source assets. Skip catalog rows marked as duplicates.
- Store the BasinATLAS ZIP once. Do not import extracted global levels 1-12 into MinIO.
- Before removing extracted BasinATLAS files, verify the ZIP checksum/readability, the selected
  L10 output, and all downstream artifacts. Cleanup remains a separately invoked maintenance
  operation with dry-run output.
- Keep current harmonized and derived files in the file lake until their Iceberg schemas are
  approved. Source landing does not silently reclassify them as raw.
- Do not delete a local canonical source until its MinIO object and manifest pass verification
  and the resolved deletion is explicitly requested.

### 8.5. Dynamic landing policy

- Initial Open-Meteo IFS, ERA5-Land, and GSMaP work stops after exact provider payloads and
  manifests are durably stored.
- Preserve response bytes and provider metadata before parsing or unit conversion.
- Polling the same provider state reuses the existing content-addressed object.
- Different forecast runs, revisions, retrieval snapshots, or corrected observations remain
  distinct objects.
- Airflow DAGs call the same source-landing application service used by the CLI; scheduling does
  not define source semantics.

### 8.6. Schema gate

The following work starts only after the schema review is approved:

- canonical dynamic tables and temporal keys;
- Iceberg static tables and geometry representation;
- basin-grid weights and basin-hour features;
- MERGE/upsert and publication views;
- B0-B3 model tables;
- PostGIS, Knowledge Graph, impact, and road-routing serving models.

This gate prevents raw acquisition from being blocked by schema work while avoiding a temporary
canonical schema that later requires destructive rewrites.

## 9. Failure and Recovery Behavior

- File moves and compatibility shims are accepted only with all existing tests passing.
- Import cycles or forbidden dependency directions fail an architecture test.
- A failed source upload cannot publish a valid manifest or trigger downstream processing.
- A process restart reuses verified objects and resumes or replaces only its own staging upload.
- Catalog and object disagreement is reported for reconciliation; neither side is silently
  overwritten.
- Cleanup failures leave the verified source archive and all downstream products intact.
- No refactor, source landing, or cleanup operation modifies the content of existing raw files.

## 10. Acceptance Criteria

The repository refactor is complete when:

1. Package, CLI, Make targets, static outputs, recovery, and idempotence behavior are preserved.
2. The full existing test suite and Ruff pass after the final move.
3. No production module remains above roughly 500 lines when it contains separable concerns.
4. Architecture tests reject dependency cycles and invalid layer imports.
5. Airflow DAGs and Spark jobs are thin entrypoints into installed package code.
6. Docker build contexts exclude datasets, secrets, caches, worktrees, and large documents.
7. Compose validation and available infrastructure smoke checks pass.
8. No file under `dataset/` is moved, rewritten, or deleted by the refactor.

The source-landing slice is complete when:

1. Static canonical raw objects and manifests can be uploaded idempotently to MinIO.
2. BasinATLAS is stored once as the verified archive and extracted levels are excluded.
3. At least one fixture for each initial dynamic provider can be landed without defining a
   canonical domain table.
4. Duplicate payloads are reused and corrections/revisions remain separate.
5. Interrupted uploads recover without a valid partial object or leaked staging data.
6. Credentials do not appear in object keys, manifests, logs, or committed files.
7. Existing harmonized and derived data remain usable while schema work proceeds separately.

## 11. Out of Scope

- Changing current static formulas, AOIs, source selection, or output schemas during refactor.
- Creating empty packages for future functionality.
- Loading harmonized/derived data into Iceberg before schema approval.
- Implementing B0-B3, PostGIS, Neo4j, impact, road routing, API, or dashboard in the source-
  landing slice.
- Deleting BasinATLAS or any other source as an implicit side effect of migration.
