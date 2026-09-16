# Static Source Landing to MinIO and Iceberg Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Build the first static-source DAG that acquires or reuses approved source assets, selects basin level 12, publishes immutable raw objects and manifests to MinIO, and registers each object in the Iceberg `meta.source_objects` inventory through Polaris.

**Architecture:** A reusable landing application service composes existing source adapters and `HttpFetcher` with focused object-store and Iceberg repositories. The CLI calls the complete service, while the Airflow DAG separates publish, Iceberg registration, and cleanup into small metadata-only task boundaries. The existing static harmonization and feature workflow remains unchanged.

**Tech Stack:** Python 3.11, Pydantic 2, PyArrow filesystem, PyIceberg 0.11.1, Apache Airflow 3.3.1, Apache Polaris 1.7.0, MinIO RELEASE.2025-09-07T16-13-09Z, Pytest, Docker Compose

**Spec:** `docs/superpowers/specs/2026-09-16-l12-source-landing-and-static-feature-design.md`

**Execution checkpoint (2026-09-16):** Tasks 1-6 complete on branch `feature/static-source-landing` in `.worktrees/static-source-landing`. Task 7 is the next task; Tasks 7-8 have not started. The latest implementation commit is `59aff3e`.

## Global Constraints

- Execute in an isolated worktree created with the `using-git-worktrees` skill; the main checkout contains user-owned modifications and untracked research files.
- Preserve Python `>=3.11,<3.13`, Airflow `3.3.1`, PyIceberg `0.11.1`, Polaris `1.7.0`, and the pinned MinIO image already in the repository.
- Add no new dependency declaration. Install the project's existing `pyproject.toml` dependencies in the Airflow image, use `pyarrow.fs.S3FileSystem` for MinIO, and use the already pinned `pyiceberg[pyarrow]` for Polaris/Iceberg.
- The first DAG stops after raw-object publication, sidecar-manifest publication, and `meta.source_objects` registration.
- Use basin level 12 for HydroBASINS and BasinATLAS. Do not change the existing L10 harmonization code in this plan.
- Keep existing local raw, harmonized, derived, catalog, and QA files. Cleanup may remove only run-scoped staging content after successful Iceberg registration.
- Initial canonical SoilGrids scope is eight properties (`clay`, `sand`, `silt`, `bdod`, `cfvo`, `soc`, `wv0033`, `wv1500`), three depths, and four statistics, totaling 96 TIFF objects plus eight capabilities documents.
- Raw objects are immutable. A final-key checksum conflict fails closed and never overwrites the existing object.
- Manifests contain no credential values and describe object verification only. Iceberg `status=available` is committed afterward.
- Airflow tasks pass metadata through XCom, never source payload bytes.
- The current `dataset/harmonized/aoi/environmental_aoi.geoparquet` is a required input for CopDEM tile resolution and SoilGrids WCS bounds. Fail preflight with a clear error if it is absent; bootstrapping that AOI in an empty checkout belongs to the follow-on plan.
- Run all named focused tests after each task. Run the full test suite, Ruff, Compose validation, and Lakehouse smoke only once at the final verification task unless a failure justifies repeating them.

---

## File Structure

New production files:

```text
config/landing/static.yaml
src/flashflood_data/core/lakehouse.py
src/flashflood_data/storage/object_store.py
src/flashflood_data/storage/iceberg.py
src/flashflood_data/orchestration/__init__.py
src/flashflood_data/orchestration/landing/__init__.py
src/flashflood_data/orchestration/landing/config.py
src/flashflood_data/orchestration/landing/models.py
src/flashflood_data/orchestration/landing/bundle.py
src/flashflood_data/orchestration/landing/sources.py
src/flashflood_data/orchestration/landing/service.py
airflow/dags/static_source_landing.py
tools/smoke/source_landing.sh
```

New tests:

```text
tests/unit/core/test_lakehouse_config.py
tests/unit/storage/test_object_store.py
tests/unit/storage/test_iceberg_source_objects.py
tests/unit/orchestration/landing/test_config.py
tests/unit/orchestration/landing/test_bundle.py
tests/unit/orchestration/landing/test_sources.py
tests/unit/orchestration/landing/test_service.py
tests/contract/infra/test_static_source_landing_dag.py
tests/integration/lakehouse/test_source_landing_smoke.py
```

Existing files changed:

```text
src/flashflood_data/storage/__init__.py
src/flashflood_data/catalog/repository.py
src/flashflood_data/cli/app.py
src/flashflood_data/cli/commands/static.py
src/flashflood_data/cli/__init__.py
tests/unit/test_cli.py
tests/unit/catalog/test_catalog.py
infra/docker/airflow/Dockerfile
compose.yaml
tools/bootstrap/init_lakehouse_env.sh
tests/unit/test_lakehouse_compose.py
tests/unit/test_lakehouse_env.py
Makefile
README.md
```

### Task 1: Landing configuration and immutable contracts

**Files:**
- Create: `config/landing/static.yaml`
- Create: `src/flashflood_data/core/lakehouse.py`
- Create: `src/flashflood_data/orchestration/__init__.py`
- Create: `src/flashflood_data/orchestration/landing/__init__.py`
- Create: `src/flashflood_data/orchestration/landing/config.py`
- Create: `src/flashflood_data/orchestration/landing/models.py`
- Create: `tests/unit/core/test_lakehouse_config.py`
- Create: `tests/unit/orchestration/landing/test_config.py`

**Interfaces:**
- Consumes: existing `.env` names `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `POLARIS_CLIENT_ID`, and `POLARIS_CLIENT_SECRET`.
- Produces: `LakehouseSettings`, `LandingSourcePolicy`, `StaticLandingConfig`, `PreparedObject`, `PublishedObject`, `SourceObjectRow`, `LandingManifest`, `PublishedBatch`, `RegisteredBatch`, `LandingTaskEnvelope`, and `LandingRunSummary`.

- [x] **Step 1: Write failing settings and policy tests**

```python
def test_lakehouse_settings_use_local_defaults_and_secret_aliases(monkeypatch, tmp_path):
    monkeypatch.setenv("MINIO_ROOT_USER", "fixture-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "fixture-password")
    monkeypatch.setenv("POLARIS_CLIENT_ID", "fixture-client")
    monkeypatch.setenv("POLARIS_CLIENT_SECRET", "fixture-secret")
    settings = LakehouseSettings(
        _env_file=None,
        project_root=tmp_path,
        staging_root=tmp_path / "staging",
    )
    assert settings.minio_endpoint == "http://127.0.0.1:9000"
    assert settings.raw_bucket == "raw"
    assert settings.minio_access_key.get_secret_value() == "fixture-user"
    assert "fixture-password" not in repr(settings)


def test_static_landing_config_is_l12_and_has_exact_soil_scope():
    config = load_static_landing_config(ROOT / "config" / "landing" / "static.yaml")
    assert config.basin_level == 12
    soil = config.source("soilgrids_2_0")
    assert soil.settings_override["properties"] == [
        "clay", "sand", "silt", "bdod", "cfvo", "soc", "wv0033", "wv1500"
    ]
    assert soil.settings_override["depths"] == ["0-5cm", "5-15cm", "15-30cm"]
    assert soil.settings_override["statistics"] == ["mean", "Q0.05", "Q0.5", "Q0.95"]
```

- [x] **Step 2: Run the focused tests and verify the missing imports**

Run: `pytest tests/unit/core/test_lakehouse_config.py tests/unit/orchestration/landing/test_config.py -q`

Expected: FAIL because `core.lakehouse` and `orchestration.landing` do not exist.

- [x] **Step 3: Implement validated runtime settings**

Create `LakehouseSettings` with these public fields and aliases:

```python
class LakehouseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    minio_endpoint: str = "http://127.0.0.1:9000"
    minio_region: str = "us-east-1"
    raw_bucket: str = "raw"
    minio_access_key: SecretStr = Field(validation_alias="MINIO_ROOT_USER")
    minio_secret_key: SecretStr = Field(validation_alias="MINIO_ROOT_PASSWORD")
    polaris_uri: str = "http://127.0.0.1:8181/api/catalog"
    polaris_catalog: str = "flood_lakehouse"
    polaris_client_id: SecretStr = Field(validation_alias="POLARIS_CLIENT_ID")
    polaris_client_secret: SecretStr = Field(validation_alias="POLARIS_CLIENT_SECRET")
    project_root: Path = Field(default_factory=Path.cwd, validation_alias="FLASHFLOOD_PROJECT_ROOT")
    staging_root: Path = Field(
        default=Path("dataset/lakehouse/staging"),
        validation_alias="FLASHFLOOD_STAGING_ROOT",
    )
```

Validate that endpoints use `http` or `https`, bucket names contain no slash, and both roots are
resolved absolute paths. Keep secret fields as `SecretStr` through every factory.

- [x] **Step 4: Implement immutable landing models and YAML loading**

Define the exact policy values:

```yaml
basin_level: 12
sources:
  - source_id: hydrobasins_v1c
    mode: shapefile_bundle
    filename_contains: hybas_as_lev12_v1c.shp
    output_name: hydrobasins_l12.zip
    selection: {basin_level: 12}
  - source_id: basinatlas_v10
    mode: shapefile_bundle
    filename_contains: BasinATLAS_v10_lev12.shp
    output_name: basinatlas_l12.zip
    selection: {basin_level: 12}
  - source_id: hydrorivers_v10
    mode: shapefile_bundle
    filename_contains: HydroRIVERS_v10_as.shp
    output_name: hydrorivers_asia.zip
    selection: {region: asia}
  - source_id: cop_dem_glo30_2024_1
    mode: individual
    selection: {coverage: environmental_aoi}
  - source_id: soilgrids_2_0
    mode: individual
    selection: {depth_interval_cm: [0, 30]}
    settings_override:
      properties: [clay, sand, silt, bdod, cfvo, soc, wv0033, wv1500]
      depths: [0-5cm, 5-15cm, 15-30cm]
      statistics: [mean, Q0.05, Q0.5, Q0.95]
```

Use frozen Pydantic models. `LandingSourcePolicy.mode` is
`Literal["individual", "shapefile_bundle"]`; `selection` and `settings_override` reject
credential-like keys using the existing catalog-model validation helpers. `StaticLandingConfig`
rejects duplicate source IDs and any basin level other than 12.

Define the cross-task models with JSON-safe fields. `SourceObjectRow` must exactly represent the
Iceberg row contract, and `PublishedBatch` may contain only source IDs, object/manifest URIs,
checksums, small metadata maps, and run-scoped cleanup paths. It must never contain file bytes.
`LandingTaskEnvelope` is a discriminated success/failure model: success contains one published or
registered batch; failure contains only `source_id` and a sanitized error code. Every envelope
must round-trip through `model_dump(mode="json")` and `model_validate`.

- [x] **Step 5: Run focused tests and lint**

Run: `pytest tests/unit/core/test_lakehouse_config.py tests/unit/orchestration/landing/test_config.py -q && ruff check src/flashflood_data/core/lakehouse.py src/flashflood_data/orchestration/landing tests/unit/core/test_lakehouse_config.py tests/unit/orchestration/landing/test_config.py`

Expected: PASS.

- [x] **Step 6: Commit the contracts**

```bash
git add config/landing/static.yaml src/flashflood_data/core/lakehouse.py src/flashflood_data/orchestration tests/unit/core/test_lakehouse_config.py tests/unit/orchestration/landing/test_config.py
git commit -m "feat: define source landing contracts"
```

### Task 2: Immutable MinIO publication

**Files:**
- Create: `src/flashflood_data/storage/object_store.py`
- Modify: `src/flashflood_data/storage/__init__.py`
- Create: `tests/unit/storage/test_object_store.py`

**Interfaces:**
- Consumes: `LakehouseSettings`, local `Path` payloads, deterministic object keys, and SHA-256 values.
- Produces: `ObjectStore`, `PyArrowS3ObjectStore.from_settings(settings)`, and `ObjectPublisher.publish_file(...) -> PublishedObject`.

- [x] **Step 1: Write failing idempotence and conflict tests with an in-memory fake**

```python
def test_publish_uses_run_staging_and_reuses_verified_final(tmp_path):
    payload = tmp_path / "asset.bin"
    payload.write_bytes(b"verified-source")
    store = MemoryObjectStore()
    publisher = ObjectPublisher(store, bucket="raw")

    first = publisher.publish_file(
        payload,
        final_key="static/source/1/asset/asset.bin",
        run_id="run-1",
        media_type="application/octet-stream",
    )
    second = publisher.publish_file(
        payload,
        final_key="static/source/1/asset/asset.bin",
        run_id="run-2",
        media_type="application/octet-stream",
    )

    assert first.reused is False
    assert second.reused is True
    assert store.keys() == {"raw/static/source/1/asset/asset.bin"}


def test_publish_fails_closed_on_final_checksum_conflict(tmp_path):
    store = MemoryObjectStore({"raw/static/source/1/asset.bin": b"old"})
    payload = tmp_path / "asset.bin"
    payload.write_bytes(b"new")
    with pytest.raises(ObjectConflict):
        ObjectPublisher(store, "raw").publish_file(
            payload,
            final_key="static/source/1/asset.bin",
            run_id="run-1",
            media_type="application/octet-stream",
        )
    assert store.read("raw/static/source/1/asset.bin") == b"old"
```

Also test that a simulated copy failure leaves only the run-scoped staging key and that a retry
replaces only that staging key before publishing the final object.

- [x] **Step 2: Run the test and verify missing storage classes**

Run: `pytest tests/unit/storage/test_object_store.py -q`

Expected: FAIL because `flashflood_data.storage.object_store` is absent.

- [x] **Step 3: Implement the object-store boundary and S3 adapter**

Define a narrow protocol:

```python
class ObjectStore(Protocol):
    def exists(self, key: str) -> bool: ...
    def size(self, key: str) -> int: ...
    def sha256(self, key: str) -> str: ...
    def upload(self, local_path: Path, key: str, metadata: Mapping[str, str]) -> None: ...
    def copy(self, source_key: str, destination_key: str) -> None: ...
    def delete(self, key: str) -> None: ...
```

`PyArrowS3ObjectStore.from_settings` constructs:

```python
fs.S3FileSystem(
    access_key=settings.minio_access_key.get_secret_value(),
    secret_key=settings.minio_secret_key.get_secret_value(),
    region=settings.minio_region,
    scheme=urlsplit(settings.minio_endpoint).scheme,
    endpoint_override=urlsplit(settings.minio_endpoint).netloc,
    force_virtual_addressing=False,
)
```

Stream uploads and hashes in 8 MiB chunks. Never call `read_bytes()` for a source payload.

- [x] **Step 4: Implement staging, verification, promotion, and reuse**

`ObjectPublisher.publish_file` must:

```python
checksum = sha256_file(local_path)
staging_key = f"_staging/{safe_run_id}/{checksum}/{local_path.name}"
if store.exists(final_key):
    assert_verified_match(store, final_key, local_path.stat().st_size, checksum)
    return PublishedObject(..., reused=True)
store.upload(local_path, staging_key, {"sha256": checksum, "content-type": media_type})
assert_verified_match(store, staging_key, local_path.stat().st_size, checksum)
store.copy(staging_key, final_key)
assert_verified_match(store, final_key, local_path.stat().st_size, checksum)
store.delete(staging_key)
return PublishedObject(..., reused=False)
```

Sanitize `run_id`, object-key components, and filenames with an allowlist. Reject `..`, empty
segments, credentials, query strings, and absolute filesystem paths.

- [x] **Step 5: Run focused tests and lint**

Run: `pytest tests/unit/storage/test_object_store.py -q && ruff check src/flashflood_data/storage tests/unit/storage/test_object_store.py`

Expected: PASS.

- [x] **Step 6: Commit object publication**

```bash
git add src/flashflood_data/storage/object_store.py src/flashflood_data/storage/__init__.py tests/unit/storage/test_object_store.py
git commit -m "feat: publish immutable raw objects"
```

### Task 3: Source acquisition, L12 selection, and deterministic bundles

**Files:**
- Create: `src/flashflood_data/orchestration/landing/bundle.py`
- Create: `src/flashflood_data/orchestration/landing/sources.py`
- Modify: `src/flashflood_data/catalog/repository.py`
- Create: `tests/unit/orchestration/landing/test_bundle.py`
- Create: `tests/unit/orchestration/landing/test_sources.py`
- Modify: `tests/unit/catalog/test_catalog.py`

**Interfaces:**
- Consumes: `LandingSourcePolicy`, existing `SourceSpec`, `SourceContext`, `AssetCatalog`, `HttpFetcher`, and validated `AssetRecord` values.
- Produces: `AssetCatalog.raw_assets(source_id) -> list[AssetRecord]`, `acquire_validated_assets(...) -> tuple[AssetRecord, ...]`, `prepare_source_objects(...) -> tuple[PreparedObject, ...]`, and `build_deterministic_zip(...) -> BundleResult`.

- [x] **Step 1: Write failing bundle tests**

```python
def test_deterministic_zip_is_order_independent_and_streamed(tmp_path):
    shp = write_member(tmp_path / "basin.shp", b"shape")
    dbf = write_member(tmp_path / "basin.dbf", b"attributes")
    first = build_deterministic_zip((shp, dbf), tmp_path / "first.zip")
    second = build_deterministic_zip((dbf, shp), tmp_path / "second.zip")
    assert first.checksum == second.checksum
    assert first.members == second.members
    with ZipFile(first.path) as archive:
        assert archive.namelist() == ["basin.dbf", "basin.shp"]


def test_l12_policy_rejects_l10_and_requires_core_shapefile_members(validated_records):
    policy = LandingSourcePolicy(
        source_id="hydrobasins_v1c",
        mode="shapefile_bundle",
        filename_contains="hybas_as_lev12_v1c.shp",
        output_name="hydrobasins_l12.zip",
        selection={"basin_level": 12},
    )
    prepared = prepare_source_objects(policy, validated_records, staging_root=tmp_path)
    assert [item.filename for item in prepared] == ["hydrobasins_l12.zip"]
    assert {member.suffix for member in prepared[0].members} >= {".shp", ".shx", ".dbf", ".prj"}
```

Add cases for a missing `.dbf`, a duplicate canonical asset, a non-validated asset, a SoilGrids
path outside the eight-property/three-depth/four-statistic scope, and 96 selected SoilGrids TIFFs
plus eight capabilities documents.

- [x] **Step 2: Run bundle and selector tests and verify failure**

Run: `pytest tests/unit/catalog/test_catalog.py tests/unit/orchestration/landing/test_bundle.py tests/unit/orchestration/landing/test_sources.py -q`

Expected: FAIL because bundle and source preparation modules are absent.

- [x] **Step 3: Implement memory-bounded deterministic ZIP creation**

Sort members by archive name. For every member create a `ZipInfo` with timestamp
`(1980, 1, 1, 0, 0, 0)`, Unix mode `0o100644`, UTF-8 flag, and `ZIP_DEFLATED`. Stream with
`shutil.copyfileobj(source, archive.open(info, "w", force_zip64=True), length=8 * 1024 * 1024)`.
Return the ZIP SHA-256 plus each member's relative name, size, and SHA-256.

Reject duplicate member names and require `.shp`, `.shx`, `.dbf`, and `.prj`; retain `.cpg`,
`.sbn`, `.sbx`, and `.shp.xml` when present.

- [x] **Step 4: Implement acquisition by composing existing adapters and fetcher**

`acquire_validated_assets` performs this exact loop without invoking harmonization:

```python
spec = base_spec.model_copy(
    update={"settings": dict(base_spec.settings) | dict(policy.settings_override)}
)
adapter = build_adapter(spec)
while True:
    available = catalog.raw_assets(source_id=spec.source_id)
    remotes = adapter.resolve(context, available)
    pending = [remote for remote in remotes if not reusable(remote, available)]
    if not pending:
        break
    for remote in pending:
        fetched = fetch_with_adapter_hook(adapter, fetcher, context, remote)
        validation = adapter.validate_raw(Path(fetched.storage_path))
        if not validation.passed:
            catalog.transition(fetched.asset_id, AssetStatus.FAILED,
                               error_code="raw_validation_failed",
                               error_message="raw payload validation failed")
            raise RawSourceValidationError(fetched.asset_id)
        catalog.transition(fetched.asset_id, AssetStatus.VALIDATED)
return canonical_validated_assets(catalog, spec.source_id)
```

Add `AssetCatalog.raw_assets(source_id)` as a sorted query over records whose `kind` is `RAW`;
return every lifecycle state because the adapter/fetcher owns recovery. Keep local-content checksum
verification as a separate public method used by `reusable`. Test source filtering, kind filtering,
stable order, and a missing on-disk payload. Do not modify `StaticPipeline` or change existing stage
behavior.
Call `inventory_existing(context)` once in run preparation so the existing L12 and HydroRIVERS
bundles are registered before policies select them.

Before resolving CopDEM or SoilGrids, verify
`dataset/harmonized/aoi/environmental_aoi.geoparquet` exists and is a readable, non-empty layer in
EPSG:4326 after reprojection. Raise `SourcePreconditionError("environmental_aoi_missing")` or
`SourcePreconditionError("environmental_aoi_invalid")` before any remote request.

- [x] **Step 5: Implement exact source selection**

- Shapefile policies select one canonical, non-duplicate, validated primary record whose path
  contains `filename_contains`, resolve member paths only from `metadata_json.bundle_members`, and
  write the deterministic ZIP under `<staging_root>/<run_id>/<source_id>/`.
- Copernicus DEM selects canonical validated raw records for the source ID and publishes each tile
  separately.
- SoilGrids selects exactly the configured capabilities XML documents and TIFF paths represented
  by the configured property/depth/statistic cross product. Report the complete missing set before
  publishing any SoilGrids object.
- `PreparedObject.selection` includes `basin_level`, DEM tile ID, or SoilGrids property/depth/
  statistic as appropriate.

- [x] **Step 6: Run focused and existing adapter tests**

Run: `pytest tests/unit/catalog/test_catalog.py tests/unit/orchestration/landing tests/contract/static/sources/test_soilgrids_adapter.py tests/integration/static/test_existing_inventory.py -q`

Expected: PASS, including all unchanged existing adapter behavior.

- [x] **Step 7: Commit source preparation**

```bash
git add src/flashflood_data/catalog/repository.py src/flashflood_data/orchestration/landing/bundle.py src/flashflood_data/orchestration/landing/sources.py tests/unit/catalog/test_catalog.py tests/unit/orchestration/landing/test_bundle.py tests/unit/orchestration/landing/test_sources.py
git commit -m "feat: prepare canonical L12 source objects"
```

### Task 4: Iceberg `meta.source_objects` inventory

**Files:**
- Create: `src/flashflood_data/storage/iceberg.py`
- Modify: `src/flashflood_data/storage/__init__.py`
- Create: `tests/unit/storage/test_iceberg_source_objects.py`

**Interfaces:**
- Consumes: `LakehouseSettings` and batches of `SourceObjectRow`.
- Produces: `load_polaris_catalog(settings) -> Catalog` and `SourceObjectInventory(catalog, table_identifier=("meta", "source_objects")).register_many(rows) -> RegisteredBatch`.

- [x] **Step 1: Write failing schema, batch, reuse, and conflict tests**

```python
def test_register_many_appends_one_batch_and_returns_snapshot(rows):
    table = FakeTable(existing=[])
    inventory = SourceObjectInventory(FakeCatalog(table), ("meta", "source_objects"))
    result = inventory.register_many(rows)
    assert table.appended.num_rows == len(rows)
    assert result.object_ids == [row.object_id for row in rows]
    assert result.snapshot_id == 42


def test_register_many_reuses_matching_identity_and_rejects_conflict(row):
    matching = FakeTable(existing=[row.model_dump(mode="python")])
    assert SourceObjectInventory(FakeCatalog(matching)).register_many([row]).reused == 1
    conflicting = matching.with_checksum(row.object_id, "different")
    with pytest.raises(SourceObjectConflict):
        SourceObjectInventory(FakeCatalog(conflicting)).register_many([row])
```

Assert that the created schema uses required strings for IDs/URIs/checksum, `int64` for size,
nullable `int32` for basin level, UTC timestamps, and nullable strings for provider times and JSON.

- [x] **Step 2: Run the inventory test and verify the missing repository**

Run: `pytest tests/unit/storage/test_iceberg_source_objects.py -q`

Expected: FAIL because `storage.iceberg` does not exist.

- [x] **Step 3: Implement Polaris loading and table creation**

```python
catalog = load_catalog(
    settings.polaris_catalog,
    type="rest",
    uri=settings.polaris_uri,
    warehouse=settings.polaris_catalog,
    credential=(
        f"{settings.polaris_client_id.get_secret_value()}:"
        f"{settings.polaris_client_secret.get_secret_value()}"
    ),
    scope="PRINCIPAL_ROLE:ALL",
)
catalog.create_namespace_if_not_exists(("meta",))
table = catalog.create_table_if_not_exists(
    ("meta", "source_objects"),
    schema=source_objects_arrow_schema(),
    properties={"format-version": "2", "write.format.default": "parquet"},
)
```

Keep the table identifier injectable so the smoke test can use an isolated namespace.

- [x] **Step 4: Implement idempotent batch registration**

Collect requested object IDs, scan only `object_id`, `checksum`, `object_uri`, and `manifest_uri`
using `EqualTo` for a single row or `In` for a batch, compare every existing identity, and append
all missing rows in one `pyarrow.Table`. Refresh after append and return
`table.current_snapshot().snapshot_id`.

Retry optimistic commit failures a bounded three times. Each retry refreshes the table and repeats
the identity check before appending. Never treat a conflicting checksum or URI as reuse.

- [x] **Step 5: Run focused tests and lint**

Run: `pytest tests/unit/storage/test_iceberg_source_objects.py -q && ruff check src/flashflood_data/storage tests/unit/storage/test_iceberg_source_objects.py`

Expected: PASS.

- [x] **Step 6: Commit the Iceberg inventory**

```bash
git add src/flashflood_data/storage/iceberg.py src/flashflood_data/storage/__init__.py tests/unit/storage/test_iceberg_source_objects.py
git commit -m "feat: register raw objects in Iceberg"
```

### Task 5: Landing service, manifests, recovery, and partial failure

**Files:**
- Create: `src/flashflood_data/orchestration/landing/service.py`
- Modify: `src/flashflood_data/orchestration/landing/__init__.py`
- Create: `tests/unit/orchestration/landing/test_service.py`

**Interfaces:**
- Consumes: source policies, prepared objects, `ObjectPublisher`, and `SourceObjectInventory`.
- Produces: `StaticSourceLandingService.prepare_run`, `publish_source`, `register_batch`, `cleanup_batch`, and `run`.

- [x] **Step 1: Write failing lifecycle and recovery tests**

```python
def test_service_publishes_manifest_before_registering_and_cleans_after_commit(service):
    batch = service.publish_source("hydrobasins_v1c", run_id="run-1")
    assert service.store.exists(batch.objects[0].object_key)
    assert service.store.exists(batch.objects[0].manifest_key)
    assert service.staging.exists()
    registered = service.register_batch(batch)
    service.cleanup_batch(registered)
    assert registered.snapshot_id == 42
    assert not service.staging.exists()


def test_run_continues_independent_source_and_reports_partial_failure(service):
    service.fail_source("basinatlas_v10", code="fixture_failure")
    summary = service.run(["hydrobasins_v1c", "basinatlas_v10"], run_id="run-1")
    assert summary.status == "partial_failure"
    assert summary.completed_sources == ["hydrobasins_v1c"]
    assert summary.failed_sources == ["basinatlas_v10"]
    assert summary.errors == {"basinatlas_v10": "fixture_failure"}
```

Add recovery cases for object-only, object-plus-manifest, and already-registered states. Assert
that exception messages and secret values never appear in `LandingRunSummary`.

- [x] **Step 2: Run service tests and verify missing implementation**

Run: `pytest tests/unit/orchestration/landing/test_service.py -q`

Expected: FAIL because `StaticSourceLandingService` does not exist.

- [x] **Step 3: Implement deterministic key and identity construction**

Use:

```text
static/<source_id>/<source_version>/<selection-segments>/<asset_id>/<filename>
```

Sort selection keys and render only validated scalar values. Build `object_id` as SHA-256 over a
canonical JSON document containing `source_id`, `source_version`, `asset_id`, selection, content
checksum, and manifest schema version.

- [x] **Step 4: Implement immutable manifests**

Build `LandingManifest` from the prepared and published object. Include source URI after passing
it through `storage.http.redaction.redact`, member checksums, source/archive checksum when known,
selection, retrieval time, request fingerprint, license, and `object_status="verified"`.
Serialize with sorted keys and compact separators, calculate its SHA-256, and publish it through
the same staging-and-conflict path as other objects. Write the JSON first to
`<staging_root>/<run_id>/<source_id>/manifests/<object_id>.json`; this path is included in the
run-scoped cleanup list and is never passed through XCom as bytes.

- [x] **Step 5: Implement split service methods and complete CLI wrapper**

```python
def publish_source(self, source_id: str, run_id: str) -> PublishedBatch: ...
def register_batch(self, batch: PublishedBatch) -> RegisteredBatch: ...
def cleanup_batch(self, batch: RegisteredBatch) -> None: ...
def run(self, source_ids: Sequence[str], run_id: str | None = None) -> LandingRunSummary: ...
```

`publish_source` acquires, validates, prepares, publishes payloads, and publishes manifests.
`register_batch` performs one Iceberg append per source. `cleanup_batch` verifies the snapshot ID
and removes only cleanup paths beneath `<staging_root>/<run_id>/<source_id>`.

`run` calls the three methods per source, continues after typed source failures, sorts source
lists, and reports `completed`, `partial_failure`, or `failed`. It records error codes such as
`source_validation_failed`, `object_conflict`, and `iceberg_commit_failed`, never raw exception
text.

- [x] **Step 6: Run all landing unit tests and architecture checks**

Run: `pytest tests/unit/orchestration/landing tests/unit/storage tests/contract/test_architecture.py -q`

Expected: PASS.

- [x] **Step 7: Commit the application service**

```bash
git add src/flashflood_data/orchestration/landing tests/unit/orchestration/landing/test_service.py
git commit -m "feat: orchestrate recoverable source landing"
```

### Task 6: `land-static` CLI command

**Files:**
- Modify: `src/flashflood_data/cli/app.py`
- Modify: `src/flashflood_data/cli/commands/static.py`
- Modify: `src/flashflood_data/cli/__init__.py`
- Modify: `tests/unit/test_cli.py`

**Interfaces:**
- Consumes: `StaticSourceLandingService.run(source_ids, run_id)`.
- Produces: `flashflood-data land-static [--source ID] [--run-id ID] [--json-summary]`.

- [x] **Step 1: Write failing CLI routing and exit-code tests**

```python
def test_land_static_routes_sources_and_run_id(monkeypatch, tmp_path):
    RecordingLandingService.calls = []
    monkeypatch.setattr(CLI_MODULE, "build_static_landing_service", RecordingLandingService)
    result = CliRunner().invoke(app, [
        "land-static", "--root", str(tmp_path),
        "--source", "hydrobasins_v1c", "--run-id", "manual-1", "--json-summary",
    ])
    assert result.exit_code == 0
    assert RecordingLandingService.calls == [(["hydrobasins_v1c"], "manual-1")]
    assert json.loads(result.stdout)["status"] == "completed"


@pytest.mark.parametrize("status", ["partial_failure", "failed"])
def test_land_static_returns_nonzero_for_incomplete_run(monkeypatch, tmp_path, status):
    monkeypatch.setattr(CLI_MODULE, "build_static_landing_service", service_returning(status))
    result = CliRunner().invoke(app, ["land-static", "--root", str(tmp_path)])
    assert result.exit_code == 1
```

- [x] **Step 2: Run the CLI tests and verify the command is absent**

Run: `pytest tests/unit/test_cli.py -q`

Expected: FAIL because `land-static` is not listed or routed.

- [x] **Step 3: Add the service factory and CLI command**

The factory loads `LakehouseSettings`, `config/landing/static.yaml`, existing source specs, local
`ProjectPaths`, `AssetCatalog`, `HttpFetcher`, `PyArrowS3ObjectStore`, and the Polaris inventory.
The command emits exactly one sorted JSON summary. Configuration failures exit 2; completed exits
0; partial or failed exits 1.

Do not alter `fetch`, `validate`, `run-static`, or existing profile behavior.

- [x] **Step 4: Run CLI and static regression tests**

Run: `pytest tests/unit/test_cli.py tests/unit/static/test_pipeline.py tests/integration/static/test_static_pipeline_idempotence.py -q`

Expected: PASS.

- [x] **Step 5: Commit the CLI**

```bash
git add src/flashflood_data/cli tests/unit/test_cli.py
git commit -m "feat: expose static source landing CLI"
```

### Task 7: Airflow runtime, staging, pool, and DAG

**Files:**
- Create: `airflow/dags/static_source_landing.py`
- Modify: `infra/docker/airflow/Dockerfile`
- Modify: `compose.yaml`
- Modify: `tools/bootstrap/init_lakehouse_env.sh`
- Modify: `tests/unit/test_lakehouse_compose.py`
- Modify: `tests/unit/test_lakehouse_env.py`
- Create: `tests/contract/infra/test_static_source_landing_dag.py`

**Interfaces:**
- Consumes: split methods on `StaticSourceLandingService` and JSON-safe batch models.
- Produces: paused manual DAG `static_source_landing`, fixed source TaskGroups, and Airflow pool `source_landing_writer` with one slot.

- [ ] **Step 1: Write failing Compose, image, bootstrap, and DAG contract tests**

```python
def test_airflow_has_project_data_config_and_staging_mounts():
    volumes = services()["airflow-scheduler"]["volumes"]
    assert "./dataset:/opt/flashflood/dataset:Z" in volumes
    assert "./config:/opt/flashflood/config:ro,Z" in volumes
    assert "${LAKEHOUSE_DATA_ROOT:-./dataset/lakehouse}/staging:/opt/airflow/staging:Z" in volumes


def test_static_source_landing_dag_is_thin_and_source_isolated():
    text = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(text)
    assert "static_source_landing" in text
    assert "source_landing_writer" in text
    assert {"hydrobasins_v1c", "basinatlas_v10", "hydrorivers_v10",
            "cop_dem_glo30_2024_1", "soilgrids_2_0"} <= string_literals(tree)
    assert not ({"geopandas", "rasterio", "pyiceberg", "pyarrow"} & imported_roots(tree))
```

Also assert the Dockerfile installs the local `flashflood-data` package and the initializer creates
`lakehouse/staging` without printing secrets.

- [ ] **Step 2: Run infra tests and verify the missing mounts and DAG**

Run: `pytest tests/unit/test_lakehouse_compose.py tests/unit/test_lakehouse_env.py tests/contract/infra/test_static_source_landing_dag.py -q`

Expected: FAIL on the new expectations.

- [ ] **Step 3: Install application code in the Airflow image**

After installing `requirements/lakehouse.txt`, copy `pyproject.toml` and `src/` into
`/opt/flashflood-build` and run:

```dockerfile
RUN python -m pip install --no-cache-dir \
      --constraint "${HOME}/constraints.txt" \
      /opt/flashflood-build \
    && python -m pip check \
    && python -c 'import flashflood_data, geopandas, pyarrow, pyiceberg, rasterio'
```

This installs only dependencies already declared by the project and keeps the Airflow constraints
active. Do not copy `dataset/`, `.env`, or research documents into the image.

- [ ] **Step 4: Add runtime mounts, endpoints, credentials, and the one-slot pool**

Extend the Airflow common environment with:

```yaml
FLASHFLOOD_PROJECT_ROOT: /opt/flashflood
FLASHFLOOD_STAGING_ROOT: /opt/airflow/staging
MINIO_ENDPOINT: http://minio:9000
MINIO_ROOT_USER: ${MINIO_ROOT_USER:?run make lakehouse-init}
MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:?run make lakehouse-init}
AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER:?run make lakehouse-init}
AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD:?run make lakehouse-init}
AWS_REGION: us-east-1
```

Add the three tested mounts. In the `airflow-init` service command, after database migration and
user creation, run
`airflow pools set source_landing_writer 1 "Serializes local catalog and Iceberg writers"`.
Create the staging directory in `init_lakehouse_env.sh`; that host script must not invoke Airflow.

- [ ] **Step 5: Implement the thin Airflow DAG**

Use `airflow.sdk.dag`, `task`, and `task_group`. Configure `schedule=None`, `catchup=False`,
`max_active_runs=1`, paused creation, two retries, and fixed source groups loaded from the landing
configuration.

Each TaskGroup has three tasks:

```python
published = publish_source.override(pool="source_landing_writer")(source_id, run_id)
registered = register_batch.override(pool="source_landing_writer")(published)
result = cleanup_batch.override(pool="source_landing_writer")(registered)
```

All three use the one-slot pool until the local Parquet catalog gains inter-process locking.
Each wrapper accepts and returns a JSON representation of `LandingTaskEnvelope`. A failed envelope
is passed through unchanged by downstream wrappers; a typed exception becomes a failed envelope
with a sanitized error code. Wrappers do not raise per-source exceptions, so every TaskGroup can
finish independently. A final task with `trigger_rule="all_done"` validates every envelope, writes
the combined summary, and raises `AirflowException` when the application status is partial or
failed, making the incomplete DAG run visible.

- [ ] **Step 6: Run infra tests and Compose validation**

Run: `pytest tests/unit/test_lakehouse_compose.py tests/unit/test_lakehouse_env.py tests/contract/infra/test_static_source_landing_dag.py -q && docker compose config --quiet`

Expected: PASS.

- [ ] **Step 7: Commit Airflow integration**

```bash
git add airflow/dags/static_source_landing.py infra/docker/airflow/Dockerfile compose.yaml tools/bootstrap/init_lakehouse_env.sh tests/unit/test_lakehouse_compose.py tests/unit/test_lakehouse_env.py tests/contract/infra/test_static_source_landing_dag.py
git commit -m "feat: orchestrate source landing in Airflow"
```

### Task 8: End-to-end smoke, operator commands, and documentation

**Files:**
- Create: `tools/smoke/source_landing.sh`
- Create: `tests/integration/lakehouse/test_source_landing_smoke.py`
- Modify: `Makefile`
- Modify: `README.md`

**Interfaces:**
- Consumes: running MinIO, Polaris, and Airflow services.
- Produces: `make lakehouse-source-landing-smoke` and documented CLI/DAG recovery workflow.

- [ ] **Step 1: Write failing shell/Make contract test**

In `test_source_landing_smoke.py`, invoke `make --dry-run lakehouse-source-landing-smoke` and
assert it resolves to `tools/smoke/source_landing.sh`. Read the shell script and assert it uses an
isolated `smoke_<run-id>` namespace, a `_smoke/` raw prefix, and exact cleanup calls for both.

- [ ] **Step 2: Run the smoke contract and verify the target is absent**

Run: `pytest tests/integration/lakehouse/test_source_landing_smoke.py -q`

Expected: FAIL because the script and Make target do not exist.

- [ ] **Step 3: Implement the live round-trip smoke script**

Following the existing `tools/smoke/python_runtime.sh` pattern, pipe a Python program into the
Airflow scheduler container. The program must:

1. create a small temporary source file containing a random run ID;
2. publish it under `raw/_smoke/<run-id>/fixture.bin`;
3. publish its manifest;
4. create `smoke_<run-id>.source_objects` through the injectable inventory identifier;
5. register one row and assert the snapshot contains the object ID;
6. rerun publication and registration and assert reuse with no second row;
7. delete the exact smoke table, namespace, object, manifest, and local temporary file in a
   `finally` block.

The shell script prints only phase names and success state. It must not print environment values,
credential-bearing catalog configuration, or complete source URIs with query strings.

- [ ] **Step 4: Add Make target and focused documentation**

Add `lakehouse-source-landing-smoke` to `.PHONY`. Document:

```bash
make lakehouse-up
make lakehouse-source-landing-smoke
docker compose exec airflow-api-server airflow dags unpause static_source_landing
```

Explain that MinIO stores source bytes, `meta.source_objects` inventories them, and `bronze.*`
parsing is a later pipeline. Document object layout, partial-failure retry, and the five initial
source groups. Preserve unrelated user-authored README sections during conflict resolution.

- [ ] **Step 5: Run final verification**

Run:

```bash
pytest -q
ruff check .
docker compose config --quiet
make lakehouse-up
make lakehouse-smoke
make lakehouse-python-smoke
make lakehouse-source-landing-smoke
docker compose exec -T airflow-api-server airflow dags list --output json
```

Expected:

- all tests and Ruff pass;
- Compose validates;
- PostgreSQL, MinIO, Polaris, and Airflow health checks pass;
- Python/PyIceberg runtime checks pass in host and Airflow;
- source-landing smoke publishes, registers, reuses, and cleans its isolated fixture;
- DAG list contains `static_source_landing` and contains no example DAG.

- [ ] **Step 6: Review the final diff for scope and secrets**

Run:

```bash
git diff --check
git diff --stat
git grep -nE '(fixture-password|fixture-secret|MINIO_ROOT_PASSWORD=.+|POLARIS_CLIENT_SECRET=.+)' -- ':!docs/superpowers/plans/*'
```

Expected: no whitespace errors, no generated data files, and no populated secret assignment.

- [ ] **Step 7: Commit the verified feature**

```bash
git add tools/smoke/source_landing.sh tests/integration/lakehouse/test_source_landing_smoke.py Makefile README.md
git commit -m "docs: verify static source landing workflow"
```

## Deferred Follow-On Plan

Create a separate implementation plan from sections 8-11 of the approved spec for:

- parsing L12 vectors into `bronze.*` Iceberg tables;
- bootstrapping the three AOI polygons when no prior harmonized AOI exists;
- changing harmonization from L10 to configuration-driven L12;
- long-form SoilGrids features and quantile uncertainty;
- conditioned DEM flow direction/accumulation, outlets, longest flow path, HAND, TWI, and `Tc`;
- normalized BasinATLAS fields and the current wide feature view;
- `model.routing_parameter` FAST/CENTRAL/SLOW rows.

That plan begins only after this source-landing plan has a passing MinIO/Polaris round trip.
