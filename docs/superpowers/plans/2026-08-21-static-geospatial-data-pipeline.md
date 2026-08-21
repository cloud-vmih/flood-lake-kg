# Sơn La Static Geospatial Data Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a reproducible, file-first pipeline that inventories the existing lake, acquires every missing static source for the approved Sơn La AOIs, preserves raw files, produces L10 static features and spatial mappings, and publishes machine-readable QA plus an interactive map.

**Architecture:** A Python 3.11 CLI drives manifest-backed source adapters through inventory, AOI construction, acquisition, validation, harmonization, derivation, and QA stages. Raw inputs are immutable and registered in an atomic Parquet catalog; downstream GeoParquet, Parquet, and COG outputs carry dependency fingerprints so an unchanged second run is a no-op. Source adapters share download and validation primitives, while administration, hydrology, rasters, OSM, feature builders, and QA remain focused modules.

**Tech Stack:** Python 3.11, Typer, Pydantic v2, HTTPX, pandas/GeoPandas/Pyogrio/Shapely/PyProj, Rasterio/rasterstats/NumPy, PyArrow/GeoParquet, openpyxl, Jinja2, MapLibre GL JS, pytest/respx, Ruff, pip-tools.

**Spec:** `docs/superpowers/specs/2026-08-21-static-geospatial-data-pipeline-design.md`

## Global Constraints

- Run on Linux from `/home/cloud/cloud/TLCN/Project`; use `/home/cloud/.pyenv/shims/python3.11`, not the system Python 3.14.
- Use HydroBASINS level 10 as the primary hazard unit and retain validated level 9 and level 8 parents.
- Core AOI is the post-2025 Sơn La boundary; Hydrological AOI adds one direct upstream L10 hop; Environmental Download AOI adds a 10 km metric buffer; Exposure AOI adds 10 km around Core and clips to Vietnam.
- Store harmonized geometries in `EPSG:4326`; perform metric calculations in `EPSG:32648`.
- Preserve original raw payloads, checksums, source URI/version/license/retrieval time, and never repair raw geometry.
- Register existing files in place; do not move or duplicate the legacy `dataset/` tree.
- Keep newly acquired static raw data at or below the 8 GiB soft cap and retain at least 10 GiB free before starting an acquisition batch.
- Read Copernicus credentials only from environment variables or an ignored `.env`; never log or commit credentials.
- Keep DEM, SoilGrids, WorldCover, and WorldPop on their native meaningful grids; use nearest-neighbour for categorical WorldCover and population-preserving rules for WorldPop.
- A source or QA failure must stop the affected dependency chain; never fabricate values, silently switch sources, or force ambiguous administrative matches.
- `run-static` must not invoke cleanup; deletion requires a resolved dry-run list and separate explicit user approval.
- This milestone is file-first: do not add MinIO, Iceberg, PostGIS, Spark/Sedona, Airflow, Neo4j, an API, or a production dashboard.

---

## File and Responsibility Map

```text
pyproject.toml                         package metadata, runtime/dev dependencies, CLI entry point
requirements.lock                     fully resolved Python 3.11 dependency lock
Makefile                              repeatable setup, test, lint, smoke, and preflight commands
.env.example                          names of CDSE settings, never real secrets
README.md                             operator workflow and output inventory
config/study_area.yaml                AOIs, CRS, storage, QA, and budget thresholds
config/features.yaml                  BasinATLAS fields, soil depth bands, WorldCover classes
config/osmconf.ini                    GDAL OSM tags exposed as columns
config/sources/*.yaml                 versioned declarations grouped by source family
src/flashflood_data/cli.py            Typer commands and process exit codes only
src/flashflood_data/config.py         validated YAML and environment settings
src/flashflood_data/paths.py          all project/dataset paths
src/flashflood_data/models.py         shared enums and Pydantic records
src/flashflood_data/catalog.py        atomic assets/runs Parquet catalog and legal transitions
src/flashflood_data/io_atomic.py      checksums, immutable targets, quarantine, atomic writes
src/flashflood_data/budget.py         transfer/temporary/free-space preflight
src/flashflood_data/registry.py       load source declarations and construct adapters
src/flashflood_data/http.py           retrying streamed downloads with safe resume
src/flashflood_data/inventory.py      register and deduplicate legacy assets in place
src/flashflood_data/aoi.py            Core/Hydrological/Environmental/Exposure AOIs
src/flashflood_data/vector.py         vector schema, CRS, geometry, clip, and GeoParquet helpers
src/flashflood_data/raster.py         raster validation, clip/mosaic, COG, and coverage helpers
src/flashflood_data/pipeline.py       stage graph, runs, fingerprints, skip/rebuild decisions
src/flashflood_data/cleanup.py        report-only cleanup candidate resolver
src/flashflood_data/sources/base.py   SourceAdapter contract and SourceContext
src/flashflood_data/sources/existing.py legacy inventory rules
src/flashflood_data/sources/admin.py  official 2025 map, legal evidence, GADM historical geometry
src/flashflood_data/sources/soilgrids.py ISRIC WCS coverage discovery/download
src/flashflood_data/sources/cop_dem.py CDSE OData search/token/download
src/flashflood_data/sources/worldcover.py ESA tile-index selection/download
src/flashflood_data/sources/osm.py    Geofabrik snapshot and tagged layer extraction
src/flashflood_data/harmonize/hydro.py HydroBASINS hierarchy, topology, BasinATLAS, HydroRIVERS
src/flashflood_data/harmonize/exposure.py WorldPop, OSM, and historical-event normalization
src/flashflood_data/derive/terrain.py DEM metric warp, slope, relief, zonal summaries
src/flashflood_data/derive/soil.py     depth-weighted soil and uncertainty summaries
src/flashflood_data/derive/landcover.py categorical class fractions
src/flashflood_data/derive/hydrology.py river length, density, gradient, baseline attributes
src/flashflood_data/derive/population.py Core-AOI population evidence
src/flashflood_data/derive/mappings.py all basin/entity and temporal crosswalk tables
src/flashflood_data/derive/profile.py  one-row-per-L10 static feature assembly
src/flashflood_data/qa/checks.py       reusable data-quality gates
src/flashflood_data/qa/report.py       JSON/Parquet/HTML QA reports
src/flashflood_data/qa/map.py          MapLibre QA bundle and raster previews
src/flashflood_data/qa/templates/*     deterministic report and map templates
tests/unit/*                           pure unit tests
tests/contract/*                       adapter contract tests with local payload fixtures
tests/integration/*                    miniature complete pipeline and idempotence tests
tests/fixtures/*                       tiny generated vector/raster/table/HTTP fixtures
```

The adapter modules share the catalog, downloader, and harmonizers but do not call each other. The pipeline owns ordering. Derived modules accept paths or data frames and return explicit data frames/paths, which keeps them testable without the network.

### Task 1: Establish the Python 3.11 package, validated configuration, paths, and CLI shell

**Files:**
- Create: `pyproject.toml`
- Create: `Makefile`
- Create: `.env.example`
- Create: `config/study_area.yaml`
- Create: `src/flashflood_data/__init__.py`
- Create: `src/flashflood_data/config.py`
- Create: `src/flashflood_data/paths.py`
- Create: `src/flashflood_data/cli.py`
- Test: `tests/unit/test_config.py`
- Test: `tests/unit/test_cli.py`

**Interfaces:**
- Produces: `ProjectPaths.discover(root: Path | None = None) -> ProjectPaths`
- Produces: `load_study_area(path: Path) -> StudyAreaConfig`
- Produces: `EnvironmentSettings` with `cdse_username: SecretStr | None` and `cdse_password: SecretStr | None`
- Produces: Typer application `flashflood_data.cli:app`

- [ ] **Step 1: Write failing configuration and CLI tests**

```python
# tests/unit/test_config.py
from pathlib import Path
from flashflood_data.config import load_study_area
from flashflood_data.paths import ProjectPaths

def test_study_area_has_approved_scopes(tmp_path: Path) -> None:
    cfg = tmp_path / "study.yaml"
    cfg.write_text(
        "province_origin_code: '14'\nhydrobasins_level: 10\nupstream_hops: 1\n"
        "raster_buffer_km: 10\nexposure_buffer_km: 10\n"
        "processing_crs: EPSG:32648\nstorage_crs: EPSG:4326\n"
        "new_raw_soft_cap_gib: 8\nminimum_free_gib: 10\n",
        encoding="utf-8",
    )
    result = load_study_area(cfg)
    assert result.hydrobasins_level == 10
    assert result.processing_crs == "EPSG:32648"

def test_paths_never_escape_root(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    assert paths.raw == tmp_path / "dataset" / "raw"
    assert paths.catalog == tmp_path / "dataset" / "catalog"
```

```python
# tests/unit/test_cli.py
from typer.testing import CliRunner
from flashflood_data.cli import app

def test_cli_lists_static_stages() -> None:
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in ("inventory", "fetch", "validate", "harmonize", "derive", "map", "run-static", "cleanup"):
        assert command in result.stdout
```

- [ ] **Step 2: Run the tests and confirm the package is absent**

Run: `/home/cloud/.pyenv/shims/python3.11 -m pytest tests/unit/test_config.py tests/unit/test_cli.py -q`

Expected: FAIL during collection with `ModuleNotFoundError: No module named 'flashflood_data'`.

- [ ] **Step 3: Add package metadata and lockable dependencies**

```toml
# pyproject.toml
[build-system]
requires = ["setuptools>=75", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "flashflood-data"
version = "0.1.0"
requires-python = ">=3.11,<3.13"
dependencies = [
  "typer>=0.12,<1", "pydantic>=2.8,<3", "pydantic-settings>=2.4,<3",
  "PyYAML>=6,<7", "httpx>=0.27,<1", "rich>=13,<15",
  "numpy>=1.26,<3", "pandas>=2.2,<3", "pyarrow>=17,<22",
  "geopandas>=1.0,<2", "pyogrio>=0.9,<1", "shapely>=2.0,<3", "pyproj>=3.6,<4",
  "rasterio>=1.3,<2", "rasterstats>=0.20,<1", "openpyxl>=3.1,<4",
  "jinja2>=3.1,<4", "pillow>=10,<13"
]

[project.optional-dependencies]
dev = ["pytest>=8,<10", "pytest-cov>=5,<8", "respx>=0.21,<1", "ruff>=0.6,<1", "pip-tools>=7.4,<8"]

[project.scripts]
flashflood-data = "flashflood_data.cli:app"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "--strict-markers"

[tool.ruff]
line-length = 100
target-version = "py311"
```

Run:

```bash
/home/cloud/.pyenv/shims/python3.11 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install -e '.[dev]'
.venv/bin/pip-compile --extra dev --output-file requirements.lock pyproject.toml
```

Expected: `requirements.lock` is created and `.venv/bin/python --version` starts with `Python 3.11`.

- [ ] **Step 4: Implement the approved configuration and paths**

```python
# src/flashflood_data/config.py
from pathlib import Path
import yaml
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class StudyAreaConfig(BaseModel):
    province_origin_code: str = "14"
    hydrobasins_level: int = Field(default=10, ge=1, le=12)
    upstream_hops: int = Field(default=1, ge=0, le=3)
    raster_buffer_km: float = Field(default=10, gt=0)
    exposure_buffer_km: float = Field(default=10, gt=0)
    processing_crs: str = "EPSG:32648"
    storage_crs: str = "EPSG:4326"
    new_raw_soft_cap_gib: float = 8
    minimum_free_gib: float = 10
    admin_expected_count: int = 75
    admin_expected_communes: int = 67
    admin_expected_wards: int = 8
    admin_gap_overlap_max_pct: float = 0.1
    legal_area_diff_max_pct: float = 2.0
    commune_basin_coverage_min_pct: float = 99.5
    commune_basin_coverage_max_pct: float = 100.5
    environmental_raster_coverage_min_pct: float = 99.0
    admin_area_exceptions: list[str] = Field(default_factory=list)

class EnvironmentSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="FLASHFLOOD_", extra="ignore")
    cdse_username: SecretStr | None = None
    cdse_password: SecretStr | None = None

def load_study_area(path: Path) -> StudyAreaConfig:
    return StudyAreaConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
```

```python
# src/flashflood_data/paths.py
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    dataset: Path
    raw: Path
    harmonized: Path
    derived: Path
    catalog: Path
    qa: Path

    @classmethod
    def discover(cls, root: Path | None = None) -> "ProjectPaths":
        resolved = (root or Path.cwd()).resolve()
        dataset = resolved / "dataset"
        return cls(resolved, dataset, dataset / "raw", dataset / "harmonized",
                   dataset / "derived", dataset / "catalog", dataset / "qa")

    def ensure_output_dirs(self) -> None:
        for path in (self.raw, self.harmonized, self.derived, self.catalog, self.qa):
            path.mkdir(parents=True, exist_ok=True)
```

Write `config/study_area.yaml` with every value from `StudyAreaConfig` and `.env.example` with `FLASHFLOOD_CDSE_USERNAME=` and `FLASHFLOOD_CDSE_PASSWORD=`. Register the eight Typer command names listed in the spec; during this scaffold task each accepts `--root PATH` and calls this explicit availability guard, which later component tasks replace command by command:

```python
def stage_unavailable(stage: str) -> NoReturn:
    typer.echo(json.dumps({"stage": stage, "status": "unavailable"}), err=True)
    raise typer.Exit(code=2)
```

- [ ] **Step 5: Run tests, lint, and commit**

Run: `.venv/bin/pytest tests/unit/test_config.py tests/unit/test_cli.py -q`

Expected: `2 passed`.

Run: `.venv/bin/ruff check src tests`

Expected: `All checks passed!`

```bash
git add pyproject.toml requirements.lock Makefile .env.example config/study_area.yaml src tests/unit/test_config.py tests/unit/test_cli.py
git commit -m "build: scaffold static data pipeline"
```

### Task 2: Add immutable asset models, atomic catalog state, checksums, and storage preflight

**Files:**
- Create: `src/flashflood_data/models.py`
- Create: `src/flashflood_data/catalog.py`
- Create: `src/flashflood_data/io_atomic.py`
- Create: `src/flashflood_data/budget.py`
- Test: `tests/unit/test_catalog.py`
- Test: `tests/unit/test_io_atomic.py`
- Test: `tests/unit/test_budget.py`
- Create: `tests/conftest.py`

**Interfaces:**
- Consumes: `ProjectPaths` and `StudyAreaConfig` from Task 1
- Produces: `AssetStatus`, `AssetKind`, `AssetRecord`, `RunRecord`, `RemoteAsset`, `SourceSpec`, `SourceFile`, `ValidationResult`
- Produces: `AssetCatalog(paths: ProjectPaths)` with `upsert`, `get`, `transition`, `find_reusable`, `begin_run`, and `end_run`
- Produces: `sha256_file(path: Path) -> str`, `sha256_bundle(paths: Sequence[Path]) -> str`, `atomic_target(final_path: Path) -> Iterator[Path]`
- Produces: `StorageBudget.preflight(new_bytes: int, temporary_bytes: int) -> BudgetDecision`

- [ ] **Step 1: Write failing state, atomicity, and budget tests**

```python
def test_catalog_rejects_illegal_transition(catalog, raw_asset) -> None:
    catalog.upsert(raw_asset)
    with pytest.raises(IllegalTransition):
        catalog.transition(raw_asset.asset_id, AssetStatus.HARMONIZED)

def test_atomic_target_does_not_publish_failed_write(tmp_path: Path) -> None:
    target = tmp_path / "asset.bin"
    with pytest.raises(RuntimeError):
        with atomic_target(target) as partial:
            partial.write_bytes(b"broken")
            raise RuntimeError("stop")
    assert not target.exists()
    assert not target.with_suffix(".bin.partial").exists()

def test_budget_rejects_soft_cap_even_with_free_disk(fake_disk_usage) -> None:
    budget = StorageBudget(Path("/data"), soft_cap_bytes=8 * 2**30, minimum_free_bytes=10 * 2**30,
                           existing_new_raw_bytes=7 * 2**30)
    decision = budget.preflight(new_bytes=2 * 2**30, temporary_bytes=0)
    assert not decision.allowed
    assert decision.reason == "new_raw_soft_cap"
```

```python
# tests/conftest.py
def make_test_asset(path: Path, *, status: AssetStatus) -> AssetRecord:
    return AssetRecord(
        asset_id="fixture-asset", source_id="fixture-source", source_version="1",
        kind=AssetKind.RAW, source_uri="https://example.invalid/raw.bin",
        storage_path=str(path), media_type="application/octet-stream",
        size_bytes=path.stat().st_size, checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
        license_id="fixture-license", pipeline_run_id="fixture-run", status=status,
    )

@pytest.fixture
def project_paths(tmp_path: Path) -> ProjectPaths:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    return paths

@pytest.fixture
def catalog(project_paths: ProjectPaths) -> AssetCatalog:
    return AssetCatalog(project_paths)

@pytest.fixture
def raw_asset(tmp_path: Path) -> AssetRecord:
    payload = tmp_path / "raw.bin"
    payload.write_bytes(b"fixture")
    return make_test_asset(payload, status=AssetStatus.DISCOVERED)
```

The budget test's `fake_disk_usage` fixture monkeypatches `shutil.disk_usage` to 40 GiB total, 10 GiB used, and 30 GiB free.

- [ ] **Step 2: Run the focused tests and confirm missing interfaces**

Run: `.venv/bin/pytest tests/unit/test_catalog.py tests/unit/test_io_atomic.py tests/unit/test_budget.py -q`

Expected: FAIL during collection because `flashflood_data.catalog` does not exist.

- [ ] **Step 3: Define shared records and legal states**

```python
# src/flashflood_data/models.py
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from pydantic import BaseModel, Field

class AssetStatus(StrEnum):
    DISCOVERED = "discovered"
    FETCHING = "fetching"
    FETCHED = "fetched"
    VALIDATED = "validated"
    HARMONIZED = "harmonized"
    DERIVED = "derived"
    FAILED = "failed"
    STALE = "stale"
    QUARANTINED = "quarantined"

class AssetKind(StrEnum):
    RAW = "raw"
    HARMONIZED = "harmonized"
    DERIVED = "derived"
    QA = "qa"

class AssetRecord(BaseModel):
    asset_id: str
    source_id: str
    source_version: str
    kind: AssetKind
    source_uri: str
    storage_path: str
    media_type: str
    size_bytes: int = Field(ge=0)
    checksum_algorithm: str = "sha256"
    checksum: str
    retrieved_at: datetime
    source_valid_time: str | None = None
    license_id: str
    bbox_wgs84_json: str | None = None
    crs: str | None = None
    resolution_json: str | None = None
    pipeline_run_id: str
    status: AssetStatus
    dependency_fingerprint: str | None = None
    duplicate_of_asset_id: str | None = None
    metadata_json: str = "{}"
    error_code: str | None = None
    error_message: str | None = None

class RunRecord(BaseModel):
    run_id: str
    command: str
    started_at: datetime
    ended_at: datetime | None = None
    status: str = "running"
    config_fingerprint: str

class RemoteAsset(BaseModel):
    asset_id: str
    source_id: str
    source_version: str
    uri: str
    target_relative_path: Path
    media_type: str
    license_id: str
    expected_size: int | None = None
    expected_checksum: str | None = None
    source_valid_time: str | None = None
    request_method: str = "GET"
    request_form: dict[str, str] = Field(default_factory=dict)

class SourceSpec(BaseModel):
    source_id: str
    adapter: str
    version: str
    license_id: str
    enabled: bool = True
    settings: dict[str, object] = Field(default_factory=dict)

class SourceFile(BaseModel):
    sources: list[SourceSpec]

class ValidationResult(BaseModel):
    passed: bool
    checks: dict[str, bool]
    metrics: dict[str, float | int | str] = Field(default_factory=dict)
    messages: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: Implement catalog, checksums, atomic targets, and budget decision**

```python
# core transition table in catalog.py
LEGAL_TRANSITIONS = {
    AssetStatus.DISCOVERED: {AssetStatus.FETCHING, AssetStatus.VALIDATED, AssetStatus.FAILED},
    AssetStatus.FETCHING: {AssetStatus.FETCHED, AssetStatus.FAILED, AssetStatus.QUARANTINED},
    AssetStatus.FETCHED: {AssetStatus.VALIDATED, AssetStatus.FAILED, AssetStatus.QUARANTINED},
    AssetStatus.VALIDATED: {AssetStatus.HARMONIZED, AssetStatus.DERIVED, AssetStatus.STALE},
    AssetStatus.HARMONIZED: {AssetStatus.DERIVED, AssetStatus.STALE},
    AssetStatus.DERIVED: {AssetStatus.STALE},
    AssetStatus.FAILED: {AssetStatus.FETCHING},
    AssetStatus.STALE: {AssetStatus.FETCHING, AssetStatus.HARMONIZED, AssetStatus.DERIVED},
    AssetStatus.QUARANTINED: set(),
}

class IllegalTransition(ValueError):
    pass

def transition(self, asset_id: str, target: AssetStatus, **updates: object) -> AssetRecord:
    record = self.get(asset_id)
    if target not in LEGAL_TRANSITIONS[record.status]:
        raise IllegalTransition(f"{record.status} -> {target}")
    changed = record.model_copy(update={"status": target, **updates})
    self.upsert(changed)
    return changed
```

`AssetCatalog` stores `assets.parquet` and `runs.parquet` under `dataset/catalog/`. Every mutation reads the current table, replaces the row by key, writes through `atomic_target`, then renames. `find_reusable(source_id, source_version, dependency_fingerprint)` returns only `VALIDATED`, `HARMONIZED`, or `DERIVED` assets whose file still exists and checksum still matches.

```python
# src/flashflood_data/budget.py
@dataclass(frozen=True)
class BudgetDecision:
    allowed: bool
    reason: str
    projected_new_raw_bytes: int
    projected_free_bytes: int
```

```python
# src/flashflood_data/io_atomic.py
@contextmanager
def atomic_target(final_path: Path) -> Iterator[Path]:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    partial = final_path.with_name(final_path.name + ".partial")
    partial.unlink(missing_ok=True)
    try:
        yield partial
        partial.replace(final_path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
```

`StorageBudget.preflight` compares `(existing_new_raw_bytes + new_bytes)` with the 8 GiB cap and compares free bytes after `new_bytes + temporary_bytes` with the 10 GiB reserve. Return a frozen `BudgetDecision(allowed, reason, projected_new_raw_bytes, projected_free_bytes)`; never begin a fetch when `allowed` is false.

- [ ] **Step 5: Run the focused tests and commit**

Run: `.venv/bin/pytest tests/unit/test_catalog.py tests/unit/test_io_atomic.py tests/unit/test_budget.py -q`

Expected: all tests pass, including catalog round-trip of empty and non-empty Parquet files.

```bash
git add src/flashflood_data/models.py src/flashflood_data/catalog.py src/flashflood_data/io_atomic.py src/flashflood_data/budget.py tests/conftest.py tests/unit/test_catalog.py tests/unit/test_io_atomic.py tests/unit/test_budget.py
git commit -m "feat: add asset catalog and storage guards"
```

### Task 3: Build the source registry, adapter contract, and safe HTTP fetcher

**Files:**
- Create: `config/sources/existing.yaml`
- Create: `src/flashflood_data/registry.py`
- Create: `src/flashflood_data/http.py`
- Create: `src/flashflood_data/sources/__init__.py`
- Create: `src/flashflood_data/sources/base.py`
- Test: `tests/unit/test_registry.py`
- Test: `tests/contract/test_http_fetcher.py`

**Interfaces:**
- Consumes: `SourceSpec`, `RemoteAsset`, `AssetRecord`, `ValidationResult`, `AssetCatalog`, `StorageBudget`, `ProjectPaths`
- Produces: `SourceContext(paths, catalog, study_area, environment, run_id)`
- Produces: abstract `SourceAdapter.resolve(context, available) -> list[RemoteAsset]`, `validate_raw(path) -> ValidationResult`, and `harmonize(context, assets) -> list[AssetRecord]`
- Produces: `load_source_specs(config_dir: Path) -> dict[str, SourceSpec]`
- Produces: `HttpFetcher.fetch(remote: RemoteAsset, run_id: str) -> AssetRecord`

- [ ] **Step 1: Write failing registry and streamed-download contract tests**

```python
def test_registry_rejects_duplicate_source_id(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text("sources:\n  - source_id: x\n    adapter: existing\n    version: '1'\n    license_id: x\n")
    (tmp_path / "b.yaml").write_text("sources:\n  - source_id: x\n    adapter: existing\n    version: '2'\n    license_id: x\n")
    with pytest.raises(ValueError, match="duplicate source_id: x"):
        load_source_specs(tmp_path)

@respx.mock
def test_fetcher_publishes_only_verified_payload(fetcher, remote_asset, tmp_path: Path) -> None:
    payload = b"valid-payload"
    respx.get(remote_asset.uri).mock(return_value=httpx.Response(200, content=payload))
    remote_asset.expected_checksum = hashlib.sha256(payload).hexdigest()
    record = fetcher.fetch(remote_asset, "run-1")
    assert Path(record.storage_path).read_bytes() == payload
    assert not Path(record.storage_path + ".partial").exists()
    assert record.status is AssetStatus.FETCHED
```

- [ ] **Step 2: Run the tests and confirm the registry/fetcher are missing**

Run: `.venv/bin/pytest tests/unit/test_registry.py tests/contract/test_http_fetcher.py -q`

Expected: FAIL during collection for missing modules.

- [ ] **Step 3: Implement the adapter boundary and registry validation**

```python
# src/flashflood_data/sources/base.py
@dataclass(frozen=True)
class SourceContext:
    paths: ProjectPaths
    catalog: AssetCatalog
    study_area: StudyAreaConfig
    environment: EnvironmentSettings
    run_id: str

class SourceAdapter(ABC):
    def __init__(self, spec: SourceSpec) -> None:
        self.spec = spec

    @abstractmethod
    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        raise NotImplementedError

    @abstractmethod
    def validate_raw(self, path: Path) -> ValidationResult:
        raise NotImplementedError

    @abstractmethod
    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        raise NotImplementedError
```

`load_source_specs` loads every `*.yaml` in sorted order, validates each document as `SourceFile`, flattens its `sources`, rejects duplicate IDs across files, and returns only enabled records unless `include_disabled=True` is passed. This lets `existing.yaml` declare HydroBASINS, BasinATLAS, HydroRIVERS, WorldPop, and historical evidence without mixing their catalog identities. `build_adapter(spec)` imports only from this fixed lazy map and never from an arbitrary YAML module path: `existing→sources.existing.ExistingAdapter`, `admin_current→sources.admin.CurrentAdminAdapter`, `gadm_admin→sources.admin.GadmAdminAdapter`, `soilgrids→sources.soilgrids.SoilGridsAdapter`, `cop_dem→sources.cop_dem.CopDemAdapter`, `worldcover→sources.worldcover.WorldCoverAdapter`, and `geofabrik_osm→sources.osm.GeofabrikOsmAdapter`.

- [ ] **Step 4: Implement bounded retry, resume, validation, quarantine, and redaction**

```python
# essential fetch loop in src/flashflood_data/http.py
for attempt in range(1, self.max_attempts + 1):
    try:
        headers = {"Range": f"bytes={partial.stat().st_size}-"} if partial.exists() else {}
        with self.client.stream(remote.request_method, remote.uri, headers=headers,
                                data=remote.request_form or None, follow_redirects=True) as response:
            response.raise_for_status()
            mode = "ab" if response.status_code == 206 and partial.exists() else "wb"
            with partial.open(mode) as handle:
                for chunk in response.iter_bytes(1024 * 1024):
                    handle.write(chunk)
        self._verify_size_and_checksum(partial, remote)
        partial.replace(final_path)
        return self._record(remote, final_path, run_id)
    except (httpx.TimeoutException, httpx.NetworkError, RetryableStatus) as exc:
        if attempt == self.max_attempts:
            raise DownloadFailed(remote.asset_id) from exc
        self.sleep(min(2 ** (attempt - 1), 16))
```

Resume only when the server returns `206` and a compatible `Content-Range`; otherwise restart the partial file. Retry only timeouts, network failures, `429`, and `5xx`, honoring `Retry-After` up to 60 seconds. A checksum or payload mismatch moves the partial into `dataset/raw/_quarantine/<asset_id>/`, records `QUARANTINED`, and raises. Logger filters replace CDSE username, password, bearer tokens, and signed query strings with `[REDACTED]`.

- [ ] **Step 5: Declare the existing sources and pass contract tests**

```yaml
# config/sources/existing.yaml
sources:
  - source_id: hydrobasins_v1c
    adapter: existing
    version: "1c"
    license_id: HydroSHEDS-free-academic
    settings: {inventory_source_id: hydrobasins_v1c}
  - source_id: basinatlas_v10
    adapter: existing
    version: "10"
    license_id: HydroATLAS-free-academic
    settings: {inventory_source_id: basinatlas_v10}
  - source_id: hydrorivers_v10
    adapter: existing
    version: "10"
    license_id: HydroSHEDS-free-academic
    settings: {inventory_source_id: hydrorivers_v10}
  - source_id: worldpop_vnm_2025
    adapter: existing
    version: R2025A-v1
    license_id: CC-BY-4.0
    settings: {inventory_source_id: worldpop_vnm_2025}
  - source_id: historical_flood_evidence_2020_2026
    adapter: existing
    version: "2026-08-14"
    license_id: project-evidence-compilation
    settings: {inventory_source_id: historical_flood_evidence_2020_2026}
```

All missing external source IDs and endpoints are added by their adapter tasks after the exact source contract is implemented. Endpoint URLs and license URLs belong in YAML, not Python.

Run: `.venv/bin/pytest tests/unit/test_registry.py tests/contract/test_http_fetcher.py -q`

Expected: all tests pass, including retry, non-resumable restart, checksum quarantine, and secret-redaction cases.

```bash
git add config/sources src/flashflood_data/registry.py src/flashflood_data/http.py src/flashflood_data/sources tests/unit/test_registry.py tests/contract/test_http_fetcher.py
git commit -m "feat: add source adapters and safe downloader"
```

### Task 4: Inventory and deduplicate the existing data lake without moving files

**Files:**
- Create: `src/flashflood_data/inventory.py`
- Create: `src/flashflood_data/sources/existing.py`
- Modify: `src/flashflood_data/cli.py`
- Test: `tests/unit/test_inventory.py`
- Test: `tests/integration/test_existing_inventory.py`

**Interfaces:**
- Consumes: `ProjectPaths`, `AssetCatalog`, `AssetRecord`, `sha256_file`, `sha256_bundle`
- Produces: `InventoryRule(source_id, version, kind, glob, media_type, bundle_suffixes)`
- Produces: `inventory_existing(context: SourceContext) -> list[AssetRecord]`
- Produces: CLI `flashflood-data inventory --root PATH [--rehash]`

- [ ] **Step 1: Write a failing compound-shapefile and duplicate test**

```python
def test_inventory_hashes_shapefile_as_bundle_and_marks_duplicate(fixture_lake, context) -> None:
    first = fixture_lake / "a" / "hybas_as_lev10_v1c.shp"
    second = fixture_lake / "copy" / "hybas_as_lev10_v1c.shp"
    clone_shapefile_bundle(first, second)
    records = inventory_existing(context)
    hydro = [r for r in records if r.source_id == "hydrobasins_v1c"]
    assert len(hydro) == 2
    assert hydro[0].checksum == hydro[1].checksum
    assert sum(r.duplicate_of_asset_id is not None for r in hydro) == 1
    assert first.exists() and second.exists()
```

- [ ] **Step 2: Run the tests and confirm inventory is missing**

Run: `.venv/bin/pytest tests/unit/test_inventory.py tests/integration/test_existing_inventory.py -q`

Expected: FAIL because `inventory_existing` is not defined.

- [ ] **Step 3: Implement explicit legacy rules and exclusions**

```python
@dataclass(frozen=True)
class InventoryRule:
    source_id: str
    version: str
    kind: AssetKind
    glob: str
    media_type: str
    bundle_suffixes: tuple[str, ...]

DEFAULT_RULES = (
    InventoryRule("hydrobasins_v1c", "1c", AssetKind.RAW, "hybas_as_lev01-12_v1c/hybas_as_lev??_v1c.shp", "application/x-esri-shapefile", (".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".shp.xml")),
    InventoryRule("hydrobasins_lake_sample_v1c", "1c", AssetKind.RAW, "Data/**/hybas_lake_as_lev08_v1c.shp", "application/x-esri-shapefile", (".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".shp.xml")),
    InventoryRule("basinatlas_v10", "10", AssetKind.RAW, "BasinATLAS_Data_v10_shp/BasinATLAS_v10_shp/BasinATLAS_v10_lev??.shp", "application/x-esri-shapefile", (".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx")),
    InventoryRule("basinatlas_archive_v10", "10", AssetKind.RAW, "BasinATLAS_Data_v10_shp.zip", "application/zip", (".zip",)),
    InventoryRule("hydrorivers_v10", "10", AssetKind.RAW, "HydroRIVERS_v10_as_shp/**/HydroRIVERS_v10_as.shp", "application/x-esri-shapefile", (".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx")),
    InventoryRule("worldpop_vnm_2025", "R2025A-v1", AssetKind.RAW, "Data/**/vnm_pop_2025_CN_100m_R2025A_v1.tif", "image/tiff", (".tif",)),
    InventoryRule("historical_flood_evidence_2020_2026", "2026-08-14", AssetKind.RAW, "Lu_Son_La_2020_2026.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", (".xlsx",)),
    InventoryRule("legacy_population_sample", "unversioned", AssetKind.HARMONIZED, "Data/hybas_vnm_with_pop.shp", "application/x-esri-shapefile", (".shp", ".shx", ".dbf", ".prj", ".cpg")),
    InventoryRule("legacy_population_sample", "unversioned", AssetKind.DERIVED, "Data/pop_by_basin_vnm.csv", "text/csv", (".csv",)),
    InventoryRule("legacy_population_sample_script", "unversioned", AssetKind.RAW, "Data/main.py", "text/x-python", (".py",)),
)
```

Sort matches by relative path, calculate a compound checksum from `(relative suffix, file checksum)` pairs, and choose the lexicographically first matching storage path as canonical for equal content. Exclude `dataset/raw`, `harmonized`, `derived`, `catalog`, `qa`, every `.partial`, and `dataset/Data/venv`. Preserve all duplicate files; record only `duplicate_of_asset_id`.

- [ ] **Step 4: Wire inventory CLI and create a deterministic report**

The command opens a run, registers records as `DISCOVERED`, validates readable known formats, transitions readable legacy assets directly to `VALIDATED`, and atomically writes `dataset/catalog/inventory.json` with counts and bytes by source. `--rehash` bypasses matching `(path, size, mtime_ns)` cache metadata; the default skips rehashing unchanged files.

Run: `.venv/bin/pytest tests/unit/test_inventory.py tests/integration/test_existing_inventory.py -q`

Expected: all tests pass and the fixture files retain their original paths and mtimes.

- [ ] **Step 5: Run the real read-only inventory, inspect, and commit code only**

Run: `.venv/bin/flashflood-data inventory --root /home/cloud/cloud/TLCN/Project`

Expected: report includes 12 standard HydroBASINS levels, lake-L8 sample bundles, 12 BasinATLAS levels plus archive, one HydroRIVERS bundle, at least one WorldPop raster, the sample script/outputs, and one 30-row historical workbook; no legacy path is moved.

```bash
git add src/flashflood_data/inventory.py src/flashflood_data/sources/existing.py src/flashflood_data/cli.py tests/unit/test_inventory.py tests/integration/test_existing_inventory.py
git commit -m "feat: inventory existing geospatial assets"
```

### Task 5: Acquire and validate the 75 current Sơn La administrative units and Core AOI

**Files:**
- Create: `config/sources/admin.yaml`
- Create: `src/flashflood_data/sources/admin.py`
- Create: `src/flashflood_data/vector.py`
- Create: `tests/fixtures/admin/unit_index.json`
- Create: `tests/fixtures/admin/unit_03664.geojson`
- Create: `tests/fixtures/admin/resolution_page.html`
- Test: `tests/contract/test_current_admin_adapter.py`
- Test: `tests/unit/test_vector.py`

**Interfaces:**
- Consumes: `SourceAdapter`, `SourceContext`, `RemoteAsset`, `ValidationResult`, `atomic_target`
- Produces: `CurrentAdminAdapter.resolve(context, available) -> list[RemoteAsset]`
- Produces: `normalize_current_admin(index_path: Path, geometry_paths: Sequence[Path]) -> GeoDataFrame`
- Produces: `validate_vector(gdf, required_columns, expected_crs) -> ValidationResult`
- Produces: `write_geoparquet(gdf, path, storage_crs="EPSG:4326") -> Path`
- Produces: `dataset/harmonized/admin/admin_commune_2025.geoparquet` and `dataset/harmonized/aoi/core_aoi.geoparquet`

- [ ] **Step 1: Write failing two-pass discovery and normalization tests**

```python
@respx.mock
def test_current_admin_discovers_geometry_after_index_is_available(adapter, context, index_asset) -> None:
    first = adapter.resolve(context, [])
    assert {item.asset_id for item in first} >= {"sonla-admin-2025-index", "resolution-1681-page"}
    second = adapter.resolve(context, [index_asset])
    assert second[0].request_method == "POST"
    assert second[0].request_form == {"id": "diaphanhanhchinhcapxa_2025.1338"}

def test_normalized_current_admin_keeps_legal_and_lookup_fields(admin_fixtures) -> None:
    result = normalize_current_admin(admin_fixtures.index, [admin_fixtures.geometry])
    row = result.iloc[0]
    assert row.current_commune_code == "03664"
    assert row.current_commune_name == "Phường Chiềng An"
    assert row.predecessors_text == "Phường Chiềng An, Xã Chiềng Xôm, Xã Chiềng Đen"
    assert str(result.crs) == "EPSG:4326"
```

- [ ] **Step 2: Run the tests and confirm the admin adapter is missing**

Run: `.venv/bin/pytest tests/contract/test_current_admin_adapter.py tests/unit/test_vector.py -q`

Expected: FAIL because `CurrentAdminAdapter` and vector helpers are undefined.

- [ ] **Step 3: Declare and implement exact official-evidence requests**

```yaml
# config/sources/admin.yaml
sources:
  - source_id: sonla_admin_2025
    adapter: admin_current
    version: "2025-07-01"
    license_id: public-administrative-reference
    settings:
      index_url: https://sapnhap.bando.com.vn/p.co_dvhc
      index_form: {ma: "0"}
      geometry_url: https://sapnhap.bando.com.vn/pread_json
      province_origin_code: "14"
      resolution_page: https://congbao.chinhphu.vn/van-ban/nghi-quyet-so-1681-nq-ubtvqh15-45137.htm
```

The first resolve pass returns POST index and legal-page assets. The second reads the exact saved index JSON, filters `magoc == "14"`, and emits one POST geometry asset per `malk`. A third pass parses the saved legal page and emits the linked Resolution 1681 PDF asset. Asset IDs and target paths are deterministic: `sonla-admin-2025-unit-<ma>` and `dataset/raw/admin/sonla_2025/units/<ma>.geojson`. If an already fetched response changes, catalog it as a new checksum/version and stop; never overwrite the earlier payload.

- [ ] **Step 4: Normalize geometry without modifying raw payloads and enforce the legal counts**

```python
CURRENT_FIELD_MAP = {
    "a02_xa": "current_commune_code",
    "a03_ten": "current_commune_name",
    "a04_tentinh": "province_name",
    "a05_truocsn": "predecessors_text",
    "a06_trungtamhc": "admin_center",
    "a07_dt": "legal_area_km2",
    "a08_ds": "legal_population",
}

def classify_unit(name: str) -> str:
    if name.startswith("Phường "):
        return "ward"
    if name.startswith("Xã "):
        return "commune"
    raise ValueError(f"unexpected current unit type: {name}")
```

Concatenate exactly one feature from every response, map fields above, add `lookup_id`, `valid_from=2025-07-01`, `valid_to=null`, raw asset IDs, and geometry-repair flags. Reject anything other than 75 unique codes, 67 communes, and 8 wards. Compare area in `EPSG:32648` to `legal_area_km2`; keep per-unit difference and fail over 2% unless the code is named in an explicit `admin_area_exceptions` list in study config. Dissolve the validated geometries to one Core AOI and report aggregate interior gap/overlap; fail over 0.1%.

- [ ] **Step 5: Run adapter tests, then the live admin-only fetch**

Run: `.venv/bin/pytest tests/contract/test_current_admin_adapter.py tests/unit/test_vector.py -q`

Expected: all fixture tests pass, including invalid geometry repair reporting and 74/76-unit rejection.

Run: `.venv/bin/flashflood-data fetch --root /home/cloud/cloud/TLCN/Project --source sonla_admin_2025`

Expected: 1 index JSON, 75 unit GeoJSON files, the resolution page, and its PDF are immutable under `dataset/raw/admin/`; no credential prompt is needed.

Run: `.venv/bin/flashflood-data harmonize --root /home/cloud/cloud/TLCN/Project --source sonla_admin_2025`

Expected: the current layer contains exactly `75` features and Core AOI contains exactly `1` dissolved geometry.

- [ ] **Step 6: Commit code, config, and small fixtures only**

```bash
git add config/sources/admin.yaml src/flashflood_data/sources/admin.py src/flashflood_data/vector.py tests/fixtures/admin tests/contract/test_current_admin_adapter.py tests/unit/test_vector.py
git commit -m "feat: acquire current Son La administration"
```

### Task 6: Acquire pre-reform geometry and build the old-to-current temporal crosswalk

**Files:**
- Create: `config/sources/historical_admin.yaml`
- Modify: `src/flashflood_data/sources/admin.py`
- Create: `src/flashflood_data/derive/__init__.py`
- Create: `src/flashflood_data/derive/mappings.py`
- Create: `tests/fixtures/admin/gadm_old_communes.geojson`
- Test: `tests/contract/test_historical_admin_adapter.py`
- Test: `tests/unit/test_admin_crosswalk.py`

**Interfaces:**
- Consumes: validated current admin GeoParquet and its `predecessors_text`; GADM 4.1 Vietnam archive
- Produces: `GadmAdminAdapter`
- Produces: `normalize_admin_name(value: str) -> str`
- Produces: `parse_predecessors(value: str) -> list[ParsedAdminName]`
- Produces: `build_admin_crosswalk(current: GeoDataFrame, historical: GeoDataFrame) -> DataFrame`
- Produces: `dataset/harmonized/admin/admin_commune_historical.geoparquet`
- Produces: `dataset/derived/mappings/admin_commune_crosswalk.parquet`

- [ ] **Step 1: Write failing deterministic and ambiguous-match tests**

```python
def test_crosswalk_maps_full_predecessor_by_name_and_overlap(current_admin, historical_admin) -> None:
    result = build_admin_crosswalk(current_admin, historical_admin)
    old = result.loc[result.old_admin_name == "Chiềng Xôm"].iloc[0]
    assert old.current_commune_code == "03664"
    assert old.relationship_type == "merged"
    assert old.match_status == "matched"
    assert old.valid_to == date(2025, 6, 30)

def test_crosswalk_never_forces_duplicate_name(duplicate_name_current, duplicate_name_old) -> None:
    result = build_admin_crosswalk(duplicate_name_current, duplicate_name_old)
    assert set(result.match_status) == {"ambiguous"}
    assert result.current_commune_code.isna().all()
```

- [ ] **Step 2: Run tests and confirm historical handling is absent**

Run: `.venv/bin/pytest tests/contract/test_historical_admin_adapter.py tests/unit/test_admin_crosswalk.py -q`

Expected: FAIL because `GadmAdminAdapter` and `build_admin_crosswalk` do not exist.

- [ ] **Step 3: Download the GADM archive and read it without mutating/extracting raw**

```yaml
# config/sources/historical_admin.yaml
sources:
  - source_id: gadm_vnm_4_1
    adapter: gadm_admin
    version: "4.1"
    license_id: gadm-license
    settings:
      archive_url: https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_VNM_shp.zip
      license_url: https://gadm.org/license.html
      province_name: Sơn La
      historical_valid_to: "2025-06-30"
```

Preserve the ZIP under `dataset/raw/admin/gadm/4.1/`. Read `gadm41_VNM_3.shp` through GDAL `/vsizip/` or a Pyogrio ZIP URI built from the archive path, filter normalized `NAME_1 == "Sơn La"`, and emit a historical GeoParquet with `old_admin_id=GID_3`, old province/district/commune names, source version, `valid_from=null`, and `valid_to=2025-06-30`. Read ADM0 from the same ZIP to produce `dataset/harmonized/admin/vietnam_boundary.geoparquet` for Exposure AOI clipping.

- [ ] **Step 4: Implement name parsing plus spatial evidence and explicit match states**

```python
@dataclass(frozen=True)
class ParsedAdminName:
    written_name: str
    admin_type: str
    normalized_name: str

ADMIN_PREFIXES = ("xã ", "phường ", "thị trấn ")

def normalize_admin_name(value: str) -> str:
    text = unicodedata.normalize("NFC", value).strip().casefold()
    for prefix in ADMIN_PREFIXES:
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    return " ".join(text.split())
```

Split official `predecessors_text` on commas, retain the written type, and generate candidates by normalized name. Resolve only one candidate whose old geometry overlaps the new geometry and whose overlap fraction is at least `0.95`; unchanged same-name units use `relationship_type="unchanged"`, otherwise use `"merged"`. Zero candidates become `unresolved`; multiple viable candidates become `ambiguous`. Persist candidate IDs and overlap metrics as JSON even when unresolved. Because the verified Sơn La source has no partial-merger phrase, fail on `"một phần"` instead of guessing a split fraction.

- [ ] **Step 5: Run tests and live harmonization**

Run: `.venv/bin/pytest tests/contract/test_historical_admin_adapter.py tests/unit/test_admin_crosswalk.py -q`

Expected: all tests pass; ambiguous and unresolved rows retain null `current_commune_code`.

Run: `.venv/bin/flashflood-data fetch --root /home/cloud/cloud/TLCN/Project --source gadm_vnm_4_1`

Run: `.venv/bin/flashflood-data harmonize --root /home/cloud/cloud/TLCN/Project --source gadm_vnm_4_1`

Expected: historical and Vietnam boundary GeoParquet files exist; crosswalk row count equals the number of parsed predecessor names and every row has one of `matched`, `ambiguous`, or `unresolved`.

- [ ] **Step 6: Commit**

```bash
git add config/sources/historical_admin.yaml src/flashflood_data/sources/admin.py src/flashflood_data/derive/__init__.py src/flashflood_data/derive/mappings.py tests/fixtures/admin/gadm_old_communes.geojson tests/contract/test_historical_admin_adapter.py tests/unit/test_admin_crosswalk.py
git commit -m "feat: build temporal commune crosswalk"
```

### Task 7: Select L10 basins, direct upstream closure, L9/L8 parents, and all four AOIs

**Files:**
- Create: `src/flashflood_data/aoi.py`
- Create: `src/flashflood_data/harmonize/__init__.py`
- Create: `src/flashflood_data/harmonize/hydro.py`
- Create: `tests/fixtures/hydro/basins_l8.geojson`
- Create: `tests/fixtures/hydro/basins_l9.geojson`
- Create: `tests/fixtures/hydro/basins_l10.geojson`
- Test: `tests/unit/test_hydro_aoi.py`
- Test: `tests/integration/test_hydro_harmonize.py`

**Interfaces:**
- Consumes: Core AOI, Vietnam boundary, HydroBASINS levels 8/9/10, BasinATLAS L10, HydroRIVERS
- Produces: `select_l10_with_upstream(l10, core, hops=1) -> GeoDataFrame`
- Produces: `build_basin_hierarchy(l10, l9, l8) -> DataFrame`
- Produces: `build_study_areas(core, selected_l10, vietnam, config) -> StudyAreas`
- Produces: `harmonize_hydro(paths, study_areas) -> list[Path]`
- Produces: `dataset/harmonized/hydro/subbasin_l10.geoparquet`
- Produces: `dataset/harmonized/hydro/subbasin_hierarchy.parquet`
- Produces: `dataset/harmonized/hydro/basinatlas_l10.geoparquet`
- Produces: `dataset/harmonized/hydro/river_reach.geoparquet`
- Produces: Core, Hydrological, Environmental Download, and Exposure AOI GeoParquet files

- [ ] **Step 1: Write failing one-hop, hierarchy, and border-scope tests**

```python
def test_one_hop_adds_only_direct_upstream(core, l10_chain) -> None:
    selected = select_l10_with_upstream(l10_chain, core, hops=1)
    assert set(selected.HYBAS_ID) == {100, 101}  # 100 intersects; 101 NEXT_DOWN=100
    assert 102 not in set(selected.HYBAS_ID)     # 102 NEXT_DOWN=101 is two hops away

def test_parent_lookup_uses_geometry_and_crosschecks_pfaf(l10, l9, l8) -> None:
    hierarchy = build_basin_hierarchy(l10, l9, l8)
    assert hierarchy.loc[0, ["HYBAS_ID", "parent_l9_hybas_id", "parent_l8_hybas_id"]].tolist() == [100, 90, 80]

def test_environment_can_cross_border_but_exposure_cannot(study_areas) -> None:
    assert not study_areas.environmental.within(study_areas.vietnam)
    assert study_areas.exposure.difference(study_areas.vietnam).area == pytest.approx(0.0)
```

- [ ] **Step 2: Run tests and confirm selection functions are missing**

Run: `.venv/bin/pytest tests/unit/test_hydro_aoi.py tests/integration/test_hydro_harmonize.py -q`

Expected: FAIL because `flashflood_data.aoi` and hydro harmonization are absent.

- [ ] **Step 3: Implement topology selection and validated parents**

```python
def select_l10_with_upstream(l10: gpd.GeoDataFrame, core: BaseGeometry, hops: int = 1) -> gpd.GeoDataFrame:
    direct = set(l10.loc[l10.intersects(core), "HYBAS_ID"].astype(int))
    selected = set(direct)
    frontier = direct
    for _ in range(hops):
        upstream = set(l10.loc[l10["NEXT_DOWN"].astype(int).isin(frontier), "HYBAS_ID"].astype(int))
        selected |= upstream
        frontier = upstream
    return l10.loc[l10["HYBAS_ID"].astype(int).isin(selected)].copy()
```

For each L10 polygon, spatially join its point-on-surface to exactly one L9 and one L8 polygon. Validate that the child `PFAF_ID` string starts with each parent `PFAF_ID` string; fail on zero/multiple parents or prefix disagreement. This avoids assuming that numeric truncation alone is safe. Check that every non-zero `NEXT_DOWN` referencing a selected internal basin exists; external downstream exits are marked `scope_exit=true`, not reported as broken.

- [ ] **Step 4: Build four scope files and harmonize existing hydrology**

```python
@dataclass(frozen=True)
class StudyAreas:
    core: BaseGeometry
    hydrological: BaseGeometry
    environmental: BaseGeometry
    exposure: BaseGeometry
    vietnam: BaseGeometry
```

Project geometries to `EPSG:32648` before buffering. Environmental is `selected_l10.union_all().buffer(10_000)`. Exposure is `core.buffer(10_000).intersection(vietnam)`. Store each result back in `EPSG:4326` under `dataset/harmonized/aoi/`. Filter standard `hybas_as_lev10_v1c`, never the lake-customized L8 sample. Filter BasinATLAS L10 by selected `HYBAS_ID`; filter HydroRIVERS by the Hydrological AOI and clip line geometry at its boundary. Preserve all raw HydroATLAS fields in the harmonized L10 subset.

- [ ] **Step 5: Run tests and the real hydro stage**

Run: `.venv/bin/pytest tests/unit/test_hydro_aoi.py tests/integration/test_hydro_harmonize.py -q`

Expected: all tests pass.

Run: `.venv/bin/flashflood-data harmonize --root /home/cloud/cloud/TLCN/Project --source hydrobasins_v1c --source basinatlas_v10 --source hydrorivers_v10`

Expected: unique selected L10 IDs, exactly one L9/L8 parent per L10, and four AOI GeoParquet files. The command prints selected/intersecting/upstream counts and output checksums.

- [ ] **Step 6: Commit**

```bash
git add src/flashflood_data/aoi.py src/flashflood_data/harmonize tests/fixtures/hydro tests/unit/test_hydro_aoi.py tests/integration/test_hydro_harmonize.py
git commit -m "feat: construct level 10 hydrological study area"
```

### Task 8: Add reusable raster inspection, AOI clip/mosaic, COG, and coverage validation

**Files:**
- Create: `src/flashflood_data/raster.py`
- Create: `tests/fixtures/raster/make_fixtures.py`
- Test: `tests/unit/test_raster.py`
- Test: `tests/integration/test_raster_harmonize.py`

**Interfaces:**
- Consumes: Rasterio datasets, AOI geometry, `ValidationResult`, `atomic_target`
- Produces: `inspect_raster(path: Path) -> RasterMetadata`
- Produces: `validate_raster(path: Path, expected: RasterExpectation) -> ValidationResult`
- Produces: `mosaic_clip_to_cog(inputs, aoi, output, resampling, dst_crs=None) -> Path`
- Produces: `raster_coverage_ratio(path, aoi) -> float`

- [ ] **Step 1: Generate tiny rasters and write failing semantic tests**

```python
def test_categorical_clip_uses_nearest_and_retains_codes(class_tiles, aoi, tmp_path) -> None:
    output = mosaic_clip_to_cog(class_tiles, aoi, tmp_path / "classes.tif", Resampling.nearest)
    with rasterio.open(output) as src:
        assert set(np.unique(src.read(1))) <= {0, 10, 20, 30}
        assert src.profile["driver"] == "GTiff"
        assert src.overviews(1)

def test_coverage_counts_nodata_inside_aoi(partial_raster, aoi) -> None:
    assert raster_coverage_ratio(partial_raster, aoi) == pytest.approx(0.75)
```

- [ ] **Step 2: Run tests and confirm raster helpers are missing**

Run: `.venv/bin/pytest tests/unit/test_raster.py tests/integration/test_raster_harmonize.py -q`

Expected: FAIL because `flashflood_data.raster` does not exist.

- [ ] **Step 3: Implement metadata and structural validation**

```python
@dataclass(frozen=True)
class RasterMetadata:
    driver: str
    width: int
    height: int
    count: int
    dtype: str
    nodata: float | int | None
    crs: str
    bounds: tuple[float, float, float, float]
    transform: tuple[float, float, float, float, float, float]
    pixel_size: tuple[float, float]
    tiled: bool
    overviews: tuple[int, ...]
    size_bytes: int

@dataclass(frozen=True)
class RasterExpectation:
    dtypes: tuple[str, ...]
    crs: str | None
    resolution_range: tuple[float, float] | None
    aoi: BaseGeometry
```

`RasterMetadata` contains driver, width, height, band count, dtype, nodata, CRS, bounds, transform, pixel sizes, tiled flag, overview levels, and file size. `validate_raster` opens every band, rejects empty/HTML payloads disguised as TIFF, checks expected dtype/CRS/resolution ranges, checks nodata semantics, and verifies that bounds intersect the declared AOI. Return failed checks rather than changing the file.

- [ ] **Step 4: Implement COG output without forcing a common grid**

```python
COG_PROFILE = {
    "driver": "GTiff", "tiled": True, "blockxsize": 512, "blockysize": 512,
    "compress": "DEFLATE", "predictor": 2, "BIGTIFF": "IF_SAFER",
}
```

Use `rasterio.merge.merge` for same-grid source tiles, `rasterio.mask.mask` for AOI cropping, and `rasterio.warp.reproject` only when `dst_crs` is explicit. Build overviews `[2, 4, 8, 16]` up to the available size. Use `predictor=3` for floating rasters and `predictor=2` for integer rasters. Validate the completed temporary COG before atomic rename. `raster_coverage_ratio` rasterizes the AOI on the native grid and returns valid covered pixels divided by AOI pixels.

- [ ] **Step 5: Run tests and commit**

Run: `.venv/bin/pytest tests/unit/test_raster.py tests/integration/test_raster_harmonize.py -q`

Expected: all tests pass, continuous values remain within tolerance, categorical output contains no invented class, and a deliberately truncated TIFF fails validation.

```bash
git add src/flashflood_data/raster.py tests/fixtures/raster tests/unit/test_raster.py tests/integration/test_raster_harmonize.py
git commit -m "feat: add raster harmonization primitives"
```

### Task 9: Acquire SoilGrids mean and uncertainty for eight properties and six depths

**Files:**
- Create: `config/sources/soilgrids.yaml`
- Create: `src/flashflood_data/sources/soilgrids.py`
- Create: `tests/fixtures/soilgrids/wv0033_capabilities.xml`
- Create: `tests/fixtures/soilgrids/wv0033_describe.xml`
- Test: `tests/contract/test_soilgrids_adapter.py`
- Test: `tests/integration/test_soilgrids_harmonize.py`

**Interfaces:**
- Consumes: Environmental Download AOI, `SourceAdapter`, `HttpFetcher`, raster helpers
- Produces: `SoilGridsAdapter`
- Produces: `parse_coverages(capabilities_xml: bytes) -> set[str]`
- Produces: `build_wcs_getcoverage(property_id, depth, statistic, bbox) -> RemoteAsset`
- Produces: 96 immutable raw WCS GeoTIFF subsets and 96 harmonized COGs

- [ ] **Step 1: Write failing capability and exact-request tests**

```python
def test_capabilities_include_verified_wv0033_mean_and_uncertainty(fixture_xml) -> None:
    coverage_ids = parse_coverages(fixture_xml.read_bytes())
    assert "wv0033_0-5cm_mean" in coverage_ids
    assert "wv0033_100-200cm_uncertainty" in coverage_ids

def test_resolve_emits_eight_by_six_by_two_coverages(adapter, context, capabilities_assets) -> None:
    assets = adapter.resolve(context, capabilities_assets)
    coverage_assets = [a for a in assets if "GetCoverage" in a.uri]
    assert len(coverage_assets) == 8 * 6 * 2
    assert all(a.target_relative_path.suffix == ".tif" for a in coverage_assets)
```

- [ ] **Step 2: Run tests and confirm the SoilGrids adapter is absent**

Run: `.venv/bin/pytest tests/contract/test_soilgrids_adapter.py tests/integration/test_soilgrids_harmonize.py -q`

Expected: FAIL because `SoilGridsAdapter` is undefined.

- [ ] **Step 3: Declare exact products and discover rather than assume coverage IDs**

```yaml
sources:
  - source_id: soilgrids_2_0
    adapter: soilgrids
    version: "2.0"
    license_id: CC-BY-4.0
    settings:
      endpoint_template: https://maps.isric.org/mapserv?map=/map/{property}.map
      properties: [clay, sand, silt, bdod, cfvo, wv0010, wv0033, wv1500]
      depths: [0-5cm, 5-15cm, 15-30cm, 30-60cm, 60-100cm, 100-200cm]
      statistics: [mean, uncertainty]
      format: GEOTIFF_INT16
      output_crs: EPSG:4326
```

Fetch and preserve one `GetCapabilities` XML per property before requesting data. Require every configured `<property>_<depth>_<statistic>` coverage to be advertised. Parse `DescribeCoverage` axis labels and supported CRS, then construct `GetCoverage` with the Environmental AOI bounding box in an advertised CRS. Use WCS `2.0.1`, URL-encode repeated `SUBSET` parameters correctly, and never substitute the similar but wrong property name `wv003` for verified `wv0033`.

- [ ] **Step 4: Download AOI subsets and harmonize without combining semantics**

Write exact WCS responses to `dataset/raw/soilgrids/2.0/<property>/<depth>/<statistic>.tif`. Validate `int16`, coverage intersection, nodata, and the advertised grid. Clip each raw subset to the exact Environmental AOI and write `dataset/harmonized/soilgrids/<property>/<depth>/<statistic>.tif`. Do not rescale raw values during harmonization; store unit and scale-factor metadata from SoilGrids documentation in catalog metadata and apply scale only during feature derivation.

- [ ] **Step 5: Run tests and a real capability-only preflight**

Run: `.venv/bin/pytest tests/contract/test_soilgrids_adapter.py tests/integration/test_soilgrids_harmonize.py -q`

Expected: all tests pass, a fixture missing one uncertainty coverage is rejected, and no categorical resampling is used.

Run: `.venv/bin/flashflood-data fetch --root /home/cloud/cloud/TLCN/Project --source soilgrids_2_0 --resolve-only`

Expected: `96 coverages available`, an estimated byte count, and `budget: allowed`; no coverage TIFF is downloaded by `--resolve-only`.

- [ ] **Step 6: Commit**

```bash
git add config/sources/soilgrids.yaml src/flashflood_data/sources/soilgrids.py tests/fixtures/soilgrids tests/contract/test_soilgrids_adapter.py tests/integration/test_soilgrids_harmonize.py
git commit -m "feat: add SoilGrids AOI acquisition"
```

### Task 10: Acquire authenticated Copernicus DEM GLO-30 tiles through CDSE OData

**Files:**
- Create: `config/sources/cop_dem.yaml`
- Create: `src/flashflood_data/sources/cop_dem.py`
- Create: `tests/fixtures/cop_dem/search_n21_e103.json`
- Test: `tests/contract/test_cop_dem_adapter.py`
- Test: `tests/integration/test_cop_dem_harmonize.py`

**Interfaces:**
- Consumes: Environmental Download AOI, `EnvironmentSettings`, `HttpFetcher`, raster helpers
- Produces: `CopDemAdapter`
- Produces: `grid_ids_for_geometry(geometry) -> list[str]`
- Produces: `CdseTokenClient.get_access_token() -> SecretStr`
- Produces: `select_dem_product(response, dataset, grid_id) -> CdseProduct`
- Produces: `dataset/harmonized/rasters/dem_glo30.tif`

- [ ] **Step 1: Write failing grid, metadata-selection, and secret tests**

```python
def test_grid_ids_cover_each_intersecting_one_degree_cell(aoi_box) -> None:
    assert grid_ids_for_geometry(aoi_box) == ["N20_E103", "N20_E104", "N21_E103", "N21_E104"]

def test_product_selection_requires_fixed_dataset(search_fixture) -> None:
    product = select_dem_product(search_fixture, "COP-DEM_GLO-30-DGED/2024_1", "N21_E103")
    assert product.product_type == "SAR_DGE_30_A4AD"
    assert product.dataset == "COP-DEM_GLO-30-DGED/2024_1"

def test_missing_cdse_credentials_stops_before_download(context, spec) -> None:
    with pytest.raises(MissingCredentials, match="FLASHFLOOD_CDSE_USERNAME"):
        CopDemAdapter(spec).resolve(context, [])
```

- [ ] **Step 2: Run tests and confirm CDSE support is missing**

Run: `.venv/bin/pytest tests/contract/test_cop_dem_adapter.py tests/integration/test_cop_dem_harmonize.py -q`

Expected: FAIL because `flashflood_data.sources.cop_dem` does not exist.

- [ ] **Step 3: Implement exact dataset search and non-logging token exchange**

```python
class CdseProduct(BaseModel):
    product_id: str
    name: str
    grid_id: str
    dataset: str
    product_type: str
    modification_date: datetime
    content_length: int | None
    checksum: str | None
```

```yaml
sources:
  - source_id: cop_dem_glo30_2024_1
    adapter: cop_dem
    version: "2024_1"
    license_id: COP-DEM-30
    settings:
      token_url: https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token
      catalogue_url: https://catalogue.dataspace.copernicus.eu/odata/v1/Products
      download_template: https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value
      dataset: COP-DEM_GLO-30-DGED/2024_1
      product_type: SAR_DGE_30_A4AD
```

Generate one-degree `gridId` values from the Environmental AOI and query each with both `gridId` and exact `dataset` OData string-attribute filters plus `$expand=Attributes`. Reject zero results, mixed product types, wrong dataset, or multiple active results after ordering by `ModificationDate desc`. Request an OAuth token with `client_id=cdse-public`, username, password, and `grant_type=password`; keep the token in memory as `SecretStr`, refresh once on `401`, and redact request bodies/authorization headers.

- [ ] **Step 4: Preserve native products, extract DEM bands safely, and mosaic**

Download each product to `dataset/raw/cop_dem/2024_1/<grid_id>/<product_name>` through the generic fetcher. If the product is an archive, inspect members first, reject absolute or `..` paths, and locate exactly one file whose name ends `_DEM.tif`; do not publish an extracted raw member as a replacement for the archive. Validate each DEM tile, mosaic/clip to Environmental AOI with bilinear resampling only if reprojection occurs, and write `dataset/harmonized/rasters/dem_glo30.tif` as a float-capable COG. Require at least 99% Hydrological AOI coverage.

- [ ] **Step 5: Run tests and authenticated resolve-only preflight**

Run: `.venv/bin/pytest tests/contract/test_cop_dem_adapter.py tests/integration/test_cop_dem_harmonize.py -q`

Expected: all tests pass, ZIP-slip fixture is rejected, and secrets do not appear in captured logs.

After the user places credentials in ignored `.env`, run:

Run: `.venv/bin/flashflood-data fetch --root /home/cloud/cloud/TLCN/Project --source cop_dem_glo30_2024_1 --resolve-only`

Expected: a deterministic grid-ID/product list and budget estimate; `.env` remains untracked in `git status`.

- [ ] **Step 6: Commit**

```bash
git add config/sources/cop_dem.yaml src/flashflood_data/sources/cop_dem.py tests/fixtures/cop_dem tests/contract/test_cop_dem_adapter.py tests/integration/test_cop_dem_harmonize.py
git commit -m "feat: add Copernicus DEM acquisition"
```

### Task 11: Select and acquire only intersecting ESA WorldCover 2021 v200 tiles

**Files:**
- Create: `config/sources/worldcover.yaml`
- Create: `src/flashflood_data/sources/worldcover.py`
- Create: `tests/fixtures/worldcover/grid.geojson`
- Test: `tests/contract/test_worldcover_adapter.py`
- Test: `tests/integration/test_worldcover_harmonize.py`

**Interfaces:**
- Consumes: Environmental Download AOI, public AWS grid, `HttpFetcher`, raster helpers
- Produces: `WorldCoverAdapter`
- Produces: `select_worldcover_tiles(grid, aoi) -> list[WorldCoverTile]`
- Produces: `dataset/harmonized/rasters/worldcover_2021.tif`

- [ ] **Step 1: Write failing tile-selection and categorical tests**

```python
def test_only_intersecting_worldcover_tiles_are_resolved(grid_fixture, environmental_aoi) -> None:
    tiles = select_worldcover_tiles(grid_fixture, environmental_aoi)
    assert [tile.tile_id for tile in tiles] == sorted({"N18E102", "N21E102"})

def test_worldcover_rejects_unknown_class(harmonized_worldcover) -> None:
    with pytest.raises(ValueError, match="unknown WorldCover class 99"):
        validate_worldcover_classes(harmonized_worldcover)
```

- [ ] **Step 2: Run tests and confirm the adapter is absent**

Run: `.venv/bin/pytest tests/contract/test_worldcover_adapter.py tests/integration/test_worldcover_harmonize.py -q`

Expected: FAIL because `WorldCoverAdapter` is missing.

- [ ] **Step 3: Implement public grid and URL resolution**

```python
@dataclass(frozen=True)
class WorldCoverTile:
    tile_id: str
    uri: str
    bounds: tuple[float, float, float, float]
    content_length: int
```

```yaml
sources:
  - source_id: esa_worldcover_2021_v200
    adapter: worldcover
    version: "2021-v200"
    license_id: CC-BY-4.0
    settings:
      grid_url: https://esa-worldcover.s3.eu-central-1.amazonaws.com/v100/2020/esa_worldcover_2020_grid.geojson
      tile_template: https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/ESA_WorldCover_10m_2021_v200_{tile}_Map.tif
      valid_classes: [10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100]
      nodata: 0
```

Preserve the 5 MB official grid file as raw metadata. ESA's v200 product uses the same 3×3-degree tile IDs represented by this published v100 grid; intersect its `ll_tile` geometries with the Environmental AOI, sort IDs, and issue a HEAD request for each resolved v200 tile before the budget decision. Save COG tiles under `dataset/raw/worldcover/2021-v200/map/`; public access needs no AWS account or CLI.

- [ ] **Step 4: Mosaic with categorical semantics and validate coverage**

Validate each tile as byte COG in `EPSG:4326` with only configured classes plus nodata. Mosaic and exact-clip using `Resampling.nearest`; write `dataset/harmonized/rasters/worldcover_2021.tif`. Reject any invented code, and require at least 99% Hydrological AOI valid coverage while separately reporting nodata/water pixels.

- [ ] **Step 5: Run tests, resolve-only preflight, and commit**

Run: `.venv/bin/pytest tests/contract/test_worldcover_adapter.py tests/integration/test_worldcover_harmonize.py -q`

Expected: all tests pass.

Run: `.venv/bin/flashflood-data fetch --root /home/cloud/cloud/TLCN/Project --source esa_worldcover_2021_v200 --resolve-only`

Expected: a small sorted tile list and an exact `Content-Length` sum within the shared 8 GiB budget.

```bash
git add config/sources/worldcover.yaml src/flashflood_data/sources/worldcover.py tests/fixtures/worldcover tests/contract/test_worldcover_adapter.py tests/integration/test_worldcover_harmonize.py
git commit -m "feat: add WorldCover tile acquisition"
```

### Task 12: Preserve a timestamped Geofabrik Vietnam PBF and extract Exposure-AOI OSM layers

**Files:**
- Create: `config/sources/osm.yaml`
- Create: `config/osmconf.ini`
- Create: `src/flashflood_data/sources/osm.py`
- Create: `tests/fixtures/osm/points.geojson`
- Create: `tests/fixtures/osm/lines.geojson`
- Create: `tests/fixtures/osm/multipolygons.geojson`
- Test: `tests/contract/test_osm_adapter.py`
- Test: `tests/unit/test_osm_normalize.py`

**Interfaces:**
- Consumes: Exposure AOI, Geofabrik headers/MD5, Pyogrio GDAL OSM driver
- Produces: `GeofabrikOsmAdapter`
- Produces: `parse_geofabrik_metadata(headers, md5_text) -> SnapshotMetadata`
- Produces: `extract_osm_layers(pbf_path, exposure_aoi, osmconf_path) -> OSMOutputs`
- Produces: `dataset/harmonized/exposure/road_segment.geoparquet`
- Produces: `dataset/harmonized/exposure/bridge.geoparquet`
- Produces: `dataset/harmonized/exposure/facility.geoparquet`
- Produces: `dataset/harmonized/exposure/settlement.geoparquet`
- Produces: `dataset/harmonized/exposure/water_context.geoparquet`

- [ ] **Step 1: Write failing snapshot and stable-segment tests**

```python
def test_snapshot_target_uses_last_modified_date(headers, md5_text) -> None:
    metadata = parse_geofabrik_metadata(headers, md5_text)
    assert metadata.target_name == "vietnam-20260820.osm.pbf"
    assert metadata.md5 == "b1946ac92492d2347c6235b4d2611184"

def test_osm_segments_have_stable_source_identity(line_fixture) -> None:
    roads = normalize_roads(line_fixture)
    assert roads.segment_id.tolist() == ["way/42:000", "way/42:001"]
    assert roads.osm_id.tolist() == ["way/42", "way/42"]
```

- [ ] **Step 2: Run tests and confirm OSM support is missing**

Run: `.venv/bin/pytest tests/contract/test_osm_adapter.py tests/unit/test_osm_normalize.py -q`

Expected: FAIL because `GeofabrikOsmAdapter` is undefined.

- [ ] **Step 3: Resolve and preserve a dated PBF with upstream integrity evidence**

```python
@dataclass(frozen=True)
class SnapshotMetadata:
    source_valid_time: datetime
    target_name: str
    content_length: int
    md5: str

@dataclass(frozen=True)
class OSMOutputs:
    roads: Path
    bridges: Path
    facilities: Path
    settlements: Path
    water_context: Path
```

```yaml
sources:
  - source_id: geofabrik_vietnam_snapshot
    adapter: geofabrik_osm
    version: snapshot
    license_id: ODbL-1.0
    settings:
      pbf_url: https://download.geofabrik.de/asia/vietnam-latest.osm.pbf
      md5_url: https://download.geofabrik.de/asia/vietnam-latest.osm.pbf.md5
```

HEAD the PBF, derive `source_valid_time` and filename date from `Last-Modified`, fetch the `.md5` sidecar first, then download the PBF. Store both under `dataset/raw/osm/geofabrik/<YYYYMMDD>/`. Validate MD5 as source integrity and SHA-256 as local catalog integrity. If Last-Modified changes between resolve and GET, discard the partial and re-resolve so name and payload cannot disagree.

- [ ] **Step 4: Configure GDAL tags and normalize only approved exposure entities**

Expose `highway`, `bridge`, `tunnel`, `access`, `surface`, `smoothness`, `amenity`, `healthcare`, `emergency`, `government`, `place`, `natural`, `water`, `waterway`, `landuse`, `name`, and `other_tags` in `config/osmconf.ini`. Use `pyogrio.read_dataframe` on PBF layers `lines`, `multilinestrings`, `points`, and `multipolygons` with the Exposure AOI bbox, then exact-clip geometries.

Classify:

- roads: non-null `highway`, retaining access/surface/status/bridge/tunnel;
- bridges: road features with truthy `bridge` or standalone mapped bridge geometry;
- facilities: `amenity` in hospital/clinic/school/kindergarten/college/university/fire_station/police/townhall/shelter, or matching `healthcare`, `emergency`, `government`;
- settlements: non-null `place` in city/town/village/hamlet/locality;
- water context: reservoirs/water bodies for QA only.

Explode multipart roads in source order and build `segment_id = <osm_type>/<osm_numeric_id>:<zero-padded part index>`. Preserve source tags as canonical JSON and output all layers in `EPSG:4326`.

- [ ] **Step 5: Run tests and resolve-only snapshot preflight**

Run: `.venv/bin/pytest tests/contract/test_osm_adapter.py tests/unit/test_osm_normalize.py -q`

Expected: all tests pass, unsupported tags remain in `tags_json`, and geometries outside Exposure AOI are absent.

Run: `.venv/bin/flashflood-data fetch --root /home/cloud/cloud/TLCN/Project --source geofabrik_vietnam_snapshot --resolve-only`

Expected: timestamp, MD5, `Content-Length`, and budget decision are printed; the PBF is not downloaded.

- [ ] **Step 6: Commit**

```bash
git add config/sources/osm.yaml config/osmconf.ini src/flashflood_data/sources/osm.py tests/fixtures/osm tests/contract/test_osm_adapter.py tests/unit/test_osm_normalize.py
git commit -m "feat: add timestamped OSM exposure extraction"
```

### Task 13: Harmonize WorldPop and replay the 30 historical evidence rows against valid-time administration

**Files:**
- Create: `src/flashflood_data/harmonize/exposure.py`
- Create: `tests/fixtures/events/historical_events.xlsx`
- Test: `tests/unit/test_historical_events.py`
- Test: `tests/integration/test_worldpop_harmonize.py`

**Interfaces:**
- Consumes: registered WorldPop raster, Core AOI, 2025 current administration, old-current crosswalk, historical workbook
- Produces: `harmonize_worldpop(raw_path, core_aoi, output) -> Path`
- Produces: `read_historical_events(path: Path) -> DataFrame`
- Produces: `resolve_event_administration(events, current_admin, crosswalk) -> DataFrame`
- Produces: `dataset/harmonized/rasters/worldpop_2025.tif`
- Produces: `dataset/harmonized/events/historical_flood_event_2020_2026.parquet`

- [ ] **Step 1: Write failing workbook and valid-time replay tests**

```python
def test_workbook_reads_only_numbered_evidence_rows(fixture_workbook) -> None:
    events = read_historical_events(fixture_workbook)
    assert events.event_id.tolist() == ["historical-flood:1", "historical-flood:2"]
    assert "original_place_text" in events

def test_pre_reform_event_uses_old_to_new_crosswalk(events, current_admin, crosswalk) -> None:
    result = resolve_event_administration(events, current_admin, crosswalk)
    old_event = result.loc[result.event_year == 2024].iloc[0]
    assert json.loads(old_event.current_commune_codes_json) == ["03664"]
    assert old_event.match_status == "matched"

def test_district_only_event_is_not_forced(events, current_admin, crosswalk) -> None:
    result = resolve_event_administration(events, current_admin, crosswalk)
    district_only = result.loc[result.original_place_text == "một số khu vực trong huyện"].iloc[0]
    assert district_only.match_status == "unresolved"
    assert district_only.current_commune_codes_json == "[]"
```

- [ ] **Step 2: Run tests and confirm exposure harmonization is absent**

Run: `.venv/bin/pytest tests/unit/test_historical_events.py tests/integration/test_worldpop_harmonize.py -q`

Expected: FAIL because `flashflood_data.harmonize.exposure` does not exist.

- [ ] **Step 3: Clip WorldPop to Core AOI without changing its population grid**

Select the canonical validated `vnm_pop_2025_CN_100m_R2025A_v1.tif` record from inventory, record any checksum-identical duplicate, and clip it directly on the native transform with no reprojection. Write a float COG with original nodata and pixel values to `dataset/harmonized/rasters/worldpop_2025.tif`. Validate coverage against Core AOI, not Environmental or foreign upstream AOIs, and store source resolution and pixel-inclusion policy metadata.

- [ ] **Step 4: Normalize evidence fields and replay by event date**

```python
EVENT_COLUMNS = {
    "STT": "source_row_number", "Năm": "event_year",
    "Huyện/Khu vực huyện": "original_district_text",
    "Xã/Địa điểm cụ thể": "original_place_text",
    "Ngày xảy ra": "original_date_text", "Thời gian/Giờ xảy ra": "original_time_text",
    "Loại sự kiện": "event_type", "Mô tả ngắn": "description",
    "Nguồn tin": "source_name", "Link minh chứng": "evidence_url", "Ghi chú": "notes",
}
```

Read rows whose `STT` is numeric, preserving every original text field. Parse date text into inclusive `event_date_start`/`event_date_end` where deterministic and retain null parsed dates plus the original text otherwise. For dates before `2025-07-01`, extract explicit old commune names and follow matched crosswalk rows. For later dates, match explicit current names directly. Status is `matched` only when every explicit commune candidate resolves; `ambiguous` when explicit candidates have multiple or partial resolutions; `unresolved` when no explicit commune can be resolved. Store sorted codes/candidates as JSON and confidence `1.0`, `0.5`, or `0.0`; never infer a commune from a former district alone.

- [ ] **Step 5: Run tests and harmonize real existing exposure inputs**

Run: `.venv/bin/pytest tests/unit/test_historical_events.py tests/integration/test_worldpop_harmonize.py -q`

Expected: all tests pass, including interval dates and post-2025 direct-name cases.

Run: `.venv/bin/flashflood-data harmonize --root /home/cloud/cloud/TLCN/Project --source worldpop_vnm_2025 --source historical_flood_evidence_2020_2026`

Expected: WorldPop COG covers Core AOI and event Parquet contains exactly 30 evidence rows; each row has `matched`, `ambiguous`, or `unresolved`.

- [ ] **Step 6: Commit**

```bash
git add src/flashflood_data/harmonize/exposure.py tests/fixtures/events/historical_events.xlsx tests/unit/test_historical_events.py tests/integration/test_worldpop_harmonize.py
git commit -m "feat: harmonize population and historical evidence"
```

### Task 14: Wire the manifest-driven stage graph, fingerprints, recovery, and CLI commands

**Files:**
- Create: `src/flashflood_data/pipeline.py`
- Modify: `src/flashflood_data/cli.py`
- Modify: `Makefile`
- Test: `tests/unit/test_pipeline.py`
- Test: `tests/integration/test_pipeline_recovery.py`

**Interfaces:**
- Consumes: all adapters and shared records from Tasks 1–13
- Produces: `Stage` enum and `StaticPipeline`
- Produces: `dependency_fingerprint(asset_checksums, config, processor_version) -> str`
- Produces: `StaticPipeline.run(stages, source_ids, resolve_only=False) -> RunSummary`
- Produces: working CLI commands `inventory`, `fetch`, `validate`, `harmonize`, `derive`, `map`, and `run-static`

- [ ] **Step 1: Write failing fingerprint, no-op, and independent-failure tests**

```python
def test_fingerprint_is_order_independent() -> None:
    left = dependency_fingerprint(["b", "a"], {"level": 10}, "0.1.0")
    right = dependency_fingerprint(["a", "b"], {"level": 10}, "0.1.0")
    assert left == right

def test_unchanged_second_run_skips_fetch_and_build(fake_pipeline) -> None:
    first = fake_pipeline.run([Stage.FETCH, Stage.HARMONIZE], ["source-a"])
    second = fake_pipeline.run([Stage.FETCH, Stage.HARMONIZE], ["source-a"])
    assert first.fetched == 1 and first.harmonized == 1
    assert second.fetched == 0 and second.harmonized == 0 and second.reused == 2

def test_source_failure_keeps_independent_completed_assets(fake_pipeline_with_failure) -> None:
    summary = fake_pipeline_with_failure.run([Stage.FETCH], ["good", "bad"])
    assert summary.failed_sources == ["bad"]
    assert summary.completed_sources == ["good"]
```

- [ ] **Step 2: Run tests and confirm pipeline orchestration is missing**

Run: `.venv/bin/pytest tests/unit/test_pipeline.py tests/integration/test_pipeline_recovery.py -q`

Expected: FAIL because `StaticPipeline` is undefined.

- [ ] **Step 3: Implement the stage graph and deterministic reuse decision**

```python
class Stage(StrEnum):
    INVENTORY = "inventory"
    BOOTSTRAP_ADMIN = "bootstrap_admin"
    AOI = "aoi"
    FETCH = "fetch"
    VALIDATE = "validate"
    HARMONIZE = "harmonize"
    DERIVE = "derive"
    QA = "qa"

STATIC_ORDER = (Stage.INVENTORY, Stage.BOOTSTRAP_ADMIN, Stage.AOI, Stage.FETCH,
                Stage.VALIDATE, Stage.HARMONIZE, Stage.DERIVE, Stage.QA)

class RunSummary(BaseModel):
    run_id: str
    status: Literal["completed", "partial_failure", "failed"]
    fetched: int = 0
    validated: int = 0
    harmonized: int = 0
    derived: int = 0
    reused: int = 0
    completed_sources: list[str] = Field(default_factory=list)
    failed_sources: list[str] = Field(default_factory=list)

def dependency_fingerprint(checksums: Iterable[str], config: Mapping[str, object],
                           processor_version: str) -> str:
    payload = {"checksums": sorted(checksums), "config": config, "processor": processor_version}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
```

At stage entry, locate an output by source/version/fingerprint and verify its file checksum. Reuse only when all match. Mark mismatches `STALE`, rebuild to a new atomic target, and never overwrite a valid raw asset. `BOOTSTRAP_ADMIN` acquires, validates, and harmonizes only `sonla_admin_2025` and `gadm_vnm_4_1`; this supplies Core and Vietnam boundaries before `AOI` resolves hydrological/environmental/exposure extents. Resolve adapters repeatedly until no new `RemoteAsset` IDs appear, which supports the admin index→75 geometries→legal PDF flow. End a run as `completed`, `partial_failure`, or `failed` with per-source errors; continue only independent branches.

- [ ] **Step 4: Wire CLI options and exact exit behavior**

Every stage command supports repeated `--source`, `--root`, `--resolve-only` where applicable, and `--json-summary`. `run-static` executes `STATIC_ORDER` and accepts `--stop-after <stage>` for a verified prefix run; `fetch --resolve-only` performs metadata requests and budget checks but no source payload GET. Exit `0` when selected work succeeds, `1` on source/QA failure, and `2` for configuration/credential/usage errors. `run-static` refuses live network work unless `--profile live`; integration fixtures use `--profile smoke`.

Make targets:

```make
setup:
	/home/cloud/.pyenv/shims/python3.11 -m venv .venv
	.venv/bin/pip install -r requirements.lock
test:
	.venv/bin/pytest -q
lint:
	.venv/bin/ruff check src tests
smoke:
	.venv/bin/flashflood-data run-static --profile smoke --root .
preflight:
	.venv/bin/flashflood-data run-static --profile live --root . --stop-after aoi
	.venv/bin/flashflood-data fetch --profile live --root . --resolve-only
```

- [ ] **Step 5: Run orchestration tests and commit**

Run: `.venv/bin/pytest tests/unit/test_pipeline.py tests/integration/test_pipeline_recovery.py -q`

Expected: all tests pass; a simulated network failure leaves `.partial` absent, good-source output valid, and run status `partial_failure`.

```bash
git add src/flashflood_data/pipeline.py src/flashflood_data/cli.py Makefile tests/unit/test_pipeline.py tests/integration/test_pipeline_recovery.py
git commit -m "feat: orchestrate resumable static pipeline"
```

### Task 15: Derive terrain, soil, land-cover, and hydrology features per selected L10 basin

**Files:**
- Create: `config/features.yaml`
- Create: `src/flashflood_data/derive/terrain.py`
- Create: `src/flashflood_data/derive/soil.py`
- Create: `src/flashflood_data/derive/landcover.py`
- Create: `src/flashflood_data/derive/hydrology.py`
- Test: `tests/unit/test_terrain_features.py`
- Test: `tests/unit/test_soil_features.py`
- Test: `tests/unit/test_landcover_features.py`
- Test: `tests/unit/test_hydrology_features.py`

**Interfaces:**
- Consumes: selected L10, DEM, SoilGrids, WorldCover, HydroRIVERS, BasinATLAS
- Produces: `derive_terrain_features(dem_path: Path, basins: GeoDataFrame, processing_crs: str) -> DataFrame`
- Produces: `depth_weighted_soil(raster_paths: Mapping[tuple[str, str, str], Path], basins: GeoDataFrame, bands_cm: Sequence[tuple[int, int]]) -> DataFrame`
- Produces: `derive_landcover_fractions(worldcover_path: Path, basins: GeoDataFrame) -> DataFrame`
- Produces: `derive_hydrology_features(rivers: GeoDataFrame, basins: GeoDataFrame, dem_path: Path | None, basinatlas: GeoDataFrame | None) -> DataFrame`
- Produces: four intermediate Parquet tables keyed uniquely by `HYBAS_ID`

- [ ] **Step 1: Write failing numerical feature tests**

```python
def test_relief_and_slope_are_metric(synthetic_dem, basin) -> None:
    result = derive_terrain_features(synthetic_dem, basin, processing_crs="EPSG:32648")
    assert result.loc[0, "elevation_relief_m"] == pytest.approx(90.0)
    assert 0 < result.loc[0, "slope_deg_mean"] < 90

def test_soil_0_30_weighting_uses_5_10_15_cm_layers() -> None:
    values = {"0-5cm": 10.0, "5-15cm": 20.0, "15-30cm": 40.0}
    assert weighted_depth_value(values, 0, 30) == pytest.approx((10*5 + 20*10 + 40*15) / 30)

def test_landcover_fractions_sum_to_one(class_raster, basin) -> None:
    result = derive_landcover_fractions(class_raster, basin)
    fraction_columns = [c for c in result if c.startswith("landcover_fraction_")]
    assert result[fraction_columns].sum(axis=1).iloc[0] == pytest.approx(1.0)

def test_drainage_density_is_length_over_area(rivers, basin) -> None:
    result = derive_hydrology_features(rivers, basin, dem=None, basinatlas=None)
    assert result.loc[0, "drainage_density_km_per_km2"] == pytest.approx(2.5)
```

- [ ] **Step 2: Run tests and confirm feature modules are absent**

Run: `.venv/bin/pytest tests/unit/test_terrain_features.py tests/unit/test_soil_features.py tests/unit/test_landcover_features.py tests/unit/test_hydrology_features.py -q`

Expected: FAIL during collection for missing derive modules.

- [ ] **Step 3: Implement terrain and soil units explicitly**

Warp a working DEM to `EPSG:32648` at a 30 m target grid using bilinear interpolation, compute slope in degrees with metric `x`/`y` gradients, and retain working rasters under `dataset/derived/terrain/`. Compute elevation min/mean/max/relief and slope mean/p90/max per L10 with native nodata excluded.

Use these SoilGrids raw-value divisors in `config/features.yaml`: texture `clay/sand/silt: 10`, `bdod: 100`, `cfvo: 10`, and `wv0010/wv0033/wv1500: 10`. Divide mapped integers before aggregation. Compute thickness-weighted `0-30cm` from 5/10/15 cm layers and `30-100cm` from 30/40 cm layers for both mean and uncertainty. Keep source coverage and valid-pixel counts; do not use 100–200 cm in these two features, but retain its harmonized COG.

- [ ] **Step 4: Implement categorical and hydrological features**

Map WorldCover codes to stable names:

```yaml
worldcover_classes:
  10: tree_cover
  20: shrubland
  30: grassland
  40: cropland
  50: built_up
  60: bare_sparse
  70: snow_ice
  80: permanent_water
  90: herbaceous_wetland
  95: mangroves
  100: moss_lichen
```

Calculate native-grid pixel fractions with nodata reported separately. In `EPSG:32648`, intersect HydroRIVERS with each L10, sum length, divide by basin area, and sample DEM at line endpoints for length-weighted stream gradient; flag reversed/flat/nodata reaches. Select exactly these BasinATLAS baseline fields into the feature table: `dis_m3_pyr`, `run_mm_syr`, `inu_pc_smn`, `inu_pc_smx`, `lka_pc_sse`, `dor_pc_pva`, `ria_ha_ssu`, `riv_tc_ssu`, `gwt_cm_sav`, `ele_mt_sav`, `ele_mt_smn`, `ele_mt_smx`, `slp_dg_sav`, `sgr_dk_sav`, and `pre_mm_syr`. Validate all configured names exist before selection.

- [ ] **Step 5: Run feature tests and commit**

Run: `.venv/bin/pytest tests/unit/test_terrain_features.py tests/unit/test_soil_features.py tests/unit/test_landcover_features.py tests/unit/test_hydrology_features.py -q`

Expected: all tests pass; each module emits unique `HYBAS_ID`, coverage evidence, and no infinity values.

```bash
git add config/features.yaml src/flashflood_data/derive tests/unit/test_terrain_features.py tests/unit/test_soil_features.py tests/unit/test_landcover_features.py tests/unit/test_hydrology_features.py
git commit -m "feat: derive static basin predictors"
```

### Task 16: Build spatial mapping tables, Core-AOI population evidence, and the final static profile

**Files:**
- Modify: `src/flashflood_data/derive/mappings.py`
- Create: `src/flashflood_data/derive/population.py`
- Create: `src/flashflood_data/derive/profile.py`
- Test: `tests/unit/test_spatial_mappings.py`
- Test: `tests/unit/test_population.py`
- Test: `tests/integration/test_static_profile.py`

**Interfaces:**
- Consumes: all harmonized vectors/rasters and Task 15 intermediate tables
- Produces: `map_subbasin_commune(basins: GeoDataFrame, communes: GeoDataFrame) -> DataFrame`
- Produces: `map_subbasin_lines(basins: GeoDataFrame, lines: GeoDataFrame, entity_id: str) -> DataFrame`
- Produces: `map_subbasin_points(basins: GeoDataFrame, points: GeoDataFrame, entity_id: str) -> DataFrame`
- Produces: `map_subbasin_population(worldpop: Path, basins: GeoDataFrame, core: BaseGeometry) -> DataFrame`
- Produces: `aggregate_population_by_basin(worldpop, basins, core) -> DataFrame`
- Produces: `assemble_static_profile(basins, feature_tables, run_id) -> GeoDataFrame`
- Produces: every mapping table plus `subbasin_static_feature.geoparquet`

- [ ] **Step 1: Write failing relationship, population, and one-row invariant tests**

```python
def test_commune_mapping_carries_both_area_fractions(basins, communes) -> None:
    result = map_subbasin_commune(basins, communes)
    assert {"intersection_area_km2", "basin_fraction", "commune_fraction"} <= set(result)
    assert result.groupby("current_commune_code").commune_fraction.sum().between(0.995, 1.005).all()

def test_population_is_clipped_to_core_not_whole_upstream_basin(worldpop, basin_crossing_core, core) -> None:
    result = aggregate_population_by_basin(worldpop, basin_crossing_core, core)
    assert result.loc[0, "population_scope"] == "core_aoi_only"
    assert result.loc[0, "population_sum"] == pytest.approx(30.0)
    assert result.loc[0, "coverage_ratio"] == pytest.approx(1.0)

def test_profile_has_exactly_one_row_per_selected_l10(basins, feature_tables) -> None:
    profile = assemble_static_profile(basins, feature_tables, "run-1")
    assert profile.HYBAS_ID.is_unique
    assert len(profile) == len(basins)
```

- [ ] **Step 2: Run tests and confirm outputs are missing**

Run: `.venv/bin/pytest tests/unit/test_spatial_mappings.py tests/unit/test_population.py tests/integration/test_static_profile.py -q`

Expected: FAIL because the mapping/profile functions do not exist.

- [ ] **Step 3: Implement metric overlays and stable relationship schemas**

Use `EPSG:32648` for overlay area/length, then output Parquet without geometry unless geometry is an explicit product. Required columns:

- commune: `HYBAS_ID`, `current_commune_code`, `intersection_area_km2`, `basin_fraction`, `commune_fraction`, `quality_flags_json`, provenance;
- river: `HYBAS_ID`, `HYRIV_ID`, `intersected_length_km`, `boundary_case`, provenance;
- road: `HYBAS_ID`, `segment_id`, `osm_id`, `intersected_length_km`, `quality_flags_json`, provenance;
- bridge: `HYBAS_ID`, bridge source ID, OSM ID, relationship geometry/length, `boundary_case`, provenance;
- facility/settlement: `HYBAS_ID`, source ID, `relationship_type` (`within`, `touches`, `nearest_boundary_tie`), tags JSON, provenance.

When a point lies exactly on multiple basin boundaries, emit all tied relationships with `boundary_case=true`; never choose one arbitrarily. Write `map_subbasin_commune.parquet`, `map_subbasin_river.parquet`, `map_subbasin_road.parquet`, `map_subbasin_bridge.parquet`, `map_subbasin_facility.parquet`, and `map_subbasin_settlement.parquet`.

- [ ] **Step 4: Aggregate population with evidence and no double counting**

Intersect every selected L10 with Core AOI before raster aggregation. Use native WorldPop pixels whose centers fall within each disjoint clipped zone, sum original person-count values, and record mean, contributing pixel count, nodata count, AOI pixel count, coverage ratio, source resolution, and `population_scope="core_aoi_only"`. Confirm a raster pixel is assigned to at most one L10; boundary center ties follow the lower numeric `HYBAS_ID` and are counted/reported. Write `map_subbasin_population.parquet`.

- [ ] **Step 5: Assemble final profile with strict joins and provenance**

Start from selected L10 and left-join each feature table after asserting unique `HYBAS_ID`. Missing optional observations remain null with coverage/quality flags; a missing feature-table key is a QA failure, not a dropped basin. Include hierarchy/topology, `SUB_AREA`, `UP_AREA`, geometry, Task 15 features, population evidence, `pipeline_run_id`, dependency fingerprint, and JSON mapping of feature groups to source asset IDs. Write in `EPSG:4326` to `dataset/derived/subbasin_static_feature.geoparquet`.

- [ ] **Step 6: Run tests and commit**

Run: `.venv/bin/pytest tests/unit/test_spatial_mappings.py tests/unit/test_population.py tests/integration/test_static_profile.py -q`

Expected: all tests pass, all foreign keys resolve, and profile cardinality equals selected L10 cardinality.

```bash
git add src/flashflood_data/derive/mappings.py src/flashflood_data/derive/population.py src/flashflood_data/derive/profile.py tests/unit/test_spatial_mappings.py tests/unit/test_population.py tests/integration/test_static_profile.py
git commit -m "feat: build basin mappings and static profile"
```

### Task 17: Enforce all quality gates and publish machine-readable reports plus a MapLibre QA map

**Files:**
- Create: `src/flashflood_data/qa/__init__.py`
- Create: `src/flashflood_data/qa/checks.py`
- Create: `src/flashflood_data/qa/report.py`
- Create: `src/flashflood_data/qa/map.py`
- Create: `src/flashflood_data/qa/templates/report.html.j2`
- Create: `src/flashflood_data/qa/templates/map.html.j2`
- Test: `tests/unit/test_qa_checks.py`
- Test: `tests/integration/test_qa_report.py`
- Test: `tests/integration/test_qa_map.py`

**Interfaces:**
- Consumes: full catalog, study config, harmonized and derived outputs
- Produces: `CheckResult`, `QAReport`, `run_quality_gates(paths, config) -> QAReport`
- Produces: `publish_report(report, qa_dir) -> list[Path]`
- Produces: `publish_qa_map(paths, qa_dir) -> Path`
- Produces: `dataset/qa/report.{json,parquet,html}` and `dataset/qa/map/index.html`

- [ ] **Step 1: Write failing gate and publication tests**

```python
def test_admin_count_gate_is_fatal_at_74(qa_fixture) -> None:
    report = run_quality_gates(qa_fixture.with_admin_count(74), qa_fixture.config)
    check = report.by_id("admin.current.count")
    assert not check.passed and check.severity == "fatal"

def test_population_gate_uses_core_scope(qa_fixture) -> None:
    report = run_quality_gates(qa_fixture.paths, qa_fixture.config)
    assert report.by_id("population.scope").passed
    assert report.by_id("population.no_double_count").passed

def test_map_manifest_lists_every_required_layer(complete_outputs, tmp_path) -> None:
    index = publish_qa_map(complete_outputs, tmp_path)
    manifest = json.loads((index.parent / "layer-manifest.json").read_text())
    assert set(manifest) >= {"communes", "subbasins_l10", "rivers", "roads", "bridges",
                              "facilities", "settlements", "dem", "worldcover", "worldpop"}
```

- [ ] **Step 2: Run tests and confirm QA modules are absent**

Run: `.venv/bin/pytest tests/unit/test_qa_checks.py tests/integration/test_qa_report.py tests/integration/test_qa_map.py -q`

Expected: FAIL because `flashflood_data.qa` is missing.

- [ ] **Step 3: Implement exact checks from the approved spec**

```python
@dataclass(frozen=True)
class CheckResult:
    check_id: str
    passed: bool
    severity: Literal["info", "warning", "fatal"]
    expected: str
    actual: str
    message: str
    asset_ids: tuple[str, ...] = ()

@dataclass(frozen=True)
class QAReport:
    run_id: str
    config_fingerprint: str
    checks: tuple[CheckResult, ...]

    def by_id(self, check_id: str) -> CheckResult:
        return next(check for check in self.checks if check.check_id == check_id)

    @property
    def fatal_failures(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if not check.passed and check.severity == "fatal")
```

Implement and register these fatal gates:

- current admin is exactly 75/67/8 with unique codes and valid geometry;
- gap/overlap is at most 0.1% and legal area difference at most 2% absent an approved exception;
- L10 IDs are unique, every L9/L8 parent is valid, and topology references are internal or marked scope exits;
- commune coverage by L10 is 99.5–100.5%;
- every mapping foreign key exists;
- DEM/SoilGrids/WorldCover valid coverage is at least 99% of Hydrological AOI and WorldPop is evaluated against Core AOI;
- population has pixel/nodata/coverage evidence and no double counting;
- all 30 historical rows have a legal match status and confidence;
- every used raw asset has URI/version/license/retrieval/checksum;
- pipeline-acquired raw bytes do not exceed 8 GiB and disk reserve is not breached.

Warnings include geometry repair counts, boundary ties, unresolved event names, source nodata, and external downstream scope exits. A fatal check makes the command exit `1` but still publishes the report.

- [ ] **Step 4: Publish deterministic report formats**

Sort checks by `check_id`. Write JSON with run/config fingerprints, a flat Parquet table for analysis, and an HTML summary showing fatal/warning/info groups and links to local artifacts. Never embed credentials, bearer tokens, or signed URLs. Report paths are relative to project root so the directory remains movable.

- [ ] **Step 5: Build the lightweight MapLibre inspection bundle**

Simplify display-only vector copies in `EPSG:4326` with metric tolerances of 20 m for basins/communes, 10 m for rivers, and 5 m for roads while preserving unsimplified analytical files. Write layer GeoJSON files under `dataset/qa/map/data/`, downsample DEM/WorldCover/WorldPop to PNG previews under `previews/`, and write geographic image bounds in `layer-manifest.json`. The template loads pinned `maplibre-gl@5.6.2` from the CDN, adds visibility toggles, source ID/name popups, QA-warning styling, and no warning/routing logic.

- [ ] **Step 6: Run tests and commit**

Run: `.venv/bin/pytest tests/unit/test_qa_checks.py tests/integration/test_qa_report.py tests/integration/test_qa_map.py -q`

Expected: all tests pass; changing output row order does not change report or layer-manifest checksums.

```bash
git add src/flashflood_data/qa tests/unit/test_qa_checks.py tests/integration/test_qa_report.py tests/integration/test_qa_map.py
git commit -m "feat: publish static data QA artifacts"
```

### Task 18: Add recoverable cleanup reporting and the operator runbook

**Files:**
- Create: `src/flashflood_data/cleanup.py`
- Modify: `src/flashflood_data/cli.py`
- Create: `README.md`
- Test: `tests/unit/test_cleanup.py`
- Test: `tests/unit/test_readme_commands.py`

**Interfaces:**
- Consumes: inventory catalog, BasinATLAS archive, extracted level bundles
- Produces: `CleanupCandidate` and `build_cleanup_report(paths, family, keep_levels) -> CleanupReport`
- Produces: CLI `cleanup --dry-run --family basinatlas --keep-level 10`
- Produces: `dataset/qa/cleanup/basinatlas-dry-run.json`

- [ ] **Step 1: Write failing archive-safety and no-deletion tests**

```python
def test_cleanup_report_keeps_level_10_and_proves_recovery(cleanup_fixture) -> None:
    report = build_cleanup_report(cleanup_fixture.paths, "basinatlas", {10})
    assert all("lev10" not in str(item.path) for item in report.candidates)
    assert report.archive_readable
    assert report.archive_checksum == cleanup_fixture.archive_sha256

def test_dry_run_never_removes_candidate(cleanup_fixture) -> None:
    before = {p: p.stat().st_size for p in cleanup_fixture.extracted_files}
    runner.invoke(app, ["cleanup", "--dry-run", "--root", str(cleanup_fixture.root),
                        "--family", "basinatlas", "--keep-level", "10"])
    assert before == {p: p.stat().st_size for p in cleanup_fixture.extracted_files}
```

- [ ] **Step 2: Run tests and confirm cleanup reporting is missing**

Run: `.venv/bin/pytest tests/unit/test_cleanup.py tests/unit/test_readme_commands.py -q`

Expected: FAIL because `build_cleanup_report` is absent.

- [ ] **Step 3: Implement report-only exact candidate resolution**

```python
@dataclass(frozen=True)
class CleanupCandidate:
    path: Path
    bundle_members: tuple[Path, ...]
    size_bytes: int
    recovery_archive: Path
    recovery_archive_checksum: str
    recoverable: bool

@dataclass(frozen=True)
class CleanupReport:
    candidates: tuple[CleanupCandidate, ...]
    archive_readable: bool
    archive_checksum: str
    reclaimable_bytes: int
```

Validate `dataset/BasinATLAS_Data_v10_shp.zip` by SHA-256 and ZIP central-directory read. Verify the retained L10 bundle and derived static profile before listing levels 1–9 and 11–12 as candidates. Each candidate contains an absolute resolved path, bundle members, bytes, archive path/checksum, and `recoverable=true`. Reject paths outside `dataset/BasinATLAS_Data_v10_shp/BasinATLAS_v10_shp/`, symlinks, unresolved globs, unreadable archive, or missing retained L10. The implemented command has only `--dry-run`; it contains no unlink/rmtree call.

- [ ] **Step 4: Document setup, credentials, stages, outputs, and recovery**

README must include these exact operator commands:

```bash
/home/cloud/.pyenv/shims/python3.11 -m venv .venv
.venv/bin/pip install -r requirements.lock
cp .env.example .env
.venv/bin/flashflood-data inventory --root .
.venv/bin/flashflood-data run-static --profile live --root . --stop-after aoi
.venv/bin/flashflood-data fetch --profile live --root . --resolve-only
.venv/bin/flashflood-data run-static --profile live --root .
.venv/bin/flashflood-data cleanup --dry-run --family basinatlas --keep-level 10 --root .
.venv/bin/python -m http.server 8000 --directory dataset/qa/map
```

Explain that the user enters CDSE credentials locally in `.env`, never in chat or Git; raw assets remain under ignored `dataset/raw`; catalog states and `.partial` recovery; AOI meanings; 8 GiB cap; output schemas; QA failures; and the fact that cleanup reporting does not delete data.

- [ ] **Step 5: Run tests and commit**

Run: `.venv/bin/pytest tests/unit/test_cleanup.py tests/unit/test_readme_commands.py -q`

Expected: all tests pass and dry-run candidate fixture checksums remain unchanged.

```bash
git add src/flashflood_data/cleanup.py src/flashflood_data/cli.py README.md tests/unit/test_cleanup.py tests/unit/test_readme_commands.py
git commit -m "docs: add static pipeline operations and cleanup report"
```

### Task 19: Prove the full smoke workflow, crawl the live static sources, and verify idempotence

**Files:**
- Create: `tests/fixtures/static_pipeline/build_fixture_lake.py`
- Create: `tests/integration/test_static_pipeline_smoke.py`
- Create: `tests/integration/test_static_pipeline_idempotence.py`
- Create: `tests/integration/test_data_quality_gates.py`
- Modify: `Makefile`
- Modify: `README.md`

**Interfaces:**
- Consumes: complete pipeline and all adapters from Tasks 1–18
- Produces: a hermetic miniature end-to-end test and the populated live file lake
- Verifies: first run completeness, second-run no-op, full QA, storage cap, and ignored raw files

- [ ] **Step 1: Write the failing end-to-end smoke and idempotence assertions**

```python
def test_static_pipeline_smoke(smoke_project, mocked_sources) -> None:
    first = run_cli(["run-static", "--profile", "smoke", "--root", str(smoke_project)])
    assert first.exit_code == 0
    assert (smoke_project / "dataset/derived/subbasin_static_feature.geoparquet").exists()
    assert (smoke_project / "dataset/qa/map/index.html").exists()
    assert not load_qa(smoke_project).fatal_failures

def test_static_pipeline_second_run_is_noop(smoke_project, mocked_sources) -> None:
    run_cli(["run-static", "--profile", "smoke", "--root", str(smoke_project)])
    before = output_checksums(smoke_project)
    second = run_cli(["run-static", "--profile", "smoke", "--root", str(smoke_project),
                      "--json-summary"])
    assert second.json["fetched"] == 0
    assert second.json["harmonized"] == 0
    assert second.json["derived"] == 0
    assert before == output_checksums(smoke_project)
```

- [ ] **Step 2: Run the tests and observe the first uncovered integration contract**

Run: `.venv/bin/pytest tests/integration/test_static_pipeline_smoke.py tests/integration/test_static_pipeline_idempotence.py tests/integration/test_data_quality_gates.py -q -x`

Expected: FAIL at the first missing fixture adapter, stage registration, or output contract; use that exact failure to complete the integration wiring without weakening an assertion.

- [ ] **Step 3: Build a hermetic miniature lake and satisfy every source class**

`build_fixture_lake.py` creates three current communes, four historical communes, three L10 basins plus L9/L8 parents, one direct-upstream link, two river reaches, a 10×10 DEM, SoilGrids mean/uncertainty rasters for all configured property/depth combinations, a categorical WorldCover raster, a population raster, OSM point/line/polygon fixtures, and two historical events. Mock every HTTP/OData/WCS response with `respx`; do not access the network in pytest. The smoke profile overrides expected admin counts to fixture counts but retains the same percentages, CRS, file formats, state transitions, and output schemas.

- [ ] **Step 4: Run all automated verification and commit the stable implementation**

Run: `.venv/bin/ruff check src tests`

Expected: `All checks passed!`

Run: `.venv/bin/pytest -q`

Expected: all unit, contract, and integration tests pass with no skipped adapter contract.

Run: `.venv/bin/pytest --cov=flashflood_data --cov-report=term-missing -q`

Expected: at least 85% statement coverage overall and 100% coverage for catalog legal transitions, storage budget rejection, checksum quarantine, and cleanup target guards.

```bash
git add tests/fixtures/static_pipeline tests/integration/test_static_pipeline_smoke.py tests/integration/test_static_pipeline_idempotence.py tests/integration/test_data_quality_gates.py Makefile README.md
git commit -m "test: cover complete static data workflow"
```

- [ ] **Step 5: Bootstrap the small boundary sources, then preflight all AOI-dependent downloads**

Confirm `.env` exists locally with `FLASHFLOOD_CDSE_USERNAME` and `FLASHFLOOD_CDSE_PASSWORD`, then run:

Run: `.venv/bin/flashflood-data inventory --root /home/cloud/cloud/TLCN/Project`

Run: `.venv/bin/flashflood-data run-static --profile live --root /home/cloud/cloud/TLCN/Project --stop-after aoi --json-summary`

Expected: current admin and GADM boundary assets are acquired and validated, all four AOIs are built, and no environmental/exposure source payload has been downloaded.

Run: `.venv/bin/flashflood-data fetch --profile live --root /home/cloud/cloud/TLCN/Project --resolve-only --json-summary`

Expected: every enabled source resolves, CDSE authentication succeeds, 75 admin geometries and 96 SoilGrids coverages are listed, source `Content-Length` values are known or conservatively estimated, projected new raw bytes are at most 8 GiB, and projected free space is at least 10 GiB. If either budget gate fails, stop before the crawl and reduce only source tiles/AOI—not raw retention or QA.

- [ ] **Step 6: Crawl and build the complete live static lake**

Run: `.venv/bin/flashflood-data run-static --profile live --root /home/cloud/cloud/TLCN/Project --json-summary`

Expected: exit `0`; all planned raw assets reach `VALIDATED`; harmonized products, every mapping, the one-row-per-L10 static profile, QA reports, and MapLibre map exist. A transient source failure may leave the run `partial_failure`; rerun the same command until all failed source branches resume and exit `0` rather than deleting successful assets.

- [ ] **Step 7: Verify live completion criteria and unchanged rerun**

Run: `.venv/bin/flashflood-data validate --profile live --root /home/cloud/cloud/TLCN/Project --json-summary`

Expected: no fatal QA gate, exactly 75 current units, 30 historical evidence rows with legal statuses, one profile row per selected L10, valid mapping foreign keys, raster coverage thresholds met, and new raw total within 8 GiB.

Run: `.venv/bin/flashflood-data run-static --profile live --root /home/cloud/cloud/TLCN/Project --json-summary`

Expected: `fetched=0`, `harmonized=0`, `derived=0`, unchanged output checksums, and only a new run record/report timestamp.

Run: `git status --short --ignored`

Expected: `.env`, `.venv/`, and `dataset/` appear only as ignored entries; no secret or raw payload is staged.

- [ ] **Step 8: Produce the optional cleanup report without deleting anything**

Run: `.venv/bin/flashflood-data cleanup --dry-run --family basinatlas --keep-level 10 --root /home/cloud/cloud/TLCN/Project`

Expected: exact extracted unused BasinATLAS bundles, reclaimable bytes, readable recovery archive and checksum are written to `dataset/qa/cleanup/basinatlas-dry-run.json`; every listed file still exists after the command.

## Final Acceptance Checklist

- [ ] `dataset/catalog/assets.parquet` contains provenance and SHA-256 for every used raw and downstream asset.
- [ ] Existing HydroBASINS, BasinATLAS, HydroRIVERS, WorldPop, workbook, and sample files stayed at their original paths.
- [ ] Current admin has 75 units (67 communes, 8 wards), Core AOI passes topology/area gates, and the old-current crosswalk retains ambiguous/unresolved cases.
- [ ] L10 is primary, one direct upstream hop is present, and every selected L10 has validated L9 and L8 parents.
- [ ] All eight SoilGrids properties have mean and uncertainty at six depths; raw coverage responses remain available.
- [ ] COP-DEM GLO-30, WorldCover 2021 v200, timestamped OSM PBF, and all exact raw tiles/products remain available.
- [ ] Static feature profile has one row per selected L10 and all mapping tables pass referential checks.
- [ ] WorldPop values are labeled Core-AOI-only and include pixel/nodata/coverage evidence without double counting.
- [ ] Every one of the 30 evidence rows has original text, valid-time replay status, candidate evidence, and confidence.
- [ ] QA JSON/Parquet/HTML and MapLibre map load from `dataset/qa/` with all required layers.
- [ ] New raw data are at most 8 GiB, disk reserve is respected, and the second unchanged run is a no-op.
- [ ] Cleanup remains a report only; no existing data has been deleted.
