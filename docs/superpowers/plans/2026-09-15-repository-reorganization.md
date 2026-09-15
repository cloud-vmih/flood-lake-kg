# Repository Reorganization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the working static pipeline and infrastructure into explicit, dependency-controlled packages without changing CLI behavior, data semantics, or generated artifacts.

**Architecture:** Preserve `flashflood_data` and all external commands while moving code in behavior-preserving slices. Domain code lives under `static/`; shared configuration, catalog, storage, and HTTP transport sit below domains; CLI, Airflow, Spark entrypoints, and orchestration sit above them. Compatibility exports keep each intermediate commit usable and are removed after every repository consumer uses canonical imports.

**Tech Stack:** Python 3.11, Typer, Pydantic, GeoPandas, Rasterio, PyArrow, Pytest, Ruff, Docker Compose, Apache Airflow, Apache Spark, Apache Iceberg, Apache Polaris, MinIO.

**Spec:** `docs/superpowers/specs/2026-09-15-repository-organization-and-source-landing-design.md`

## Global Constraints

- Execute in an isolated worktree created with the `using-git-worktrees` skill; the current checkout contains unrelated user changes.
- Preserve package name `flashflood_data`, executable `flashflood-data`, `config/`, `compose.yaml`, `airflow/dags/`, `spark/jobs/`, and every existing Make target name.
- Do not change formulas, AOIs, source selection, schemas, catalog semantics, CLI output, or generated data paths.
- Do not modify, move, copy, or delete any file under `dataset/`; runtime infrastructure smoke
  checks must use an already provisioned disposable stack whose persistent state is outside this
  checkout.
- Do not combine behavior changes with module moves.
- Keep provider adapters separate; do not create `static_sources.py`, `common/`, or `utils.py` dumping grounds.
- Create a future-domain directory only in the task that adds its first working module.
- Keep hydrological basin routing distinct from road-network routing.
- Run focused tests first, then `.venv/bin/pytest -q` and `.venv/bin/ruff check src tests` before each task commit.
- Do not stage or commit unrelated worktree changes.
- Source landing into MinIO is a separate implementation plan. This plan only prepares its package and infrastructure boundaries.

## Target File Map

| Current path | Canonical target |
|---|---|
| `config.py` | `core/config.py` |
| `paths.py` | `core/paths.py` |
| `models.py` | `catalog/models.py` |
| `catalog.py` | `catalog/repository.py`, exported by `catalog/__init__.py` |
| `inventory.py` | `catalog/inventory.py` |
| `io_atomic.py` | `storage/atomic.py` |
| `http.py` | `storage/http/{errors,models,redaction,resume,transfer,quarantine,fetcher}.py` |
| `sources/` | `static/sources/` |
| `registry.py` | `static/sources/registry.py` |
| `aoi.py` | `static/harmonize/aoi.py` |
| `harmonize/` | `static/harmonize/` |
| `raster.py`, `vector.py` | `static/spatial/{raster,vector}.py` |
| `derive/{terrain,soil,landcover,hydrology,population,features,static}.py` | `static/features/` |
| `derive/mappings.py` | `static/mappings/spatial.py` and `static/mappings/admin.py` |
| `derive/profile.py` | `static/features/profile.py` and `static/mappings/builder.py` |
| `qa/` | `static/qa/` with subject-specific check modules |
| `pipeline.py`, `composition.py` | `static/workflow/` |
| `cli.py` | `cli/{app.py,commands/static.py,__init__.py}` |
| `infra/{airflow,spark}` | `infra/docker/{airflow,spark}` |
| `infra/{postgres,polaris}` | `infra/services/{postgres,polaris}` |
| host scripts in `infra/scripts/` | `tools/{bootstrap,smoke}/` |

---

### Task 1: Protect Docker builds and record architecture constraints

**Files:**
- Create: `.dockerignore`
- Create: `tests/contract/infra/test_docker_context.py`
- Create: `tests/contract/test_architecture.py`

**Interfaces:**
- Consumes: repository paths and Python import syntax.
- Produces: Docker-context exclusions and reusable architecture checks that later tasks extend.

- [ ] **Step 1: Write the failing Docker-context contract test**

Create `tests/contract/infra/test_docker_context.py`:

```python
from pathlib import Path


ROOT = Path(__file__).parents[3]


def dockerignore_lines() -> set[str]:
    path = ROOT / ".dockerignore"
    assert path.is_file()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def test_docker_context_excludes_local_state_and_large_documents() -> None:
    assert {
        "dataset/",
        ".venv/",
        ".git/",
        ".worktrees/",
        "**/__pycache__/",
        ".pytest_cache/",
        "docs/*.pdf",
        "docs/*.docx",
        "docs/*.pptx",
        "docs/*.xlsx",
    } <= dockerignore_lines()


def test_docker_context_does_not_exclude_build_inputs() -> None:
    lines = dockerignore_lines()
    assert "requirements/" not in lines
    assert "infra/" not in lines
    assert "spark/jobs/" not in lines
```

- [ ] **Step 2: Run the focused test and confirm RED**

Run:

```bash
.venv/bin/pytest tests/contract/infra/test_docker_context.py -q
```

Expected: FAIL because `.dockerignore` does not exist.

- [ ] **Step 3: Add the exact Docker exclusions**

Create `.dockerignore`:

```text
# Local datasets and persistent service state
dataset/

# VCS, worktrees, environments, caches and build output
.git/
.worktrees/
.venv/
venv/
**/__pycache__/
*.py[cod]
.pytest_cache/
.ruff_cache/
build/
dist/
*.egg-info/

# Local secrets and editor state
.env
.env.*
!.env.example
.idea/
.vscode/

# Large research documents are not image inputs
docs/*.pdf
docs/*.docx
docs/*.pptx
docs/*.xlsx
```

- [ ] **Step 4: Add a dependency-direction test scaffold**

Create `tests/contract/test_architecture.py`:

```python
from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).parents[2]
PACKAGE = ROOT / "src" / "flashflood_data"


def imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_package_does_not_import_host_tools() -> None:
    offenders = {
        str(path.relative_to(ROOT)): sorted(name for name in imports(path) if name == "tools" or name.startswith("tools."))
        for path in PACKAGE.rglob("*.py")
        if any(name == "tools" or name.startswith("tools.") for name in imports(path))
    }
    assert offenders == {}
```

- [ ] **Step 5: Record the dataset metadata baseline**

Run once from the isolated worktree before any refactor task:

```bash
find dataset -type f -printf '%p\t%s\t%T@\n' | sort > /tmp/floodlake-reorg-dataset-before.txt
```

Expected: the command exits 0. Keep this file outside Git for the final no-change comparison.

- [ ] **Step 6: Run task verification**

Run:

```bash
.venv/bin/pytest tests/contract/infra/test_docker_context.py tests/contract/test_architecture.py -q
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
```

Expected: all commands exit 0; the full suite reports the current test count plus the new tests.

- [ ] **Step 7: Commit Task 1**

```bash
git add .dockerignore tests/contract/infra/test_docker_context.py tests/contract/test_architecture.py
git commit -m "build: protect docker build context"
```

---

### Task 2: Extract core configuration and catalog packages

**Files:**
- Create: `src/flashflood_data/core/__init__.py`
- Create: `src/flashflood_data/core/config.py`
- Create: `src/flashflood_data/core/paths.py`
- Create: `src/flashflood_data/catalog/__init__.py`
- Create: `src/flashflood_data/catalog/models.py`
- Create: `src/flashflood_data/catalog/repository.py`
- Create: `src/flashflood_data/catalog/inventory.py`
- Modify: `src/flashflood_data/config.py`
- Modify: `src/flashflood_data/paths.py`
- Modify: `src/flashflood_data/models.py`
- Delete: `src/flashflood_data/catalog.py`
- Modify: `src/flashflood_data/inventory.py`
- Create: `tests/contract/test_compatibility_imports.py`
- Modify: imports in `src/flashflood_data/` and `tests/` that consume these modules

**Interfaces:**
- Consumes: existing `EnvironmentSettings`, `StudyAreaConfig`, `ProjectPaths`, catalog models, `AssetCatalog`, checksum helpers, and inventory API.
- Produces: canonical imports under `core` and `catalog`; legacy imports remain identity-compatible for the migration.

- [ ] **Step 1: Write failing canonical/legacy identity tests**

Create `tests/contract/test_compatibility_imports.py`:

```python
def test_core_compatibility_exports_are_identical() -> None:
    from flashflood_data.config import StudyAreaConfig as LegacyConfig
    from flashflood_data.core.config import StudyAreaConfig
    from flashflood_data.core.paths import ProjectPaths
    from flashflood_data.paths import ProjectPaths as LegacyPaths

    assert LegacyConfig is StudyAreaConfig
    assert LegacyPaths is ProjectPaths


def test_catalog_compatibility_exports_are_identical() -> None:
    from flashflood_data.catalog import AssetCatalog, AssetRecord
    from flashflood_data.catalog.models import AssetRecord as CanonicalRecord
    from flashflood_data.catalog.repository import AssetCatalog as CanonicalCatalog
    from flashflood_data.models import AssetRecord as LegacyRecord

    assert AssetCatalog is CanonicalCatalog
    assert AssetRecord is CanonicalRecord
    assert LegacyRecord is CanonicalRecord
```

- [ ] **Step 2: Confirm the canonical imports fail**

Run:

```bash
.venv/bin/pytest tests/contract/test_compatibility_imports.py -q
```

Expected: FAIL because `flashflood_data.core` and `flashflood_data.catalog.models` do not exist.

- [ ] **Step 3: Move configuration and path definitions with explicit shims**

Move the complete definitions from `config.py` to `core/config.py` and from `paths.py` to
`core/paths.py`. Create `core/__init__.py`:

```python
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig, load_study_area
from flashflood_data.core.paths import ProjectPaths

__all__ = ["EnvironmentSettings", "ProjectPaths", "StudyAreaConfig", "load_study_area"]
```

Replace `config.py` with:

```python
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig, load_study_area

__all__ = ["EnvironmentSettings", "StudyAreaConfig", "load_study_area"]
```

Replace `paths.py` with:

```python
from flashflood_data.core.paths import ProjectPaths

__all__ = ["ProjectPaths"]
```

- [ ] **Step 4: Move catalog models, repository, and inventory**

Move every definition from `models.py` to `catalog/models.py`, `catalog.py` to
`catalog/repository.py`, and `inventory.py` to `catalog/inventory.py`. Update their imports to
use `flashflood_data.core` and `flashflood_data.catalog.models`.

Create `catalog/__init__.py` with the existing public surface:

```python
from flashflood_data.catalog.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    RunRecord,
    SourceFile,
    SourceSpec,
    ValidationResult,
)
from flashflood_data.catalog.repository import (
    AssetCatalog,
    IllegalTransition,
    sha256_bundle,
    sha256_file,
)

__all__ = [
    "AssetCatalog",
    "AssetKind",
    "AssetRecord",
    "AssetStatus",
    "IllegalTransition",
    "RemoteAsset",
    "RunRecord",
    "SourceFile",
    "SourceSpec",
    "ValidationResult",
    "sha256_bundle",
    "sha256_file",
]
```

Replace `models.py` and `inventory.py` with explicit re-exports of the names currently imported
by repository code and tests. Delete the old `catalog.py`, because the `catalog/` package itself
provides the compatibility surface.

- [ ] **Step 5: Update internal imports to canonical core/catalog paths**

Use these mappings throughout `src/flashflood_data`:

```text
flashflood_data.config       -> flashflood_data.core.config
flashflood_data.paths        -> flashflood_data.core.paths
flashflood_data.models       -> flashflood_data.catalog.models
flashflood_data.inventory    -> flashflood_data.catalog.inventory
```

Keep `from flashflood_data.catalog import AssetCatalog, sha256_file` valid through
`catalog/__init__.py`.

- [ ] **Step 6: Run focused and full verification**

Run:

```bash
.venv/bin/pytest tests/contract/test_compatibility_imports.py tests/unit/test_config.py tests/unit/test_catalog.py tests/unit/test_inventory.py -q
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit Task 2**

```bash
git add src/flashflood_data tests/contract/test_compatibility_imports.py
git commit -m "refactor: extract core and catalog packages"
```

---

### Task 3: Extract atomic storage and move HTTP behind a package facade

**Files:**
- Create: `src/flashflood_data/storage/__init__.py`
- Create: `src/flashflood_data/storage/atomic.py`
- Create: `src/flashflood_data/storage/http/__init__.py`
- Create: `src/flashflood_data/storage/http/errors.py`
- Create: `src/flashflood_data/storage/http/models.py`
- Create: `src/flashflood_data/storage/http/redaction.py`
- Create: `src/flashflood_data/storage/http/fetcher.py`
- Modify: `src/flashflood_data/io_atomic.py`
- Modify: `src/flashflood_data/http.py`
- Modify: `tests/contract/test_http_fetcher.py`
- Modify: `tests/contract/test_compatibility_imports.py`

**Interfaces:**
- Consumes: Task 2 core/catalog packages and the current `HttpFetcher` behavior.
- Produces: `storage.atomic.atomic_target` and `storage.http.HttpFetcher`; the old import paths continue to return the same objects.

- [ ] **Step 1: Add failing storage compatibility assertions**

Append to `tests/contract/test_compatibility_imports.py`:

```python
def test_storage_compatibility_exports_are_identical() -> None:
    from flashflood_data.http import DownloadFailed as LegacyDownloadFailed
    from flashflood_data.http import HttpFetcher as LegacyFetcher
    from flashflood_data.io_atomic import atomic_target as legacy_atomic_target
    from flashflood_data.storage.atomic import atomic_target
    from flashflood_data.storage.http import DownloadFailed, HttpFetcher

    assert LegacyDownloadFailed is DownloadFailed
    assert LegacyFetcher is HttpFetcher
    assert legacy_atomic_target is atomic_target
```

- [ ] **Step 2: Confirm the new imports fail**

Run:

```bash
.venv/bin/pytest tests/contract/test_compatibility_imports.py::test_storage_compatibility_exports_are_identical -q
```

Expected: FAIL because `flashflood_data.storage` does not exist.

- [ ] **Step 3: Move atomic writes and define the HTTP facade**

Move `atomic_target` unchanged into `storage/atomic.py`. Create `storage/__init__.py`:

```python
from flashflood_data.storage.atomic import atomic_target

__all__ = ["atomic_target"]
```

Move exception classes to `storage/http/errors.py`, `_ResumeState` and `_VerifiedPayload` to
`storage/http/models.py`, redaction functions plus `SecretRedactionFilter` to
`storage/http/redaction.py`, and initially move the complete `HttpFetcher` class to
`storage/http/fetcher.py` with imports adjusted to the new modules.

Create `storage/http/__init__.py`:

```python
from flashflood_data.storage.http.errors import (
    BudgetRejected,
    DownloadFailed,
    DownloadLocked,
    ExistingAssetConflict,
    PayloadMismatch,
)
from flashflood_data.storage.http.fetcher import HttpFetcher
from flashflood_data.storage.http.redaction import SecretRedactionFilter

__all__ = [
    "BudgetRejected",
    "DownloadFailed",
    "DownloadLocked",
    "ExistingAssetConflict",
    "HttpFetcher",
    "PayloadMismatch",
    "SecretRedactionFilter",
]
```

Replace `io_atomic.py` and `http.py` with explicit compatibility exports. At this intermediate
step, keep quarantine publication inside `storage/http/fetcher.py`; update string monkeypatch
targets in `tests/contract/test_http_fetcher.py` to
`flashflood_data.storage.http.fetcher.uuid4` and
`flashflood_data.storage.http.fetcher.os.link`.

- [ ] **Step 4: Run the HTTP contract before decomposition**

Run:

```bash
.venv/bin/pytest tests/contract/test_http_fetcher.py tests/contract/test_compatibility_imports.py -q
```

Expected: PASS with identical download, resume, quarantine, and redaction behavior.

- [ ] **Step 5: Run full verification and commit the move**

Run:

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
```

Then commit:

```bash
git add src/flashflood_data tests/contract
git commit -m "refactor: move storage and http transport"
```

---

### Task 4: Split HTTP transfer, resume, and quarantine responsibilities

**Files:**
- Create: `src/flashflood_data/storage/http/resume.py`
- Create: `src/flashflood_data/storage/http/transfer.py`
- Create: `src/flashflood_data/storage/http/quarantine.py`
- Modify: `src/flashflood_data/storage/http/fetcher.py`
- Modify: `tests/contract/test_http_fetcher.py`
- Create: `tests/unit/storage/test_http_boundaries.py`

**Interfaces:**
- Consumes: `HttpFetcher.fetch(remote: RemoteAsset, run_id: str) -> AssetRecord` from Task 3.
- Produces: the same public method, backed by focused private collaborators; no HTTP or catalog behavior changes.

- [ ] **Step 1: Add a failing module-size and public-interface test**

Create `tests/unit/storage/test_http_boundaries.py`:

```python
from pathlib import Path

from flashflood_data.storage.http import HttpFetcher


ROOT = Path(__file__).parents[3]
HTTP_DIR = ROOT / "src" / "flashflood_data" / "storage" / "http"


def test_http_fetcher_keeps_public_fetch_interface() -> None:
    assert callable(HttpFetcher.fetch)
    assert callable(HttpFetcher.lock_path)


def test_http_responsibilities_are_split_into_reviewable_modules() -> None:
    required = {"fetcher.py", "resume.py", "transfer.py", "quarantine.py", "redaction.py"}
    assert required <= {path.name for path in HTTP_DIR.glob("*.py")}
    assert all(len(path.read_text(encoding="utf-8").splitlines()) <= 500 for path in HTTP_DIR.glob("*.py"))
```

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/pytest tests/unit/storage/test_http_boundaries.py -q
```

Expected: FAIL because the three collaborator modules are absent and `fetcher.py` exceeds 500 lines.

- [ ] **Step 3: Extract explicit method groups**

Move methods without altering their bodies or call order:

```text
resume.py:
  lock_path, _target_lock, _state_dir, _target_key, _resume_state_path,
  _remote_fingerprint, _target_path, _owned_resume_state, _write_resume_state,
  _write_verified_state, _persist_resume_state, _read_resume_state,
  _publication_state_matches, _discard_resume, _cleanup_success

transfer.py:
  _download_attempt, _stream_full_response, _stream_response, _stream_chunks,
  _response_validator, _compatible_range_length, _retry_after

quarantine.py:
  _quarantine_stale_target, _has_pending_quarantine, _reconcile_pending_quarantine,
  _discard_and_mark_failed, _verify_or_quarantine, _quarantine,
  _quarantine_error_message, _publish_quarantine_evidence
```

Implement the collaborators as private classes instantiated by `HttpFetcher.__init__`. Pass
`ProjectPaths`, `AssetCatalog`, environment, retry configuration, and callbacks explicitly;
do not let a collaborator import `HttpFetcher`. Keep catalog lifecycle and top-level retry
orchestration in `fetcher.py`.

Use these exact constructor seams: `ResumeStore(paths: ProjectPaths, catalog: AssetCatalog)`,
`TransferClient(client: httpx.Client, chunk_size: int)`, and
`QuarantineStore(paths: ProjectPaths, catalog: AssetCatalog)`. After extraction, update the
`uuid4` and `os.link` monkeypatch strings to `flashflood_data.storage.http.quarantine.uuid4`
and `flashflood_data.storage.http.quarantine.os.link`.

Private helper signatures may retain leading underscores, but all dependencies must be method
arguments or constructor fields rather than imports back to `fetcher.py`.

- [ ] **Step 4: Run the complete HTTP behavior contract**

Run:

```bash
.venv/bin/pytest tests/contract/test_http_fetcher.py tests/unit/storage/test_http_boundaries.py -q
```

Expected: PASS; the existing contract remains the characterization suite for retries, ranges,
locks, resume files, quarantine evidence, conflicts, redaction, and recovery.

- [ ] **Step 5: Run full verification and commit**

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
git add src/flashflood_data/storage/http tests/contract/test_http_fetcher.py tests/unit/storage/test_http_boundaries.py
git commit -m "refactor: split http transfer responsibilities"
```

---

### Task 5: Move static sources, spatial primitives, and harmonization

**Files:**
- Create: `src/flashflood_data/static/__init__.py`
- Create: `src/flashflood_data/static/sources/`
- Create: `src/flashflood_data/static/spatial/{raster.py,vector.py}`
- Create: `src/flashflood_data/static/harmonize/`
- Modify: `src/flashflood_data/sources/` into compatibility exports
- Modify: `src/flashflood_data/harmonize/` into compatibility exports
- Modify: `src/flashflood_data/{aoi.py,raster.py,vector.py,registry.py,budget.py}` into compatibility exports
- Modify: static source and harmonization tests to import canonical paths
- Modify: `tests/contract/test_architecture.py`

**Interfaces:**
- Consumes: Task 2 catalog/core and Task 3 storage HTTP/atomic interfaces.
- Produces: canonical static source adapters, raster/vector operations, AOI construction, and harmonizers.

- [ ] **Step 1: Add failing static-package compatibility assertions**

Append to `tests/contract/test_compatibility_imports.py`:

```python
def test_static_source_and_spatial_exports_are_identical() -> None:
    from flashflood_data.sources.cop_dem import CopDemAdapter as LegacyDem
    from flashflood_data.static.sources.cop_dem import CopDemAdapter
    from flashflood_data.raster import validate_raster as legacy_validate
    from flashflood_data.static.spatial.raster import validate_raster

    assert LegacyDem is CopDemAdapter
    assert legacy_validate is validate_raster
```

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/pytest tests/contract/test_compatibility_imports.py::test_static_source_and_spatial_exports_are_identical -q
```

Expected: FAIL because `flashflood_data.static` does not exist.

- [ ] **Step 3: Move leaf modules before adapters**

Move `raster.py` and `vector.py` into `static/spatial/`, `aoi.py` into
`static/harmonize/aoi.py`, `harmonize/hydro.py` and `harmonize/exposure.py` into the canonical
harmonize package. Update imports to `core`, `catalog`, `storage.atomic`, and `static.spatial`.
Replace old files with explicit exports for every currently imported public symbol.

- [ ] **Step 4: Move source registry and adapters**

Move `sources/base.py`, `existing.py`, `cop_dem.py`, `osm.py`, `soilgrids.py`,
`worldcover.py`, and `registry.py` into `static/sources/`. Move `budget.py` to
`static/sources/budget.py`. Keep the old `sources` package modules, `registry.py`, and
`budget.py` as explicit compatibility exports.

- [ ] **Step 5: Split the admin source module**

Create these focused modules under `static/sources/`:

```text
admin_shared.py       classify_unit and shared JSON/text/number helpers
admin_current.py      normalize_current_admin, validate_current_admin, CurrentAdminAdapter
admin_historical.py   normalize_historical_admin, build_sonla_reference_boundary, GadmAdminAdapter
admin.py              explicit facade exporting the existing public names
```

Keep `sources/admin.py` as a compatibility facade importing from `static.sources.admin`.

- [ ] **Step 6: Extend dependency-direction checks**

Add to `tests/contract/test_architecture.py`:

```python
def test_static_domain_does_not_import_cli_or_top_level_orchestration() -> None:
    static_dir = PACKAGE / "static"
    forbidden = ("flashflood_data.cli", "flashflood_data.orchestration")
    offenders = {
        str(path.relative_to(ROOT)): sorted(name for name in imports(path) if name.startswith(forbidden))
        for path in static_dir.rglob("*.py")
        if any(name.startswith(forbidden) for name in imports(path))
    }
    assert offenders == {}
```

- [ ] **Step 7: Run source, spatial, and harmonization verification**

Run:

```bash
.venv/bin/pytest tests/contract tests/unit/test_raster.py tests/unit/test_vector.py tests/unit/test_hydro_aoi.py tests/unit/test_osm_normalize.py tests/integration/test_hydro_harmonize.py tests/integration/test_raster_harmonize.py tests/integration/test_worldcover_harmonize.py tests/integration/test_worldpop_harmonize.py tests/integration/test_soilgrids_harmonize.py tests/integration/test_cop_dem_harmonize.py -q
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 8: Commit Task 5**

```bash
git add src/flashflood_data tests
git commit -m "refactor: organize static sources and harmonization"
```

---

### Task 6: Move static features and split mapping/profile responsibilities

**Files:**
- Create: `src/flashflood_data/static/features/`
- Create: `src/flashflood_data/static/mappings/{__init__.py,admin.py,spatial.py,builder.py}`
- Modify: `src/flashflood_data/derive/` into compatibility exports
- Modify: feature, mapping, population, and profile tests to use canonical imports

**Interfaces:**
- Consumes: canonical static harmonized inputs from Task 5.
- Produces: feature builders keyed by `HYBAS_ID`, mapping builders, static profile assembly, and unchanged Task 15/16 handler outputs.

- [ ] **Step 1: Add failing feature/mapping compatibility tests**

Append to `tests/contract/test_compatibility_imports.py`:

```python
def test_static_feature_and_mapping_exports_are_identical() -> None:
    from flashflood_data.derive.terrain import derive_terrain_features as legacy_terrain
    from flashflood_data.derive.mappings import map_subbasin_commune as legacy_map
    from flashflood_data.static.features.terrain import derive_terrain_features
    from flashflood_data.static.mappings.spatial import map_subbasin_commune

    assert legacy_terrain is derive_terrain_features
    assert legacy_map is map_subbasin_commune
```

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/pytest tests/contract/test_compatibility_imports.py::test_static_feature_and_mapping_exports_are_identical -q
```

Expected: FAIL because canonical feature and mapping modules do not exist.

- [ ] **Step 3: Move focused feature modules**

Move these modules without changing their function bodies:

```text
derive/features.py    -> static/features/config.py
derive/terrain.py     -> static/features/terrain.py
derive/soil.py        -> static/features/soil.py
derive/landcover.py   -> static/features/landcover.py
derive/hydrology.py   -> static/features/hydrology.py
derive/population.py  -> static/features/population.py
derive/static.py      -> static/features/builder.py
```

Create `static/features/__init__.py` with only stable public builders. Replace old modules with
explicit compatibility exports.

- [ ] **Step 4: Split admin and spatial mappings**

Move admin-name parsing and `build_admin_crosswalk` from `derive/mappings.py` into
`static/mappings/admin.py`. Move `_metric_layers`, `map_subbasin_commune`,
`map_subbasin_lines`, and `map_subbasin_points` into `static/mappings/spatial.py`.
Create `static/mappings/__init__.py` that exports the five public mapping functions.

- [ ] **Step 5: Split profile assembly from mapping output orchestration**

Move `_checked_feature_table`, canonicalization/fingerprint helpers,
`assemble_static_profile`, and `write_static_profile` into `static/features/profile.py`.
Move `Task16MapInputs`, map-table construction, provenance assignment, writes, and
`task16_map_handler` into `static/mappings/builder.py`. Keep `derive/profile.py` as an explicit
facade while migration consumers still use it.

- [ ] **Step 6: Run feature and profile verification**

Run:

```bash
.venv/bin/pytest tests/unit/test_feature_config.py tests/unit/test_terrain_features.py tests/unit/test_soil_features.py tests/unit/test_landcover_features.py tests/unit/test_hydrology_features.py tests/unit/test_population.py tests/unit/test_spatial_mappings.py tests/unit/test_static_predictor_tables.py tests/integration/test_static_profile.py -q
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
```

Expected: all commands exit 0; fixture schemas, row order, values, and output checksums remain
identical to the pre-move characterization results.

- [ ] **Step 7: Commit Task 6**

```bash
git add src/flashflood_data tests
git commit -m "refactor: organize static features and mappings"
```

---

### Task 7: Split static QA by subject

**Files:**
- Create: `src/flashflood_data/static/qa/{__init__.py,models.py,shared.py,admin.py,hydro.py,mappings.py,raster.py,population.py,events.py,provenance.py,runner.py,handler.py,report.py,map.py}`
- Modify: `src/flashflood_data/qa/` into compatibility exports
- Modify: `tests/unit/test_qa_checks.py`
- Modify: `tests/integration/test_data_quality_gates.py`
- Modify: `tests/integration/test_qa_report.py`
- Modify: `tests/integration/test_qa_map.py`
- Create: `tests/unit/static/qa/test_module_boundaries.py`

**Interfaces:**
- Consumes: `ProjectPaths`, `StudyAreaConfig`, catalog records, static tables, and raster/vector validators.
- Produces: unchanged `run_quality_gates(paths, config) -> QAReport`, `task17_qa_handler`, report files, and QA map bundle.

- [ ] **Step 1: Add a failing QA-boundary test**

Create `tests/unit/static/qa/test_module_boundaries.py`:

```python
from pathlib import Path


ROOT = Path(__file__).parents[4]
QA_DIR = ROOT / "src" / "flashflood_data" / "static" / "qa"


def test_quality_checks_are_split_by_subject() -> None:
    assert {
        "admin.py",
        "hydro.py",
        "mappings.py",
        "raster.py",
        "population.py",
        "events.py",
        "provenance.py",
        "runner.py",
    } <= {path.name for path in QA_DIR.glob("*.py")}
    assert all(len(path.read_text(encoding="utf-8").splitlines()) <= 500 for path in QA_DIR.glob("*.py"))
```

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/pytest tests/unit/static/qa/test_module_boundaries.py -q
```

Expected: FAIL because `static/qa` does not exist.

- [ ] **Step 3: Move QA value types and shared helpers**

Move `CheckResult`, `QAReport`, and `QualityGateFailure` into `models.py`. Move `_check`,
configuration fingerprinting, file lookup, geometry loading, asset lookup, strict ID parsing,
and shared entity parsing into `shared.py`. Preserve dataclass fields and serialization exactly.

- [ ] **Step 4: Move each check group without changing evaluation order**

Each subject module exports the same concrete interface:
`checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]`.

Move the existing private groups into the matching modules. Implement `runner.py` so
`run_quality_gates` calls them in the original order:

```python
CHECK_GROUPS = (
    admin.checks,
    hydro.checks,
    mappings.checks,
    raster.checks,
    population.checks,
    events.checks,
    provenance.checks,
)
```

Move `task17_qa_handler` to `handler.py`, reporting to `report.py`, and map publication to
`map.py`. Export the legacy public API from `static/qa/__init__.py` and the old `qa/` package.

- [ ] **Step 5: Preserve report equivalence**

Add a characterization assertion to `tests/integration/test_data_quality_gates.py` that compares
the ordered `(check_id, passed, severity, expected, actual, message)` tuples produced through
the legacy and canonical imports on the same fixture.

- [ ] **Step 6: Run QA verification**

```bash
.venv/bin/pytest tests/unit/test_qa_checks.py tests/unit/static/qa/test_module_boundaries.py tests/integration/test_data_quality_gates.py tests/integration/test_qa_report.py tests/integration/test_qa_map.py -q
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
```

Expected: all commands exit 0 and ordered QA results remain identical.

- [ ] **Step 7: Commit Task 7**

```bash
git add src/flashflood_data tests
git commit -m "refactor: split static quality checks"
```

---

### Task 8: Split static workflow and remove the pipeline/composition cycle

**Files:**
- Create: `src/flashflood_data/static/workflow/{__init__.py,stages.py,summary.py,fingerprint.py,dependencies.py,handlers.py,runner.py}`
- Modify: `src/flashflood_data/pipeline.py`
- Modify: `src/flashflood_data/composition.py`
- Modify: pipeline and integration tests to use canonical imports
- Modify: `tests/contract/test_architecture.py`

**Interfaces:**
- Consumes: canonical core/catalog/storage/static packages from Tasks 2-7.
- Produces: unchanged `Stage`, `STATIC_ORDER`, `RunSummary`, `dependency_fingerprint`, `StaticPipeline`, and `default_stage_handlers` behavior without lazy cross-imports.

- [ ] **Step 1: Add failing workflow compatibility and cycle tests**

Append to `tests/contract/test_compatibility_imports.py`:

```python
def test_static_workflow_exports_are_identical() -> None:
    from flashflood_data.pipeline import StaticPipeline as LegacyPipeline
    from flashflood_data.pipeline import Stage as LegacyStage
    from flashflood_data.static.workflow import Stage, StaticPipeline

    assert LegacyPipeline is StaticPipeline
    assert LegacyStage is Stage
```

Append to `tests/contract/test_architecture.py`:

```python
def test_static_workflow_has_no_legacy_pipeline_imports() -> None:
    workflow = PACKAGE / "static" / "workflow"
    forbidden = {"flashflood_data.pipeline", "flashflood_data.composition"}
    offenders = {
        str(path.relative_to(ROOT)): sorted(imports(path) & forbidden)
        for path in workflow.rglob("*.py")
        if imports(path) & forbidden
    }
    assert offenders == {}
```

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/pytest tests/contract/test_compatibility_imports.py::test_static_workflow_exports_are_identical tests/contract/test_architecture.py::test_static_workflow_has_no_legacy_pipeline_imports -q
```

Expected: FAIL because `static.workflow` does not exist.

- [ ] **Step 3: Extract workflow value objects and pure fingerprinting**

Move `Stage` and `STATIC_ORDER` to `stages.py`, `RunSummary` to `summary.py`, and
`dependency_fingerprint` to `fingerprint.py`. Create `workflow/__init__.py`:

```python
from flashflood_data.static.workflow.fingerprint import dependency_fingerprint
from flashflood_data.static.workflow.runner import StaticPipeline
from flashflood_data.static.workflow.stages import STATIC_ORDER, Stage
from flashflood_data.static.workflow.summary import RunSummary

__all__ = ["STATIC_ORDER", "RunSummary", "Stage", "StaticPipeline", "dependency_fingerprint"]
```

- [ ] **Step 4: Split dependency resolution from stage handlers**

Move raw record grouping, Task 15/16 dependency paths, stage fingerprints, and reusable-output
checks from `composition.py` to `dependencies.py`. Move input assembly plus derive/map/QA
handlers to `handlers.py`. Import `Stage` directly from `stages.py`; remove both lazy imports
from the old composition module.

- [ ] **Step 5: Move the runner and install legacy facades**

Move `StaticPipeline` and its private run methods to `runner.py`. Replace `pipeline.py` with
explicit exports from `static.workflow`. Replace `composition.py` with an explicit export of
`default_stage_handlers` from `static.workflow.handlers`.

- [ ] **Step 6: Run workflow verification**

```bash
.venv/bin/pytest tests/unit/test_pipeline.py tests/integration/test_pipeline_recovery.py tests/integration/test_static_pipeline_smoke.py tests/integration/test_static_pipeline_idempotence.py tests/contract/test_architecture.py tests/contract/test_compatibility_imports.py -q
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
```

Expected: all commands exit 0; fixture output, recovery, reuse, and idempotence assertions remain unchanged.

- [ ] **Step 7: Commit Task 8**

```bash
git add src/flashflood_data tests
git commit -m "refactor: split static workflow orchestration"
```

---

### Task 9: Split CLI commands while preserving the entrypoint

**Files:**
- Create: `src/flashflood_data/cli/__init__.py`
- Create: `src/flashflood_data/cli/app.py`
- Create: `src/flashflood_data/cli/commands/__init__.py`
- Create: `src/flashflood_data/cli/commands/static.py`
- Delete: `src/flashflood_data/cli.py`
- Modify: `pyproject.toml` only if import verification shows the existing entrypoint cannot resolve the package export
- Modify: `tests/unit/test_cli.py`
- Modify: `tests/integration/test_data_quality_gates.py`
- Modify: `tests/integration/test_static_pipeline_smoke.py`
- Modify: `tests/integration/test_static_pipeline_idempotence.py`

**Interfaces:**
- Consumes: Task 8 `static.workflow` API.
- Produces: `flashflood_data.cli:app` and the same inventory/fetch/validate/harmonize/derive/map/run-static/cleanup commands and output.

- [ ] **Step 1: Add an entrypoint contract before moving the module**

Append to `tests/unit/test_cli.py`:

```python
from importlib.metadata import entry_points


def test_installed_entrypoint_resolves_the_exported_app() -> None:
    from flashflood_data.cli import app

    script = next(ep for ep in entry_points(group="console_scripts") if ep.name == "flashflood-data")
    assert script.value == "flashflood_data.cli:app"
    assert script.load() is app
```

- [ ] **Step 2: Run the contract before the move**

Run:

```bash
.venv/bin/pytest tests/unit/test_cli.py -q
```

Expected: PASS, establishing the current entrypoint behavior.

- [ ] **Step 3: Create the package and move static commands**

Move Typer option declarations, `_run_stage`, `_emit_summary`, `inventory`, `fetch`, `validate`,
`harmonize`, `derive`, `map_stage`, `run_static`, and `cleanup` into
`cli/commands/static.py`. Create `cli/app.py` with the Typer instance and explicit registration
of those command functions. Create `cli/__init__.py`:

```python
from flashflood_data.cli.app import app

__all__ = ["app"]
```

Delete `cli.py`. Retain `flashflood_data.cli:app` in `pyproject.toml` unless the focused test
proves a packaging change is required.

- [ ] **Step 4: Verify command names and JSON/error contracts**

Run:

```bash
.venv/bin/pytest tests/unit/test_cli.py tests/integration/test_data_quality_gates.py tests/integration/test_static_pipeline_smoke.py tests/integration/test_static_pipeline_idempotence.py -q
.venv/bin/flashflood-data --help
```

Expected: tests pass and help lists `inventory`, `fetch`, `validate`, `harmonize`, `derive`,
`map`, `run-static`, and `cleanup`.

- [ ] **Step 5: Run full verification and commit**

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
git add src/flashflood_data/cli src/flashflood_data/cli.py tests pyproject.toml
git commit -m "refactor: organize cli commands"
```

---

### Task 10: Reorganize Dockerfiles, service bootstrap, and host tools

**Files:**
- Create: `infra/docker/airflow/Dockerfile`
- Create: `infra/docker/spark/Dockerfile`
- Create: `infra/services/postgres/init-multiple-databases.sh`
- Create: `infra/services/polaris/bootstrap.sh`
- Create: `tools/bootstrap/{check_docker_access.sh,init_lakehouse_env.sh,setup_python_runtime.sh}`
- Create: `tools/smoke/{lakehouse.sh,python_runtime.sh,spark_iceberg.sh}`
- Delete: replaced files under `infra/{airflow,spark,postgres,polaris,scripts}/`
- Modify: `compose.yaml`
- Modify: `Makefile`
- Modify: infrastructure unit tests
- Modify: `README.md`

**Interfaces:**
- Consumes: existing Compose services, scripts, Make targets, and pinned images.
- Produces: the same operational commands with Docker build files, container bootstrap, and host tools separated by responsibility.

- [ ] **Step 1: Update infrastructure tests to require target paths**

Change path constants and assertions to:

```python
AIRFLOW_DOCKERFILE = ROOT / "infra/docker/airflow/Dockerfile"
SPARK_DOCKERFILE = ROOT / "infra/docker/spark/Dockerfile"
POLARIS_BOOTSTRAP = ROOT / "infra/services/polaris/bootstrap.sh"
SPARK_SMOKE = ROOT / "tools/smoke/spark_iceberg.sh"
```

Add assertions in `tests/unit/test_lakehouse_compose.py`:

```python
def test_compose_uses_organized_infrastructure_paths() -> None:
    text = COMPOSE.read_text(encoding="utf-8")
    assert "infra/docker/airflow/Dockerfile" in text
    assert "infra/docker/spark/Dockerfile" in text
    assert "infra/services/postgres/init-multiple-databases.sh" in text
    assert "infra/services/polaris/bootstrap.sh" in text
    assert "infra/scripts/" not in text
```

- [ ] **Step 2: Run focused tests and confirm RED**

Run:

```bash
.venv/bin/pytest tests/unit/test_lakehouse_compose.py tests/unit/test_lakehouse_env.py tests/unit/test_lakehouse_operations.py tests/unit/test_python_lakehouse_runtime.py tests/unit/test_spark_runtime.py tests/unit/test_spark_job.py -q
```

Expected: FAIL because target paths do not exist and Compose/Make still reference old paths.

- [ ] **Step 3: Move files and update Compose references**

Move file contents and executable permissions unchanged. Update Dockerfile references and bind
mounts in `compose.yaml`. Keep Airflow build context `.`. Set the Spark build context to
`infra/docker/spark` and its Dockerfile to `Dockerfile`; the Spark Dockerfile uses no repository
`COPY` input. Replace the three hard-coded `./dataset/lakehouse` bind-mount prefixes with
`${LAKEHOUSE_DATA_ROOT:-./dataset/lakehouse}`. The default remains unchanged, while an exported
`LAKEHOUSE_DATA_ROOT` lets runtime verification use disposable state outside the checkout.

- [ ] **Step 4: Update Make targets without renaming them**

Apply these path substitutions in `Makefile`:

```text
infra/scripts/init-lakehouse-env.sh       -> tools/bootstrap/init_lakehouse_env.sh
infra/scripts/check-docker-access.sh      -> tools/bootstrap/check_docker_access.sh
infra/scripts/setup-lakehouse-python.sh   -> tools/bootstrap/setup_python_runtime.sh
infra/scripts/smoke-lakehouse.sh          -> tools/smoke/lakehouse.sh
infra/scripts/smoke-lakehouse-python.sh   -> tools/smoke/python_runtime.sh
infra/scripts/smoke-spark.sh              -> tools/smoke/spark_iceberg.sh
```

Update the scripts' repository-root calculation for their new depth. Do not change commands,
credentials, service names, or cleanup scope.

- [ ] **Step 5: Update README paths and dry-run every Make interface**

Update the repository tree and operational references in `README.md`. Run:

```bash
make --dry-run setup test lint smoke inventory preflight preflight-aoi resolve-live live qa-map lakehouse-python-setup lakehouse-airflow-build lakehouse-python-smoke lakehouse-init lakehouse-up lakehouse-status lakehouse-smoke lakehouse-down spark-build spark-up spark-status spark-down spark-smoke
```

Expected: exit 0 and every printed tool path exists.

- [ ] **Step 6: Run infrastructure and repository verification**

```bash
.venv/bin/pytest tests/unit/test_lakehouse_compose.py tests/unit/test_lakehouse_env.py tests/unit/test_lakehouse_operations.py tests/unit/test_python_lakehouse_runtime.py tests/unit/test_spark_runtime.py tests/unit/test_spark_job.py tests/unit/test_makefile.py tests/contract/infra/test_docker_context.py -q
docker compose config --quiet
.venv/bin/pytest -q
.venv/bin/ruff check src tests
git diff --check
```

Expected: all commands exit 0. Runtime smoke commands are executed in Step 7 only when Docker
access and built images are available.

- [ ] **Step 7: Run available runtime smoke checks against disposable state**

Run these only when Docker access and required images are available:

```bash
export LAKEHOUSE_DATA_ROOT=/tmp/floodlake-reorg-lakehouse
make lakehouse-up
make lakehouse-smoke
make lakehouse-python-smoke
make spark-smoke
make spark-down
make lakehouse-down
```

Expected: each available command exits 0, and all service state is written below
`/tmp/floodlake-reorg-lakehouse`. If Docker or image access is unavailable, record the exact
missing prerequisite in the task handoff; do not weaken structural tests or alter service
configuration to bypass it.

- [ ] **Step 8: Commit Task 10**

```bash
git add .dockerignore compose.yaml Makefile README.md infra tools tests
git commit -m "refactor: organize infrastructure and tools"
```

---

### Task 11: Move tests with their domains and remove migration shims

**Files:**
- Move: unit tests into `tests/unit/{core,catalog,storage,static}/`
- Move: provider tests into `tests/contract/static/sources/`
- Move: static integration tests into `tests/integration/static/`
- Modify: test `ROOT` calculations and imports
- Delete: obsolete compatibility modules under the old package paths
- Delete: `tests/contract/test_compatibility_imports.py`
- Modify: `tests/contract/test_architecture.py`
- Modify: `README.md`
- Modify: `docs/DATA_CATALOG.md` only for code-path references; data facts remain unchanged

**Interfaces:**
- Consumes: canonical module paths established in Tasks 2-10.
- Produces: the final package/test tree with no repository imports through migration shims.

- [ ] **Step 1: Make architecture checks reject legacy imports**

Add to `tests/contract/test_architecture.py`:

```python
LEGACY_MODULES = {
    "flashflood_data.aoi",
    "flashflood_data.budget",
    "flashflood_data.composition",
    "flashflood_data.config",
    "flashflood_data.derive",
    "flashflood_data.harmonize",
    "flashflood_data.http",
    "flashflood_data.inventory",
    "flashflood_data.io_atomic",
    "flashflood_data.models",
    "flashflood_data.paths",
    "flashflood_data.pipeline",
    "flashflood_data.qa",
    "flashflood_data.raster",
    "flashflood_data.registry",
    "flashflood_data.sources",
    "flashflood_data.vector",
}


def test_repository_uses_canonical_module_paths() -> None:
    roots = (ROOT / "src", ROOT / "tests", ROOT / "airflow", ROOT / "spark")
    offenders: dict[str, list[str]] = {}
    for base in roots:
        for path in base.rglob("*.py"):
            used = sorted(
                name
                for name in imports(path)
                if any(name == legacy or name.startswith(f"{legacy}.") for legacy in LEGACY_MODULES)
            )
            if used:
                offenders[str(path.relative_to(ROOT))] = used
    assert offenders == {}
```

- [ ] **Step 2: Run the architecture test and confirm RED**

Run:

```bash
.venv/bin/pytest tests/contract/test_architecture.py::test_repository_uses_canonical_module_paths -q
```

Expected: FAIL and list remaining imports that still use compatibility paths.

- [ ] **Step 3: Move tests and update canonical imports**

Use this directory mapping:

```text
tests/unit/test_config.py, test_budget.py                       -> tests/unit/core/
tests/unit/test_catalog.py, test_inventory.py                  -> tests/unit/catalog/
tests/unit/test_io_atomic.py                                   -> tests/unit/storage/
static feature, mapping, AOI, raster, vector and QA unit tests -> tests/unit/static/
tests/contract/test_*_adapter.py, test_http_fetcher.py          -> tests/contract/static/sources/ and tests/contract/storage/
static pipeline/harmonize/profile/QA integration tests         -> tests/integration/static/
```

Recalculate each `ROOT = Path(__file__).parents[n]` from its final location. Change imports to
canonical module paths. Keep fixture paths and test names unchanged.

- [ ] **Step 4: Remove old compatibility files**

Delete the old shim modules and packages once the architecture test reports no consumers. Keep
the intentional stable facades:

```text
flashflood_data.catalog        package public API
flashflood_data.cli            console entrypoint public API
flashflood_data.static.workflow public workflow API
```

Delete `tests/contract/test_compatibility_imports.py`, because migration-only paths are no
longer supported. Update README and code-path references in `docs/DATA_CATALOG.md`.

- [ ] **Step 5: Verify the installed package has no accidental namespace conflicts**

Run:

```bash
.venv/bin/python -c 'from flashflood_data.cli import app; from flashflood_data.catalog import AssetCatalog; from flashflood_data.static.workflow import StaticPipeline; print(app.info.name, AssetCatalog.__name__, StaticPipeline.__name__)'
.venv/bin/flashflood-data --help
.venv/bin/pytest tests/contract/test_architecture.py -q
```

Expected: imports succeed, CLI help renders, and architecture tests pass.

- [ ] **Step 6: Run final static and infrastructure verification**

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests
.venv/bin/pytest tests/integration/static/test_static_pipeline_smoke.py tests/integration/static/test_static_pipeline_idempotence.py tests/integration/static/test_pipeline_recovery.py -q
docker compose config --quiet
git diff --check
```

Expected: all commands exit 0; the integration tests use temporary fixture roots and do not
write to the repository `dataset/` tree.

- [ ] **Step 7: Confirm no dataset file changed**

Run:

```bash
find dataset -type f -printf '%p\t%s\t%T@\n' | sort > /tmp/floodlake-reorg-dataset-after.txt
diff -u /tmp/floodlake-reorg-dataset-before.txt /tmp/floodlake-reorg-dataset-after.txt
```

Expected: `diff` exits 0 with no output. If the baseline is unavailable, stop and recreate the
worktree from the original commit before claiming the no-change acceptance criterion.

- [ ] **Step 8: Commit Task 11**

```bash
git add src tests README.md docs/DATA_CATALOG.md
git commit -m "refactor: finalize repository package layout"
```

---

## Follow-on Plans

After this plan passes its final gate, create separate implementation plans in this order:

1. MinIO source-landing foundation and static canonical-source backfill, including BasinATLAS
   archive-only migration and catalog-aware cleanup.
2. Dynamic raw landing for Open-Meteo IFS, ERA5-Land, and GSMaP, stopping at immutable provider
   payloads plus operational manifests.
3. Canonical and Iceberg schemas after the schema review gate.

Those plans implement Section 8 of the design spec and must not be folded into a refactor task.
