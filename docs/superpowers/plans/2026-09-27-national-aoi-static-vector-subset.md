# National AOI and Static Vector Subset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Vietnam L12-plus-one-upstream AOI and publish only its HydroBASINS, BasinATLAS, and HydroRIVERS subsets to static Raw and Bronze.

**Architecture:** Extend the existing AOI build stage with a fifth deterministic output. Add one focused hydro-subset module that reads the audited local provider Shapefiles, selects complete features, writes a run-scoped Shapefile, and hands it to the existing deterministic ZIP publisher. Keep provider originals in local `dataset/`; MinIO receives only curated national subsets with complete selection provenance.

**Tech Stack:** Python 3.11, GeoPandas, Shapely, Pyogrio/GDAL, Pydantic 2, PyArrow, PyIceberg, pytest.

**Spec:** `docs/superpowers/specs/2026-09-27-weather-raster-slice-and-aoi-design.md`

## Global Constraints

- HydroBASINS level is exactly `12` and upstream expansion is exactly one direct hop from `config/study_area.yaml`.
- Preserve `core_aoi`, `hydrological_aoi`, `environmental_aoi`, and `exposure_aoi`; add `vietnam_hydrological_aoi`.
- HydroBASINS and BasinATLAS use the same selected `HYBAS_ID` set.
- HydroRIVERS features are selected by intersection and retain their complete source geometry.
- MinIO Raw receives curated subsets; provider originals remain local and are identified by checksum in the manifest.
- DEM, WorldCover, SoilGrids, WorldPop, OSM, and administrative source scopes do not change.
- Tests use local fixtures and never download data.

---

### Task 1: Build and persist the fifth AOI

**Files:**
- Modify: `src/flashflood_data/static/harmonize/aoi.py`
- Modify: `src/flashflood_data/static/workflow/runner.py`
- Modify: `tests/unit/static/test_hydro_aoi.py`
- Modify: `tests/unit/static/test_pipeline.py`
- Modify: `tests/integration/static/test_lakehouse_aoi.py`

**Interfaces:**
- Consumes: `select_basins_with_upstream(basins, geometry, *, hops) -> GeoDataFrame` and `StudyAreaConfig`.
- Produces: `StudyAreas.vietnam_hydrological: BaseGeometry` and `vietnam_hydrological_aoi.geoparquet`.

- [ ] **Step 1: Write failing unit tests for the national selection and fifth file**

```python
def test_write_study_areas_keeps_four_outputs_and_adds_national_hydrological(tmp_path):
    areas = StudyAreas(
        core=core,
        hydrological=sonla_hydro,
        environmental=environmental,
        exposure=exposure,
        vietnam=vietnam,
        vietnam_hydrological=vietnam_hydro,
    )
    names = {path.name for path in write_study_areas(areas, tmp_path)}
    assert names == {
        "core_aoi.geoparquet",
        "hydrological_aoi.geoparquet",
        "environmental_aoi.geoparquet",
        "exposure_aoi.geoparquet",
        "vietnam_hydrological_aoi.geoparquet",
    }
```

Add a runner test with a three-basin chain where two basins intersect Vietnam and the third is their direct upstream neighbor. Assert all three appear in the national union and that the Son La AOI remains based only on the Son La seed plus one hop.

- [ ] **Step 2: Run the focused tests and confirm the missing field/file failures**

Run: `pytest tests/unit/static/test_hydro_aoi.py tests/unit/static/test_pipeline.py tests/integration/static/test_lakehouse_aoi.py -q`

Expected: FAIL because `StudyAreas` has no `vietnam_hydrological` field and the fifth file is absent.

- [ ] **Step 3: Extend the AOI contracts and runner**

Change the builder signature explicitly:

```python
def build_study_areas(
    core: BaseGeometry,
    selected_basins: gpd.GeoDataFrame,
    vietnam: BaseGeometry,
    vietnam_selected_basins: gpd.GeoDataFrame,
    config: StudyAreaConfig,
) -> StudyAreas:
    if vietnam_selected_basins.empty:
        raise ValueError("cannot construct national hydrological AOI from an empty selection")
    vietnam_hydrological_metric = vietnam_selected_basins.to_crs(
        config.processing_crs
    ).geometry.union_all()
    return StudyAreas(
        core=_project_geometry(core_metric, config.processing_crs, config.storage_crs),
        hydrological=_project_geometry(
            hydrological_metric, config.processing_crs, config.storage_crs
        ),
        environmental=_project_geometry(
            environmental_metric, config.processing_crs, config.storage_crs
        ),
        exposure=_project_geometry(
            exposure_metric, config.processing_crs, config.storage_crs
        ),
        vietnam=_project_geometry(
            vietnam_metric, config.processing_crs, config.storage_crs
        ),
        vietnam_hydrological=_project_geometry(
            vietnam_hydrological_metric, config.processing_crs, config.storage_crs
        ),
    )
```

In `_run_aoi`, calculate both selections from the same L12 frame:

```python
selected = select_basins_with_upstream(basins, core, hops=self.study_area.upstream_hops)
vietnam_selected = select_basins_with_upstream(
    basins, vietnam, hops=self.study_area.upstream_hops
)
areas = build_study_areas(
    core, selected, vietnam, vietnam_selected, self.study_area
)
summary.metrics.update({
    f"selected_l{level}": len(selected),
    f"selected_vietnam_l{level}": len(vietnam_selected),
})
```

Write the fifth output from `write_study_areas` with `aoi=vietnam_hydrological_aoi` and the configured storage CRS.

- [ ] **Step 4: Run the focused tests**

Run: `pytest tests/unit/static/test_hydro_aoi.py tests/unit/static/test_pipeline.py tests/integration/static/test_lakehouse_aoi.py -q`

Expected: PASS.

- [ ] **Step 5: Commit the AOI change**

```bash
git add src/flashflood_data/static/harmonize/aoi.py src/flashflood_data/static/workflow/runner.py tests/unit/static/test_hydro_aoi.py tests/unit/static/test_pipeline.py tests/integration/static/test_lakehouse_aoi.py
git commit -m "feat: add Vietnam hydrological AOI"
```

---

### Task 2: Implement deterministic national hydro subsets

**Files:**
- Create: `src/flashflood_data/static/sources/hydro_subset.py`
- Create: `src/flashflood_data/static/sources/hydro_fields.py`
- Create: `tests/unit/static/test_hydro_subset.py`
- Add fixtures: `tests/fixtures/hydro/national_basins.geojson`
- Add fixtures: `tests/fixtures/hydro/national_atlas.geojson`
- Add fixtures: `tests/fixtures/hydro/national_rivers.geojson`

**Interfaces:**
- Consumes: one source Shapefile path, `vietnam_hydrological_aoi.geoparquet`, selected HydroBASINS IDs, output directory.
- Produces: `HydroSubsetResult(path: Path, source_feature_count: int, selected_feature_count: int, selected_hybas_ids: tuple[int, ...], aoi_checksum: str, selection_version: str)`.
- Produces: `build_hydro_subset(source_id: str, source_path: Path, aoi_path: Path, output_dir: Path, *, selected_hybas_ids: Collection[int] | None = None) -> HydroSubsetResult`.

- [ ] **Step 1: Write failing selection tests**

```python
def test_basinatlas_uses_hydrobasins_ids_and_allowlisted_fields(tmp_path):
    result = build_hydro_subset(
        "basinatlas_v10",
        atlas_path,
        aoi_path,
        tmp_path,
        selected_hybas_ids={101, 102},
    )
    layer = gpd.read_file(result.path)
    assert set(layer.HYBAS_ID) == {101, 102}
    assert set(layer.columns) == set(BASINATLAS_RAW_FIELDS) | {"geometry"}

def test_hydrorivers_selects_intersections_without_clipping(tmp_path):
    result = build_hydro_subset("hydrorivers_v10", rivers_path, aoi_path, tmp_path)
    layer = gpd.read_file(result.path)
    original = gpd.read_file(rivers_path).set_index("HYRIV_ID")
    assert layer.set_index("HYRIV_ID").geometry[7].equals(original.geometry[7])
```

Also assert the result is sorted by `HYBAS_ID`/`HYRIV_ID`, empty selections fail, missing CRS fails, duplicate feature IDs fail, and two executions produce identical feature order and result metadata.

- [ ] **Step 2: Run the new tests and confirm import failure**

Run: `pytest tests/unit/static/test_hydro_subset.py -q`

Expected: FAIL because `hydro_subset` and `hydro_fields` do not exist.

- [ ] **Step 3: Define the shared BasinATLAS field contract**

Move the current Bronze `_BASINATLAS_FIELDS` names into:

```python
BASINATLAS_RAW_FIELDS: Final[tuple[str, ...]] = (
    "HYBAS_ID", "NEXT_DOWN", "NEXT_SINK", "MAIN_BAS", "DIST_SINK",
    "DIST_MAIN", "SUB_AREA", "UP_AREA", "PFAF_ID", "SORT",
    "ele_mt_sav", "ele_mt_smn", "ele_mt_smx", "slp_dg_sav",
    "sgr_dk_sav", "lka_pc_sse", "dor_pc_pva", "rev_mc_usu",
    "for_pc_sse", "crp_pc_sse", "glc_pc_s22", "wet_pc_sg1",
    "wet_pc_sg2", "inu_pc_slt", "gwt_cm_sav", "run_mm_syr",
    "dis_m3_pyr", "dis_m3_pmx", "pop_ct_ssu", "ppd_pk_sav",
)
```

Use the actual capitalization reported by the source driver, matching case-insensitively and preserving the provider column names in the output.

- [ ] **Step 4: Implement the subset builder**

Read only required columns with `pyogrio.read_dataframe`, normalize both layers to EPSG:4326 for intersection, select basin IDs exactly, sort deterministically, and write a Shapefile under the run output directory. Do not intersect river geometries with the polygon.

```python
@dataclass(frozen=True)
class HydroSubsetResult:
    path: Path
    source_feature_count: int
    selected_feature_count: int
    selected_hybas_ids: tuple[int, ...]
    aoi_checksum: str
    selection_version: str = "vietnam-l12-h1-v1"
```

- [ ] **Step 5: Run the new tests**

Run: `pytest tests/unit/static/test_hydro_subset.py -q`

Expected: PASS.

- [ ] **Step 6: Commit the subset module**

```bash
git add src/flashflood_data/static/sources/hydro_subset.py src/flashflood_data/static/sources/hydro_fields.py tests/unit/static/test_hydro_subset.py tests/fixtures/hydro/national_basins.geojson tests/fixtures/hydro/national_atlas.geojson tests/fixtures/hydro/national_rivers.geojson
git commit -m "feat: build curated national hydro subsets"
```

---

### Task 3: Publish curated subsets through static landing

**Files:**
- Modify: `src/flashflood_data/orchestration/landing/sources.py`
- Modify: `src/flashflood_data/orchestration/landing/models.py`
- Modify: `config/landing/static.yaml`
- Modify: `tests/unit/orchestration/landing/test_sources.py`
- Modify: `tests/unit/orchestration/landing/test_service.py`
- Modify: `tests/contract/infra/test_static_source_landing_dag.py`

**Interfaces:**
- Consumes: `build_hydro_subset(source_id: str, source_path: Path, aoi_path: Path, output_dir: Path, *, selected_hybas_ids: Collection[int] | None = None) -> HydroSubsetResult` from Task 2 and `build_deterministic_zip(members: tuple[Path, ...], output_path: Path) -> BundleResult`.
- Produces: `_prepare_hydro_subset(policy, records, staging_root, run_id) -> tuple[PreparedObject, ...]`.
- Produces manifest selection keys `spatial_scope_id`, `aoi_checksum`, `selection_version`, `source_feature_count`, `selected_feature_count`, and `selected_hybas_ids_checksum`.

- [ ] **Step 1: Write failing landing tests**

```python
def test_prepare_hydrobasins_publishes_subset_bundle_with_provenance(
    hydro_policy, hydro_records, staging_root
):
    prepared = prepare_source_objects(
        hydro_policy, hydro_records, staging_root=staging_root, run_id="run-1"
    )
    assert len(prepared) == 1
    assert prepared[0].filename == "hydrobasins_l12_vietnam_h1.zip"
    assert prepared[0].selection["spatial_scope_id"].startswith("vietnam-l12-h1-")
    assert prepared[0].selection["source_feature_count"] == 4
    assert prepared[0].selection["selected_feature_count"] == 3
    assert prepared[0].source_archive_checksum == original_checksum
```

Add a BasinATLAS assertion that its `selected_hybas_ids_checksum` equals HydroBASINS, a HydroRIVERS assertion that the selected reach count is recorded, and two precondition tests asserting missing/invalid national AOI raises `SourcePreconditionError("vietnam_hydrological_aoi_missing")` or `SourcePreconditionError("vietnam_hydrological_aoi_invalid")` before publication.

- [ ] **Step 2: Run focused landing tests**

Run: `pytest tests/unit/orchestration/landing/test_sources.py tests/unit/orchestration/landing/test_service.py -q`

Expected: FAIL because hydro policies still package the complete source Shapefile.

- [ ] **Step 3: Add the hydro subset landing mode**

Set the three policies to `mode: hydro_subset_bundle` and distinct output names. In `prepare_source_objects`, dispatch this mode to `_prepare_hydro_subset`. HydroBASINS preparation computes and returns the selected ID list; BasinATLAS reads that same HydroBASINS source and applies the identical list; HydroRIVERS uses spatial intersection.

Keep `PreparedObject.asset_id` tied to the audited provider asset, but include AOI checksum and selection version in `selection`; the existing object identity then changes when the curated content or scope changes.

- [ ] **Step 4: Store selection evidence without oversized manifests**

Do not put thousands of IDs in JSON. Store:

```python
selection = {
    "basin_level": 12,
    "upstream_hops": 1,
    "spatial_scope_id": scope_id,
    "aoi_checksum": result.aoi_checksum,
    "selection_version": result.selection_version,
    "source_feature_count": result.source_feature_count,
    "selected_feature_count": result.selected_feature_count,
    "selected_hybas_ids_checksum": checksum_ids(result.selected_hybas_ids),
}
```

The deterministic ZIP member list and curated payload checksum remain in the existing manifest fields.

- [ ] **Step 5: Run landing and DAG contract tests**

Run: `pytest tests/unit/orchestration/landing/test_sources.py tests/unit/orchestration/landing/test_service.py tests/contract/infra/test_static_source_landing_dag.py -q`

Expected: PASS.

- [ ] **Step 6: Commit the landing integration**

```bash
git add src/flashflood_data/orchestration/landing/sources.py src/flashflood_data/orchestration/landing/models.py config/landing/static.yaml tests/unit/orchestration/landing/test_sources.py tests/unit/orchestration/landing/test_service.py tests/contract/infra/test_static_source_landing_dag.py
git commit -m "feat: land national hydro vector subsets"
```

---

### Task 4: Align Bronze parsing and one-time reset behavior

**Files:**
- Modify: `src/flashflood_data/orchestration/bronze/parsers.py`
- Modify: `config/bronze/static.yaml`
- Modify: `tests/unit/orchestration/bronze/test_parsers.py`
- Modify: `tests/unit/orchestration/bronze/test_service.py`
- Modify: `tests/contract/storage/test_meta_bronze_schemas.py`

**Interfaces:**
- Consumes: curated deterministic ZIPs and `BASINATLAS_RAW_FIELDS`.
- Produces: parser version `v2-vietnam-l12-h1` for the three hydro sources.

- [ ] **Step 1: Write failing parser tests for curated bundles**

```python
def test_curated_basinatlas_never_reintroduces_unapproved_columns(bundle):
    rows = list(
        parse_vector_object(
            source_row("basinatlas_v10"),
            bundle,
            run_id="run-1",
            parser_version="v2-vietnam-l12-h1",
        )
    )
    attrs = json.loads(rows[0]["attributes_json"])
    assert set(attrs) <= set(BASINATLAS_RAW_FIELDS)
    assert "cly_pc_sav" not in attrs
```

Add a service test proving a `v1` lineage entry does not suppress processing when config requires `v2-vietnam-l12-h1`.

- [ ] **Step 2: Run focused Bronze tests**

Run: `pytest tests/unit/orchestration/bronze/test_parsers.py tests/unit/orchestration/bronze/test_service.py -q`

Expected: FAIL until the parser imports the shared allowlist and config versions change.

- [ ] **Step 3: Share the field contract and bump parser versions**

Remove the duplicate private tuple from the parser and import `BASINATLAS_RAW_FIELDS`. Set all three hydro source parser versions to `v2-vietnam-l12-h1`. Preserve the current object-scoped atomic replacement and batch commit behavior.

- [ ] **Step 4: Run the focused tests**

Run: `pytest tests/unit/orchestration/bronze/test_parsers.py tests/unit/orchestration/bronze/test_service.py tests/contract/storage/test_meta_bronze_schemas.py -q`

Expected: PASS.

- [ ] **Step 5: Commit the Bronze alignment**

```bash
git add src/flashflood_data/orchestration/bronze/parsers.py config/bronze/static.yaml tests/unit/orchestration/bronze/test_parsers.py tests/unit/orchestration/bronze/test_service.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat: parse curated Vietnam hydro objects"
```

---

### Task 5: Document, verify, and expose the reset command

**Files:**
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`
- Modify: `docs/schema_contract/data.md`
- Modify: `docs/son_la_flood_class_diagram.drawio`
- Modify: `docs/DATA_CATALOG.md`
- Modify: `tests/unit/test_readme_commands.py`
- Modify: `tests/unit/test_makefile.py`

**Interfaces:**
- Consumes: the completed static implementation.
- Produces: operator instructions for rebuilding AOIs, resetting old lakehouse state, and rerunning the two static DAGs.

- [ ] **Step 1: Write failing documentation contract tests**

Assert README contains `vietnam_hydrological_aoi.geoparquet`, identifies the three curated sources, and provides the existing Compose reset command exactly as an opt-in operator action:

```bash
docker compose down -v --remove-orphans
make lakehouse-up
```

Also assert the Makefile test still exposes the AOI build command and does not add a separate AOI DAG.

- [ ] **Step 2: Run docs tests and confirm failure**

Run: `pytest tests/unit/test_readme_commands.py tests/unit/test_makefile.py -q`

Expected: FAIL because the new AOI and curated Raw scope are undocumented.

- [ ] **Step 3: Update docs and draw.io source XML**

Document this operator order:

```text
build AOIs -> trigger static_source_landing -> trigger static_source_to_bronze
```

State that the reset deletes local Compose volumes and is required once for existing continent-wide development data, never executed by a DAG. Update table row estimates only from measured output after a fixture or local run; do not invent production counts.

- [ ] **Step 4: Run static verification**

Run:

```bash
pytest tests/unit/static/test_hydro_aoi.py tests/unit/static/test_hydro_subset.py tests/unit/orchestration/landing tests/unit/orchestration/bronze tests/contract/infra/test_static_source_landing_dag.py tests/contract/infra/test_static_source_to_bronze_dag.py tests/contract/storage/test_meta_bronze_schemas.py -q
ruff check src/flashflood_data/static src/flashflood_data/orchestration/landing src/flashflood_data/orchestration/bronze tests/unit/static/test_hydro_subset.py
docker compose config --quiet
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit documentation and verification contracts**

```bash
git add README.md docs/pipeline_architecture_and_roadmap.md docs/schema_contract/data.md docs/son_la_flood_class_diagram.drawio docs/DATA_CATALOG.md tests/unit/test_readme_commands.py tests/unit/test_makefile.py
git commit -m "docs: describe national static vector scope"
```
