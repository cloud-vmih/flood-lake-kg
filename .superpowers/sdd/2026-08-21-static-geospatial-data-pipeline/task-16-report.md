# Task 16 Report — Basin Mappings, WorldPop Evidence, and Static Profile

## Scope

Implemented the Task 16 MAP-stage products without changing the Task 6 historical
commune crosswalk: metric spatial mappings in `derive/mappings.py`, native-grid
Core-AOI WorldPop aggregation in `derive/population.py`, and strict final-profile
composition/publication in `derive/profile.py`.

`task16_map_handler` is the concrete R-004 integration seam. It only executes at
`Stage.MAP` (checked as `"map"`), requires a composed owner source, and performs
no default input discovery. It writes all six relationship Parquets, the population
Parquet, and `subbasin_static_feature.geoparquet`; no `PROFILE` stage was created.

## TDD evidence

### RED

1. Before implementations existed:

   ```text
   .venv/bin/pytest tests/unit/test_spatial_mappings.py tests/unit/test_population.py tests/integration/test_static_profile.py -q
   ```

   failed at collection with the expected missing imports:
   `map_subbasin_commune`, `flashflood_data.derive.population`, and
   `flashflood_data.derive.profile`.

2. Before the MAP factory existed:

   ```text
   .venv/bin/pytest tests/integration/test_static_profile.py::test_task16_map_handler_publishes_all_relationship_tables_and_profile -q
   ```

   failed at collection because `Task16MapInputs` was not importable.

3. Before bridge geometry support existed:

   ```text
   .venv/bin/pytest tests/unit/test_spatial_mappings.py::test_bridge_mapping_can_preserve_the_unsimplified_relationship_geometry -q
   ```

   failed with the expected `TypeError`: `map_subbasin_lines()` did not accept
   `include_relationship_geometry`.

### GREEN

Focused verification after the minimal implementations:

```text
.venv/bin/pytest tests/unit/test_spatial_mappings.py tests/unit/test_population.py tests/integration/test_static_profile.py -q
10 passed in 0.23s
```

The real geometry/raster fixtures independently assert:

- two 50 m × 100 m basin intersections have 0.005 km² each, basin fractions of
  1.0, and commune fractions of 0.5;
- a 100 m clipped line has length 0.1 km;
- a point exactly on a shared basin boundary emits both `nearest_boundary_tie`
  rows;
- a 3-cell native count raster contributes only 10 + 20 = 30 inside the Core AOI;
- a shared-boundary raster center is assigned once to lower `HYBAS_ID` 10;
- required Task 15 tables must have one row for every selected L10, whereas an
  absent optional population observation remains null; and
- the factory publishes all Task 16 MAP assets and no other stage is used.

## Tie, double-counting, and join decisions

- All area and line overlays use EPSG:32648. Analytical geometries are not
  simplified. The bridge relationship product serializes its exact clipped geometry
  as WKT because that product explicitly requires relationship geometry.
- Point `covers` relationships emit every matching basin. When a point touches a
  shared boundary, every tied row is retained with `boundary_case=true` and
  `relationship_type="nearest_boundary_tie"`; no arbitrary basin choice is made.
- WorldPop values are kept as native person counts. Each raster cell center is
  considered only after Core-AOI inclusion and selected-L10/Core intersection. A
  center is assigned once; if it is covered by several clipped zones, the lowest
  numeric `HYBAS_ID` wins and its `boundary_center_tie_pixel_count` records the
  evidence. No resampling, value redistribution, or area weighting occurs.
- `assemble_static_profile` starts from checked selected L10 basins, rejects
  nonfinite/duplicate basin IDs, rejects duplicate feature keys, rejects required
  Task 15 tables missing selected basin keys, and rejects feature keys outside the
  selected set. It left-joins optional groups and records their absent observations
  in `quality_flags_json`, so selected L10 rows are never dropped.

## Final verification

```text
.venv/bin/pytest -q
291 passed in 8.46s

.venv/bin/ruff check src/flashflood_data/derive/mappings.py src/flashflood_data/derive/population.py src/flashflood_data/derive/profile.py tests/unit/test_spatial_mappings.py tests/unit/test_population.py tests/integration/test_static_profile.py
All checks passed!

.venv/bin/pip check
No broken requirements found.
```

`pip check` also printed a non-fatal warning that `/home/cloud/.cache/pip` is not
writable by the active user, so pip disabled its cache. It does not affect the
environment's dependency consistency.

## Concerns

`Task16MapInputs` is intentionally explicit and has no default cross-source asset
discovery. Task 19 must supply the resolved layers, source-asset IDs, and owner
source when registering the MAP handler. Existing three untracked DOCX files were
left untouched.

## Fix round 1 — review regressions

### Root causes and RED evidence

Review findings were reproduced with five real public-boundary regressions before
implementation changes:

```text
.venv/bin/pytest \
  tests/unit/test_spatial_mappings.py::test_line_mapping_rejects_source_columns_that_would_overwrite_evidence \
  tests/unit/test_population.py::test_population_reports_native_geographic_x_y_resolution_without_false_metres \
  tests/integration/test_static_profile.py::test_profile_rejects_event_evidence_group_to_prevent_static_label_leakage \
  tests/integration/test_static_profile.py::test_profile_fingerprint_tracks_content_and_geometry_but_not_row_order \
  tests/integration/test_static_profile.py::test_task16_map_handler_publishes_all_relationship_tables_and_profile -q
5 failed
```

The failures established the causes: line rows permitted source `HYBAS_ID` and
evidence-field collisions; population exposed only scalar `source_resolution_m`;
the profile accepted an `events` group; fingerprints were schema-only; and the
handler did not propagate Task15 `source_asset_ids` from `Task16MapInputs` into
profile-table attributes.

### Fix decisions

- Profile groups are now exactly allowlisted: `terrain`, `soil`, `landcover`,
  `hydrology`, plus optional `population`. Event, label, outcome, and status field
  tokens are rejected as an additional no-leakage guard.
- The handler explicitly applies its supplied source assets to every Task15 table
  as well as population before assembly. The final source-asset JSON therefore
  contains all five feature groups when population is applicable.
- The dependency fingerprint now canonically hashes selected-basin CRS, IDs,
  attributes, WKB geometry; every checked feature/population table's schema and
  values; table provenance; and profile configuration. Rows are sorted by
  `HYBAS_ID`, so input row-order permutations do not affect it, while geometry or
  value mutations do.
- Line-overlay output names are reserved before copying source attributes. A
  collision with `HYBAS_ID`, length, flags, provenance, boundary evidence, or
  relationship geometry raises a clear error rather than overwriting derived data.
- WorldPop now reports native `source_resolution_x`, `source_resolution_y`,
  `source_resolution_unit`, and `source_crs`. This accurately represents an
  EPSG:4326 non-square grid as degrees rather than a false scalar metre value.
- R-035 is deliberately deferred/carried: the explicit MAP factory remains; Task19
  owns catalog discovery and default registration, so no guessed auto-registration
  was added.

### GREEN and final verification

```text
.venv/bin/pytest tests/unit/test_spatial_mappings.py tests/unit/test_population.py tests/integration/test_static_profile.py -q
14 passed in 0.34s

.venv/bin/ruff check src/flashflood_data/derive/mappings.py src/flashflood_data/derive/population.py src/flashflood_data/derive/profile.py tests/unit/test_spatial_mappings.py tests/unit/test_population.py tests/integration/test_static_profile.py
All checks passed!

.venv/bin/pytest -q
295 passed in 8.46s

.venv/bin/pip check
No broken requirements found.
```

`pip check` again emitted only the non-fatal disabled-cache ownership warning for
`/home/cloud/.cache/pip`.
