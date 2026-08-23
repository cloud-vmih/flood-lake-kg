# Task 15 report — static basin predictors

## RED / GREEN evidence

The initial focused test run was RED during collection because all four required
derive modules were absent:

```text
ModuleNotFoundError: No module named 'flashflood_data.derive.terrain'
ModuleNotFoundError: No module named 'flashflood_data.derive.soil'
ModuleNotFoundError: No module named 'flashflood_data.derive.landcover'
ModuleNotFoundError: No module named 'flashflood_data.derive.hydrology'
```

After the first minimal implementation, the four focused modules passed
`7 passed in 0.19s`. A subsequent RED test showed the missing retained metric
DEM product (`assert 0 == 1`); it passed after block-wise metric-DEM retention.
The table writer and explicit DERIVE-handler factory were also introduced
test-first: their import failures were observed before implementation.

## Algorithms and units

- Terrain: read a bilinear `WarpedVRT` in `EPSG:32648` at a 30 m target grid,
  retain that working GeoTIFF under `dataset/derived/terrain/` when the input is
  inside a project dataset, calculate metric x/y gradients, and report elevation
  min/mean/max/relief in metres plus slope mean/p90/max in degrees. DEM coverage
  and valid-pixel counts accompany every basin row.
- Soil: retain property/depth/statistic labels in `config/features.yaml`; use the
  exact raw-integer divisors (`clay/sand/silt/cfvo/water=10`, `bdod=100`), then
  thickness-weight the 5/10/15 cm layers for 0–30 cm and the 30/40 cm layers for
  30–100 cm. Mean and uncertainty are independently traceable through column
  names. Each output records covered/valid pixel counts and coverage fraction.
- Land cover: retain the native WorldCover grid (no warp), use categorical codes
  and stable configured class names, normalize fractions over recognized valid
  classes, and report nodata and unknown-code counts separately.
- Hydrology: transform basin and river geometry to `EPSG:32648`, intersect each
  basin, sum reach length in km, divide by area in km², and optionally sample DEM
  endpoints for length-weighted m/m stream gradient. Reversed, flat, and nodata
  reaches are counted. The exact fifteen configured BasinATLAS fields are
  schema-validated before a one-to-one left join.

All derive functions normalize and validate `HYBAS_ID`, reject duplicate or
nonfinite keys, and preserve a row for every selected basin. The table writer
also rejects duplicate/nonfinite keys and numeric infinity before atomic Parquet
publication.

## Pipeline seam

`task15_derive_handler(inputs, output_dir, owner_source_id=...)` is a concrete
`Stage.DERIVE` handler factory. It only writes assets during the selected owner
source's per-source pipeline invocation and returns four catalog-ready derived
records. No default registration was added: resolving the coordinated selected
L10, DEM, 96 SoilGrids inputs, WorldCover, HydroRIVERS, and BasinATLAS assets is
the remaining Task 19 composition/input-discovery seam. MAP and QA remain
untouched.

## Verification

Focused:

```text
.venv/bin/pytest tests/unit/test_terrain_features.py tests/unit/test_soil_features.py tests/unit/test_landcover_features.py tests/unit/test_hydrology_features.py tests/unit/test_static_predictor_tables.py -q
10 passed in 0.24s
```

Full:

```text
.venv/bin/pytest -q
271 passed in 7.14s
```

Ruff:

```text
.venv/bin/ruff check .
All checks passed!
```

Dependency check:

```text
.venv/bin/pip check
No broken requirements found.
```

## Fix round 2 — fixed semantic contract

Mutation-style RED tests changed `bdod`'s divisor, reassigned WorldCover code
10, and replaced a BasinATLAS field. Each invalid configuration previously
loaded successfully. The validated repository-relative loader now rejects any
deviation from the exact eight SoilGrids divisor values, eleven stable unique
WorldCover code/name pairs, or ordered set of fifteen BasinATLAS fields, while
retaining the EPSG:32648/30 m checks.

```text
.venv/bin/pytest tests/unit/test_feature_config.py -q
4 passed in 0.08s

.venv/bin/pytest -q
281 passed in 8.32s

.venv/bin/ruff check .
All checks passed!

.venv/bin/pip check
No broken requirements found.
```

## Concerns

- The bounded raster work is confined to a basin window rather than loading an
  entire source raster; very large single-basin windows still require memory
  proportional to that basin for exact slope percentiles. The retained DEM is
  copied block-wise. A future streaming quantile/slope pass would further bound
  the per-basin peak.
- This task deliberately neither discovers default input paths nor registers a
  default handler; Task 19 must compose and fingerprint the cross-source inputs.

## Fix round 1 — regression evidence

New RED regressions covered: excluded 100–200 cm SoilGrids coverage evidence,
the incomplete 96-asset matrix, mandatory paired depth bands, non-metric terrain
CRS, nonfinite BasinATLAS values, non-square raster-window residuals, and the
authoritative repository-relative feature configuration. Before implementation,
the depth-band guard regression reached a missing-raster open instead of the
required validation, and the config-loader test failed at collection because the
loader module did not exist.

GREEN changes load all divisors, WorldCover class names, BasinATLAS field names,
and terrain semantics from `config/features.yaml` without cwd-dependent imports.
Soil derivation now requires the entire property × depth × statistic matrix and
both requested bands; coverage counts include only layers overlapping a band.
Hydrology rejects nonfinite selected baseline values at derivation time. The
fixed `raster_windows` final windows now cover a 5 × 3 raster exactly once.

```text
.venv/bin/pytest tests/unit/test_terrain_features.py tests/unit/test_soil_features.py tests/unit/test_landcover_features.py tests/unit/test_hydrology_features.py tests/unit/test_static_predictor_tables.py tests/unit/test_feature_config.py tests/unit/test_spatial_windows.py -q
17 passed in 1.10s

.venv/bin/pytest -q
278 passed in 8.32s

.venv/bin/ruff check .
All checks passed!

.venv/bin/pip check
No broken requirements found.
```
