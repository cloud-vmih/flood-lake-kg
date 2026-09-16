# L12 Source Landing and Static Feature Design

**Date:** 2026-09-16

**Status:** Approved for implementation planning

**Scope:** Static raw-source landing to MinIO and Iceberg inventory, followed later by L12
bronze, harmonized, static-feature, and routing-parameter tables

## 1. Goal

Build an Airflow-orchestrated source-ingestion path that acquires or reuses source data,
selects basin level 12 where a provider package contains multiple HydroBASINS levels, lands
immutable source objects in MinIO, publishes auditable manifests, and registers those objects in
an Iceberg inventory through Polaris.

The first DAG ends after source landing and inventory registration. It does not harmonize
geometry, build mappings, calculate basin features, or run B0-B3. This design also fixes the
contracts needed by later L12 static-feature work so the landing scope supports the approved
feature set.

This document supersedes the source-landing decisions in section 8.4 of
`2026-09-15-repository-organization-and-source-landing-design.md` where they conflict. The
canonical basin source level is now L12 rather than L10, and MinIO stores the selected L12 source
bundle rather than the complete extracted levels 1-12 or only a multi-level archive.

## 2. Boundaries and Terms

```text
provider or validated local source
  -> MinIO raw object and sidecar manifest
  -> Iceberg meta.source_objects inventory row
  -> later bronze parsing
  -> later harmonization and static features
  -> later routing parameters and B0-B3
```

- **Raw object:** Exact provider response when it already matches the selected source scope, or
  a deterministic bundle containing unmodified members selected from a provider archive.
- **Landing manifest:** Immutable JSON describing one raw object, its members, provenance,
  checksums, retrieval, validation, and selection.
- **Iceberg inventory:** The `meta.source_objects` table. It points to raw objects; it does not
  contain the bytes of ZIP, Shapefile, GeoTIFF, XML, or XLSX sources.
- **Bronze table:** Parsed rows written as Iceberg-compatible data files after landing.
- **Harmonized/feature table:** Canonical L12 geometry, normalized units, aggregates, and derived
  scientific features.

Iceberg cannot directly adopt the current ZIP, Shapefile, or GeoTIFF objects as table data files.
Its `register_table` operation registers an existing Iceberg metadata tree, while `add_files`
expects supported tabular data files that already conform to a table schema. Raw bytes therefore
remain in the `raw` bucket and become queryable rows only after a later parse or conversion.

## 3. Approved Source Scope

### 3.1. Basin sources

- `hydrobasins_v1c`: select level 12 and retain every member required to read the Shapefile.
- `basinatlas_v10`: select level 12 and retain every member required to read the Shapefile.
- `hydrorivers_v10`: retain the provider source package needed to build the L12 river subset.
- The analytical basin key is `HYBAS_ID`, exposed as `basin_id` after harmonization.
- `config/study_area.yaml` changes from level 10 to level 12 in the later harmonization slice,
  not in source-landing scientific logic.

When a provider publishes only a multi-level archive, acquisition downloads it to run-scoped
local staging, verifies it, selects only the complete L12 member set, creates a deterministic ZIP,
lands that ZIP, and deletes the downloaded archive only after MinIO and Iceberg publication
succeed. The manifest retains the original archive URI, checksum, size, and member checksums.

### 3.2. DEM

- Retain the current Copernicus DEM GLO-30 source tiles covering the environmental AOI.
- Source landing does not calculate slope, flow direction, flow accumulation, outlets, streams,
  HAND, TWI, time of concentration, or routing parameters.
- Existing DEM data are sufficient inputs for those later derivations; this design requires no
  replacement DEM.

### 3.3. SoilGrids

The initial canonical SoilGrids scope is the 0-30 cm horizon:

- properties: `clay`, `sand`, `silt`, `bdod`, `cfvo`, `soc`, `wv0033`, `wv1500`;
- source depths: `0-5cm`, `5-15cm`, `15-30cm`;
- statistics: `mean`, `Q0.05`, `Q0.5`, `Q0.95`.

This produces 96 raster responses plus capabilities documents proving the coverages exist. The
provider `uncertainty` coverage is not canonical because the project calculates the ratio from
the three downloaded quantiles. Existing deeper and `uncertainty` rasters remain untouched
locally and are not deleted. Existing `wv0010` data also remain untouched and can be added as a
separate source-scope change when a feature requires it.

Adding 30-200 cm later is configuration-driven. New depth responses are appended as new raw
objects, and new depth aggregates are appended to the long-form soil feature table without
rewriting 0-30 cm rows.

## 4. Object and Manifest Layout

Static basin objects use a selection segment:

```text
s3://raw/static/hydrobasins_v1c/1c/level=12/<asset_id>/hydrobasins_l12.zip
s3://raw/static/basinatlas_v10/10/level=12/<asset_id>/basinatlas_l12.zip
```

Other static assets use:

```text
s3://raw/static/<source_id>/<source_version>/<selection>/<asset_id>/<filename>
```

`selection` is a stable source-specific path such as a DEM tile identifier or a SoilGrids
property/depth/statistic tuple. Dynamic provider responses retain the previously approved layout:

```text
s3://raw/dynamic/<source_id>/<product>/<yyyy>/<mm>/<dd>/<retrieval_id>/<filename>
```

Every final object has a `manifest.json` in the same asset directory. The manifest includes:

- schema version, `asset_id`, deterministic `object_id`, source ID/version/type, product, and
  media type;
- selected basin level or other selection parameters when applicable;
- credential-free source URI, request fingerprint, license, and retrieval run ID;
- provider-issued, model-run, valid, available, first-seen, and retrieval times only when known;
- final object URI, byte size, checksum algorithm, and SHA-256;
- original archive checksum and size when selection came from a larger archive;
- deterministic member order, names, sizes, and SHA-256 values for bundles;
- validation result, object-verification status, sanitized error fields, and safe provider
  metadata. The immutable manifest does not claim Iceberg registration success.

ZIP construction uses stable member order and normalized ZIP metadata so identical source
members produce identical bundle bytes and SHA-256 values. Packaging does not modify member
bytes or interpret their scientific values.

## 5. Shared Landing Service and Airflow DAG

Source semantics live in an application service under `src/flashflood_data`; the CLI and Airflow
both call it. DAG files contain scheduling, dependencies, parameters, pools, and retry policy.

```text
static_source_landing
├── prepare_run
├── hydrobasins_l12
├── basinatlas_l12
├── hydrorivers
├── cop_dem
├── soilgrids_0_30
└── publish_run_summary
```

Each independent source group performs:

```text
resolve
  -> acquire or reuse validated local content
  -> validate provider payload or source archive
  -> select requested members
  -> package a bundle when the source is multi-file
  -> upload to a run-scoped MinIO staging key
  -> verify staged size and SHA-256
  -> publish and verify the immutable final object
  -> publish the sidecar manifest
  -> register the object in meta.source_objects
```

Airflow receives `run_id`, requested sources, and configuration such as `basin_level=12`. It does
not pass large payloads through XCom. A persistent staging directory is mounted into the Airflow
runtime. Staging is scoped by run and asset and is removed only after object, manifest, and
Iceberg registration succeed.

Dynamic sources use a separate scheduled DAG because their cadence, temporal identity, and
revisions differ from static sources. They reuse the same landing service and inventory table.

## 6. Iceberg Inventory Contract

`meta.source_objects` is one Iceberg table for static and dynamic source objects. It supports
SQL discovery, lineage, version selection, and input resolution for later DAGs.

| Field | Meaning |
|---|---|
| `object_id` | Deterministic identity over source, version, selection, and checksum |
| `asset_id` | Source adapter asset identity |
| `source_id`, `source_version`, `source_type`, `product` | Source classification |
| `basin_level` | Nullable; 12 for selected basin sources |
| `object_uri`, `manifest_uri` | Immutable MinIO locations |
| `media_type`, `size_bytes` | Physical object description |
| `checksum_algorithm`, `checksum` | Content verification |
| `source_uri` | Credential-free provenance URI |
| provider time fields | Nullable and populated only with evidence |
| `retrieved_at`, `first_seen_at` | Landing times in UTC |
| `ingest_run_id` | Airflow/application run identity |
| `status` | Published availability state |
| `selection_json`, `provider_metadata_json` | Versioned source-specific metadata |

Only verified, published objects receive available rows. Failures and quarantined payloads remain
in run records and manifests but are not selectable as available source objects.

The current Parquet asset catalog continues to support local acquisition, retry, and legacy
pipeline behavior. `meta.source_objects` is authoritative for objects published to MinIO. Later
pipelines resolve inputs from an explicit Iceberg snapshot instead of a mutable bucket listing.

## 7. Publication, Idempotence, and Recovery

Publication order is:

1. acquire and validate local content;
2. upload and verify the immutable MinIO object;
3. publish its immutable manifest;
4. append the inventory row and commit an Iceberg snapshot;
5. record the snapshot ID in the run summary;
6. remove the run's local staging content.

These steps cannot form one distributed transaction. Recovery uses deterministic identity and
forward repair:

- object present, manifest absent: verify the object and publish the missing manifest;
- object and manifest present, Iceberg row absent: reuse them and append the inventory row;
- matching Iceberg row present: report reuse and do not append a duplicate;
- final key present with another checksum: report a conflict and publish nothing over it;
- invalid payload: quarantine it under a run-scoped prefix and exclude it from available input.

Iceberg does not enforce a primary key. A shared Airflow writer pool serializes registration,
and the service checks deterministic `object_id` immediately before commit. Optimistic commit
conflicts reload the table and retry the identity check.

One source failure does not block independent source groups. The overall run becomes
`partial_failure`, lists successful, reused, failed, and quarantined objects, and allows retry of
only the failed source or asset.

## 8. Later Bronze and Static Tables

The first DAG does not create these tables, but its source scope supports them:

```text
bronze.hydrobasins_l12
bronze.basinatlas_l12

static.basins
static.basin_soil_feature
static.basin_terrain_features
static.basin_atlas_features
static.derived_raster_assets

model.routing_parameter
static.basin_features_current
```

- `static.basins` owns canonical L12 geometry, topology, area, hierarchy, and basin IDs.
- `static.basin_soil_feature` is a long-form physical table.
- `static.basin_terrain_features` owns scalar DEM and hydrological features per basin.
- `static.basin_atlas_features` owns selected BasinATLAS attributes after unit decoding.
- `static.derived_raster_assets` inventories flow-direction, flow-accumulation, stream, HAND,
  and TWI rasters. Those rasters are not scalar columns in the basin table.
- `model.routing_parameter` owns scenario-dependent `beta`, `K_b`, and `alpha_1h` values.
- `static.basin_features_current` is a wide read view over approved current versions.

The physical soil table contains at least:

```text
basin_id
property_id
depth_top_cm
depth_bottom_cm
metric
value
unit
feature_version
processing_run_id
source_snapshot_id
```

The wide view exposes names such as `soil_clay_pct_0_30` without making every new depth interval
a physical schema change.

## 9. Approved Static Feature Semantics

### 9.1. Soil

- Mean source layers are thickness-weighted over 0-5, 5-15, and 15-30 cm.
- `clay`, `sand`, `silt`, and `cfvo` become percent after source-specific scaling.
- `bdod` becomes `bulk_density_kg_dm3_0_30`.
- `soc` becomes `soil_organic_carbon_gkg_0_30`.
- `wv0033` and `wv1500` become true `m3/m3`, not percent.
- `awc_m3m3_0_30 = field_capacity_m3m3_0_30 - wilting_point_m3m3_0_30`.
- Uncertainty is stored per property as `(Q0.95 - Q0.05) / Q0.5`, with division-by-zero and
  NoData flags.
- SoilGrids values remain susceptibility, QA, and ablation inputs; they are not multiplied into
  IFS runoff by default.

### 9.2. Terrain and hydrological derivation

```text
hydrologically condition DEM
  -> flow direction
  -> flow accumulation
  -> stream extraction
  -> snap basin outlet
  -> longest flow path
  -> source and outlet elevation
  -> main-channel slope
  -> HAND and TWI
  -> time of concentration
```

Required scalar fields include elevation mean/min/max, relief, slope mean/P90, outlet longitude
and latitude, outlet elevation, longest flow-path length, source elevation, main-channel slope,
stream length, drainage density, HAND mean/P10, TWI mean/P90, and time of concentration.

The baseline concentration-time formula is Kirpich:

```text
Tc_minutes = 0.0195 * longest_flow_path_m^0.77 * main_channel_slope_m_m^-0.385
```

The table records `tc_formula_id`, units, feature version, input source snapshot, and quality
flags. Zero or negative channel slope, an unsnapped outlet, or an invalid flow path prevents a
valid `Tc` publication.

`model.routing_parameter` stores one row per basin and scenario:

| Scenario | `beta` | `kb_hours` | `alpha_1h` |
|---|---:|---:|---:|
| FAST | 0.5 | `0.5 * tc_hours` | `exp(-1 / kb_hours)` |
| CENTRAL | 1.0 | `tc_hours` | `exp(-1 / kb_hours)` |
| SLOW | 2.0 | `2.0 * tc_hours` | `exp(-1 / kb_hours)` |

These are uncalibrated sensitivity scenarios and retain `parameter_set_id` and
`derivation_method`.

### 9.3. BasinATLAS and topology

- HydroBASINS/BasinATLAS identifiers and topology join by `HYBAS_ID`.
- Copernicus DEM terrain is primary; BasinATLAS elevation, slope, and stream gradient are QA.
- SoilGrids is primary for soil; BasinATLAS clay, sand, and silt are excluded from the model view.
- ERA5-Land/IFS hourly runoff is primary for modelling; BasinATLAS annual runoff/discharge is QA.
- BasinATLAS field-specific scale and unit metadata are applied before publication.

The approved attributes include topology, area, sink distance, terrain QA, stream gradient, lake
and regulation, forest/cropland/artificial surface/wetland/inundation, groundwater depth, annual
runoff/discharge QA, and population impact fields.

## 10. Verified Current-Data Gaps and Guards

- L12 HydroBASINS and BasinATLAS source files already exist locally.
- The L12 AOI plus one upstream hop contains 179 basins, compared with 168 at L10.
- All requested BasinATLAS attribute names exist for those 179 basins and contain no checked
  `-9999` values.
- `HYBAS_ID=4121033820` has a ring self-intersection. Raw bytes stay unchanged; harmonization
  repairs and flags it.
- `dor_pc_pva`, `rev_mc_usu`, and `glc_pc_s22` are zero across all 179 checked basins. They may
  be retained for provenance but add no model variance in this AOI/version.
- Existing SoilGrids data lack `soc` and individual Q0.05/Q0.5/Q0.95 rasters.
- Existing SoilGrids uncertainty rasters contain sentinel `32767` without declared NoData. They
  are not canonical uncertainty inputs.
- Existing elevation, relief, slope, HydroRIVERS length, and drainage density outputs are L10 and
  must be recomputed for L12.
- Flow direction/accumulation, snapped outlets, longest flow path, HAND, TWI, `Tc`, `K_b`, and
  `alpha_1h` do not yet exist.

## 11. Acceptance Criteria

The source-landing slice is complete when:

1. CLI and Airflow use the same landing service.
2. HydroBASINS and BasinATLAS L12 bundles contain every required member and are readable.
3. Object, manifest, and local SHA-256 values agree.
4. Every successful object has exactly one available `meta.source_objects` identity.
5. Retry repairs a missing manifest or Iceberg row without uploading duplicate content.
6. A source failure leaves independent sources available and marks the run `partial_failure`.
7. An Iceberg commit failure recovers from the verified object and manifest.
8. Credentials do not appear in keys, manifests, metadata rows, or logs.
9. Staging cleanup occurs only after MinIO and Iceberg publication succeed.
10. The DAG performs no harmonization, mapping, feature derivation, or B0-B3 work.

The later L12 feature slice is complete when:

1. Exactly 179 selected L12 basins are published with valid topology and repaired geometry.
2. Required SoilGrids 0-30 cm fields use correct units and exclude sentinel values.
3. Quantile-based uncertainty is available separately for each property.
4. Required DEM flow products and scalar features pass coverage, range, and topology checks.
5. `Tc`, `K_b`, and `alpha_1h` reproduce their versioned formulas from stored inputs.
6. BasinATLAS fields have documented units and all-zero fields are flagged.
7. The current wide view resolves one version per feature group and retains snapshot lineage.

## 12. Out of Scope for the First DAG

- Parsing raw vector members into `bronze.*` Iceberg tables.
- Harmonizing geometry, CRS, field names, or units.
- Calculating soil, terrain, hydrological, mapping, population, or routing features.
- Storing ZIP or raster bytes inside an Iceberg binary column.
- Treating arbitrary raw files as Iceberg data files.
- Deleting current local raw, harmonized, derived, catalog, or QA data.
- Implementing dynamic canonical schemas, B0-B3, PostGIS, Neo4j, impact, road routing, API, or
  dashboard behavior.
