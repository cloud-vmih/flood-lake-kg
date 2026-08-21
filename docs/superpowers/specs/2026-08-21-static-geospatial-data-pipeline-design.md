# Static Geospatial Data Pipeline Design for Sơn La Flash-Flood Research

**Date:** 2026-08-21  
**Status:** Approved design  
**Project root:** `/home/cloud/cloud/TLCN/Project`

## 1. Purpose

This document specifies the first implementation milestone of the project described in `docs/TLCN_DE.docx`: acquire, preserve, harmonize, map, and validate the static geospatial data needed to build the Sơn La flash-flood prototype.

The milestone is file-first. It does not deploy MinIO, Iceberg, PostGIS, Airflow, Neo4j, an API, or the operational dashboard. Its outputs are durable file assets that those services can ingest later without downloading or recomputing the source data.

The milestone must:

- Preserve immutable raw source assets and their provenance.
- Acquire all missing static sources for the study area.
- Use HydroBASINS level 10 as the primary hazard unit.
- Preserve level 8 and 9 parent relationships for roll-up and quality checks.
- Use the post-2025 commune boundaries as the current administrative reference.
- Relate pre-2025 communes and historical event place names to the current units without forcing ambiguous matches.
- Build the static sub-basin profile and all spatial mapping tables required by the project proposal.
- Publish both machine-readable quality reports and a lightweight interactive quality-assurance map.
- Keep newly acquired static data within a soft limit of 8 GiB and preserve adequate temporary working space.

## 2. Scope and non-goals

### 2.1 Included

- Inventory and registration of the existing HydroBASINS, BasinATLAS, HydroRIVERS, WorldPop, and historical-event files.
- Acquisition of current and historical administrative boundaries, SoilGrids, Copernicus DEM GLO-30, ESA WorldCover 2021, and an OpenStreetMap snapshot.
- Construction of the hydrological area of interest.
- Raw asset validation and provenance capture.
- Vector and raster harmonization.
- Static feature engineering at HydroBASINS level 10.
- Administrative, hydrological, infrastructure, facility, settlement, and population mappings.
- Data-quality reports and an interactive QA map.
- An optional, separately approved cleanup report for unused extracted BasinATLAS levels.

### 2.2 Excluded

- GSMaP, forecasts, ERA5-Land soil-moisture ingestion, and other dynamic sources.
- Threat-score or threat-level calculation.
- Incremental Knowledge Graph construction.
- Cascade-impact reasoning and risk-aware routing.
- MinIO, Iceberg, PostGIS, Spark/Sedona, Airflow, and Neo4j deployment.
- A production dashboard or public-facing warning system.
- Automatic deletion of any existing data.

## 3. Approved decisions

| Decision | Approved choice |
|---|---|
| Delivery order | Static data first |
| Storage approach | Manifest-driven local file lake |
| Primary hazard unit | HydroBASINS level 10 |
| Basin roll-up | Level 9 and level 8 parent relationships |
| Administrative reference | Post-2025 commune boundaries |
| Historical administration | Temporal crosswalk from old to current communes |
| Runtime platform | Linux in the current project directory |
| Processing model | Python package and CLI, not isolated one-off scripts |
| Data formats | Original raw formats; GeoParquet, Parquet, and COG downstream |
| Map output | Lightweight interactive QA map, not a production dashboard |
| Spatial extent | Sơn La-intersecting L10 basins, one direct upstream hop, and source-specific buffers |
| Environmental buffer | 10 km around the hydrological AOI |
| Exposure buffer | 10 km around Sơn La, clipped to Vietnam |
| New static-data budget | 8 GiB soft cap |
| Existing data | Register in place; do not move or duplicate initially |
| Secrets | Environment variables or uncommitted local environment file |

## 4. Architecture

### 4.1 Logical flow

```text
source registry
      |
      v
inventory existing assets -----> source asset manifest
      |
      v
build administration and AOI
      |
      v
fetch missing immutable raw assets
      |
      v
validate raw assets
      |
      v
harmonize to GeoParquet / Parquet / COG
      |
      v
derive basin features and spatial mappings
      |
      v
quality reports and interactive QA map
```

### 4.2 Project structure

```text
Project/
├── pyproject.toml
├── .env.example
├── config/
│   ├── sources/
│   └── study_area.yaml
├── src/flashflood_data/
│   ├── cli.py
│   ├── catalog/
│   ├── sources/
│   ├── harmonize/
│   ├── derive/
│   └── qa/
├── dataset/
│   ├── raw/
│   ├── harmonized/
│   ├── derived/
│   ├── catalog/
│   └── qa/
├── tests/
└── docs/
```

The existing files under `dataset/` remain at their current paths during this milestone. The catalog records those paths as existing raw assets. New assets use the structured subdirectories. Moving legacy files is a separate cleanup decision.

### 4.3 Components

#### Source registry

The registry declares source identity, version, access method, license, authentication method, AOI selection strategy, expected file type, expected spatial metadata, and raw target path. Source-specific download logic remains behind a common adapter interface.

#### Asset catalog

The catalog records one row per asset and is the authoritative local record of acquisition and processing state. At minimum it stores:

- `asset_id`
- `source_id`
- `source_version`
- `source_uri`
- `storage_path`
- `media_type`
- `size_bytes`
- `checksum_algorithm`
- `checksum`
- `retrieved_at`
- `source_valid_time`, when applicable
- `license_id`
- `spatial_extent`
- `crs`
- `resolution`
- `pipeline_run_id`
- `status`
- `error_code` and `error_message`, when applicable

#### Fetch adapters

Adapters stream remote data into `.partial` files, retry transient failures, support range-based resume when the source allows it, validate the downloaded payload, and atomically rename successful downloads. They must not overwrite a valid raw asset with a different payload.

#### Harmonizers

Vector harmonizers standardize identifiers and schemas, repair geometry only in the harmonized copy, clip to the appropriate AOI, and write GeoParquet. Raster harmonizers mosaic source tiles when needed, clip them, preserve meaningful nodata metadata, and write Cloud-Optimized GeoTIFFs.

#### Derived builders

Derived builders create static features and mapping tables. Each builder declares its upstream asset checksums so unchanged dependencies can be skipped safely.

#### QA publisher

The publisher writes structured validation results and a browser-based map containing current communes, L10 sub-basins, rivers, roads, bridges, facilities, settlements, and downsampled raster previews.

#### CLI

The CLI exposes independently runnable stages:

```text
inventory
fetch
validate
harmonize
derive
map
run-static
cleanup --dry-run
```

`run-static` never invokes destructive cleanup.

## 5. Study area model

The pipeline distinguishes four spatial scopes so global environmental inputs are not confused with Vietnam-only exposure inputs:

1. **Core AOI:** the current post-2025 Sơn La provincial boundary.
2. **Hydrological AOI:** every HydroBASINS level-10 polygon intersecting the Core AOI plus every direct upstream polygon whose `NEXT_DOWN` points to a selected polygon.
3. **Environmental Download AOI:** a 10 km metric buffer around the Hydrological AOI, used for DEM, SoilGrids, WorldCover, and hydrological context. This scope may cross the national border because the environmental sources are global.
4. **Exposure AOI:** a 10 km metric buffer around the Core AOI clipped to the Vietnam national boundary, used for OSM infrastructure and population exposure. Upstream environmental context outside Vietnam does not imply a requirement to map foreign infrastructure.

The study-area configuration is equivalent to:

```yaml
core_boundary: son_la_2025
hydrobasins_level: 10
upstream_hops: 1
raster_buffer_km: 10
exposure_buffer_km: 10
exposure_country: Vietnam
processing_crs: EPSG:32648
storage_crs: EPSG:4326
```

Source tiles may extend beyond their requested AOI because raw source tiling must be preserved. Harmonized products are clipped to the scope appropriate for their use.

## 6. Source inventory and acquisition

### 6.1 Existing sources

| Source | Existing asset | Role |
|---|---|---|
| HydroBASINS | Asia levels 1-12 | Hazard polygons, hierarchy, upstream/downstream topology |
| BasinATLAS | Asia levels 1-12 and archive | Hydrological and physiographic baseline attributes |
| HydroRIVERS | Asia network | River connectivity, river length, drainage density, stream gradient support |
| WorldPop | Vietnam 2025, 100 m | Population exposure |
| Historical evidence | `Lu_Son_La_2020_2026.xlsx` | Retrospective location and impact evidence |

The standard HydroBASINS topology without inserted lakes is primary. Existing lake-customized HydroBASINS data may be retained for visual QA, but it does not drive the topology because inserted lakes can introduce special connectivity cases.

### 6.2 Missing sources

#### Current and historical administration

- The legal current-unit list and merger relationships come from Resolution 1681/NQ-UBTVQH15.
- The expected current result is 75 units: 67 communes and 8 wards.
- Current geometry is acquired from an authoritative administrative map or service when a reusable vector endpoint is available.
- If an authoritative vector download is unavailable, current units are constructed by dissolving verified pre-2025 commune geometry according to Resolution 1681.
- The official post-merger lookup map is used as a geometry and naming cross-check.
- OSM administrative boundaries are a secondary comparison or explicitly labeled fallback, not the legal authority.

The temporal crosswalk stores old and new codes and names, validity intervals, relationship type, source, spatial fraction where applicable, and confidence. Historical evidence remains attached to its original place text and receives `matched`, `ambiguous`, or `unresolved` status rather than a forced match.

#### SoilGrids

Acquire the following properties for all six standard depth intervals, limited to the Environmental Download AOI:

- `clay`
- `sand`
- `silt`
- `bdod`
- `cfvo`
- `wv0010`
- `wv0033`
- `wv1500`

Retain mean predictions and the available uncertainty representation. Derived features include depth-weighted 0-30 cm and 30-100 cm summaries; deeper source layers remain available for future analysis.

#### Copernicus DEM

Acquire COP-DEM GLO-30 tiles intersecting the Environmental Download AOI using user-supplied Copernicus Data Space credentials. Credentials are never written to source-controlled configuration or logs.

#### ESA WorldCover

Acquire WorldCover 2021 v200 10 m COG tiles intersecting the Environmental Download AOI. WorldCover remains categorical and is never resampled with a continuous-data interpolation method.

#### OpenStreetMap

Retain a timestamped Vietnam PBF snapshot as raw input, then extract the Exposure AOI. Required entities include:

- road ways and their relevant access, surface, class, bridge, tunnel, and status tags
- bridges
- hospitals, clinics, schools, emergency services, government facilities, and mapped shelters
- settlements and populated places
- major mapped reservoirs and water bodies for visual context

OSM IDs and source tags remain traceable through harmonized and derived products.

## 7. Spatial and tabular data products

### 7.1 Harmonized products

```text
admin_commune_2025.geoparquet
admin_commune_historical.geoparquet
subbasin_l10.geoparquet
subbasin_hierarchy.parquet
river_reach.geoparquet
road_segment.geoparquet
bridge.geoparquet
facility.geoparquet
settlement.geoparquet
dem_glo30.tif
worldcover_2021.tif
worldpop_2025.tif
soilgrids/<property>/<depth>.tif
```

Storage geometries use EPSG:4326. Metric calculations use EPSG:32648. Reprojection for calculation is explicit; stored raw and harmonized provenance records retain the original CRS.

### 7.2 Derived products

```text
subbasin_static_feature.geoparquet
map_subbasin_commune.parquet
map_subbasin_river.parquet
map_subbasin_road.parquet
map_subbasin_facility.parquet
map_subbasin_population.parquet
admin_commune_crosswalk.parquet
```

#### Static sub-basin feature profile

There is exactly one row per selected L10 `HYBAS_ID`. The profile contains:

- hierarchy and topology identifiers
- polygon and upstream area
- elevation summaries and relief
- slope summaries
- river length, drainage density, and stream-gradient summaries
- selected physically meaningful BasinATLAS baseline attributes
- soil texture, bulk density, coarse fragments, and water-retention summaries
- land-cover fractions
- population sum and supporting coverage metrics
- provenance and pipeline-run identifiers

The derived table does not copy all BasinATLAS attributes. Raw BasinATLAS preserves the complete source schema.

#### Sub-basin to commune

The many-to-many crosswalk stores:

- `HYBAS_ID`
- current commune code
- intersection area
- fraction of the basin in the commune
- fraction of the commune in the basin
- relationship quality flags

#### Sub-basin to river and road

Mappings store source entity ID, intersected length in the basin, relationship flags, and source provenance. Harmonized OSM road segments retain stable OSM identity plus a deterministic segment sequence.

#### Sub-basin to facility and settlement

Mappings store point or geometry relationship type, containing basin, boundary-case flags, source ID, and original tags needed for later impact and routing work.

#### Population mapping

The population mapping stores sum, mean, contributing pixel count, nodata count, coverage ratio, source resolution, and quality flag. Population is not selected merely by the WorldPop raster bounding box; it is aggregated within the Core AOI against approved commune and basin relationships. Population fields are explicitly named as Core-AOI exposure values and are not presented as whole-basin population for L10 polygons extending outside Sơn La or Vietnam.

## 8. Raster policy

The pipeline does not force DEM, SoilGrids, WorldCover, and WorldPop onto one common grid. Each retains its native meaningful resolution and is aggregated independently to L10 polygons.

- Continuous data use a suitable continuous interpolation only when reprojection is unavoidable.
- Categorical WorldCover uses nearest-neighbour resampling.
- Population processing must preserve totals and document any reprojection or pixel inclusion rule.
- Every zonal output records contributing pixel count and coverage ratio.
- Later dynamic sources must additionally record source resolution and quality support at L10 to avoid false spatial precision.

## 9. Processing flow

### 9.1 Inventory

Scan existing assets, classify them, identify likely duplicate content, calculate checksums, and register assets in place. Inventory does not reorganize the existing directory tree.

### 9.2 AOI construction

Build the current administration layer, current-to-historical crosswalk, L10 selection, one-hop upstream closure, L10-to-L9/L8 hierarchy, the Environmental Download AOI, and the Vietnam-clipped Exposure AOI.

### 9.3 Acquisition

For each missing registry asset:

1. Resolve the source version and remote metadata.
2. Estimate transfer and temporary-space requirements.
3. Stop before download if the 8 GiB soft cap or disk reserve would be violated.
4. Stream into a `.partial` file.
5. Validate expected size and payload structure.
6. Calculate checksum.
7. Atomically rename and update the manifest.

### 9.4 Raw validation

Validate file integrity, spatial bounds, CRS, resolution, nodata, schema, feature counts, and expected AOI coverage. Failed assets never feed downstream outputs.

### 9.5 Harmonization

Create normalized vector, raster, and table copies. Log geometry repair counts and area changes. Preserve native raster semantics, source identifiers, raw checksum links, and processing parameters.

### 9.6 Derivation

Build features and mappings from validated harmonized dependencies. Outputs record their dependency checksums. A repeat run skips outputs whose dependency set and processing configuration are unchanged.

### 9.7 QA publication

Write structured reports and a lightweight MapLibre map directory. The map supports layer visibility, feature inspection, coverage warnings, and downsampled raster previews. It is a research QA artifact, not an operational warning dashboard.

## 10. State, failures, and recovery

Asset state follows:

```text
discovered -> fetching -> fetched -> validated -> harmonized -> derived
                    \-> failed / stale / quarantined
```

Rules:

- A source failure does not invalidate completed independent sources.
- Transient network and rate-limit errors use bounded exponential backoff.
- Resume is used only when supported safely by the server.
- `.partial` files are never accepted as raw assets.
- Checksum mismatches or unreadable payloads are quarantined.
- Unexpected schema or source-version changes stop the affected adapter.
- A valid raw asset is not silently overwritten.
- Secrets are read from the environment and redacted from logs.
- Geometry repairs occur only in harmonized outputs and are reported.
- Missing raster coverage remains nodata and is reported; it is not silently filled.
- Source fallback is explicit in configuration and output naming.
- Current administration is not accepted if its count, topology, or legal-area checks fail.

## 11. Storage management and cleanup

New static raw assets have an 8 GiB soft cap. Acquisition uses AOI-specific source requests or source tiles rather than global downloads. The pipeline reserves working space for tile mosaics and atomic output creation.

The existing BasinATLAS archive is approximately 4 GiB, while its extracted levels occupy approximately 11 GiB. Cleanup is optional and separate:

1. Verify the archive checksum and readability.
2. Verify the selected L10 raw and pipeline outputs.
3. Run `cleanup --dry-run` to report exact targets, size recovery, and recovery source.
4. Obtain explicit user approval for the resolved file list.
5. Delete only the approved extracted unused levels.

HydroBASINS levels are comparatively small and remain available unless a later explicit cleanup decision changes that policy.

## 12. Testing strategy

### 12.1 Unit tests

- Source configuration parsing and validation
- Checksum and deterministic asset identity
- Manifest state transitions
- Basin parent and one-hop upstream selection
- Depth-weighted SoilGrids aggregation
- Stable OSM segment and mapping identifiers
- Dependency fingerprint and skip decisions

### 12.2 Adapter contract tests

Each adapter uses a small fixture to validate parsing, schema normalization, provenance extraction, and error handling without downloading the full source.

### 12.3 Integration smoke test

Run the complete workflow on a small test AOI before the full Sơn La acquisition. The smoke test must exercise inventory, one remote or fixture acquisition per source class, raw validation, harmonization, one derived feature table, mappings, and QA publication.

### 12.4 Data-quality gates

- Exactly 75 current administrative units: 67 communes and 8 wards.
- Unique administrative codes and valid geometries.
- Aggregate administrative gap and overlap no greater than 0.1% of Core-AOI area by default; per-unit legal-area difference no greater than 2% by default. Any exception is reported and requires an approved configuration override.
- Unique L10 `HYBAS_ID` values and valid L9/L8 parents.
- No unexplained dangling topology reference within the selected one-hop scope.
- For every commune, L10 basin-intersection coverage totals between 99.5% and 100.5% by default; any exception is reported with its uncovered or overlapping geometry.
- Every mapping foreign key exists.
- DEM, SoilGrids, and WorldCover cover at least 99% of the Hydrological AOI unless documented source nodata applies; WorldPop coverage is evaluated against the Core AOI rather than foreign upstream areas.
- Population aggregation includes pixel and coverage evidence and does not double-count.
- Every historical evidence row has `matched`, `ambiguous`, or `unresolved` status and confidence.
- Used raw assets include URI, version, license, retrieval time, and checksum.
- New static data remain under the approved soft cap and disk reserve.

## 13. Completion criteria

The milestone is complete only when:

1. Every planned static source has a validated raw asset. A failure report is a diagnosable stopping artifact, not successful milestone completion, unless the user explicitly approves a reduced-scope exception.
2. The static profile has exactly one row for each L10 polygon in the Hydrological AOI.
3. All approved mapping tables exist and pass referential and spatial quality gates.
4. The QA map loads and supports visibility and inspection for all required layers.
5. A second unchanged pipeline run neither downloads nor rebuilds unchanged assets.
6. Unit, contract, smoke, and data-quality tests pass.
7. No failure is hidden by silent fallback, fabricated values, or forced administrative matching.

## 14. Transition to implementation planning

After this specification is reviewed and approved, the next step is to produce a task-by-task implementation plan. Implementation must begin with project scaffolding and tests, then inventory existing assets before any large download. Full static acquisition starts only after the smoke test and storage preflight pass.
