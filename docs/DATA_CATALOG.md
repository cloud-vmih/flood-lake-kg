# Danh mục dữ liệu và từ điển thuộc tính

Tài liệu này mô tả snapshot dữ liệu hiện có trong `dataset/` sau lần chạy
`make live` hoàn tất ngày 2026-08-24. Số dòng, kích thước raster và schema bên
dưới được đọc trực tiếp từ các file hiện tại; chúng có thể thay đổi khi crawl lại
nguồn hoặc thay đổi AOI.

## 1. Quy ước chung

| Thuật ngữ | Ý nghĩa |
|---|---|
| Raw | Bản tải về bất biến, giữ nguyên để kiểm chứng và replay. |
| Harmonized | Dữ liệu đã cắt theo AOI, chuẩn hóa CRS/schema nhưng vẫn gần nguồn. |
| Derived | Feature, quan hệ không gian và bảng profile được tính từ harmonized. |
| GeoParquet | Parquet có cột `geometry` và metadata không gian. |
| `HYBAS_ID` | Khóa basin HydroBASINS; khóa trung tâm để join phần lớn bảng. |
| `current_commune_code` | Mã xã/phường hiện hành sau cải cách 2025. |
| `osm_id` | ID OSM ổn định dạng `node/<id>`, `way/<id>` hoặc `relation/<id>`. |
| `*_json` | Chuỗi JSON để giữ danh sách, tags, provenance hoặc cờ chất lượng. |
| `null` trong schema | Cột hiện tồn tại nhưng toàn bộ snapshot hiện tại là null. |

CRS lưu trữ vector mặc định là `EPSG:4326`. Các phép đo diện tích/chiều dài dùng
`EPSG:32648`; cột `processing_crs` ghi lại CRS đã dùng.

## 2. Tổng quan các tầng dữ liệu

| Tầng | Vị trí | Nội dung chính |
|---|---|---|
| Raw | `dataset/raw/` và các file legacy trong `dataset/Data/` | File nguồn bất biến, checksum và bản trùng vẫn được giữ. |
| Harmonized | `dataset/harmonized/` | Địa giới, AOI, basin L10, sông, OSM, sự kiện và raster đã chuẩn hóa. |
| Derived | `dataset/derived/` | Feature theo basin, bảng mapping và bảng feature tổng hợp. |
| Catalog | `dataset/catalog/` | Asset, checksum, provenance và lịch sử lần chạy. |
| QA | `dataset/qa/` | Report JSON/Parquet/HTML, bản đồ và preview. |

## 3. Raw assets

Raw không có một schema bảng chung vì gồm ZIP, PBF, GeoTIFF, XLSX và các bundle
sản phẩm. Metadata thống nhất của chúng nằm trong `dataset/catalog/assets.parquet`.

| `source_id` | Asset | Dung lượng | Nội dung |
|---|---:|---:|---|
| `basinatlas_archive_v10` | 1 | 4.28 GB | Archive BasinATLAS gốc được giữ để kiểm chứng. |
| `basinatlas_v10` | 12 | 11.53 GB | BasinATLAS theo các vùng/lục địa nguồn. Chỉ L10 được harmonize cho project. |
| `cop_dem_glo30_2024_1` | 10 | 1.10 GB | 10 product container Copernicus DEM GLO-30. |
| `esa_worldcover_2021_v200` | 5 | 336.35 MB | Tile WorldCover và metadata/checksum liên quan. |
| `gadm_vnm_4_1` | 1 | 9.23 MB | Địa giới Việt Nam lịch sử dùng cho replay và biên tham chiếu. |
| `geofabrik_vietnam_snapshot` | 2 | 327.06 MB | PBF Việt Nam và MD5 sidecar của snapshot Geofabrik. |
| `historical_flood_evidence_2020_2026` | 1 | 10 KB | Bảng bằng chứng sự kiện lũ 2020–2026. |
| `hydrobasins_lake_sample_v1c` | 2 | 193.63 MB | Sample/bundle HydroBASINS có sẵn ban đầu. |
| `hydrobasins_v1c` | 12 | 1.01 GB | HydroBASINS nguồn; pipeline chọn 168 basin L10 liên quan AOI. |
| `hydrorivers_v10` | 1 | 378.30 MB | Mạng sông HydroRIVERS. |
| `legacy_population_sample_script` | 1 | 1.4 KB | Script lấy dân số mẫu ban đầu. |
| `soilgrids_2_0` | 104 | 58.35 MB | 96 raster thuộc tính đất và 8 tài liệu capabilities/metadata. |
| `sonla_admin_2025` | 78 | 2.10 MB | 75 đơn vị xã/phường hiện hành và metadata pháp lý/nguồn. |
| `worldpop_vnm_2025` | 2 | 150.43 MB | Hai bản vật lý trùng checksum; catalog chọn một bản canonical. |

Tổng hiện tại: **232 raw asset**, tất cả có trạng thái `validated`. Dữ liệu trong
`dataset/raw/_quarantine/` là response SoilGrids sai CRS được giữ làm bằng chứng
recovery; pipeline không dùng chúng làm đầu vào downstream.

## 4. AOI harmonized

### `dataset/harmonized/aoi/core_aoi.geoparquet`

Một geometry phạm vi lõi Sơn La, dùng cho địa giới, dân số và sự kiện.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `aoi_id` | string | ID ổn định của Core AOI. |
| `source_feature_count` | int64 | Số feature nguồn tạo nên AOI. |
| `geometry` | geometry | Polygon/MultiPolygon Core AOI. |

### Các AOI một dòng còn lại

| File | Cột | Vai trò |
|---|---|---|
| `hydrological_aoi.geoparquet` | `aoi`, `geometry` | Core AOI cộng phạm vi thượng nguồn cần cho basin, sông, DEM và đất. |
| `environmental_aoi.geoparquet` | `aoi`, `geometry` | Phạm vi tải/cắt raster môi trường. |
| `exposure_aoi.geoparquet` | `aoi`, `geometry` | Phạm vi trích OSM: đường, cầu, cơ sở, khu dân cư và mặt nước. |

## 5. Địa giới hành chính

### `admin/admin_commune_2025.geoparquet` — 75 dòng

Khóa chính: `current_commune_code`. Snapshot gồm 67 xã và 8 phường.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `current_commune_code` | string | Mã đơn vị hiện hành, giữ số 0 đầu. |
| `current_commune_name` | string | Tên xã/phường hiện hành. |
| `province_name` | string | Tên tỉnh hiện hành. |
| `predecessors_text` | string | Danh sách/tóm tắt đơn vị tiền nhiệm theo văn bản nguồn. |
| `admin_center` | string | Trung tâm hành chính nếu nguồn có công bố. |
| `legal_area_km2` | double | Diện tích pháp lý công bố, km². |
| `legal_population` | double | Dân số pháp lý/công bố của đơn vị mới. |
| `unit_type` | string | `commune` hoặc `ward`. |
| `lookup_id` | string | ID dùng tra cứu feature nguồn. |
| `valid_from`, `valid_to` | string/null | Khoảng hiệu lực của đơn vị hành chính. |
| `raw_asset_id` | string | Asset raw tạo ra dòng. |
| `geometry` | geometry | Hình học đơn vị hành chính. |
| `geometry_was_valid` | bool | Geometry nguồn hợp lệ trước chuẩn hóa hay không. |
| `geometry_repaired` | bool | Có áp dụng sửa geometry hay không. |
| `geometry_area_km2` | double | Diện tích tính độc lập từ geometry. |
| `area_difference_pct` | double | Sai khác phần trăm giữa diện tích geometry và pháp lý. |

### `admin/admin_commune_historical.geoparquet` — 204 dòng

Khóa chính logic: `old_admin_id` + khoảng hiệu lực.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `old_admin_id` | string | ID đơn vị cũ. |
| `old_province_name` | string | Tỉnh theo địa giới cũ. |
| `old_district_name` | string | Huyện/thành phố theo địa giới cũ. |
| `old_admin_name` | string | Tên xã/phường/thị trấn cũ. |
| `source_version` | string | Phiên bản nguồn địa giới. |
| `valid_from`, `valid_to` | null/date | Khoảng thời gian có hiệu lực; `valid_to` hiện là 2025-06-30. |
| `raw_asset_id` | string | Asset raw nguồn. |
| `geometry` | geometry | Geometry lịch sử. |
| `geometry_was_valid`, `geometry_repaired` | bool | Evidence kiểm tra/sửa geometry. |

### Các biên tham chiếu

| File | Dòng | Thuộc tính |
|---|---:|---|
| `admin/sonla_reference_boundary.geoparquet` | 1 | `reference_id`, `source_version`, `raw_asset_id`, `geometry`, `geometry_was_valid`, `geometry_repaired`. |
| `admin/vietnam_boundary.geoparquet` | 1 | `country_id`, `country_name`, `source_version`, `raw_asset_id`, `geometry`, `geometry_was_valid`, `geometry_repaired`. |

## 6. Thủy văn và basin

### `hydro/subbasin_l10.geoparquet` — 168 dòng

Khóa chính: `HYBAS_ID`.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `HYBAS_ID` | int64 | ID basin L10. |
| `NEXT_DOWN` | int64 | ID basin kế tiếp ở hạ lưu; có thể ra ngoài scope. |
| `NEXT_SINK` | int64 | ID sink downstream. |
| `MAIN_BAS` | int64 | ID lưu vực chính. |
| `DIST_SINK` | double | Khoảng cách đến sink theo HydroBASINS. |
| `DIST_MAIN` | double | Khoảng cách đến outlet của lưu vực chính. |
| `SUB_AREA` | double | Diện tích riêng của basin, km². |
| `UP_AREA` | double | Tổng diện tích thượng nguồn, km². |
| `PFAF_ID` | int64 | Mã phân cấp Pfafstetter. |
| `ENDO` | int32 | Cờ lưu vực nội lưu. |
| `COAST` | int32 | Cờ basin ven biển. |
| `ORDER` | int32 | Thứ tự topology/hierarchy của basin. |
| `SORT` | int64 | Khóa sắp xếp nguồn. |
| `geometry` | geometry | Polygon basin L10. |

### `hydro/subbasin_hierarchy.parquet` — 168 dòng

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `HYBAS_ID` | int64 | Basin L10. |
| `parent_l9_hybas_id` | int64 | Basin cha ở level 9. |
| `parent_l8_hybas_id` | int64 | Basin cha ở level 8. |
| `scope_exit` | bool | `NEXT_DOWN` đi ra ngoài tập 168 basin đã chọn. |

### `hydro/river_reach.geoparquet` — 2.081 dòng

Khóa chính: `HYRIV_ID`.

| Cột | Ý nghĩa |
|---|---|
| `HYRIV_ID` | ID reach HydroRIVERS. |
| `NEXT_DOWN` | Reach kế tiếp ở hạ lưu. |
| `MAIN_RIV` | ID hệ thống sông chính. |
| `LENGTH_KM` | Chiều dài reach nguồn, km. |
| `DIST_DN_KM`, `DIST_UP_KM` | Khoảng cách xuống/upstream, km. |
| `CATCH_SKM`, `UPLAND_SKM` | Diện tích catchment riêng/tích lũy, km². |
| `ENDORHEIC` | Cờ nội lưu. |
| `DIS_AV_CMS` | Lưu lượng trung bình, m³/s. |
| `ORD_STRA`, `ORD_CLAS`, `ORD_FLOW` | Các hệ thứ tự/phân lớp dòng chảy. |
| `HYBAS_L12` | Basin L12 nguồn gắn với reach. |
| `geometry` | LineString/MultiLineString reach. |

### `hydro/basinatlas_l10.geoparquet` — 168 dòng, 295 cột

Bảng giữ nguyên nhóm thuộc tính BasinATLAS L10 để truy vết. Khóa là `HYBAS_ID`,
14 cột đầu mô tả topology/geometry tương tự HydroBASINS. Các cột còn lại dùng
quy ước tên của BasinATLAS:

| Nhóm/prefix | Nội dung |
|---|---|
| `dis_*`, `run_*`, `inu_*`, `lka_*`, `lkv_*`, `rev_*`, `dor_*`, `ria_*`, `riv_*`, `gwt_*` | Dòng chảy, runoff, ngập, hồ, điều tiết, sông và nước ngầm. |
| `ele_*`, `slp_*`, `sgr_*`, `clz_*`, `cls_*` | Cao độ, độ dốc và phân lớp địa hình/khí hậu. |
| `tmp_*`, `pre_*`, `pet_*`, `aet_*`, `ari_*`, `cmi_*`, `snw_*` | Nhiệt độ, mưa, bốc thoát hơi, khô hạn và tuyết. |
| `glc_*`, `pnv_*`, `wet_*`, `for_*`, `crp_*`, `pst_*`, `ire_*`, `gla_*`, `prm_*`, `pac_*` | Land cover, thảm thực vật, đất ngập nước và sử dụng đất. |
| `cly_*`, `slt_*`, `snd_*`, `soc_*`, `swc_*`, `lit_*`, `kar_*`, `ero_*` | Đất, carbon, nước trong đất, karst và xói mòn. |
| `pop_*`, `ppd_*`, `urb_*`, `nli_*`, `rdd_*`, `hft_*`, `gdp_*`, `hdi_*`, `gad_*` | Dân số, đô thị, ánh sáng đêm, đường, human footprint, kinh tế và hành chính. |

Suffix thường gặp: `sav`/`uav` là trung bình theo basin riêng/toàn thượng nguồn;
`smn`/`smx` là min/max; `s01`–`s12` là tháng; `syr` là năm; `sse`/`use`
là phần basin riêng/thượng nguồn. Nên giữ tên gốc khi join để không làm mất khả
năng đối chiếu data dictionary chính thức của HydroATLAS.

## 7. OSM exposure harmonized

| File | Dòng | Khóa | Nội dung |
|---|---:|---|---|
| `exposure/road_segment.geoparquet` | 67.741 | `segment_id` | Đoạn đường đã explode; `segment_id = osm_id:part_index`. |
| `exposure/bridge.geoparquet` | 1.359 | `osm_id` | Cầu dạng line. |
| `exposure/facility.geoparquet` | 63 | `osm_id` | 14 point và 49 polygon cơ sở y tế, trường học, cứu hộ, chính quyền… |
| `exposure/settlement.geoparquet` | 132 | `osm_id` | Thành phố, thị trấn, làng, bản/hamlet… |
| `exposure/water_context.geoparquet` | 806 | `osm_id` | Sông, suối, hồ và polygon ngữ cảnh mặt nước. |

Các bảng dùng chung nhóm thuộc tính:

| Cột | Ý nghĩa |
|---|---|
| `osm_id`, `osm_way_id` | ID OSM chuẩn hóa; `osm_way_id` chỉ có ý nghĩa với closed way polygon. |
| `segment_id` | ID duy nhất của từng part đường; chỉ có ở `road_segment`. |
| `highway`, `bridge`, `tunnel`, `access`, `surface`, `smoothness` | Tags giao thông. |
| `amenity`, `healthcare`, `emergency`, `government` | Tags cơ sở/dịch vụ. |
| `place`, `natural`, `water`, `waterway`, `landuse`, `name` | Tags địa danh, tự nhiên, nước và sử dụng đất. |
| `other_tags` | Tags OSM ngoài danh sách cột đã cấu hình. |
| `tags_json` | Toàn bộ tags hữu ích đã chuẩn hóa thành JSON. |
| `geometry` | Point, LineString hoặc Polygon tùy bảng. |

## 8. Sự kiện lịch sử

### `events/historical_flood_event_2020_2026.parquet` — 30 dòng

| Cột | Ý nghĩa |
|---|---|
| `event_id` | ID sự kiện ổn định. |
| `source_row_number` | Dòng trong bảng raw. |
| `event_year` | Năm xảy ra sự kiện. |
| `original_district_text`, `original_place_text` | Địa danh nguyên văn theo bằng chứng. |
| `original_date_text`, `original_time_text` | Ngày/giờ nguyên văn. |
| `event_type`, `description` | Loại và mô tả sự kiện. |
| `source_name`, `evidence_url`, `notes` | Provenance và ghi chú. |
| `event_date_start`, `event_date_end` | Khoảng ngày chuẩn hóa. |
| `administration_valid_on` | Ngày dùng để replay địa giới. |
| `administration_regime` | Chế độ địa giới cũ hoặc hiện hành. |
| `admin_candidate_names_json` | Các tên đơn vị ứng viên. |
| `current_commune_codes_json` | Mã xã/phường 2025 được ánh xạ. |
| `match_status` | `matched`, `ambiguous` hoặc `unresolved`. |
| `match_confidence` | Độ tin cậy ánh xạ từ 0 đến 1. |

## 9. Raster harmonized

| File/nhóm | Kích thước | CRS, độ phân giải | Giá trị |
|---|---|---|---|
| `rasters/dem_glo30.tif` | 8.164 × 8.015 | EPSG:4326; 1 arc-second | Cao độ Copernicus DEM, `float32`. |
| `rasters/worldcover_2021.tif` | 27.213 × 26.715 | EPSG:4326; 0,3 arc-second | Mã lớp ESA WorldCover, `uint8`, nodata 0. |
| `rasters/worldpop_2025.tif` | 2.186 × 1.749 | EPSG:4326; 0,00083333333° | Số người/pixel WorldPop constrained, `float32`, nodata -99999 được tính là 0 theo policy aggregation. |
| `soilgrids/<property>/<depth>/<stat>.tif` | 941 × 986 mỗi file | EPSG:4326; xấp xỉ 250 m | 96 raster `int16` SoilGrids. |

SoilGrids có 8 property: `clay`, `sand`, `silt`, `bdod`, `cfvo`, `wv0010`,
`wv0033`, `wv1500`; 6 lớp sâu: `0-5cm`, `5-15cm`, `15-30cm`, `30-60cm`,
`60-100cm`, `100-200cm`; và 2 statistic: `mean`, `uncertainty`.

| Property | Ý nghĩa |
|---|---|
| `clay`, `sand`, `silt` | Tỷ lệ sét, cát và limon. |
| `bdod` | Bulk density của đất khô. |
| `cfvo` | Tỷ lệ thể tích mảnh thô. |
| `wv0010`, `wv0033`, `wv1500` | Hàm lượng nước thể tích tại các mức thế nước tương ứng. |

## 10. Feature tables theo basin

Tất cả bảng trong phần này có **168 dòng**, một dòng cho mỗi `HYBAS_ID` L10.

### `terrain_features.parquet` — 11 cột

| Cột | Ý nghĩa |
|---|---|
| `HYBAS_ID` | Khóa basin. |
| `elevation_min_m`, `elevation_mean_m`, `elevation_max_m` | Cao độ min/trung bình/max, mét. |
| `elevation_relief_m` | Chênh cao max − min, mét. |
| `slope_deg_mean`, `slope_deg_p90`, `slope_deg_max` | Độ dốc trung bình, percentile 90 và max, độ. |
| `dem_covered_pixel_count`, `dem_valid_pixel_count` | Số pixel DEM nằm trong basin và số pixel hợp lệ. |
| `dem_coverage_fraction` | Tỷ lệ pixel DEM hợp lệ. |

Raster trung gian `terrain/dem_glo30_metric_30m.tif` có kích thước 7.894 × 8.262,
CRS `EPSG:32648`, pixel 30 m và được dùng để tính độ dốc/địa hình.

### `soil_features.parquet` — 129 cột

Khóa `HYBAS_ID` cộng 128 cột theo pattern:

```text
<property>_<stat>_<depth>
<property>_<stat>_<depth>_covered_pixel_count
<property>_<stat>_<depth>_valid_pixel_count
<property>_<stat>_<depth>_coverage_fraction
```

Trong đó:

- `property`: `clay`, `sand`, `silt`, `bdod`, `cfvo`, `wv0010`, `wv0033`, `wv1500`;
- `stat`: `mean` hoặc `uncertainty`;
- `depth`: `0_30cm` hoặc `30_100cm`;
- cột không suffix là trung bình có trọng số theo pixel trong basin;
- ba suffix còn lại là evidence về số pixel và độ phủ.

### `landcover_features.parquet` — 17 cột

| Nhóm cột | Ý nghĩa |
|---|---|
| `HYBAS_ID` | Khóa basin. |
| `landcover_covered_pixel_count` | Pixel WorldCover có tâm trong basin. |
| `landcover_valid_pixel_count` | Pixel có mã lớp hợp lệ. |
| `landcover_nodata_pixel_count` | Pixel nodata. |
| `landcover_unknown_pixel_count` | Pixel có mã không thuộc mapping đã duyệt. |
| `landcover_coverage_fraction` | Tỷ lệ pixel hợp lệ. |
| `landcover_fraction_<class>` | Tỷ lệ từng lớp: `tree_cover`, `shrubland`, `grassland`, `cropland`, `built_up`, `bare_sparse`, `snow_ice`, `permanent_water`, `herbaceous_wetland`, `mangroves`, `moss_lichen`. |

### `hydrology_features.parquet` — 25 cột

| Nhóm cột | Ý nghĩa |
|---|---|
| `HYBAS_ID` | Khóa basin. |
| `basin_area_km2` | Diện tích basin tính trong CRS metric. |
| `river_length_km`, `river_reach_count` | Tổng chiều dài và số reach giao basin. |
| `drainage_density_km_per_km2` | Mật độ sông = chiều dài sông/diện tích basin. |
| `stream_gradient_m_per_m` | Gradient dòng chảy tổng hợp. |
| `stream_gradient_valid_reach_count` | Reach đủ dữ liệu gradient. |
| `stream_gradient_reversed_reach_count` | Reach có hướng/cao độ đảo. |
| `stream_gradient_flat_reach_count` | Reach phẳng. |
| `stream_gradient_nodata_reach_count` | Reach thiếu dữ liệu cao độ. |
| `dis_m3_pyr`, `run_mm_syr`, `inu_pc_smn`, `inu_pc_smx`, `lka_pc_sse`, `dor_pc_pva`, `ria_ha_ssu`, `riv_tc_ssu`, `gwt_cm_sav`, `ele_mt_sav`, `ele_mt_smn`, `ele_mt_smx`, `slp_dg_sav`, `sgr_dk_sav`, `pre_mm_syr` | Thuộc tính hydrology/địa hình/mưa được chọn từ BasinATLAS. |

### `subbasin_static_feature.geoparquet` — 209 cột

Bảng feature cuối dùng cho phân tích/model. Mỗi dòng là một basin L10 và gồm:

1. 14 cột HydroBASINS từ `subbasin_l10`;
2. 10 feature địa hình sau `HYBAS_ID`;
3. 128 feature SoilGrids;
4. 16 feature land cover;
5. 24 feature thủy văn;
6. 14 cột dân số/evidence;
7. provenance: `pipeline_run_id`, `dependency_fingerprint`,
   `feature_group_source_asset_ids_json`.

Các cột thành phần giữ nguyên tên và ý nghĩa như bốn bảng feature cùng
`map_subbasin_population.parquet`. `HYBAS_ID` là khóa chính; `geometry` là polygon
L10.

## 11. Bảng mapping/quan hệ

### `map_subbasin_commune.parquet` — 503 dòng

Khóa ghép: `HYBAS_ID`, `current_commune_code`.

| Cột | Ý nghĩa |
|---|---|
| `intersection_area_km2` | Diện tích phần giao basin–xã, km². |
| `basin_fraction` | Phần diện tích basin nằm trong xã. |
| `commune_fraction` | Phần diện tích xã nằm trong basin. |
| `quality_flags_json` | Cờ chất lượng quan hệ. |
| `processing_crs` | CRS dùng tính diện tích. |
| `source_asset_ids_json` | Asset nguồn của quan hệ. |

### `map_subbasin_river.parquet` — 2.231 dòng

Khóa ghép: `HYBAS_ID`, `HYRIV_ID`. Bảng giữ các thuộc tính reach được mô tả ở
`river_reach`, cộng:

| Cột | Ý nghĩa |
|---|---|
| `intersected_length_km` | Chiều dài reach thực sự nằm trong basin. |
| `boundary_case` | Quan hệ chạm ranh giới cần lưu evidence. |
| `quality_flags_json`, `processing_crs`, `source_asset_ids_json` | QA và provenance. |

### `map_subbasin_road.parquet` — 68.099 dòng

Khóa ghép: `HYBAS_ID`, `segment_id`. Giữ tags của `road_segment`, cộng
`intersected_length_km`, `boundary_case`, `quality_flags_json`, `processing_crs`
và `source_asset_ids_json`. Một segment có thể tạo nhiều dòng nếu cắt qua nhiều
basin.

### `map_subbasin_bridge.parquet` — 1.327 dòng

Khóa ghép: `HYBAS_ID`, `osm_id`. Giữ tags cầu, các cột mapping giống road và
`relationship_geometry_wkt` chứa geometry phần giao không giản lược.

### `map_subbasin_facility.parquet` — 63 dòng

### `map_subbasin_settlement.parquet` — 121 dòng

Hai bảng point mapping cùng schema:

| Cột | Ý nghĩa |
|---|---|
| `HYBAS_ID`, `osm_id` | Basin và đối tượng OSM. |
| `relationship_type` | `within`, `touches` hoặc `nearest_boundary_tie`. |
| `tags_json` | Tags OSM. |
| `boundary_case` | Điểm nằm/chạm biên. |
| `quality_flags_json`, `processing_crs`, `source_asset_ids_json` | QA và provenance. |

### `map_subbasin_population.parquet` — 168 dòng

Khóa chính: `HYBAS_ID`.

| Cột | Ý nghĩa |
|---|---|
| `population_sum` | Tổng dân số WorldPop trong phần Core AOI của basin. |
| `population_mean` | Dân số trung bình trên toàn bộ pixel AOI được gán, kể cả nodata-as-zero. |
| `contributing_pixel_count` | Pixel WorldPop hợp lệ. |
| `nodata_pixel_count` | Pixel nodata, được tính 0 theo policy constrained. |
| `aoi_pixel_count` | Tổng pixel trong Core AOI được gán cho basin. |
| `coverage_ratio` | `contributing_pixel_count / aoi_pixel_count`. |
| `source_resolution_x`, `source_resolution_y`, `source_resolution_unit` | Độ phân giải raster nguồn. |
| `source_crs` | CRS raster nguồn. |
| `population_scope` | Hiện là `core_aoi_only`. |
| `boundary_center_tie_pixel_count` | Pixel center đồng thời nằm trên biên nhiều basin. |
| `quality_flags_json`, `source_asset_ids_json` | QA và provenance. |

## 12. Crosswalk xã cũ ↔ xã mới

### `derived/mappings/admin_commune_crosswalk.parquet` — 200 dòng

| Cột | Ý nghĩa |
|---|---|
| `old_admin_id`, `old_admin_name`, `old_admin_type` | ID, tên và loại đơn vị trước cải cách. |
| `old_admin_normalized_name` | Tên cũ đã chuẩn hóa để matching. |
| `current_commune_code`, `current_commune_name` | Đơn vị hiện hành tương ứng. |
| `relationship_type` | Kiểu quan hệ kế thừa/gộp/chia/khớp. |
| `match_status` | Trạng thái matching. |
| `valid_from`, `valid_to` | Khoảng hiệu lực của quan hệ. |
| `candidate_old_admin_ids` | Danh sách ID ứng viên khi quan hệ không đơn trị. |
| `overlap_metrics_json` | Evidence diện tích/tỷ lệ overlap dùng để đối chiếu. |

Bảng này là seam chính để replay sự kiện 2020–2026 sang địa giới hiện hành 2025.

## 13. Catalog và provenance

### `dataset/catalog/assets.parquet` — 379 dòng

| Cột | Ý nghĩa |
|---|---|
| `asset_id` | ID duy nhất của asset. |
| `source_id`, `source_version` | Nguồn và phiên bản. |
| `kind` | `raw`, `harmonized`, `derived` hoặc `qa`. |
| `source_uri` | URI nguồn hoặc URI `generated:`. |
| `storage_path` | Đường dẫn file local. |
| `media_type`, `size_bytes` | MIME type và kích thước. |
| `checksum_algorithm`, `checksum` | Thuật toán và checksum nội dung. |
| `retrieved_at`, `source_valid_time` | Thời điểm tải và thời điểm hiệu lực nguồn. |
| `license_id` | License của nguồn. |
| `bbox_wgs84_json`, `crs`, `resolution_json` | Metadata không gian nếu được ghi ở cấp asset. |
| `pipeline_run_id` | Lần chạy tạo/đăng ký asset. |
| `status` | Trạng thái lifecycle của asset. |
| `dependency_fingerprint` | Fingerprint đầu vào/config để quyết định reuse. |
| `duplicate_of_asset_id` | Asset canonical nếu file là bản trùng. |
| `metadata_json` | Metadata riêng của adapter/stage. |
| `error_code`, `error_message` | Evidence lỗi nếu asset thất bại. |

### `dataset/catalog/runs.parquet` — 44 dòng

| Cột | Ý nghĩa |
|---|---|
| `run_id` | ID lần chạy. |
| `command` | Lệnh/stage đã thực hiện. |
| `started_at`, `ended_at` | Thời gian bắt đầu/kết thúc. |
| `status` | `completed`, `partial_failure`… |
| `config_fingerprint` | Fingerprint cấu hình của lần chạy. |

## 14. QA artifacts

| File/thư mục | Nội dung |
|---|---|
| `dataset/qa/report.json` | Report máy đọc được, gồm từng `check_id`, `passed`, `severity`, `expected`, `actual`, `message`, `asset_ids`. |
| `dataset/qa/report.parquet` | Cùng nội dung QA dưới dạng bảng. |
| `dataset/qa/report.html` | Report mở trực tiếp bằng trình duyệt. |
| `dataset/qa/map/` | Bundle bản đồ kiểm tra và layer evidence. |
| `dataset/qa/map/previews/` | PNG preview của các layer/raster chính. |

Snapshot hiện tại không có fatal QA. Các warning được giữ có chủ đích: một
geometry hành chính đã repair, ba ngoại lệ diện tích pháp lý, 12 địa danh sự kiện
chưa resolve, 28 downstream scope exit và nodata của WorldPop constrained.

## 15. Quan hệ join chính

```text
admin_commune_historical --old_admin_id--> admin_commune_crosswalk
admin_commune_crosswalk --current_commune_code--> admin_commune_2025

subbasin_l10 --HYBAS_ID--> terrain_features
             --HYBAS_ID--> soil_features
             --HYBAS_ID--> landcover_features
             --HYBAS_ID--> hydrology_features
             --HYBAS_ID--> map_subbasin_population
             --HYBAS_ID--> subbasin_static_feature

subbasin_l10 + admin_commune_2025 --> map_subbasin_commune
subbasin_l10 + river_reach        --> map_subbasin_river
subbasin_l10 + OSM exposure       --> map_subbasin_road/bridge/facility/settlement
```

## 16. Kiểm tra lại schema sau khi crawl

```bash
# Inventory asset và dung lượng
.venv/bin/flashflood-data inventory --root .

# Xem schema nhanh bằng PyArrow
.venv/bin/python -c "import pyarrow.parquet as pq; print(pq.read_schema('dataset/derived/subbasin_static_feature.geoparquet'))"

# Mở QA report
xdg-open dataset/qa/report.html
```

Không sửa trực tiếp file trong `dataset/raw/`. Nếu schema nguồn thay đổi, chạy
pipeline để tạo lại harmonized/derived rồi cập nhật số dòng và schema trong tài
liệu này.
