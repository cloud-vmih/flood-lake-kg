# Hợp đồng schema FloodLakeKG Sơn La

Tài liệu này chốt mô hình logic cho [class diagram](son_la_flood_class_diagram.drawio). Đây là **schema đích**, không phải danh sách bảng đã được tạo. Hiện chỉ có raw object/manifest trong MinIO và bảng Iceberg `meta.source_objects`; các bảng Bronze nghiệp vụ, Silver L12, Gold và KG dưới đây thuộc các bước triển khai tiếp theo.

## 1. Quy ước chung

### ID, khóa và phiên bản

| Trường | Quy ước |
| --- | --- |
| `source_id`, `asset_id`, `ingest_run_id`, `pipeline_run_id` | Chuỗi ổn định; giữ nguyên ID đang dùng trong cấu hình, catalog và Airflow. |
| `object_id` | Chuỗi SHA-256 định danh object bất biến theo nội dung và selection; không đổi thành UUID. |
| `iceberg_snapshot_id` | Số nguyên 64 bit do Iceberg cấp, luôn đi cùng tên bảng/catalog; không dùng UUID tự sinh thay thế. |
| `basin_id` | Chuỗi biểu diễn chính xác `HYBAS_ID` L12, không qua số thực. `pfaf_id` cũng là chuỗi. |
| `basin_version` | Phiên bản geometry/boundary L12 đã duyệt. Mọi FK đến basin phải dùng cặp `(basin_id, basin_version)`. |
| `topology_version`, `feature_build_version`, `parameter_set_id`, `method_version` | Chuỗi định danh bất biến cho input, code và cấu hình tạo kết quả. Không dùng `is_current` thay thế version. |
| `source_feature_id`, `source_grid_id`, `road_edge_id`, `river_reach_id` | ID chỉ duy nhất trong phạm vi dataset/version; khóa ghép phải mang `source_id` hoặc version tương ứng. |
| `assessment_id`, `graph_node_id`, `graph_edge_id` | ID ổn định sinh từ business key + version/scenario, có thể biểu diễn bằng chuỗi. |

Khóa PK/FK trong diagram là **ràng buộc logic**. Iceberg không được xem là bộ máy thực thi FK; pipeline phải kiểm tra uniqueness, referential integrity và topology trước khi commit. Chỉ một thực thể phiên bản được chọn là `is_current` trong cùng business key, nhưng bản cũ vẫn được giữ để replay.

### Thời gian và khả năng tái lập

Tất cả timestamp lưu theo UTC. `source_valid_time` là thời gian mà nguồn mô tả; `model_run_time` là chu kỳ mô hình dự báo và được phép null với reanalysis; `[window_start, window_end)` là khoảng tích lũy; `valid_time` là thời điểm giá trị có hiệu lực; `available_at` là lúc nguồn có thể được biết; `retrieved_at`/`ingested_at` là thời gian hệ thống lấy/ghi; `as_of_time` là mốc tri thức dùng khi tính assessment. Không dùng dữ liệu có `available_at > as_of_time` cho replay/dự báo tại mốc đó. `issued_at` và `published_at` là thời gian phát hành sản phẩm, không thay `valid_time`.

### Kiểu dữ liệu vật lý

Các nhãn `VARCHAR`, `UUID`, `JSONB`, `TIMESTAMPTZ`, `GEOMETRY` trong sơ đồ là ký pháp logic, không phải DDL Iceberg. Khi tạo bảng Iceberg: ID dùng `string` (trừ `iceberg_snapshot_id: long`), số đo dùng `double`/`long`, thời gian dùng timestamp UTC, metadata linh hoạt dùng `struct`/`map` khi ổn định hoặc chuỗi JSON có schema version. Geometry dùng WKB nhị phân kèm `crs` và bbox, hoặc GeoParquet/GeoArrow nếu runtime được kiểm chứng. Không tạo cột tên ghép kiểu `outlet_lon / outlet_lat`: mỗi đại lượng là một cột riêng. Đơn vị nằm trong tên field hoặc data dictionary và được kiểm tra khi ghi.

## 2. Raw landing và Bronze parsed

| Bảng logic | Grain/khóa | Nội dung và ranh giới |
| --- | --- | --- |
| `meta.source_registry` | Một `(source_id, source_version)` | Provider, license, phạm vi và chính sách nguồn; cấu hình YAML vẫn là nguồn khai báo ban đầu. |
| `meta.source_objects` **đã có** | Một `object_id` bất biến | `asset_id`, `source_id`, version, `object_uri`, `manifest_uri`, checksum, size, selection và thời gian nguồn; một dòng tham chiếu payload trong MinIO. |
| `meta.ingest_attempts` | Một `(ingest_run_id, source_id, asset_id, attempt_no)` | Request fingerprint, HTTP status, lỗi và thời gian thử; thất bại không trở thành object `available`. |
| `meta.pipeline_runs` | Một `pipeline_run_id` | Code/config/image, input và output snapshot theo `(table, iceberg_snapshot_id)`, quality result. Quan hệ run–snapshot nên là bảng con, không phải mảng object ID trong một hàng. |
| `bronze.basin_polygon_raw` | Một `(object_id, source_feature_id)` | Feature HydroBASINS/BasinATLAS sau parse, giữ tên field gốc và geometry WKB/CRS, không đổi tên thành feature mô hình. BasinATLAS dùng allowlist 30 field; file đầy đủ vẫn ở Raw. L12 được lọc ngay từ landing. |
| `bronze.river_reach_raw` | Một `(object_id, source_feature_id)` | Reach HydroRIVERS với field gốc và geometry. |
| `bronze.osm_feature_raw` | Một `(object_id, osm_type, osm_id)` | Feature OSM trong AOI đã chọn, tags gốc và geometry; không nhầm với road graph đã harmonize. |
| `bronze.raster_coverage` | Một `(object_id, band_or_layer)` | DEM, SoilGrids, WorldCover, WorldPop: URI object, layer/property/depth/statistic, bbox, CRS, resolution, nodata, dtype, checksum. Pixel raster vẫn nằm trong object; không nhân mỗi pixel thành hàng Iceberg chỉ để có bảng Bronze. |
| `bronze.historical_event_raw` | Một `(object_id, source_record_id)` | Dòng sự kiện/văn bản gốc và reference tới tài liệu, chưa gán cứng một basin. |
| `bronze.weather_grid_value` | Một `(object_id, source_grid_id, variable, vertical_level, valid_time, window_start, window_end, source_revision)` | Bản ghi ERA5-Land/IFS/GSMaP sau parse nếu chọn lưu dạng hàng; kèm `source_cycle_id`, `model_run_time` nullable, `available_at`, unit và value kind. Quyết định lưu hàng hay chunk được đo kích thước trước khi ingest lớn. |

Mỗi bảng Bronze có `object_id` để quay về byte raw, `ingest_run_id`, `parser_version`, quality flag và cột gốc cần thiết. Không trộn các provider có grain khác nhau vào một bảng rộng toàn nullable. `RAW_INGEST_MANIFEST` trong diagram được hiểu là `meta.source_objects` + object manifest JSON; `parser_version` thuộc bảng Bronze, không thuộc raw landing.

## 3. Silver L12 và quan hệ không gian

| Bảng logic | Grain/khóa | Quy tắc |
| --- | --- | --- |
| `silver.dim_basin` | `(basin_id, basin_version)` | Geometry L12, `next_down_id`, `main_basin_id`, `pfaf_id`, `basin_area_km2`, `upstream_area_km2`; area/topology lấy HydroBASINS/BasinATLAS. `next_down_id` chỉ tới basin cùng version hoặc được gắn cờ ra ngoài AOI. |
| `silver.basin_edge` | `(topology_version, upstream_basin_id)` | Cạnh trực tiếp `NEXT_DOWN` tới `downstream_basin_id`, hai đầu cùng `basin_version`, có QA cycle/outlet; không chứa velocity, attenuation hay thời gian truyền theo kịch bản. |
| `silver.basin_static_feature` | `(basin_id, basin_version, feature_build_version)` | Một hàng feature/đơn vị lưu vực, theo dictionary dưới đây. DEM là nguồn terrain chính, SoilGrids là nguồn soil chính; Atlas terrain chỉ QA. |
| `silver.basin_feature_lineage` | `(basin_id, basin_version, feature_build_version, object_id, role)` | Nhiều raw object có thể góp vào một hàng feature; thay cho một `feature_source_manifest_id`. |
| `silver.source_grid` | `(source_id, source_grid_version, source_grid_id)` | Geometry ô lưới, CRS, resolution. |
| `silver.grid_basin_weight` | `(source_id, source_grid_version, source_grid_id, basin_id, basin_version, geometry_processing_version)` | `intersection_area_m2`, `weight_by_basin`, `weight_by_grid`, coverage QA; version của cả grid và basin là bắt buộc. |

Các bảng river, road, facility dùng khóa ghép ID + version ở cả bảng chính, bảng bridge và FK hai đầu cạnh. Sự kiện lũ có thể chạm nhiều basin: tách `silver.observed_flood_event` khỏi `silver.event_basin`. Tài liệu gốc và chunk có bảng/document ID riêng, event–evidence là quan hệ nhiều–nhiều. Dữ liệu dân số từ WorldPop chỉ bảo đảm population theo sản phẩm đã landing; trẻ em/người già cần nguồn riêng hoặc để null, không suy ra từ tổng dân số.

### Dictionary `silver.basin_static_feature`

| Nhóm | Field chuẩn |
| --- | --- |
| DEM địa hình | `elevation_mean_m`, `elevation_min_m`, `elevation_max_m`, `relief_m`, `slope_mean_deg`, `slope_p90_deg` |
| DEM routing | `outlet_lon`, `outlet_lat`, `outlet_elevation_m`, `longest_flow_path_m`, `source_elevation_m`, `main_channel_slope_m_m`, `stream_length_km`, `drainage_density_km_km2`, `hand_mean_m`, `hand_p10_m`, `twi_mean`, `twi_p90` |
| Tham số suy ra | `tc_hours`, `kb_fast_hours`, `kb_central_hours`, `kb_slow_hours`; beta của ba kịch bản lần lượt `0.5`, `1`, `2`. `alpha_1h` thuộc parameter set/method version, không coi là quan trắc DEM. |
| SoilGrids 0–30 cm | `soil_clay_pct_0_30`, `soil_sand_pct_0_30`, `soil_silt_pct_0_30`, `bulk_density_kg_dm3_0_30`, `coarse_fragments_pct_0_30`, `soil_organic_carbon_gkg_0_30`, `field_capacity_m3m3_0_30`, `wilting_point_m3m3_0_30`, `awc_m3m3_0_30`, `soil_uncertainty_ratio` |
| Atlas/land cover | `forest_pct`, `cropland_pct`, `artificial_surface_pct`, `wetland_pct`, `lake_area_pct`, `degree_of_regulation_pct`, `upstream_reservoir_volume`, `longterm_inundation_pct`, `groundwater_depth_cm`; Atlas elevation/slope/runoff/discharge là QA/reference, không thay nguồn chính. |

`flow_direction`, `flow_accumulation` là **raster theo pixel**, không là một số trong hàng basin. Lưu URI/layer/processing version của chúng trong `bronze.raster_coverage` và bảng asset lineage; field basin chỉ giữ các số tổng hợp/suy ra. Soil 30–200 cm sau này thêm depth-specific feature hoặc bảng `(basin_id, depth_interval)`; không thay ý nghĩa field 0–30 cm hiện có.

## 4. Gold, serving và Knowledge Graph

| Sản phẩm | Grain/điều kiện chốt |
| --- | --- |
| `gold.basin_forcing` | Một basin/version × source product × `source_cycle_id` × `valid_time` × cửa sổ tích lũy × revision; `model_run_time` nullable và không là thành phần PK bắt buộc. Giữ `available_at`, `as_of_time`, coverage, unit và source snapshot. |
| `gold.basin_dynamic_indicator` | Một forcing/assessment context × basin × valid time × indicator version; FFG chỉ xuất khi có phương pháp/nguồn tham số đã hiệu chuẩn hoặc được gắn rõ là proxy. |
| `gold.hydro_state` | Một basin/version × valid time × scenario × hydro model version × parameter set × input revision; `q_out_m3s` là **modeled/proxy discharge**, không là trạm đo. `beta`, `kb_hours`, `alpha_1h` được gắn parameter set/scenario. |
| `gold.hazard_assessment` | Một basin/version × valid time × `as_of_time` × policy/model/parameter version × revision. Tách threat, static susceptibility, exposure, impact và alert; exposure chỉ đi vào impact/ưu tiên xử lý, không xác định sự kiện lũ. |
| `serving.basin_latest` | Một `(basin_id, basin_version, scenario_id, product_view)` theo quy tắc chọn revision, `as_of_time` và quality/freshness rõ ràng; có thể materialize từ Gold. `NO_DATA` khác `LOW`. |
| `gold.model_evaluation` | Một experiment/window/label-catalog version; POD/FAR/CSI/lead time chỉ có nghĩa khi nhãn quan trắc và quy tắc ghép basin–thời gian đã được kiểm chứng. |

Road disruption và evacuation route là **scenario outputs**; route phải lưu phiên bản road graph, thời điểm hiệu lực và danh sách cạnh có thứ tự hoặc bảng route-edge con. Không biểu diễn tuyến như “an toàn” nếu chỉ có threat proxy, thiếu xác nhận ngập/khả năng đi lại.

KG được dựng lại từ Silver/Gold bằng mapping có version; không trở thành nguồn sự thật độc lập. `KG_NODE` có `(node_type, business_key, business_version)` duy nhất logic; `KG_EDGE` có predicate được kiểm soát domain/range, hai node đầu cuối, validity, rule/extraction version và evidence. Edge topology lấy từ `silver.basin_edge`; assessment/road passability theo thời gian và scenario là state/edge phiên bản, không ghi đè thuộc tính road node tĩnh. Khi snapshot hoặc assessment bị thay thế, loader đóng hiệu lực/retract edge cũ và upsert idempotent. `QDRANT_COLLECTION` là chỉ mục vector ngoài KG, tham chiếu `chunk_id`; không là node cùng ontology. `MENTIONS` edge là nguồn chuẩn, tránh thêm mảng `mentioned_node_ids` trùng lặp.

## 5. Thứ tự triển khai từ hợp đồng này

1. Giữ `meta.source_objects` đang chạy; thêm kiểm tra contract ID, timestamps và URI. Thiết kế `meta.ingest_attempts`/run lineage tách khỏi object inventory.
2. Tạo Bronze parsed schema và parser theo từng source, bắt đầu từ HydroBASINS L12, BasinATLAS L12, raster coverage và historical evidence; xác nhận grain bằng mẫu thật trước khi ingest hàng loạt.
3. Xây `silver.dim_basin`, `silver.basin_edge`, grid–basin weights, static feature và lineage; QA uniqueness, FK logic, cycle, coverage, units, null/missing.
4. Khi dữ liệu động được landing và Bronze có thời gian chuẩn, xây forcing → hydro state → assessment → serving với replay `as_of_time`.
5. Sau khi Silver/Gold ổn định, dựng KG projection và routing scenario; kiểm tra idempotency, version và retraction.
