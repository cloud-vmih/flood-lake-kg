# Thiết kế loại bỏ package `static`

## Mục tiêu

Loại bỏ `src/flashflood_data/static` bằng cách chuyển code đang dùng về đúng vertical slice,
chuyển primitive địa không gian và source-object contract sang `shared`, đồng thời bỏ workflow
file-first đã được các DAG thay thế.

## Cấu trúc đích

```text
src/flashflood_data/
  pipelines/
    study_area/                 # bootstrap admin và dựng 5 AOI L12
    static_landing/adapters/    # admin crosswalk, WorldPop acquisition helper
    silver_static_features/     # kernel feature để Bronze → Silver dùng tiếp
    flood_events/               # parser dữ liệu event legacy
  shared/
    contracts/source_objects.py
    geospatial/raster.py
    geospatial/vector.py
  storage/                      # không import pipelines
```

## Quyết định

- `build-aoi` thay `run-static --stop-after aoi`; vẫn fetch, validate và harmonize hai nguồn
  `sonla_admin_2025`, `gadm_vnm_4_1` khi input AOI chưa có.
- `static/spatial` là primitive dùng chung nên chuyển sang `shared/geospatial`.
- Feature terrain/soil/hydrology/landcover/population chuyển nguyên trạng vào
  `pipelines/silver_static_features`; chưa tạo DAG Silver trong refactor này.
- Admin crosswalk thuộc acquisition của admin; WorldPop helper thuộc adapter Landing; event
  workbook parser thuộc `flood_events`.
- Xóa CLI/Make targets file-first `inventory`, `fetch`, `validate`, `harmonize`, `derive`, `map`,
  `run-static`, `smoke`, `preflight`, `live`, `qa-map`. Giữ `land-static`, `bronze` và lệnh mới
  `build-aoi`.
- Di chuyển DTO được `storage` dùng sang `shared/contracts`; Landing re-export để caller hiện
  tại không phải đổi toàn bộ trong cùng lúc.
- Giữ nguyên thuật toán hình học, checksum, schema, DAG ID, task ID, object key và dữ liệu hiện có.

## Nghiệm thu

- Không còn thư mục hoặc import `flashflood_data.static`.
- `storage` không import `flashflood_data.pipelines`.
- `make lakehouse-aoi` gọi `flashflood-data build-aoi` và integration test vẫn tạo đủ AOI L12.
- Các adapter đang chạy tiếp tục dùng cùng hàm raster/vector và cùng kết quả.
- Toàn bộ test, Ruff, CLI help và Docker Compose config đều qua.
