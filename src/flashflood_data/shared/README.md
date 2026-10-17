# Shared contracts and primitives

`shared` chỉ chứa code có nhiều pipeline cùng dùng và không chứa quy tắc riêng của một nguồn.

| Đường dẫn | Vai trò |
| --- | --- |
| `contracts/source_objects.py` | DTO bất biến cho Raw object, batch đăng ký và cleanup; pipeline và storage cùng dùng. |
| `geospatial/raster.py` | Validate, clip/mosaic COG và tính coverage raster. |
| `geospatial/vector.py` | Validate và ghi vector dùng chung. |
| `geospatial/zonal.py` | Cửa sổ raster/zonal primitive cho Silver feature kernel. |
| `geospatial/overlay.py` | Primitive overlay/map không gắn với một DAG. |
| `metadata/` | Registry, run, quality, snapshot và lineage dùng chung. |
| `batching.py` | Chia batch xác định cho mapped task. |
| `quality/` | Finding/disposition, policy hash, quarantine repository, Meta audit và coordinator publish dùng chung. |
| `spark/` | Compatibility matrix DTO, build signature và session Spark/Sedona/Iceberg dùng cho Silver. |
| `storage_budget.py` | Kiểm tra dung lượng trước khi tải/publish. |

`storage/` là lớp hạ tầng kế bên: HTTP, atomic local file, MinIO và Iceberg. Nó có thể import
`core`, `catalog` và `shared`, nhưng không được import `pipelines`. Quy tắc chọn tile, field,
AOI hoặc provider phải nằm trong pipeline sở hữu nguồn đó.
