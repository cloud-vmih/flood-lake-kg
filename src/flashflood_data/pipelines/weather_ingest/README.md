# Dynamic weather ingest pipeline

Package dùng chung cho bốn DAG GSMaP NOW, GSMaP Standard, ERA5-Land và IFS/Open-Meteo. Mỗi DAG lập kế hoạch window thiếu, fetch, scope theo AOI Sơn La, publish Raw/Meta, parse raster slice Bronze và tiến watermark liên tục.

## Thứ tự đọc

1. `config/pipelines/weather/<source>.yaml` — schedule, stream, variables, overlap và batch size.
2. `airflow.py` — task graph dùng chung và Airflow pool.
3. `runtime.py` — composition root và incremental inventory matching.
4. `planner.py`, `watermarks.py` — safe end, window thiếu và cursor.
5. `providers/` — request/fetch riêng GSMaP, ERA5-Land và IFS.
6. `subset.py`, `landing.py` — cắt AOI và publish Raw bất biến.
7. `grids.py` — đăng ký source grid.
8. `bronze.py`, `parsers.py`, `quality.py` — raster slice arrays, DQ partition và commit Bronze.
9. `lifecycle.py`, `fetch_audit.py` — retention Raw transient và attempt audit.
10. `models.py`, `config.py` — DTO và config contract.

## Entrypoint

Các file `airflow/dags/*_ingest.py` gọi `build_project_weather_dag()` với DAG ID và tên YAML. Provider code không nằm trong DAG wrapper.

## Input và output

- Input: API/FTP provider, AOI L12 và source grid.
- Output: scoped Raw object, `meta.source_objects`, `meta.object_lifecycle`, `silver.source_grid`,
  `bronze.weather_raster_slice`, `quarantine.rejected_objects`, `quarantine.rejected_rows` và Meta audit.
- Shared dependency: `shared.batching`, `shared.metadata`, `storage`.

Window đã có được reuse theo inventory; bounded overlap hỗ trợ late/revised data mà không quét lại toàn bộ lịch sử.
Fatal object không retry như lỗi mạng. Slice reject không làm mất slice hợp lệ; object có slice
reject giữ Raw, còn object chỉ warning vẫn được cleanup theo retention sau bảy ngày.
