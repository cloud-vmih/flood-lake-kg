# Bronze quality migration pipeline

Audit hoặc sửa có chọn lọc dữ liệu Bronze được tạo trước khi DQ/quarantine được bật.

## Thứ tự đọc

1. `bronze_quality.py` — planner provider-free, checkpoint và action.
2. `runtime.py` — đọc snapshot Iceberg, kiểm tra Raw MinIO, audit/apply action.
3. `airflow.py` — DAG manual `bronze_quality_backfill`, pause mặc định và dùng `bronze_writer`.

`mode=audit` không đổi target Bronze và không gọi provider. `mode=apply` chỉ normalize row hoặc
reprocess từ Raw đã đăng ký. Identity checkpoint là dataset + snapshot + policy + object; Raw mất
được ghi `mark_raw_unavailable` thay vì tự tải lại.
