# Runbook Data Quality và quarantine

Tài liệu này dùng khi Landing, Bronze hoặc migration báo `failed_data_quality`,
`succeeded_with_warning`, có row trong `quarantine.*`, hoặc cần kiểm tra dữ liệu Bronze cũ.

## 1. Ý nghĩa trạng thái

| Trạng thái | Ý nghĩa | Hành động |
| --- | --- | --- |
| `succeeded` | Tất cả row được publish với `quality_status=passed`. | Không cần xử lý. |
| `succeeded_with_warning` | Có warning, row bị loại, hoặc object khác trong batch bị block; phần hợp lệ đã commit. | Xem Meta và quarantine trước khi quyết định reprocess. |
| `failed_data_quality` | Lỗi dữ liệu fatal; object không được publish vào bảng đích. | Sửa input/policy rồi reprocess có chủ đích; không retry như lỗi mạng. |
| `failed` | Lỗi vận hành như timeout, MinIO/Polaris hoặc commit conflict. | Sửa hạ tầng và retry cùng run/input. |

Vocabulary row được publish chỉ có `passed` và `warning`. Row reject chỉ nằm trong
`quarantine.rejected_rows`; object fatal nằm trong `quarantine.rejected_objects`.

## 2. Kiểm tra nhanh bằng Trino

Mở DBeaver hoặc Trino CLI, chọn catalog `lakehouse` rồi chạy:

```sql
SELECT severity, status, count(*) AS finding_count
FROM lakehouse.meta.quality_results
GROUP BY severity, status
ORDER BY severity, status;

SELECT target_dataset_id, rule_id, reason_code, count(*) AS object_count
FROM lakehouse.quarantine.rejected_objects
GROUP BY target_dataset_id, rule_id, reason_code
ORDER BY object_count DESC;

SELECT target_dataset_id, rule_ids, count(*) AS row_count
FROM lakehouse.quarantine.rejected_rows
GROUP BY target_dataset_id, rule_ids
ORDER BY row_count DESC;
```

Tìm evidence của một object:

```sql
SELECT *
FROM lakehouse.quarantine.rejected_objects
WHERE object_id = '<object_id>' OR scope_key = '<asset_id-or-object_id>'
ORDER BY rejected_at DESC;

SELECT *
FROM lakehouse.quarantine.rejected_rows
WHERE object_id = '<object_id>'
ORDER BY rejected_at DESC;

SELECT object_id, source_id, object_uri, manifest_uri, checksum, selection_json
FROM lakehouse.meta.source_objects
WHERE object_id = '<object_id>';
```

`rejection_id` bỏ qua Airflow run ID. Retry cùng input, policy và rule upsert cùng evidence,
không nhân bản rejection.

## 3. Audit dữ liệu Bronze hiện có

DAG `bronze_quality_backfill` là DAG manual, paused khi tạo và chỉ có một active run.
Chạy `audit` trước; chế độ này ghi Meta/quarantine nhưng không rewrite bảng Bronze và không gọi
provider:

```bash
make dq-audit-plan DATASET=bronze.weather_raster_slice LIMIT=100
```

Hoặc trigger trong Airflow với config:

```json
{
  "dataset_id": "bronze.weather_raster_slice",
  "mode": "audit",
  "object_limit": 100,
  "force": false,
  "snapshot_id": 0
}
```

`snapshot_id=0` pin current snapshot ở đầu run. Planner trả một trong các action:

| Action | Xử lý |
| --- | --- |
| `audit_only` | Bronze đã có đủ cột để audit tại chỗ. |
| `rewrite_bronze` | Chuẩn hóa trạng thái legacy, ví dụ `flagged` thành `warning`. |
| `reprocess_raw` | Đọc lại object đã đăng ký trong MinIO; không fetch provider. |
| `mark_raw_unavailable` | Raw đã hết hạn hoặc mất; giữ evidence và dừng. |
| `skip` | Checkpoint apply của đúng dataset/snapshot/policy/object đã hoàn tất. |

Sau khi review tỷ lệ reject/fatal, trigger lại với `mode=apply`. Apply chỉ tác động các object
trong batch plan. Muốn đánh giá lại checkpoint đã hoàn tất thì đặt `force=true`.

## 4. Reprocess và xử lý `raw_unavailable`

Với `reprocess_raw`, migration đọc URI trong `meta.source_objects` và gọi parser Bronze tương ứng.
Nó không import hay gọi API provider. Nếu cần chạy lại pipeline gốc thay vì migration:

- Static: trigger `static_source_to_bronze` với `source_id` và `force_reprocess=true`.
- Weather: trigger đúng DAG nguồn với `force_reprocess=true`; inventory vẫn lọc theo object đã
  đăng ký và Bronze parser đọc từ MinIO.

Với `mark_raw_unavailable`, kiểm tra retention trước. Không tự động fetch lại. Chỉ chạy Landing
hoặc backfill provider khi dữ liệu đó còn cần cho Silver và giấy phép/availability cho phép.

Object weather có slice reject được đánh dấu lifecycle `failed`, nên Raw transient không hết hạn.
Object chỉ có warning vẫn được hết hạn sau 7 ngày. GSMaP NOW/IFS malformed trước khi đăng ký Raw
chỉ lưu structured evidence; payload staging lớn không được sao chép lần hai.

## 5. Retry đúng loại lỗi

- Không retry tự động `DataQualityBlock`: checksum mismatch, container hỏng, CRS không dùng được,
  grid thiếu/thừa cell hoặc mọi row bị reject sẽ cho cùng kết quả.
- Retry lỗi vận hành: timeout provider, DNS, MinIO upload/copy timeout, Polaris commit conflict.
- Khi một batch có object hợp lệ và object fatal, phần hợp lệ vẫn commit một lần; object fatal bị
  xóa slice Bronze cũ theo policy và được ghi quarantine.
- Khi coordinator dừng sau target commit nhưng trước Meta, chạy lại cùng identity để repair Meta;
  writer so sánh nội dung và không tạo business key trùng.

## 6. Smoke test

Khi local MinIO/Polaris/Airflow đang chạy:

```bash
make lakehouse-dq-smoke
```

Smoke tạo namespace cô lập, kiểm tra mixed valid/reject/warning, fatal object và retry idempotent,
sau đó purge đúng namespace của run. Trong pytest, bật kiểm tra service thật bằng
`FLASHFLOOD_RUN_LAKEHOUSE_INTEGRATION=1`.
