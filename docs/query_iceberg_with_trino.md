# Đọc bảng Iceberg bằng Trino, CloudBeaver và DBeaver

Trino là SQL engine đọc các bảng Iceberg hiện có qua Polaris REST catalog và MinIO. Service này thuộc Compose profile `query`, không khởi động cùng stack mặc định. Catalog Trino tên `lakehouse`; catalog Polaris phía sau tên `flood_lakehouse`. Trino Iceberg connector được cấu hình `READ_ONLY` để tránh sửa bảng trong lúc khảo sát.

## Khởi động và kiểm tra

Chạy ở thư mục gốc project:

```bash
make query-up
make query-status
make query-smoke
```

`query-smoke` liệt kê bảng trong `meta` và đọc một `object_id` từ `meta.source_objects`. Nếu máy mới clone chưa ingest dữ liệu, bảng có thể chưa tồn tại; cần chạy Landing/Bronze hoặc chuyển snapshot MinIO/Polaris từ môi trường khác trước.

Mở Trino Web UI tại <http://127.0.0.1:8083/> để xem trạng thái và lịch sử query. Để duyệt bảng và chạy SQL trên trình duyệt, dùng CloudBeaver ở phần dưới.

```bash
docker compose --profile query exec -T trino trino \
  --catalog lakehouse --schema bronze \
  --execute 'SHOW TABLES FROM lakehouse.bronze'
```

Khi không cần query, chỉ dừng Trino; MinIO, Polaris và Airflow tiếp tục chạy:

```bash
make query-down
```

## CloudBeaver: xem dữ liệu trên trình duyệt

Khởi động riêng Trino và CloudBeaver, không build hay khởi động lại Airflow:

```bash
make query-ui-up
make query-ui-status
```

Mở <http://127.0.0.1:8978/>. Lần đầu, hoàn tất màn hình thiết lập và tạo tài khoản quản trị CloudBeaver. Sau đó tạo kết nối **Trino** với các giá trị sau (nếu danh sách driver chưa có Trino, chọn JDBC và nhập URL):

| Trường | Giá trị |
| --- | --- |
| Host | `trino` |
| Port | `8080` |
| Catalog | `lakehouse` |
| Schema ban đầu | `bronze`, `silver` hoặc `meta` |
| Username | `analyst` |
| Password | Để trống |
| JDBC URL nếu nhập thủ công | `jdbc:trino://trino:8080/lakehouse/bronze` |

`trino:8080` là địa chỉ giữa hai container; không điền `127.0.0.1:8083` trong CloudBeaver. Lần kết nối đầu có thể cần tải JDBC driver. Dùng cây **Database Navigator** để mở schema/bảng, tab **Data** để xem các hàng, hoặc **SQL Editor** để chạy truy vấn mẫu dưới đây. Trino vẫn ở chế độ chỉ đọc đối với Iceberg, nên CloudBeaver không ghi sửa bảng qua kết nối này.

Khi chỉ muốn tắt giao diện, chạy `make query-ui-down`; Trino vẫn chạy cho DBeaver/CLI. Nếu muốn tắt cả Trino, chạy tiếp `make query-down`. Tài khoản và kết nối CloudBeaver được giữ trong Docker volume `cloudbeaver_workspace` giữa các lần bật/tắt. Port `8978` chỉ bind vào localhost.

## Kết nối DBeaver desktop

Tạo kết nối **Trino** trong DBeaver với các giá trị:

| Trường | Giá trị |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `8083` |
| Catalog | `lakehouse` |
| Schema | `bronze` hoặc `meta` |
| Username | `analyst` (Trino local chưa bật xác thực người dùng) |
| Password | Để trống |
| JDBC URL nếu nhập thủ công | `jdbc:trino://127.0.0.1:8083/lakehouse/bronze` |

DBeaver có thể yêu cầu tải JDBC driver lần đầu. Port chỉ bind vào localhost; khi chạy Docker trong WSL2, kết nối từ Windows thường dùng `127.0.0.1:8083`. Không mở port này ra mạng công cộng khi Trino chưa có xác thực người dùng.

## Truy vấn mẫu

Các tên bảng có dạng `lakehouse.<namespace>.<table>`:

```sql
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.meta;
SHOW TABLES FROM lakehouse.bronze;
DESCRIBE lakehouse.bronze.historical_event_raw;
```

Xem 10 Raw object đã đăng ký:

```sql
SELECT source_id, source_version, object_uri, size_bytes, retrieved_at
FROM lakehouse.meta.source_objects
LIMIT 10;
```

Xem 10 dòng flood evidence ở Bronze:

```sql
SELECT source_record_id, event_text, source_valid_time
FROM lakehouse.bronze.historical_event_raw
LIMIT 10;
```

Xem vài polygon BasinATLAS mà không in WKB lớn:

```sql
SELECT source_feature_id,
       json_extract_scalar(source_fields_json, '$.HYBAS_ID') AS hybas_id,
       bbox_wgs84
FROM lakehouse.bronze.basin_polygon_raw
WHERE source_id = 'basinatlas_v10'
LIMIT 10;
```

Khi cần kiểm tra hình học cụ thể, `geometry_wkb` là binary; Trino đọc bằng `ST_GeomFromBinary()`. Ví dụ chỉ lấy geometry của một feature đã biết:

```sql
SELECT source_feature_id,
       ST_AsText(ST_GeomFromBinary(geometry_wkb)) AS geometry_wkt
FROM lakehouse.bronze.basin_polygon_raw
WHERE source_id = 'basinatlas_v10'
  AND source_feature_id = '<ID_CAN_XEM>'
LIMIT 1;
```

`bbox_wgs84` luôn ở EPSG:4326, còn `geometry_wkb` giữ CRS của dữ liệu nguồn theo cột `crs`. Trino không tự đổi CRS khi đọc WKB. Bảng `bronze.raster_coverage` chỉ có header/bbox/band; pixel vẫn ở Raw MinIO. Không dùng `SELECT *` hoặc tải toàn bộ bảng basin/river/OSM xuống DBeaver; luôn chọn cột cần xem, lọc theo nguồn/ID và dùng `LIMIT`.

## Phạm vi quyền đọc

`iceberg.security=READ_ONLY` chặn `CREATE`, `INSERT`, `UPDATE`, `DELETE` qua Iceberg connector. Đây là rào chắn chống ghi nhầm trong Trino, không phải cơ chế phân quyền theo người dùng: Trino local hiện chưa bật xác thực và container dùng credential của stack. Chỉ dùng profile này trên máy cá nhân hoặc mạng tin cậy.

Trino không thay thế MinIO Console: MinIO Console xem file Raw, Trino đọc **các hàng của bảng Iceberg**. CloudBeaver và DBeaver cung cấp trình duyệt bảng và SQL editor; Trino Web UI chủ yếu hiển thị trạng thái query.
