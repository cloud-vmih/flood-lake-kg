# Pipeline dữ liệu địa không gian tĩnh phục vụ cảnh báo lũ quét Sơn La

Repository này xây dựng data platform cho đồ án về lũ quét tại Sơn La. Pipeline file hiện hữu
đang chuẩn hóa feature theo **HydroBASINS level 10 (L10)**; pipeline source landing mới chọn
**level 12 (L12) ngay từ đầu** cho HydroBASINS và BasinATLAS, lưu byte nguồn vào MinIO rồi đăng
ký inventory trong Iceberg. Các bước parse, harmonize và feature L12 sẽ được xây tiếp trên lớp
raw này.

Toàn bộ dữ liệu nguồn được lưu dưới `dataset/`. Pipeline **không xóa hoặc ghi đè raw data hợp lệ**. Chạy lại cùng cấu hình sẽ tái sử dụng artifact có fingerprint/checksum phù hợp; nếu lần chạy trước bị gián đoạn, có thể chạy lại chính lệnh đó để tiếp tục.

## Những phần đã hoàn thành

- Tạo package Python 3.11, CLI `flashflood-data`, cấu hình nguồn và catalog asset có checksum.
- Inventory 34 asset sẵn có, gồm HydroBASINS, BasinATLAS, HydroRIVERS, WorldPop, bảng sự kiện lịch sử và script/dữ liệu mẫu; các file gốc vẫn nằm tại đường dẫn ban đầu.
- Chọn các lưu vực L10 cắt Sơn La và một lớp lưu vực thượng nguồn trực tiếp; giữ quan hệ cha L9/L8.
- Xây dựng bốn vùng quan tâm (AOI):
  - **Core AOI:** địa giới Sơn La hiện hành sau cải cách năm 2025.
  - **Hydrological AOI:** Core cộng các lưu vực L10 thượng nguồn trực tiếp.
  - **Environmental Download AOI:** Hydrological AOI đệm thêm 10 km.
  - **Exposure AOI:** Core đệm 10 km và cắt theo biên giới Việt Nam.
- Thu thập/chuẩn hóa địa giới 75 đơn vị cấp xã hiện hành (67 xã, 8 phường), địa giới lịch sử GADM và bảng đối chiếu **xã cũ ↔ xã mới** để replay sự kiện giai đoạn 2020–2026.
- Tích hợp các nguồn dữ liệu:
  - HydroBASINS v1c, BasinATLAS v10 và HydroRIVERS v10.
  - SoilGrids 2.0: 8 thuộc tính, 6 tầng sâu, gồm mean và uncertainty.
  - Copernicus DEM GLO-30.
  - ESA WorldCover 2021 v200.
  - WorldPop Việt Nam 2025.
  - Geofabrik OpenStreetMap Việt Nam.
- Sinh feature theo từng lưu vực L10: địa hình, đất, lớp phủ và thủy văn.
- Sinh các bảng mapping lưu vực–xã, lưu vực–sông, lưu vực–đường/công trình, dân số và static profile cuối cùng.
- Xây dựng quality gates, báo cáo JSON/Parquet/HTML và bản đồ QA MapLibre.
- Hoàn thành workflow hermetic end-to-end, kiểm tra idempotence và recovery bằng bộ test tự động.
- Hoàn thành source landing L12 cho năm nhóm nguồn tĩnh, gồm immutable object/manifest trong
  MinIO, inventory `meta.source_objects` trong Iceberg và DAG Airflow chạy thủ công.

Task 18 về báo cáo dọn các level BasinATLAS không dùng đã được chủ động bỏ để giảm phạm vi. Vì vậy pipeline không cung cấp thao tác xóa tự động và không xóa dữ liệu hiện có.

## Cấu trúc chính

```text
.
├── config/                 # Cấu hình nguồn, AOI và feature
├── dataset/
│   ├── catalog/            # Catalog, checksum và trạng thái các asset
│   ├── raw/                # Dữ liệu tải mới, bất biến
│   ├── harmonized/         # Dữ liệu đã chuẩn hóa
│   ├── derived/            # Feature, mapping và static profile
│   ├── qa/                 # Báo cáo và bản đồ QA
│   ├── Data/               # Script/dữ liệu dân số mẫu có sẵn
│   └── ...                 # Các bộ HydroBASINS/BasinATLAS/HydroRIVERS gốc
├── src/flashflood_data/    # Mã nguồn pipeline và CLI
├── tests/                  # Unit, contract và integration tests
├── Makefile                # Các lệnh thường dùng
└── requirements.lock       # Dependency được khóa phiên bản
```

Không di chuyển, chỉnh sửa hoặc xóa thủ công file trong `dataset/raw/` và các thư mục dữ liệu gốc. Catalog dùng đường dẫn và checksum để quyết định asset nào có thể được tái sử dụng.

## Yêu cầu môi trường

- Linux hoặc môi trường có GDAL/GEOS tương thích.
- Python 3.11.
- Kết nối Internet cho lần crawl live.
- Tài khoản Copernicus Data Space Ecosystem (CDSE) để tải Copernicus DEM.
- Trước khi crawl: lượng raw mới dự kiến không quá **8 GiB** và vẫn còn tối thiểu **10 GiB** dung lượng trống sau khi tải.

## Cài đặt

Từ thư mục gốc repository:

```bash
make setup
```

Lệnh trên tạo `.venv` và cài dependency từ `requirements.lock`. Có thể kiểm tra môi trường bằng:

```bash
make test
make lint
```

## Cấu hình tài khoản Copernicus

Sao chép file mẫu và điền thông tin CDSE trên máy local:

```bash
cp .env.example .env
```

Nội dung `.env`:

```dotenv
FLASHFLOOD_CDSE_USERNAME=ten_dang_nhap_cua_ban
FLASHFLOOD_CDSE_PASSWORD=mat_khau_cua_ban
```

`.env` đã được Git ignore. Không commit file này, không ghi credential vào YAML và không gửi credential qua chat/log.

## Crawl toàn bộ dữ liệu

Nên chạy tuần tự bốn bước dưới đây. Các lệnh phải được thực thi tại thư mục gốc repository.

### 1. Inventory dữ liệu đang có

```bash
make inventory
```

Lệnh tương đương:

```bash
.venv/bin/flashflood-data inventory --root .
```

Inventory chỉ đăng ký và kiểm tra dữ liệu hiện hữu; không di chuyển file. Trạng thái gần nhất trước live crawl là 34 asset hợp lệ với tổng dung lượng khoảng 17.55 GB.

### 2. Dựng địa giới và các AOI trước

```bash
make preflight-aoi
```

Lệnh tương đương:

```bash
.venv/bin/flashflood-data run-static \
  --profile live \
  --root . \
  --stop-after aoi \
  --json-summary
```

Bước này bootstrap địa giới hiện hành/lịch sử và dựng các AOI, nhưng chưa tải payload môi trường và exposure dung lượng lớn.

### 3. Resolve nguồn và kiểm tra dung lượng

```bash
make resolve-live
```

Lệnh tương đương:

```bash
.venv/bin/flashflood-data fetch \
  --profile live \
  --root . \
  --resolve-only \
  --json-summary
```

Kiểm tra JSON summary trước khi tiếp tục. Chỉ chạy full crawl khi:

- CDSE authentication thành công.
- Tất cả nguồn bật trong `config/sources/*.yaml` resolve được.
- Dữ liệu raw mới dự kiến không vượt 8 GiB.
- Dung lượng dự phòng sau tải còn ít nhất 10 GiB.

Có thể chạy ba bước preflight liên tiếp bằng:

```bash
make preflight
```

### 4. Chạy full live pipeline

```bash
make live
```

Lệnh tương đương:

```bash
.venv/bin/flashflood-data run-static \
  --profile live \
  --root . \
  --json-summary
```

Workflow đầy đủ thực hiện:

```text
inventory → bootstrap admin → AOI → fetch → validate
          → harmonize → derive → map/profile → QA
```

Nếu một nguồn tạm thời lỗi mạng, pipeline vẫn giữ các nhánh và raw asset đã hoàn thành. Sửa nguyên nhân rồi chạy lại `make live`; không xóa catalog hoặc raw data để chạy lại từ đầu.

Nếu CDSE trả `DAT-ZIP-608: Access forbidden`, username/password đã được xác thực nhưng tài khoản chưa được dịch vụ Download/Zipper cấp quyền tải product COP DEM. Hãy kiểm tra quyền/điều khoản của tài khoản trên CDSE hoặc gửi `trace-id` của response cho bộ phận hỗ trợ CDSE, rồi chạy lại `make live`. Không cần xóa raw, catalog hay chạy lại các source đã hoàn thành.

## Kiểm tra idempotence sau khi crawl

Sau lần live đầu tiên thành công, chạy lại:

```bash
make live
```

Nếu nguồn và cấu hình không thay đổi, các artifact hợp lệ phải được tái sử dụng và số asset fetch/harmonize/derive mới phải bằng 0 hoặc không phát sinh rebuild ngoài dự kiến. Nếu checksum đầu ra bị thay đổi hoặc thiếu file, pipeline sẽ đánh dấu stale và dựng lại artifact liên quan.

## Xem kết quả QA

Khởi động web server local:

```bash
make qa-map
```

Sau đó mở <http://127.0.0.1:8000/>. Các kết quả QA machine-readable nằm tại:

```text
dataset/qa/report.json
dataset/qa/report.parquet
dataset/qa/report.html
dataset/qa/map/
```

Không nên bỏ qua quality gate lỗi trước khi dùng static profile cho bước Knowledge Graph hoặc mô hình học máy.

## Một số lệnh hữu ích

```bash
# Chạy toàn bộ test
make test

# Kiểm tra style/lint
make lint

# Chạy workflow fixture nhỏ, không crawl Internet
make smoke

# Xem trợ giúp CLI
.venv/bin/flashflood-data --help

# Chỉ xử lý một nguồn khi cần chẩn đoán
.venv/bin/flashflood-data fetch --profile live --root . --source <source_id>
```

Các `source_id` chính gồm:

```text
sonla_admin_2025
gadm_vnm_4_1
hydrobasins_v1c
basinatlas_v10
hydrorivers_v10
soilgrids_2_0
cop_dem_glo30_2024_1
esa_worldcover_2021_v200
worldpop_vnm_2025
geofabrik_vietnam_snapshot
historical_flood_evidence_2020_2026
```

## Hạ tầng Lakehouse cục bộ

Phase 2 hiện cung cấp một Docker Compose stack cố định gồm PostgreSQL, MinIO, Apache Polaris,
Airflow 3 và một cụm Spark standalone tùy chọn. Đây mới là nền tảng
lưu trữ/catalog/orchestration: **chưa ingest dữ liệu thời tiết**. Pipeline static cùng toàn bộ
raw data hiện hữu vẫn giữ nguyên.

Yêu cầu Docker Engine và Docker Compose v2. Nếu lệnh báo không có quyền truy cập Docker socket, tự thêm tài khoản hiện tại vào nhóm `docker`, sau đó đăng xuất và đăng nhập lại:

```bash
sudo usermod -aG docker "$USER"
```

Lưu ý: thành viên nhóm `docker` có quyền tương đương root trên máy. Các script của project không tự gọi `sudo`.

Khởi tạo secrets và thư mục persistent, sau đó khởi động stack:

```bash
make lakehouse-init
make lakehouse-up
make lakehouse-smoke
```

Cài cùng runtime forecast/Iceberg vào `.venv` và image Airflow, rồi kiểm tra kết nối đọc với
Polaris:

```bash
make lakehouse-python-setup
make lakehouse-airflow-build
make lakehouse-python-smoke
```

Runtime hiện khóa Xarray, PyIceberg, cfgrib và ecCodes. Lệnh smoke cuối cần stack đang chạy;
nó chỉ kiểm tra version, bộ giải mã GRIB và gọi `list_namespaces()` qua Polaris, không tải dữ
liệu, không tạo bảng và không ghi vào MinIO.

### Landing nguồn tĩnh vào MinIO và Iceberg

DAG thủ công `static_source_landing` thực hiện đúng lớp source landing: acquire hoặc tái sử dụng
payload đã validate, chọn HydroBASINS/BasinATLAS L12, đóng gói Shapefile thành ZIP xác định byte,
đưa object cùng manifest vào bucket MinIO `raw`, rồi đăng ký một row cho mỗi object trong
`meta.source_objects` qua Polaris. DAG dừng tại đó; parse sang `bronze.*`, harmonize, mapping,
feature và B0–B3 thuộc các pipeline sau.

Năm nhóm nguồn ban đầu chạy độc lập:

- `hydrobasins_v1c`: bundle L12;
- `basinatlas_v10`: bundle L12;
- `hydrorivers_v10`: bundle Asia;
- `cop_dem_glo30_2024_1`: từng source tile của Environmental AOI;
- `soilgrids_2_0`: 8 capabilities và 96 GeoTIFF thuộc 8 property, 3 depth 0–30 cm và 4 statistic.

Object có layout bất biến:

```text
s3://raw/static/<source_id>/<source_version>/<selection>/<asset_id>/<filename>
s3://raw/static/<source_id>/<source_version>/<selection>/<asset_id>/manifest.json
```

MinIO giữ byte nguồn; Iceberg chỉ giữ inventory URI/checksum/lineage. File ZIP, Shapefile và
GeoTIFF không được đăng ký trực tiếp làm data file của bảng Iceberg. Chạy smoke round-trip trước
khi bật DAG:

```bash
make lakehouse-up
make lakehouse-source-landing-smoke
docker compose exec airflow-api-server airflow dags unpause static_source_landing
```

Sau khi unpause, trigger DAG trong Airflow UI/API. DAG không có schedule và được tạo ở trạng thái
pause để tránh tự tải dữ liệu. Nếu một source lỗi, các source khác vẫn hoàn thành và run được đánh
dấu `partial_failure`. Sau khi sửa nguyên nhân, clear ba task trong TaskGroup bị lỗi hoặc chạy lại
riêng source đó trong container:

```bash
docker compose exec -T airflow-scheduler \
  flashflood-data land-static --root /opt/flashflood \
  --source soilgrids_2_0 --run-id <run-id> --json-summary
```

Retry dùng checksum và `object_id` xác định để khôi phục object, manifest hoặc Iceberg row còn
thiếu mà không tạo bản sao. Staging chỉ được xóa sau khi Iceberg commit thành công.

### Spark và Iceberg

Spark không tự khởi động cùng stack cơ sở. Build image đã khóa Spark 4.1.3, Scala 2.13 và
Iceberg 1.11.0, sau đó bật một master cùng một worker bằng:

```bash
make spark-build
make spark-up
make spark-status
make spark-smoke
make spark-down
```

`make spark-smoke` kiểm tra master thấy đúng một worker, tạo namespace riêng có tên ngẫu
nhiên, ghi và đọc lại ba dòng qua Iceberg REST catalog/MinIO rồi xóa đúng bảng và namespace
vừa tạo. Có thể chạy lặp lại; lệnh không đụng đến bảng dữ liệu khác. `make spark-down` chỉ xóa
container Spark, không dừng PostgreSQL, MinIO, Polaris hay Airflow.

Hai giao diện Spark chỉ bind vào localhost:

- Spark master UI: <http://127.0.0.1:8081>
- Spark worker UI: <http://127.0.0.1:8082>

Master và worker có tổng giới hạn RAM 3.25 GiB; job smoke dùng thêm tối đa 1.5 GiB trong thời
gian chạy. Vì vậy chỉ bật profile Spark khi cần xử lý dữ liệu.

`make lakehouse-init` tạo hoặc bổ sung `.env` nhưng không thay credential đã có. Không commit hay chia sẻ `.env`. Có thể xem trạng thái và dừng stack bằng:

```bash
make lakehouse-status
make lakehouse-down
```

`make lakehouse-down` chỉ dừng/xóa container và network Compose; không xóa dữ liệu persistent. Toàn bộ state nằm dưới `dataset/lakehouse/`, tách biệt với raw data:

```text
dataset/lakehouse/postgres/     # database Airflow và Polaris
dataset/lakehouse/minio/        # bucket private raw và warehouse
dataset/lakehouse/airflow/logs/ # log Airflow
dataset/lakehouse/staging/      # staging theo run, xóa sau Iceberg commit
```

Các địa chỉ chỉ bind vào localhost:

- MinIO S3 API: <http://127.0.0.1:9000>
- MinIO Console: <http://127.0.0.1:9001>
- Polaris REST API: <http://127.0.0.1:8181>
- Airflow UI/API: <http://127.0.0.1:8080>
- PostgreSQL: `127.0.0.1:5432`

Tài khoản MinIO và Airflow local nằm trong `.env`. Catalog Polaris mặc định là `flood_lakehouse`, dùng bucket `s3://warehouse/`; bucket `raw` được giữ private cho dữ liệu nguồn. Các service nền chạy lâu có tổng giới hạn RAM 4.25 GiB, chưa tính profile Spark tùy chọn. Lần chạy đầu cần tải image Docker nên sẽ lâu hơn; những lần sau tái sử dụng image và state hiện có.

## Trạng thái hiện tại

- Task 1–17: hoàn thành.
- Task 18: bỏ theo quyết định phạm vi; không có cleanup tự động.
- Task 19: phần code, smoke workflow, recovery và idempotence đã hoàn thành.
- Source landing L12 đã có CLI, DAG Airflow, MinIO publication, manifest và Iceberg
  `meta.source_objects`; dữ liệu chỉ được upload khi operator trigger DAG/lệnh `land-static`.
- Live crawl đã hoàn tất đủ 11 nguồn: HydroBASINS/BasinATLAS/HydroRIVERS, địa giới 2025, lịch sử 2020–2026, WorldCover, WorldPop, OSM, SoilGrids và COP DEM. SoilGrids có 96 raw GeoTIFF và 96 COG harmonized; OSM có 5 bảng exposure harmonized; 10 product container COP DEM được giữ nguyên trong raw.
- Các bước derive, map và QA đã chạy hoàn tất cho 168 basin L10. Lần chạy live xác nhận gần nhất có trạng thái `completed`, không có source lỗi và reuse 331 asset đã catalog hóa.
- Commit hoàn thiện core workflow gần nhất: `9544449`.
