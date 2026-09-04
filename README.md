# Pipeline dữ liệu địa không gian tĩnh phục vụ cảnh báo lũ quét Sơn La

Repository này xây dựng **data lake dạng file** cho đồ án về lũ quét tại Sơn La. Đơn vị phân tích chính là lưu vực **HydroBASINS level 10 (L10)**; L8 và L9 chỉ được giữ làm quan hệ cha. Pipeline thu thập, kiểm tra, chuẩn hóa và liên kết dữ liệu thủy văn, địa hình, đất, lớp phủ, dân số, giao thông và địa giới hành chính.

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
- Hoàn thành workflow hermetic end-to-end, kiểm tra idempotence và recovery. Hiện có **347 test pass**.

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

Phase 2 hiện cung cấp một Docker Compose stack cố định gồm PostgreSQL, MinIO, Apache Polaris và Airflow 3. Đây mới là nền tảng lưu trữ/catalog/orchestration: **chưa có Spark và chưa ingest dữ liệu thời tiết**. Pipeline static cùng toàn bộ raw data hiện hữu vẫn giữ nguyên.

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
```

Các địa chỉ chỉ bind vào localhost:

- MinIO S3 API: <http://127.0.0.1:9000>
- MinIO Console: <http://127.0.0.1:9001>
- Polaris REST API: <http://127.0.0.1:8181>
- Airflow UI/API: <http://127.0.0.1:8080>
- PostgreSQL: `127.0.0.1:5432`

Tài khoản MinIO và Airflow local nằm trong `.env`. Catalog Polaris mặc định là `flood_lakehouse`, dùng bucket `s3://warehouse/`; bucket `raw` được giữ private cho dữ liệu nguồn. Các service chạy lâu có tổng giới hạn RAM 4.25 GiB. Lần chạy đầu cần tải image Docker nên sẽ lâu hơn; những lần sau tái sử dụng image và state hiện có.

## Trạng thái hiện tại

- Task 1–17: hoàn thành.
- Task 18: bỏ theo quyết định phạm vi; không có cleanup tự động.
- Task 19: phần code, smoke workflow, recovery và idempotence đã hoàn thành.
- Live crawl đã hoàn tất đủ 11 nguồn: HydroBASINS/BasinATLAS/HydroRIVERS, địa giới 2025, lịch sử 2020–2026, WorldCover, WorldPop, OSM, SoilGrids và COP DEM. SoilGrids có 96 raw GeoTIFF và 96 COG harmonized; OSM có 5 bảng exposure harmonized; 10 product container COP DEM được giữ nguyên trong raw.
- Các bước derive, map và QA đã chạy hoàn tất cho 168 basin L10. Lần chạy live xác nhận gần nhất có trạng thái `completed`, không có source lỗi và reuse 331 asset đã catalog hóa.
- Commit hoàn thiện core workflow gần nhất: `9544449`.
