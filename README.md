# FloodLakeKG — Geospatial Lakehouse cho lũ quét Sơn La

Repository xây dựng Lakehouse địa không gian để tích hợp dữ liệu theo phiên bản, tạo feature theo
lưu vực nhỏ và chuẩn bị dữ liệu cho threat B0–B3, Knowledge Graph và routing lũ quét tại Sơn La.

Luồng vận hành hiện tại dùng **HydroBASINS level 12 (L12)**, hai DAG static và ba DAG
weather động:

```mermaid
flowchart LR
    A[make lakehouse-aoi] --> B[4 AOI L12]
    B --> C[static_source_landing]
    C --> D[MinIO raw]
    C --> E[Iceberg meta]
    D --> F[static_source_to_bronze]
    E --> F
    F --> G[Iceberg bronze]
    F --> H[Quality và lineage trong meta]
    G --> I[Silver L12 - kế hoạch tiếp theo]
    B --> W[gsmap / era5_land / ifs ingest]
    W --> D
    W --> E
    W --> G
```

Pipeline file-first L10 trước đây không còn là luồng vận hành của project. Artifact L10 trong
`dataset/harmonized`, `dataset/derived` và `dataset/qa` chỉ được giữ để đối chiếu; README không
hướng dẫn chạy lại pipeline đó.

## Trạng thái hiện tại

Cập nhật ngày **24/09/2026**:

| Hạng mục | Trạng thái |
| --- | --- |
| PostgreSQL, MinIO, Polaris và Airflow 3 | Đã vận hành bằng Docker Compose |
| AOI | Đã dựng đủ Core, Hydrological, Environmental và Exposure theo L12 |
| Static Source Landing | 10 nguồn đang hoạt động; source flood-event legacy đã gỡ khỏi DAG nhưng dữ liệu cũ vẫn được giữ |
| Static Bronze | Sáu bảng Bronze đã có dữ liệu; parser chạy idempotent theo `object_id` |
| Trino/DBeaver | Đã có profile đọc Iceberg qua Polaris |
| Spark/Iceberg | Đã có profile tùy chọn và smoke test ghi–đọc bảng tạm |
| Silver L12 | Chưa triển khai |
| GSMaP, ERA5-Land và IFS | Code và ba DAG Raw → Meta → Bronze đã hoàn tất; sample GSMaP Standard/NOW đã chạy trọn luồng, ERA5-Land và IFS chưa chạy provider thật; cả ba DAG đang pause |
| Threat B0–B3, KG, routing và dashboard | Chưa triển khai |

Snapshot Iceberg được kiểm tra gần nhất:

| Bảng | Số dòng | Nội dung |
| --- | ---: | --- |
| `meta.source_objects` | 209 | Raw object bất biến, gồm hai object GSMaP sample và dữ liệu static/legacy |
| `meta.pipeline_runs` | 212 | Run landing/parse và quyết định publish |
| `meta.quality_results` | 1.211 | Kết quả quality rule theo run/object/snapshot |
| `meta.table_snapshot_ref` | 204 | Liên kết run với Iceberg snapshot |
| `meta.lineage_edges` | 204 | Lineage Raw object → Bronze snapshot |
| `bronze.basin_polygon_raw` | 1.194.591 | HydroBASINS L12 và BasinATLAS L12 |
| `bronze.river_reach_raw` | 1.428.959 | HydroRIVERS Asia |
| `bronze.admin_boundary_raw` | 12.012 | Địa giới Sơn La và GADM lịch sử |
| `bronze.raster_coverage` | 111 | Metadata raster; pixel vẫn nằm trong MinIO |
| `bronze.historical_event_raw` | 32 | Dữ liệu legacy được giữ lại; static DAG không còn cập nhật bảng này |
| `bronze.osm_feature_raw` | 763.704 | Nhóm OSM phục vụ lũ và facility thiết yếu |
| `bronze.weather_grid_value` | 800 | Hai lát GSMaP Standard/NOW lúc 00:00 UTC ngày 20/09/2026, 400 ô AOI mỗi lát |

Các số trên là snapshot, không phải hằng số. Dùng Trino hoặc `bronze reconcile` để kiểm tra trạng
thái thực tế sau mỗi lần chạy DAG.

Sample GSMaP là backfill có giới hạn nên không đẩy `meta.ingest_watermarks`. Hai payload Raw đã
đăng ký trong `meta.source_objects`; 800 row Bronze có `quality_status='passed'`. ERA5-Land và IFS
vẫn cần sample provider thật trước khi chạy catch-up lớn.

## Yêu cầu môi trường

- Linux x86_64 hoặc Windows + WSL2 x86_64.
- Docker Engine và Docker Compose v2.
- Python 3.11, Git, GNU Make và OpenSSL.
- Tài khoản Copernicus Data Space Ecosystem để tải DEM.
- Nên có ít nhất 10 GiB dung lượng trống sau khi tải dữ liệu.

Trên WSL2, đặt repository trong filesystem Linux, ví dụ `~/projects/FloodLakeKG`; không đặt repository dưới `/mnt/c/`
vì bind mount và quyền file sẽ chậm, dễ lỗi hơn. Cũng không đặt volume PostgreSQL/MinIO dưới `/mnt/c/`.
Nếu user chưa dùng được Docker mà không có `sudo`, chạy `sudo usermod -aG docker "$USER"`, đăng
xuất rồi đăng nhập lại WSL.

## Cài đặt và khởi động

Bootstrap đầy đủ trên máy mới:

```bash
make bootstrap
```

Lệnh này kiểm tra prerequisite, tạo `.venv`, cài runtime, sinh secret local, build image, khởi
động stack và chạy smoke test. Nếu Python 3.11 có đường dẫn khác:

```bash
make bootstrap PYTHON=/duong/dan/toi/python3.11
```

Nếu chỉ muốn tạo môi trường Python trước khi dựng Docker, dùng:

```bash
make setup PYTHON=/duong/dan/toi/python3.11
```

Các lệnh vận hành stack:

```bash
make lakehouse-up
make lakehouse-status
make lakehouse-smoke
make lakehouse-down
```

`make lakehouse-down` chỉ dừng container và network, không xóa state. Dữ liệu persistent nằm tại:

```text
dataset/lakehouse/postgres/
dataset/lakehouse/minio/
dataset/lakehouse/airflow/logs/
dataset/lakehouse/staging/
```

Có thể đặt `LAKEHOUSE_DATA_ROOT=/duong/dan/tuyet/doi` trong `.env` để chuyển state sang ổ Linux
khác. Không commit `.env`.

### Cấu hình Copernicus DEM

Chạy `make lakehouse-init` hoặc tạo `.env` từ file mẫu, rồi điền:

```dotenv
FLASHFLOOD_CDSE_USERNAME=ten_dang_nhap
FLASHFLOOD_CDSE_PASSWORD=mat_khau
```

Không ghi credential vào YAML, Git, chat hoặc log.

### Cấu hình nguồn weather động

Ba DAG weather chỉ đọc tên credential từ môi trường. Điền các giá trị cần dùng vào `.env`:

```dotenv
# JAXA GSMaP: URL template do tài khoản/archive cung cấp; hỗ trợ placeholder
# {year}, {month}, {day}, {hour}, {minute}, {product}.
GSMAP_STANDARD_URL_TEMPLATE=https://host/path/{year}/{month}/{day}/file-{hour}{minute}.dat.gz
GSMAP_NOW_URL_TEMPLATE=https://host/path/{year}/{month}/{day}/file-{hour}{minute}.dat.gz
GSMAP_USERNAME=...
GSMAP_PASSWORD=...

# Copernicus CDS API
CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=...

# Không bắt buộc với endpoint Open-Meteo public; điền khi dùng commercial key.
OPEN_METEO_API_KEY=
```

Tài khoản CDS phải chấp nhận terms của dataset ERA5-Land trước lần tải đầu. Không đặt secret trong
`config/dynamic/*.yaml`.

Tài liệu provider: [JAXA GSMaP user guide](https://sharaku.eorc.jaxa.jp/GSMaP/guide.html),
[Copernicus CDS API setup](https://cds.climate.copernicus.eu/how-to-api),
[ERA5-Land hourly](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land) và
[Open-Meteo Single Runs](https://open-meteo.com/en/docs/single-runs-api).

### Cập nhật code trong image Airflow

Package `src/flashflood_data` được cài vào image, không bind mount trực tiếp. Sau khi sửa code
Landing/Bronze, build và recreate các service Airflow trước khi trigger DAG:

```bash
make lakehouse-airflow-build
docker compose up -d --no-deps --force-recreate \
  airflow-api-server airflow-scheduler airflow-dag-processor
make lakehouse-status
```

## Bước 1 — Dựng AOI L12

Đặt `hybas_as_lev12_v1c.*` trong `dataset/hybas_as_lev01-12_v1c/`, sau đó chạy:

```bash
make lakehouse-aoi
```

Lệnh tạo bốn file trong `dataset/harmonized/aoi/`:

| AOI | Ý nghĩa |
| --- | --- |
| `core_aoi.geoparquet` | Địa giới Sơn La hiện hành |
| `hydrological_aoi.geoparquet` | Core và các basin L12 thượng nguồn được chọn |
| `environmental_aoi.geoparquet` | Hydrological AOI cộng buffer để lấy dữ liệu môi trường |
| `exposure_aoi.geoparquet` | Core cộng buffer và giới hạn trong Việt Nam |

Các DAG không tự dựng AOI. Cần chạy lại bước này khi thay đổi basin level, upstream hops hoặc
buffer trong `config/study_area.yaml`.

### AOI ảnh hưởng dữ liệu raster thế nào

| Nguồn | Đơn vị Raw | Hành vi khi AOI thay đổi |
| --- | --- | --- |
| Copernicus DEM GLO-30 | Tile DEM | Tính lại tile giao Environmental AOI; chỉ tải tile còn thiếu |
| ESA WorldCover 2021 | Tile lớp phủ đất | Tính lại tile giao Environmental AOI; chỉ tải tile còn thiếu |
| SoilGrids 2.0 | GeoTIFF theo property × depth × statistic | Kiểm tra extent thực tế; phủ đủ thì tái sử dụng, thiếu thì tải bbox mới với ID mới |
| WorldPop 2025 | Raster Việt Nam | Raw không được cắt theo AOI ở Landing |

Raw object cũ luôn được giữ. AOI mở rộng chỉ tạo object mới khi xuất hiện tile mới hoặc vượt
extent SoilGrids cũ.

## Bước 2 — DAG `static_source_landing`

DAG Landing acquire hoặc tái sử dụng nguồn, validate, upload object/manifest lên bucket `raw`,
đăng ký Iceberg Meta và ghi audit. DAG dừng ở Raw, chưa parse field và chưa harmonize Silver.

```mermaid
flowchart TD
    R[register_meta_registry] --> P[publish_source theo source_id]
    P --> B[register_batch]
    B --> C[cleanup_batch]
    C --> A[audit_registered_meta]
    A --> S[publish_run_summary]
```

Các task có vai trò khác nhau:

| Task | Tác dụng |
| --- | --- |
| `register_meta_registry` | Upsert khai báo nguồn và dataset vào `meta.source_registry`, `meta.dataset_registry` |
| `publish_source` | Acquire/reuse, validate và upload object cùng manifest lên MinIO |
| `register_batch` | Đăng ký từng object thực tế vào `meta.source_objects` trong một Iceberg commit |
| `cleanup_batch` | Xóa staging và bản local tạm chỉ sau khi commit thành công |
| `audit_registered_meta` | Ghi `ingest_attempts`, `quality_results`, `table_snapshot_ref`, `pipeline_runs` |
| `publish_run_summary` | Tổng hợp source thành công/thất bại cho toàn DAG run |

Mười source hiện được cấu hình trong `config/landing/static.yaml`:

```text
sonla_admin_2025
gadm_vnm_4_1
hydrobasins_v1c
basinatlas_v10
hydrorivers_v10
worldpop_vnm_2025
geofabrik_vietnam_snapshot
cop_dem_glo30_2024_1
soilgrids_2_0
esa_worldcover_2021_v200
```

Object có layout bất biến:

```text
s3://raw/static/<source_id>/<source_version>/<selection>/<asset_id>/<filename>
s3://raw/static/<source_id>/<source_version>/<selection>/<asset_id>/manifest.json
```

MinIO giữ byte nguồn. Iceberg giữ URI, checksum, selection, audit và lineage; GeoTIFF/ZIP không
được đăng ký trực tiếp làm data file của bảng Iceberg.

### Chạy Landing DAG

```bash
make lakehouse-source-landing-smoke
docker compose exec -T airflow-api-server \
  airflow dags unpause static_source_landing
docker compose exec -T airflow-scheduler \
  airflow dags trigger static_source_landing
```

DAG không có schedule. Có thể trigger trong Airflow UI tại <http://127.0.0.1:8080/>.

Chạy lại là an toàn: checksum và `object_id` giúp tái sử dụng object/manifest/row đã tồn tại. Nếu
AOI mở rộng, DEM và WorldCover chỉ bổ sung tile thiếu; SoilGrids chỉ tạo bbox raw mới khi raster
cũ không còn phủ đủ.

Nếu một source lỗi, các source khác vẫn có thể hoàn thành và run được đánh dấu `partial_failure`.
Sau khi sửa nguyên nhân, trigger lại DAG; không xóa bucket Raw hoặc bảng Meta để chạy lại từ đầu.

## Bước 3 — DAG `static_source_to_bronze`

DAG Bronze đọc `meta.source_objects`, tải object từ MinIO, parse định dạng nguồn, chạy quality
gate và ghi Iceberg theo từng `object_id`:

| Bảng Bronze | Nguồn |
| --- | --- |
| `bronze.basin_polygon_raw` | HydroBASINS, BasinATLAS |
| `bronze.river_reach_raw` | HydroRIVERS |
| `bronze.admin_boundary_raw` | Sơn La 2025, GADM |
| `bronze.raster_coverage` | DEM, SoilGrids, WorldCover, WorldPop |
| `bronze.osm_feature_raw` | OSM hydrology, hydraulic structure, critical transport/facility |

`bronze.historical_event_raw` và Raw/Meta liên quan vẫn được giữ từ các lần chạy trước. Source
`historical_flood_evidence_2020_2026` đã được gỡ khỏi cả hai static DAG để chờ pipeline
flood-event riêng tiếp quản; trigger lại static DAG không xử lý source này.

`bronze.raster_coverage` chỉ lưu header, bbox, band, checksum và URI. Pixel vẫn nằm trong object
Raw ở MinIO; Bronze không mosaic hoặc cắt raster theo AOI.

### Idempotence của Bronze

Mỗi lần chạy, task `discover_<source_id>` vẫn kiểm tra toàn bộ object khả dụng. Một object được
skip khi đồng thời có:

- pipeline run đã publish thành công;
- lineage đúng `(object_id, target_table, parser_version)`;
- slice của object vẫn tồn tại trong bảng Bronze.

Object mới từ Landing được parse và thêm slice mới. Object cũ hợp lệ không tạo parse task. Bật
`force_reprocess` sẽ xử lý lại object và replace đúng slice của object đó, không append trùng.

### Chạy Bronze DAG

```bash
make lakehouse-meta-bronze-smoke
docker compose exec -T airflow-api-server \
  airflow dags unpause static_source_to_bronze
docker compose exec -T airflow-scheduler \
  airflow dags trigger static_source_to_bronze
```

Chỉ chạy một source:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger \
  static_source_to_bronze \
  --conf '{"source_id":"geofabrik_vietnam_snapshot"}'
```

Chủ động parse lại source:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger \
  static_source_to_bronze \
  --conf '{"source_id":"geofabrik_vietnam_snapshot","force_reprocess":true}'
```

Đối soát Raw, Bronze và Meta:

```bash
.venv/bin/flashflood-data bronze reconcile
```

## Bước 4 — Ba DAG weather động

| DAG | Schedule | Product | Giới hạn mặc định mỗi run |
| --- | --- | --- | ---: |
| `gsmap_ingest` | phút 17 mỗi giờ | Gauge Standard v8 và Gauge NOW v8 | 336 object |
| `era5_land_ingest` | 02:43 hằng ngày | ERA5-Land hourly | 3 tháng hoàn chỉnh |
| `ifs_ingest` | phút 12 mỗi 6 giờ | IFS HRES Single Runs | 28 cycle |

Schedule chỉ đánh thức DAG. Mỗi lần chạy, planner đọc `meta.ingest_watermarks`, lập các cửa sổ từ
cursor tới mốc an toàn của provider, đối chiếu `meta.source_objects`, rồi tải object thiếu hoặc
nằm trong overlap cần kiểm tra revision. Vì `catchup=False`, Airflow không tạo hàng loạt DAG run
cho thời gian Docker đã tắt; application tự bù phần thiếu khi stack chạy lại.
ERA5-Land chỉ tiến theo ranh giới tháng đã hoàn chỉnh để identity của request ổn định và không tạo
các chunk tháng chồng lấn. Mốc an toàn hiện dùng lag bảo thủ; nếu provider chưa có object dự kiến,
task fetch fail và watermark giữ nguyên để lần sau thử lại.
Gauge NOW có cửa sổ mưa một giờ nhưng phát hành mỗi 30 phút, nên planner tạo cả asset `HH:00` và
`HH:30`. Parser đọc binary float32 little-endian và đổi các mã `-4`, `-8`, `-99` thành null.

Trong mỗi DAG có hai TaskGroup:

```text
register_dynamic_registry

landing_raw
  load_cursor → determine_available_end → plan_expected_windows
  → extract_missing_objects → fetch_missing_or_revised
  → register_raw_and_meta
  → verify_contiguous_coverage → advance_cursor

bronze
  discover_unparsed_objects → parse_bronze → publish_bronze_update
```

Raw được lưu bất biến theo checksum dưới:

```text
s3://raw/weather/<source_id>/<product>/YYYY/MM/DD/<asset_id>/<checksum>/<filename>
s3://raw/weather/<source_id>/<product>/YYYY/MM/DD/<asset_id>/<checksum>/<filename>.manifest.json
```

`meta.source_objects` giữ inventory; `meta.ingest_watermarks` giữ cursor riêng theo product/stream;
`bronze.weather_grid_value` giữ từng giá trị tại grid/time theo đúng biến và unit của nguồn. Nếu
Bronze lỗi sau khi Raw đã commit, lần chạy sau chỉ discover và parse lại Raw còn thiếu, không tải
lại payload đã commit ngoài cửa sổ overlap.
Với ERA5-Land, Bronze giữ đúng accumulation gốc từ 00 UTC; timestamp 00 UTC mang cửa sổ 24 giờ
trước đó. Việc de-accumulate sang lượng theo giờ thuộc pipeline Silver để không làm mất nghĩa gốc.

Build lại image sau khi pull code vì Airflow cần thêm `cdsapi`:

```bash
make lakehouse-airflow-build
docker compose up -d --no-deps --force-recreate \
  airflow-api-server airflow-scheduler airflow-dag-processor
```

Sau đó unpause DAG cần chạy:

```bash
docker compose exec -T airflow-api-server airflow dags unpause gsmap_ingest
docker compose exec -T airflow-api-server airflow dags unpause era5_land_ingest
docker compose exec -T airflow-api-server airflow dags unpause ifs_ingest
```

Trigger catch-up theo watermark:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger gsmap_ingest
docker compose exec -T airflow-scheduler airflow dags trigger era5_land_ingest
docker compose exec -T airflow-scheduler airflow dags trigger ifs_ingest
```

Backfill một khoảng rõ ràng không đẩy cursor operational:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger gsmap_ingest \
  --conf '{"mode":"backfill","start":"2020-01-01T00:00:00Z","end":"2020-02-01T00:00:00Z"}'
```

`max_objects` trong DAG conf cho phép giảm/tăng batch của một lần chạy. Khoảng lớn hơn giới hạn
sẽ được xử lý qua các lần catch-up tiếp theo; không truyền hàng chục nghìn object qua một XCom.
ERA5-Land và IFS dùng `hydrological_aoi.geoparquet`; GSMaP giữ file provider trong Raw và chỉ
phát sinh các row Bronze nằm trong bbox AOI này.

Ba DAG được tạo với `is_paused_upon_creation=True` và `max_active_runs=1`. Fetch dùng pool
`weather_fetch` có bốn slot, retry exponential backoff tối đa 30 phút; ghi Raw/Meta và Bronze dùng
hai pool một slot riêng để tránh nhiều writer cùng commit Iceberg. Hiện `publish_bronze_update`
vẫn chạy sau khi nhóm Bronze thành công kể cả khi danh sách object cần parse rỗng; downstream về
sau phải dựa vào snapshot/lineage hoặc reconciliation, không dùng asset event làm nguồn sự thật.

## Xem log Airflow

Trong Airflow UI: mở DAG → DAG run → task instance → **Logs**.

```bash
docker compose logs -f \
  airflow-scheduler airflow-dag-processor airflow-api-server

find dataset/lakehouse/airflow/logs -type f | sort
```

Mọi container Airflow phải dùng cùng `AIRFLOW_API_SECRET_KEY`. Nếu đổi key, recreate đồng thời
API server, scheduler và DAG processor; nếu chỉ recreate một container, token log cũ có thể báo
`InvalidSignatureError`.

## Đọc dữ liệu Iceberg

Khởi động Trino read-only:

```bash
make query-up
make query-status
make query-smoke
```

Ví dụ truy vấn:

```bash
docker compose --profile query exec -T trino trino \
  --catalog lakehouse --schema bronze \
  --execute 'SELECT source_id, object_id, bbox_wgs84 FROM raster_coverage LIMIT 10'
```

Trino Web UI tại <http://127.0.0.1:8083/> chỉ hiển thị trạng thái query. Dùng CLI hoặc DBeaver để
viết SQL. Hướng dẫn DBeaver và truy vấn mẫu nằm tại
[docs/query_iceberg_with_trino.md](docs/query_iceberg_with_trino.md).

Các endpoint local:

| Service | Địa chỉ |
| --- | --- |
| Airflow | <http://127.0.0.1:8080/> |
| MinIO Console | <http://127.0.0.1:9001/> |
| MinIO S3 API | `127.0.0.1:9000` |
| Polaris REST API | `127.0.0.1:8181` |
| Trino | <http://127.0.0.1:8083/> |
| PostgreSQL | `127.0.0.1:5432` |

## Spark tùy chọn

Spark không khởi động cùng stack cơ sở:

```bash
make spark-build
make spark-up
make spark-status
make spark-smoke
make spark-down
```

Smoke test tạo một bảng Iceberg tạm qua Polaris/MinIO, đọc lại rồi xóa bảng. Spark hiện là runtime
chuẩn bị cho transform lớn; hai DAG static hiện tại chạy bằng Python/Airflow.

## Cấu trúc code liên quan pipeline DAG

```text
.
├── airflow/dags/
│   ├── static_source_landing.py       # Source → Raw MinIO + Meta
│   ├── static_source_to_bronze.py     # Raw/Meta → Bronze + audit/lineage
│   ├── gsmap_ingest.py                # GSMaP Raw → Bronze
│   ├── era5_land_ingest.py            # ERA5-Land Raw → Bronze
│   └── ifs_ingest.py                  # IFS/Open-Meteo Raw → Bronze
├── config/
│   ├── study_area.yaml                # L12, upstream hops, buffer và CRS
│   ├── sources/*.yaml                 # Adapter, version và policy của nguồn
│   ├── dynamic/*.yaml                 # Product, schedule, biến, overlap và chunk weather
│   ├── landing/static.yaml            # 10 source đang hoạt động và selection của Landing
│   ├── bronze/static.yaml             # source → parser/table/version
│   ├── bronze/osm.yaml                # Allowlist OSM phục vụ lũ
│   └── meta/static.yaml               # Registry nguồn/dataset
├── src/flashflood_data/
│   ├── orchestration/landing/         # Acquire, publish, register, audit, cleanup
│   ├── orchestration/bronze/          # Discover, parse, quality, reconcile
│   ├── orchestration/weather/          # Planner, watermark, provider, Raw/Bronze, DAG factory
│   ├── orchestration/meta/            # Ghi registry, run, quality, snapshot, lineage
│   ├── static/sources/                # Adapter theo từng nhà cung cấp
│   └── storage/                       # MinIO, Polaris và Iceberg table/schema
├── infra/                             # Docker image và cấu hình service
├── tools/bootstrap/                   # Init môi trường và prerequisite
├── tools/smoke/                       # Smoke test stack/pipeline
├── tests/                             # Unit, contract và integration tests
├── compose.yaml
└── Makefile
```

Adapter là lớp nối pipeline chung với chi tiết từng nguồn. Ví dụ SoilGrids adapter tạo WCS bbox,
DEM adapter chọn tile CDSE và WorldCover adapter chọn tile lớp phủ. DAG chỉ điều phối dependency,
retry, mapping và pool; logic dữ liệu nằm trong package để test và CLI recovery dùng chung.

## Pipeline tiếp theo

Thứ tự triển khai dự kiến:

1. Chạy sample thật cho GSMaP, ERA5-Land và IFS; xác minh credential, quota, format, latency và
   kích thước trước khi backfill lớn.
2. `static_bronze_to_silver`: schema chuẩn L12, topology, raster/OSM aggregate theo basin.
3. `flood_event_landing`, `flood_event_to_bronze` và `flood_event_harmonize`: tách sự kiện lũ
   khỏi static pipeline.
4. `weather_bronze_to_silver` và `weather_silver_to_gold`: map grid → basin L12, chọn
   final/provisional và tạo basin-hour forcing.
5. Threat B0–B3, Knowledge Graph, routing, API và dashboard.

Schema contract hiện tại nằm tại [docs/schema_contract/data.md](docs/schema_contract/data.md).
Tổng quan pipeline và kế hoạch tiếp theo nằm tại
[docs/pipeline_architecture_and_roadmap.md](docs/pipeline_architecture_and_roadmap.md).

## Phát triển và kiểm tra

```bash
make test
make lint
docker compose config --quiet
```

Bộ test gần nhất: **579 passed**. Trước khi push, không commit `.env`, volume dưới
`dataset/lakehouse/` hoặc credential cá nhân.
