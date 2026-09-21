# FloodLakeKG — Geospatial Lakehouse và Knowledge Graph cho lũ quét Sơn La

Repository này xây dựng prototype nghiên cứu để tích hợp dữ liệu địa không gian theo phiên bản,
đánh giá threat theo lưu vực nhỏ, sàng lọc phơi nhiễm và hỗ trợ phân tích tác động lũ quét tại
Sơn La.

Repository hiện có hai luồng cần phân biệt:

- Pipeline file-first cũ đã crawl, harmonize, derive, map và QA dữ liệu tĩnh cho **168 lưu vực
  HydroBASINS level 10 (L10)**. L8 và L9 được giữ làm quan hệ cha.
- Pipeline Lakehouse hiện tại chọn **level 12 (L12) ngay từ source landing** cho HydroBASINS và
  BasinATLAS. Toàn bộ 11 nguồn tĩnh đã được đưa vào Raw/MinIO, đăng ký trong Iceberg và parse vào
  Bronze. Silver L12, dữ liệu động, threat B0–B3, Knowledge Graph, routing và ứng dụng demo là
  các bước tiếp theo.

## Trạng thái dự án

Cập nhật ngày **21/09/2026**. Trạng thái dưới đây dựa trên artifact, mã nguồn, catalog đang chạy
và test hiện có trong repository; “hoàn thành” không đồng nghĩa đã được GVHD nghiệm thu.

| Hạng mục | Trạng thái | Bằng chứng hiện có |
|---|---|---|
| Dữ liệu địa không gian tĩnh file-first | Hoàn thành | 11 nguồn, 168 lưu vực L10, raw/harmonized/derived, mapping và QA |
| Nền tảng Lakehouse & Source Landing | Đã vận hành | PostgreSQL, MinIO, Polaris, Airflow; 207 raw object của 11 nguồn trong `meta.source_objects` |
| Meta & Bronze Lakehouse tĩnh | Đã nạp dữ liệu | 5 bảng Meta và 6 bảng Bronze đang có dữ liệu; contract/code đã khai báo đủ 9 bảng Meta |
| Runtime Python và Spark/Iceberg | Hoàn thành ở mức smoke test | Xarray/cfgrib/ecCodes/PyIceberg; Spark ghi–đọc–dọn bảng Iceberg tạm qua Polaris/MinIO |
| Khảo sát nguồn động | Một phần | Đã khảo sát IFS HRES; chưa lưu probe tái lập và chưa tải/đọc GSMaP thật |
| Silver, ingest động và threat B0–B3 | Chưa triển khai | Chưa có bảng Silver L12, basin-hour dataset, pixel weights hoặc kết quả B0–B3 |
| Exposure động, Knowledge Graph, routing, API/dashboard | Chưa triển khai | Chưa có PostGIS/Neo4j serving, graph rules, route engine hoặc demo đầu cuối |


Theo dõi chi tiết 52 task và các cổng nghiệm thu tại [docs/PROGRESS_TRACKING.md](docs/PROGRESS_TRACKING.md). Kế hoạch thí nghiệm threat engine nằm tại [docs/Ke_hoach_trien_khai_B0_B3_Son_La.docx](docs/Ke_hoach_trien_khai_B0_B3_Son_La.docx).

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
- Hoàn thành workflow hermetic end-to-end, kiểm tra idempotence và recovery.
- Dựng stack Lakehouse cục bộ gồm PostgreSQL, MinIO, Polaris và Airflow 3.
- Bổ sung runtime Python cho dữ liệu thời tiết/Iceberg và cụm Spark 4.1.3 + Iceberg 1.11.0 tùy chọn.
- Kiểm tra thành công vòng ghi–đọc–dọn bảng Iceberg tạm qua Polaris và MinIO.
- Hoàn thành source landing cho 11 nguồn tĩnh, gồm immutable object/manifest trong MinIO,
  inventory `meta.source_objects` trong Iceberg và DAG Airflow chạy thủ công.
- Parse toàn bộ source object hiện có vào sáu bảng Bronze; OSM chỉ giữ các nhóm phục vụ lũ và
  facility thiết yếu.
- Ghi audit cho Bronze vào `meta.pipeline_runs`, `meta.quality_results`,
  `meta.table_snapshot_ref` và `meta.lineage_edges`.
- Bộ test đầy đủ được xác nhận ngày 21/09/2026: **536 passed**.

Pipeline không cung cấp thao tác cleanup tự động cho các level BasinATLAS ngoài phạm vi và không xóa dữ liệu hiện có.

## Tóm tắt dữ liệu đã crawl

Snapshot dữ liệu gần nhất được tạo sau lần chạy `make live` hoàn tất ngày **24/08/2026**. Catalog hiện ghi nhận **234 asset nguồn đã validate, tổng dung lượng khoảng 19,38 GB**. Trong đó có 229 asset thuộc các nguồn chính của pipeline và 5 asset mẫu/legacy đã có từ trước. Catalog đầy đủ gồm 379 bản ghi: 234 `validated`, 117 `harmonized` và 28 `derived`.

| Nhóm dữ liệu | Raw đã lưu | Sản phẩm sau chuẩn hóa/xử lý |
|---|---:|---|
| HydroBASINS v1c | 12 asset · 1,01 GB | 168 basin L10 và 168 quan hệ cha L9/L8 |
| BasinATLAS v10 | 13 asset · 15,81 GB | 168 basin L10, mỗi dòng có 295 cột thuộc tính nguồn |
| HydroRIVERS v10 | 1 asset · 378,30 MB | 2.081 river reach trong AOI thủy văn |
| Địa giới 2025 và GADM lịch sử | 79 asset · 11,33 MB | 75 xã/phường hiện hành, 204 đơn vị lịch sử và 200 dòng crosswalk |
| SoilGrids 2.0 | 104 asset · 58,35 MB | 96 GeoTIFF raw, 96 raster harmonized và feature đất cho 168 basin |
| Copernicus DEM GLO-30 | 10 asset · 1,10 GB | DEM mosaic 30 m và feature địa hình cho 168 basin |
| ESA WorldCover 2021 | 5 asset · 336,35 MB | Raster lớp phủ và tỷ lệ lớp phủ cho 168 basin |
| WorldPop Việt Nam 2025 | 2 asset · 150,43 MB | Raster dân số và mapping dân số cho 168 basin; hai bản raw trùng checksum |
| OpenStreetMap Việt Nam | 2 asset · 327,06 MB | 67.741 đoạn đường, 1.359 cầu, 63 facility, 132 settlement và 806 đối tượng mặt nước |
| Bằng chứng lũ 2020–2026 | 1 asset · 10 KB | 30 dòng sự kiện đã chuẩn hóa để tiếp tục rà soát và gắn vị trí |

Các đầu ra quan hệ hiện có gồm 503 dòng basin–xã, 2.231 basin–sông, 68.099 basin–đường, 1.327 basin–cầu, 63 basin–facility, 121 basin–settlement và 168 basin–dân số. Bốn AOI, static profile 209 cột, báo cáo QA và bản đồ kiểm tra cũng đã được sinh. Chi tiết schema, khóa join, đơn vị và provenance nằm trong [docs/DATA_CATALOG.md](docs/DATA_CATALOG.md).

Các số trên mô tả snapshot **file-first L10**. Đây vẫn là nguồn đối chiếu và đầu ra nghiên cứu đã
có, nhưng không phải các bảng Bronze L12 đang được xây tiếp.

Snapshot Lakehouse được kiểm tra trực tiếp qua Polaris ngày **21/09/2026**:

| Bảng Iceberg hiện có | Số dòng | Nội dung |
|---|---:|---|
| `meta.source_objects` | 207 | Object nguồn bất biến đã đăng ký từ 11 nguồn tĩnh |
| `meta.pipeline_runs` | 210 | Run landing/parse và quyết định publish |
| `meta.quality_results` | 1.209 | Kết quả quality rule theo run/object/snapshot |
| `meta.table_snapshot_ref` | 202 | Liên kết run với snapshot Iceberg |
| `meta.lineage_edges` | 202 | Lineage Raw object → Bronze snapshot |
| `bronze.basin_polygon_raw` | 1.194.591 | Polygon HydroBASINS L12 và BasinATLAS L12 |
| `bronze.river_reach_raw` | 1.428.959 | HydroRIVERS Asia giữ field nguồn |
| `bronze.admin_boundary_raw` | 12.012 | Địa giới Sơn La hiện hành và GADM lịch sử |
| `bronze.raster_coverage` | 111 | Metadata band/tile của DEM, SoilGrids, WorldCover và WorldPop; pixel vẫn ở file Raw |
| `bronze.historical_event_raw` | 32 | Dòng sự kiện giữ nội dung và field nguồn |
| `bronze.osm_feature_raw` | 763.704 | Đối tượng OSM thuộc hydrology, hydraulic structure, critical transport và critical facility |

Contract và code đã có schema cho chín bảng Meta. Catalog đang chạy mới materialize năm bảng
liệt kê ở trên; `meta.source_registry`, `meta.dataset_registry` và `meta.ingest_attempts` sẽ được
tạo/populate khi chạy lại DAG landing đã cập nhật, còn `meta.parameter_sets` chỉ có dữ liệu khi
bắt đầu cấu hình routing/threat. Repository chưa có raw mưa GSMaP, forecast/soil moisture IFS
theo thời gian, bảng Silver, basin-hour dataset hoặc kết quả threat B0–B3.

## Phần việc tiếp theo

Ưu tiên gần nhất là chạy lại `static_source_landing` để materialize ba bảng Meta landing còn thiếu,
sau đó xây pipeline Bronze → Silver L12: mapping tên field, canonical basin/topology, aggregate
raster/OSM theo basin và tạo feature có version. Song song, khảo sát nguồn động cần được biến thành
pipeline tái lập: lưu probe/access log, xác minh GSMaP/IFS thực tế, chốt AOI thử nghiệm, xây pixel
weights và data contract theo thời gian. Sau đó mới tạo basin-hour dataset và chạy bốn cấu hình
threat B0–B3 trên cùng snapshot dữ liệu.

Các khối Knowledge Graph, exposure động, incremental update, accessibility, routing, API và dashboard phụ thuộc vào dữ liệu động và threat state. Chúng chưa được triển khai trong repository. PostgreSQL hiện tại phục vụ Airflow/Polaris và **chưa được xác nhận là PostGIS**; Neo4j cũng chưa có trong Compose.

## Cấu trúc code hiện tại

Luồng Lakehouse đang chạy theo thứ tự:

```text
config/*.yaml
  → airflow/dags/static_source_landing.py
  → orchestration/landing → MinIO raw + meta.source_objects
  → airflow/dags/static_source_to_bronze.py
  → orchestration/bronze → bronze.* + Meta audit
```

Các thư mục cấp cao hiện tại:

```text
.
├── airflow/dags/           # DAG production; chỉ điều phối, không chứa logic parse chính
├── config/                 # Contract/policy cho nguồn, landing, Meta, Bronze và feature
├── dataset/
│   ├── catalog/            # Asset catalog/checksum của pipeline file-first
│   ├── raw/                # Raw file-first; không phải bucket MinIO raw
│   ├── harmonized/         # Sản phẩm chuẩn hóa L10 cũ
│   ├── derived/            # Feature, mapping và static profile L10
│   ├── qa/                 # Báo cáo và bản đồ QA
│   └── lakehouse/          # Volume PostgreSQL/MinIO, Airflow log và staging
├── docs/                   # Kế hoạch, schema contract, sơ đồ và tiến độ
├── infra/                  # Docker image và bootstrap service
├── spark/jobs/             # Job Spark/Iceberg độc lập
├── src/flashflood_data/    # Package Python dùng chung cho CLI và Airflow
├── tests/                  # Unit, contract, integration và fixture
├── tools/                  # Script bootstrap/smoke dành cho operator
├── compose.yaml            # PostgreSQL, MinIO, Polaris, Airflow và Spark tùy chọn
├── Makefile                # Entry point cho thao tác phát triển/vận hành thường dùng
├── pyproject.toml          # Metadata package, dependency trực tiếp và cấu hình test/lint
└── requirements.lock       # Dependency đã khóa phiên bản
```

### DAG, cấu hình và hạ tầng

| File | Vai trò hiện tại |
|---|---|
| `airflow/dags/static_source_landing.py` | DAG Raw: seed registry, acquire/reuse 11 nguồn, publish object/manifest lên MinIO, đăng ký `meta.source_objects`, ghi audit và cleanup staging. |
| `airflow/dags/static_source_to_bronze.py` | DAG Bronze: discover object chưa được publish hợp lệ, dynamic-map theo object, parse và ghi Bronze/Meta; nhận `source_id` và `force_reprocess`. |
| `config/study_area.yaml` | Phạm vi Sơn La, CRS, buffer và tham số AOI của pipeline file-first. |
| `config/sources/*.yaml` | Source manifest: phiên bản, adapter, URL/API, expected format và policy tải của từng nguồn. |
| `config/landing/static.yaml` | Danh sách 11 source landing, lựa chọn L12, chế độ bundle/individual và subset SoilGrids 0–30 cm. |
| `config/meta/static.yaml` | Registry nguồn/dataset, owner và tham chiếu policy access/retention của Meta. |
| `config/bronze/static.yaml` | Routing `source_id → bảng/parser`, version contract/parser và danh mục quality rule. |
| `config/bronze/osm.yaml` | Allowlist tag OSM cho thủy văn, công trình thủy, giao thông quan trọng và facility thiết yếu. |
| `config/features.yaml` | Field, depth, đơn vị và quy tắc feature của pipeline static L10 cũ. |
| `config/osmconf.ini` | Cấu hình GDAL/OGR khi đọc các field/tag OSM. |
| `compose.yaml` | Khai báo service, healthcheck, secret/env và bind mount; cùng một `AIRFLOW_API_SECRET_KEY` được dùng để ký log JWT. |
| `infra/docker/airflow/Dockerfile` | Image Airflow có package dự án và runtime địa không gian/Iceberg. |
| `infra/docker/spark/Dockerfile` | Image Spark 4.1.3 + Iceberg 1.11.0. |
| `infra/docker/polaris-bootstrap/Dockerfile` | Image bootstrap Polaris có sẵn `curl`/`jq`, không cài package khi service chạy. |
| `infra/services/postgres/init-multiple-databases.sh` | Tạo database/user riêng cho Airflow và Polaris. |
| `infra/services/polaris/bootstrap.sh` | Tạo catalog Polaris và cấu hình MinIO warehouse. |
| `spark/jobs/runtime.py` | Tạo Spark session đã cấu hình REST catalog/S3. |
| `spark/jobs/smoke_iceberg.py` | Ghi–đọc–xóa bảng Iceberg tạm để kiểm tra Spark end-to-end. |
| `tools/bootstrap/check_prerequisites.sh` | Kiểm tra Git, Make, OpenSSL, Python 3.11, Docker daemon và Compose v2 trước khi bootstrap. |
| `tools/bootstrap/init_lakehouse_env.sh` | Tạo `.env`, secret và thư mục persistent mà không ghi đè giá trị đã có. |
| `tools/bootstrap/setup_python_runtime.sh` | Cài/kiểm tra runtime forecast và Iceberg trong `.venv`. |
| `tools/smoke/*.sh` | Smoke test stack, source landing, Meta/Bronze, Python runtime và Spark/Iceberg. |

### Package Python hiện tại

`__init__.py` trong mỗi thư mục chỉ khai báo package hoặc export API; các file có logic được chia
như sau.

| Thư mục/file | Vai trò hiện tại |
|---|---|
| `catalog/models.py` | Pydantic model bất biến cho asset, source, run và validation. |
| `catalog/inventory.py` | Inventory read-only, checksum cache, kiểm tra định dạng và báo cáo tất định cho dữ liệu legacy. |
| `catalog/repository.py` | `AssetCatalog` Parquet, transition trạng thái và ghi catalog nguyên tử. |
| `cli/app.py` | CLI chính: inventory/fetch/validate/harmonize/derive/map/run-static/land-static/cleanup. |
| `cli/commands/static.py` | Export nhóm lệnh static để giữ boundary CLI. |
| `cli/commands/bronze.py` | CLI manual recovery `bronze backfill` và báo cáo `bronze reconcile`; gọi cùng service với DAG. |
| `core/config.py` | Đọc/validate study area và environment của pipeline static. |
| `core/paths.py` | Resolve các đường dẫn tương đối từ project root. |
| `core/lakehouse.py` | Validate cấu hình MinIO/Polaris từ environment. |
| `core/disk_index.py` | Unique index trên đĩa cho batch lớn, tránh giữ toàn bộ key trong RAM. |
| `orchestration/quality.py` | Contract kết quả QA dùng chung; rule cụ thể thuộc từng pipeline. |
| `orchestration/landing/config.py` | Model và loader của `config/landing/static.yaml`. |
| `orchestration/landing/models.py` | Contract JSON-safe giữa các task landing và XCom. |
| `orchestration/landing/bundle.py` | Đóng gói Shapefile sidecar thành ZIP byte-deterministic. |
| `orchestration/landing/sources.py` | Acquire/reuse asset đã validate và chọn đúng raw object; không harmonize. |
| `orchestration/landing/service.py` | Transaction prepare → publish MinIO → register Iceberg → cleanup/recovery. |
| `orchestration/landing/meta_audit.py` | Ghi attempt, QA, snapshot ref và pipeline run sau khi đăng ký Raw thành công. |
| `orchestration/meta/registry.py` | Chuyển config Meta + source spec thành row `source_registry`/`dataset_registry`. |
| `orchestration/meta/service.py` | API ghi idempotent cho registry, attempt, run, quality, snapshot và lineage. |
| `orchestration/meta/factory.py` | Ghép settings, Polaris catalog và table store thành `MetaRecorder`. |
| `orchestration/bronze/config.py` | Load routing/version và chuẩn hóa `force_reprocess`. |
| `orchestration/bronze/parsers.py` | Parser vector, raster coverage và bảng sự kiện theo field nguồn. |
| `orchestration/bronze/osm.py` | Đọc PBF theo batch 5.000 và lọc tag OSM theo policy. |
| `orchestration/bronze/quality.py` | Rule Bronze về output rỗng, bbox và business key. |
| `orchestration/bronze/service.py` | Một transaction cho một object: discover/idempotency, download staging, parse, QA, replace, audit. |
| `orchestration/bronze/factory.py` | Ghép object store, inventory, Meta và Iceberg table store cho DAG/CLI. |
| `storage/atomic.py` | Context manager ghi file local qua temporary target rồi rename nguyên tử. |
| `storage/object_store.py` | Publish/download/verify object MinIO theo checksum và immutable key. |
| `storage/iceberg.py` | Kết nối Polaris và repository chuyên biệt cho `meta.source_objects`. |
| `storage/iceberg_schemas.py` | Arrow schema vật lý của các bảng Meta/Bronze theo contract. |
| `storage/iceberg_tables.py` | Tạo bảng, upsert Meta và replace slice Bronze theo key/object. |
| `storage/http/fetcher.py` | HTTP downloader streaming an toàn cho raw asset. |
| `storage/http/models.py` | Model resume và verified payload. |
| `storage/http/resume.py` | Resume state, lock và xử lý partial download. |
| `storage/http/transfer.py` | Streaming giới hạn bộ nhớ, retry và kiểm tra range response. |
| `storage/http/quarantine.py` | Cách ly payload lỗi và publish tránh collision. |
| `storage/http/redaction.py` | Loại credential khỏi exception/log. |
| `storage/http/errors.py` | Các exception có nghĩa nghiệp vụ cho tải/verify/resume. |

Pipeline file-first L10 được giữ riêng dưới `static/`:

| Thư mục/file | Vai trò hiện tại |
|---|---|
| `static/sources/base.py` | Interface chung của source adapter. |
| `static/sources/registry.py` | Đọc source YAML và tạo adapter theo tên đã đăng ký. |
| `static/sources/existing.py` | Inventory/reuse file có sẵn mà không di chuyển. |
| `static/sources/budget.py` | Kiểm tra ngân sách dung lượng trước khi tải. |
| `static/sources/admin_shared.py` | Primitive đọc/parse dùng chung cho địa giới. |
| `static/sources/admin_current.py` | Adapter địa giới Sơn La 2025. |
| `static/sources/admin_historical.py` | Adapter GADM và boundary tham chiếu lịch sử. |
| `static/sources/admin.py` | Export tương thích cho hai adapter địa giới. |
| `static/sources/cop_dem.py` | Tìm/tải/xác minh/extract Copernicus DEM qua CDSE. |
| `static/sources/soilgrids.py` | Tạo request WCS và tải SoilGrids theo property/depth/statistic. |
| `static/sources/worldcover.py` | Chọn tile và validate class WorldCover. |
| `static/sources/osm.py` | Tải snapshot Geofabrik và sinh các lớp exposure L10 cũ. |
| `static/spatial/raster.py` | Inspect/validate/mosaic/clip raster theo cửa sổ bounded-memory. |
| `static/spatial/vector.py` | Repair/validate geometry và ghi GeoParquet nguyên tử. |
| `static/harmonize/aoi.py` | Dựng Core/Hydrological/Environmental/Exposure AOI. |
| `static/harmonize/hydro.py` | Chọn basin L10, topology cha L8/L9 và harmonize hydro. |
| `static/harmonize/exposure.py` | Harmonize WorldPop, sự kiện lịch sử và đối chiếu hành chính. |
| `static/features/config.py` | Contract feature L10. |
| `static/features/spatial.py` | Helper raster/vector cho aggregate theo basin. |
| `static/features/terrain.py` | Feature DEM/terrain trên lưới metric 30 m. |
| `static/features/soil.py` | Scale và tổng hợp SoilGrids theo độ dày. |
| `static/features/landcover.py` | Tỷ lệ class WorldCover theo basin. |
| `static/features/hydrology.py` | Feature HydroRIVERS/BasinATLAS. |
| `static/features/population.py` | Aggregate WorldPop và mapping basin–population. |
| `static/features/profile.py` | Ghép, kiểm tra và publish static profile L10. |
| `static/features/builder.py` | Orchestrate bốn nhóm predictor static. |
| `static/mappings/admin.py` | Crosswalk hành chính lịch sử–hiện hành. |
| `static/mappings/spatial.py` | Quan hệ metric basin–xã/sông/đường/point. |
| `static/mappings/builder.py` | Orchestrate mapping và static profile. |
| `static/qa/{admin,hydro,mappings,raster,population,events,provenance}.py` | Các quality check tách theo chủ đề. |
| `static/qa/models.py` | Model kết quả/check/report của QA cũ. |
| `static/qa/runner.py` | Chạy quality gate theo thứ tự. |
| `static/qa/report.py` | Xuất JSON/Parquet/HTML đã redact. |
| `static/qa/map.py` | Sinh bundle MapLibre để kiểm tra trực quan. |
| `static/qa/handler.py` | Adapter QA cho workflow stage. |
| `static/qa/shared.py` | Helper dùng chung giữa các check. |
| `static/workflow/stages.py` | Enum các stage file-first. |
| `static/workflow/dependencies.py` | Khai báo dependency giữa stage. |
| `static/workflow/fingerprint.py` | Fingerprint input/config để quyết định reuse. |
| `static/workflow/handlers.py` | Đăng ký handler derive/map/QA và kiểm tra output reusable. |
| `static/workflow/runner.py` | `StaticPipeline` resumable cho CLI `run-static`. |
| `static/workflow/summary.py` | Summary machine-readable của một run. |

Test phản chiếu boundary code: `tests/unit/` kiểm tra từng service/module,
`tests/contract/` khóa DAG/schema/kiến trúc, `tests/integration/static/` kiểm tra pipeline L10 và
`tests/integration/lakehouse/` kiểm tra MinIO–Polaris–Iceberg. `tests/fixtures/` chỉ chứa dữ liệu
nhỏ, tất định cho test.

Không di chuyển, chỉnh sửa hoặc xóa thủ công file trong `dataset/raw/` và các thư mục dữ liệu gốc.
Catalog dùng đường dẫn, fingerprint và checksum để quyết định artifact nào có thể tái sử dụng.

## Cấu trúc đích khi triển khai các tầng tiếp theo

Code mới sẽ được tổ chức **theo pipeline trước, rồi theo trách nhiệm trong pipeline**. Cấu trúc
dưới đây là đích; các thư mục Silver/Gold/KG/serving chưa tồn tại và không được hiểu là đã triển
khai:

```text
airflow/dags/
├── static/
│   ├── source_landing.py           # Raw static
│   ├── source_to_bronze.py         # Parse static
│   └── bronze_to_silver.py         # Harmonize/mapping/feature L12
├── dynamic/
│   ├── gsmap_landing.py            # Mưa quan trắc/reanalysis
│   ├── ifs_landing.py              # Forecast/runoff/soil moisture
│   └── source_to_bronze.py         # Parse theo time window/revision
├── gold/
│   ├── basin_hour.py               # Aggregate forcing theo basin-hour
│   └── threat_b0_b3.py             # Tính bốn cấu hình threat
├── kg/
│   └── publish_knowledge_graph.py  # Project snapshot đã publish sang graph
└── serving/
    └── refresh_views.py            # Refresh latest/read model

config/pipelines/
├── static/{landing,bronze,silver}.yaml
├── dynamic/{gsmap,ifs,bronze}.yaml
├── gold/{basin_hour,threat}.yaml
├── kg/publish.yaml
└── serving/views.yaml

src/flashflood_data/
├── pipelines/
│   ├── static/
│   │   ├── landing/                # chuyển code orchestration/landing hiện tại
│   │   ├── bronze/                 # chuyển code orchestration/bronze hiện tại
│   │   └── silver/                 # canonical basin, topology, soil/DEM/OSM feature L12
│   ├── dynamic/
│   │   ├── landing/                # client/provider và immutable time-window object
│   │   ├── bronze/                 # GRIB/NetCDF coverage hoặc grid value đã benchmark
│   │   └── silver/                 # chuẩn hóa variable/time/revision/grid
│   ├── gold/                       # basin-hour, routing state, B0–B3 và assessment
│   ├── kg/                         # node/edge/evidence projector
│   └── serving/                    # latest view và query model
├── platform/
│   ├── meta/                       # registry/run/quality/snapshot/lineage dùng chung mọi pipeline
│   ├── quality/                    # contract chung; từng pipeline vẫn sở hữu rule cụ thể
│   ├── storage/                    # MinIO, Iceberg, HTTP và transaction primitive
│   ├── geospatial/                 # raster/vector primitive không gắn một pipeline
│   └── observability/              # metric, structured log và alert hook
└── cli/                            # command mỏng, chỉ gọi service của pipeline

tests/
├── unit/{pipelines,platform}/
├── contract/{schemas,dags,architecture}/
├── integration/{static,dynamic,gold,kg}/
└── fixtures/{static,dynamic}/
```

Mỗi DAG chỉ tạo dependency, retry, mapping và pool. Logic tải/parse/transform nằm trong package
pipeline để CLI, Airflow và test gọi cùng một implementation. Meta, quality contract và storage
là hạ tầng dùng chung; rule QA vẫn đặt cạnh pipeline sở hữu dữ liệu. Việc chuyển file hiện tại
sang cấu trúc đích nên thực hiện theo từng pipeline cùng test import-boundary, không di chuyển
đồng loạt trong lúc xây Silver hoặc ingest động.

## Yêu cầu môi trường

- Linux x86_64 hoặc **Windows + WSL2** x86_64. Trên Windows, dùng một distribution Linux như
  Ubuntu và bật Docker Desktop → Settings → Resources → WSL Integration.
- Docker Engine + Docker Compose v2; trên WSL2 có thể dùng Docker Desktop thay cho daemon cài
  riêng trong distribution.
- Python 3.11, Git, GNU Make và OpenSSL.
- Kết nối Internet cho lần crawl live.
- Tài khoản Copernicus Data Space Ecosystem (CDSE) để tải Copernicus DEM.
- Trước khi crawl: lượng raw mới dự kiến không quá **8 GiB** và vẫn còn tối thiểu **10 GiB** dung lượng trống sau khi tải.

Trên WSL2, clone project vào filesystem Linux như `~/projects/FloodLakeKG`; **không đặt repository dưới `/mnt/c/`** vì bind mount và workload PostgreSQL/MinIO sẽ chậm hơn và dễ gặp khác biệt
permission. Stack cơ bản giới hạn khoảng 4,25 GiB RAM; khi bật Spark nên dành ít nhất 8–12 GiB
cho WSL/Docker.

## Cài đặt

### Bootstrap đầy đủ trên máy mới

Từ thư mục gốc repository, một lệnh sẽ kiểm tra prerequisite, tạo `.venv`, cài runtime, sinh
secret, build các image cần thiết, khởi động Lakehouse và chạy smoke test:

```bash
make bootstrap
```

Lệnh không chứa đường dẫn home/user cố định. Python mặc định là `python3.11`; nếu executable nằm
ở vị trí khác, truyền rõ đường dẫn:

```bash
make bootstrap PYTHON=/duong/dan/toi/python3.11
```

Có thể chỉ kiểm tra máy mà không cài hoặc khởi động service:

```bash
make doctor
```

State mặc định nằm tại `./dataset/lakehouse`. Có thể chuyển sang ổ Linux khác bằng cách đặt
`LAKEHOUSE_DATA_ROOT=/duong/dan/tuyet/doi` trong `.env`; không đặt volume database trên filesystem
Windows `/mnt/c`.

### Chỉ cài môi trường Python local

Nếu chưa cần Lakehouse, chạy:

```bash
make setup
```

Hoặc chọn Python 3.11 cụ thể:

```bash
make setup PYTHON=/duong/dan/toi/python3.11
```

Lệnh tạo `.venv` ngay trong repository và cài dependency từ `requirements.lock`. Có thể kiểm tra
môi trường bằng:

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

Repository cung cấp một Docker Compose stack cố định gồm PostgreSQL, MinIO, Apache Polaris,
Airflow 3 và một cụm Spark standalone tùy chọn. Đây là nền tảng
lưu trữ/catalog/orchestration; các bảng Meta và Bronze static đã có dữ liệu trong Iceberg. Stack
**chưa ingest dữ liệu thời tiết và chưa có Silver/Gold/KG**. Pipeline file-first cùng toàn bộ raw
data hiện hữu vẫn giữ nguyên.

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
make lakehouse-build
make lakehouse-python-smoke
```

`make lakehouse-airflow-build` vẫn được giữ để chỉ build lại image Airflow; `make lakehouse-build`
build cả Airflow và image bootstrap Polaris dùng bởi quy trình đầy đủ.

Runtime hiện khóa Xarray, PyIceberg, cfgrib và ecCodes. Lệnh smoke cuối cần stack đang chạy;
nó chỉ kiểm tra version, bộ giải mã GRIB và gọi `list_namespaces()` qua Polaris, không tải dữ
liệu, không tạo bảng và không ghi vào MinIO.

### Landing nguồn tĩnh vào MinIO và Iceberg

DAG thủ công `static_source_landing` thực hiện đúng lớp source landing: acquire hoặc tái sử dụng
payload đã validate, chọn HydroBASINS/BasinATLAS L12, đóng gói Shapefile thành ZIP xác định byte,
đưa object cùng manifest vào bucket MinIO `raw`, rồi đăng ký một row cho mỗi object trong
`meta.source_objects` qua Polaris. DAG hiện còn seed `source_registry`/`dataset_registry` và audit
attempt/run/quality/snapshot cho các batch thành công. DAG dừng tại Raw; parse sang `bronze.*`,
harmonize Silver, feature và B0–B3 thuộc các pipeline sau.

Mười một nhóm nguồn chạy độc lập về kết quả, còn writer được serialize để tránh commit đồng thời:

- `hydrobasins_v1c`: bundle L12;
- `basinatlas_v10`: bundle L12;
- `hydrorivers_v10`: bundle Asia;
- `sonla_admin_2025`: địa giới Sơn La hiện hành;
- `gadm_vnm_4_1`: địa giới Việt Nam lịch sử;
- `worldpop_vnm_2025`: raster dân số Việt Nam 2025;
- `historical_flood_evidence_2020_2026`: bảng bằng chứng sự kiện;
- `geofabrik_vietnam_snapshot`: hai snapshot PBF Việt Nam đã catalog;
- `cop_dem_glo30_2024_1`: từng source tile của Environmental AOI;
- `soilgrids_2_0`: 8 property, 3 depth 0–30 cm và 4 statistic;
- `esa_worldcover_2021_v200`: tile lớp phủ đất trong Environmental AOI.

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

### Parsing Bronze và đăng ký Meta Lineage

DAG độc lập `static_source_to_bronze` đọc các `source_objects` khả dụng từ MinIO, parse dữ liệu tĩnh theo đúng định dạng nguồn (giữ nguyên tên field, chưa áp mapping Silver; BasinATLAS chỉ đọc allowlist 30 field đã chốt và giữ file đầy đủ ở Raw), kiểm tra quality gates (khóa nghiệp vụ duy nhất, bbox WGS84 hợp lệ, output không rỗng), và thực hiện atomic overwrite theo từng `object_id` vào bảng Iceberg tương ứng:

- `bronze.basin_polygon_raw` (`hydrobasins_v1c`, `basinatlas_v10`)
- `bronze.river_reach_raw` (`hydrorivers_v10`)
- `bronze.admin_boundary_raw` (`sonla_admin_2025`, `gadm_vnm_4_1`)
- `bronze.raster_coverage` (`cop_dem_glo30_2024_1`, `soilgrids_2_0`, `esa_worldcover_2021_v200`, `worldpop_vnm_2025`)
- `bronze.historical_event_raw` (`historical_flood_evidence_2020_2026`)
- `bronze.osm_feature_raw` (`geofabrik_vietnam_snapshot` — lọc theo nhóm thủy văn, công trình thủy, giao thông quan trọng và facility thiết yếu trong `config/bronze/osm.yaml`)

Tại snapshot kiểm tra ngày 21/09/2026, cả sáu bảng trên đã có dữ liệu. Tổng số dòng được ghi ở
bảng “Tóm tắt dữ liệu đã crawl”; riêng OSM có 763.704 feature từ hai raw object.

Mỗi lần parse thành công sẽ ghi nhận bằng chứng audit vào các bảng Meta:
- `meta.pipeline_runs`: trạng thái run, thời điểm bắt đầu/kết thúc và publish decision.
- `meta.quality_results`: kết quả QA pre-commit và post-commit (gắn snapshot ID).
- `meta.table_snapshot_ref`: snapshot ID Iceberg đã commit.
- `meta.lineage_edges`: quan hệ biến đổi từ raw object ID sang output Iceberg snapshot.

Discover được idempotent ở cấp object: một object chỉ bị skip khi đồng thời có run Meta đã
publish thành công, lineage khớp `(object_id, target_table, parser_version)` và slice Bronze vẫn
tồn tại. Thiếu bất kỳ bằng chứng nào, đổi parser version hoặc bật `force_reprocess` thì object được
xử lý lại. Task discover vẫn chạy ở mỗi DAG run để kiểm tra trạng thái mới; task parse chỉ được
dynamic-map cho các object cần xử lý.

Các lệnh vận hành Bronze:

```bash
# Kiểm tra đối soát giữa raw objects, bảng Bronze và Meta
.venv/bin/flashflood-data bronze reconcile

# Backfill thủ công một nguồn (chạy thử dry-run trước)
.venv/bin/flashflood-data bronze backfill --source-id hydrobasins_v1c --dry-run

# Trigger bình thường: object đã publish hợp lệ sẽ được skip trước khi tạo parse task
docker compose exec -T airflow-scheduler airflow dags trigger static_source_to_bronze

# Tùy chọn chỉ kiểm tra/xử lý một source
docker compose exec -T airflow-scheduler airflow dags trigger \
  static_source_to_bronze \
  --conf '{"source_id":"geofabrik_vietnam_snapshot"}'

# Chủ động chạy lại dù object đã publish
docker compose exec -T airflow-scheduler airflow dags trigger \
  static_source_to_bronze \
  --conf '{"source_id":"geofabrik_vietnam_snapshot","force_reprocess":true}'

# Backfill thật ngoài Airflow sau khi đã xem dry-run
.venv/bin/flashflood-data bronze backfill --source-id hydrobasins_v1c

# Chạy smoke test đầu-cuối của Meta và Bronze trên môi trường Lakehouse
make lakehouse-meta-bronze-smoke
```

Lưu ý: trạng thái bảng được xác nhận qua `reconcile`; snapshot chỉ được publish sau khi vượt QA
gate. Airflow hiện có dữ liệu Bronze thực tế, nên chạy lại bình thường chủ yếu thực hiện discover
và skip các object đã đủ bằng chứng.

### Xem log Airflow

Xem log thuận tiện nhất trong Airflow UI tại <http://127.0.0.1:8080/>: mở DAG → DAG run → task
instance → **Logs**. Có thể theo dõi log container và file log local bằng CLI:

```bash
# Log service theo thời gian thực
docker compose logs -f airflow-scheduler airflow-dag-processor airflow-api-server

# Tìm file log task đã bind mount ra host
find dataset/lakehouse/airflow/logs -type f | sort

# Xem trạng thái các service
make lakehouse-status
```

`compose.yaml` truyền cùng `AIRFLOW_API_SECRET_KEY` vào `[api_auth] jwt_secret` và
`[api] secret_key` cho mọi container Airflow. Nếu đổi key, phải recreate toàn bộ service Airflow;
recreate một container riêng sẽ làm token log cũ không xác thực được và xuất hiện
`InvalidSignatureError`.

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

- Pipeline file-first đã crawl đủ 11 nguồn và hoàn thành derive/map/QA cho 168 basin L10.
- Source landing Lakehouse đã đưa 207 raw object vào MinIO và `meta.source_objects`; HydroBASINS
  và BasinATLAS được chọn L12 từ đầu.
- DAG `static_source_to_bronze` gần nhất đã thành công. Sáu bảng Bronze đang chứa 3.399.409 dòng
  parsed/coverage; các object cũ được skip bằng kiểm tra run + lineage + Bronze slice.
- Catalog hiện có năm bảng Meta với dữ liệu. Code/contract có chín bảng Meta; ba bảng
  landing registry/attempt cần chạy lại DAG landing để materialize, còn `parameter_sets` chờ
  pipeline routing/threat.
- OSM Bronze đã parse 763.704 feature thuộc bốn nhóm phục vụ lũ, gồm critical facility.
- Airflow, MinIO, Polaris và PostgreSQL được xác nhận healthy ngày 21/09/2026; Spark là profile
  tùy chọn và đã có smoke test riêng.
- Toàn bộ test hiện tại: **536 passed**; Ruff và Docker Compose config đều hợp lệ.
- Silver L12, dữ liệu động, threat B0–B3, Knowledge Graph, routing và serving chưa triển khai.
- Tiến độ project-wide được quản lý tại [docs/PROGRESS_TRACKING.md](docs/PROGRESS_TRACKING.md), thay cho cách đánh số Task 1–19 của kế hoạch pipeline static cũ.
