# Theo dõi tiến độ Flood Lakehouse KG — 13 tuần

Cập nhật: **07/09/2026**.

Nguồn chính: [Đề cương 13 tuần](De_cuong_13_tuan_Geospatial_Lakehouse_KG_Son_La.docx).
[TLCN_DE.docx](TLCN_DE.docx) là đề xuất gốc. Tracking theo 52 task của đề cương mới, cộng các quyết định của chủ project được ghi rõ bên dưới.

Mục tiêu: prototype nghiên cứu Geospatial Lakehouse + Knowledge Graph cho tích hợp dữ liệu theo phiên bản, threat theo sub-basin, sàng lọc phơi nhiễm, khả năng tiếp cận và **routing có xét nguy cơ** tại Sơn La. Không cam kết cảnh báo vận hành, xác suất lũ, mô phỏng ngập hay tuyến an toàn.

## 1. Quy ước

- `[x]`: đầu việc đã có sản phẩm/bằng chứng thực hiện; không tự suy thành GVHD đã nghiệm thu.
- `[ ] Một phần`: đã có thành phần, chưa đạt đủ tiêu chí task.
- `[ ] Chưa làm`: chưa có bằng chứng triển khai hoàn chỉnh.
- Giữ **52 mã task 01.1–13.4**. Tuần là thứ tự kế hoạch, không tự gán ngày lịch hay đánh dấu cả tuần hoàn thành vì đã cài tools.
- Việc đã làm trước được ghi riêng và tái sử dụng cho task mới. Probe thành công không đồng nghĩa đã có pipeline ingest.
- **Routing vẫn phải làm theo quyết định của chủ project ngày 07/09/2026.** Task 10.3–10.4 là P1 trong đề cương nguồn nhưng được đưa vào phạm vi cam kết của project; triển khai sau khi nền tảng P0 và G9 đạt.
- Kafka/Flink/Spark/OpenMetadata, storage deficit, UF định lượng và population trong phân tích tác động vẫn là mở rộng. Không xóa dữ liệu hoặc môi trường đã có vì đổi ưu tiên.
- Giữ raw và lịch sử; không đổi level basin, nguồn hoặc AOI chỉ bằng việc sửa tracking.

## 2. Kết quả thực tế đã có

### 2.1. Công việc đã làm trước ngày cập nhật

- [x] Có đề xuất tổng thể, đề cương 13 tuần và các bản sơ đồ kiến trúc. Kiến trúc P0 mới vẫn cần được chốt thành bản as-built thống nhất.
- [x] Có repository, CLI, cấu hình, tests và [README tiếng Việt](../README.md).
- [x] Crawl static hoàn tất 11 nguồn theo registry/README; giữ raw, catalog, checksum và các lớp harmonized/derived/mapping.
- [x] Có 168 basin L10, BasinATLAS, HydroRIVERS, DEM, SoilGrids, WorldCover, WorldPop, OSM, địa giới và evidence lịch sử.
- [x] Có 4 AOI, địa giới hiện hành sau cải cách 2025 và crosswalk xã cũ–xã mới. Không bảo đảm mọi tên lịch sử đã resolve hoặc đủ dữ liệu replay 2020–2026.
- [x] Có static profile và mapping basin–xã/đường/cầu/facility/settlement/sông/dân số. HydroRIVERS đã dùng cho static, chưa có graph thủy văn phục vụ hoàn chỉnh.
- [x] Đã chạy QA static và bản đồ kiểm tra. Báo cáo còn warning; không đồng nghĩa toàn bộ geometry/evidence đều sạch.
- [x] Đã dựng Docker Compose PostgreSQL, MinIO, Polaris, Airflow và persistent storage; cài Xarray, cfgrib, ecCodes, PyIceberg.
- [x] Đã dựng Spark 4.1.3 + Iceberg 1.11.0 tùy chọn; đã thử ghi–đọc–dọn bảng tạm qua Polaris/MinIO hai lần theo lịch sử làm việc.

Bằng chứng: [DATA_CATALOG](DATA_CATALOG.md), [config](../config/), [source](../src/flashflood_data/), [tests](../tests/), [Compose](../compose.yaml), [infra](../infra/), [Spark smoke](../spark/jobs/smoke_iceberg.py), `dataset/qa/report.json`, `dataset/derived/`. Commit tham chiếu: `9544449`, `b4c1bb4`, `5385f57`.

**Giới hạn xác nhận môi trường:** Compose hiện có PostgreSQL thường cho Airflow/Polaris, chưa có bằng chứng PostGIS được cài và kiểm thử; Neo4j chưa có trong Compose. Python smoke kiểm tra runtime/đọc namespace, chưa chứng minh PyIceberg write/upsert nghiệp vụ. Spark roundtrip không thay cho phép thử client P0 này. Không chạy lại live crawl hay khởi động stack trong phiên sửa tracking.

### 2.2. Khảo sát ngày 07/09/2026

- [x] Probe IFS HRES qua Open-Meteo với `models=ecmwf_ifs`: 5 vị trí trong tập basin, mỗi vị trí có 72 mốc giờ; precipitation và 4 tầng soil moisture không null trong mẫu.
- [x] Đọc metadata IFS, phân biệt thời điểm khởi tạo/khả dụng/tải; phản hồi forecast đã thử không chứa model-run ID trực tiếp.
- [x] Đối chiếu ERA5-Land-T trễ khoảng 5 ngày, không phải nguồn soil moisture realtime.
- [x] Đối chiếu GSMaP NOW: cửa sổ mưa 1 giờ, cập nhật mỗi 30 phút; không phải hai lượng mưa nửa giờ độc lập.
- [x] Tính diện tích L10/L12 cùng vùng; đếm tâm lưới IFS và ước tính ô giao basin trong bộ nhớ.
- [ ] Lưu probe thành script/notebook, raw mẫu, access log và báo cáo tái lập. Bằng chứng hội thoại không thay artifact nghiệm thu.
- [ ] Tải/đọc GSMaP thật và xác minh archive, sản phẩm/version, quyền truy cập, giấy phép và giới hạn tải.
- [ ] Xây pixel weights chính thức, dynamic ingestion, bảng Iceberg nghiệp vụ, threat, exposure động, KG và routing.

## 3. Phạm vi dữ liệu và kiến trúc

### 3.1. Bốn AOI hiện có

| AOI | Phạm vi | Vai trò |
|---|---|---|
| Core | Địa giới Sơn La sau cải cách 2025 | Địa giới, sự kiện, dân số trong vùng báo cáo |
| Hydrological | L10 giao Core + thượng nguồn trực tiếp theo NEXT_DOWN; 168 basin | Phân tích lưu vực; không đồng nghĩa toàn bộ thượng nguồn đệ quy |
| Environmental | Hydrological buffer 10 km | Vùng tải raster môi trường và ngữ cảnh |
| Exposure | Core buffer 10 km, cắt theo biên giới Việt Nam | OSM/hạ tầng; population hiện tổng hợp riêng theo Core |

AOI chồng lấn, không tải riêng lặp lại cùng ô cho bốn vùng. Để tính mưa toàn bộ 168 basin, cần ô giao Hydrological, kể cả tâm ngoài basin. Có thể lấy thêm Environmental nếu chốt cần vùng đệm.

Đề cương đặt mục tiêu cụm liên thông khoảng **20–50 basin sau khảo sát**. Chưa chốt cụm thử nghiệm: giữ 168 basin đã xử lý, không tự cắt/xóa. Việc chọn cụm phải giữ thượng nguồn cần thiết và xét mạng đường phục vụ accessibility/routing; phạm vi routing không nhất thiết trùng ranh basin.

### 3.2. Scale assessment sơ bộ

Tính nguyên polygon bằng EPSG:32648; L12 chọn theo tiền tố PFAF thuộc đúng 168 L10, không cắt theo ranh tỉnh:

| Chỉ số | L10 | L12 |
|---|---:|---:|
| Số basin | 168 | 186 |
| Diện tích nhỏ nhất (km²) | 2,24 | 2,24 |
| Trung vị (km²) | 135,53 | 132,26 |
| Trung bình (km²) | 140,91 | 127,27 |
| Lớn nhất (km²) | 467,19 | 248,87 |
| Tổng diện tích (km²) | 23.673,13 | 23.673,13 |

159/168 basin giữ nguyên hình học ở L12; 9 basin chia thành 27 basin con. **Chưa đổi L10.** H3 mới được thảo luận, không thay basin chỉ để khớp kích thước pixel mưa.

IFS O1280: trung vị 2 tâm lưới/L10; 14 basin không chứa tâm nhưng vẫn nằm trong vùng phủ. Ước tính khoảng **401 ô giao Hydrological** theo biên ô quy ước; diện tích ô địa phương khoảng 73–75 km². Chưa phải footprint xác nhận từ file mưa, số pixel có giá trị hợp lệ hay số ô của Environmental/cả bốn AOI.

Phát hiện polygon `4101033820` tự giao; thử sửa trong bộ nhớ gần như không đổi diện tích, file harmonized chưa được sửa trong khảo sát. Cần xử lý trước mapping chính thức.

### 3.3. Nguồn động và thời gian

| Nguồn | Điều đã kiểm tra | Cách sử dụng/trạng thái |
|---|---|---|
| GSMaP NOW | Lưới 0,1°; hourly rain rate mm/h, cửa sổ 1 giờ cập nhật nửa giờ | Ứng viên mưa gần hiện tại; mới đọc tài liệu, chưa có mẫu tải thật/chốt version |
| IFS HRES | O1280 khoảng 9 km; run mỗi 6 giờ; bước gốc 1 giờ trong 90 giờ đầu, sau đó API có thể nội suy hourly | Forecast model cố định đã probe, chưa ingest; soil moisture là mô phỏng, không phải quan trắc mỗi giờ |
| ERA5-Land-T | Trễ khoảng 5 ngày; ERA5-Land cuối kỳ còn muộn hơn | Hồi cứu/climatology, không dùng làm realtime |
| Wetness proxy | Từ mưa trước thời điểm phân tích, ví dụ 24h/72h | Baseline live cần triển khai; IFS soil moisture bổ sung sau khi chốt contract/chất lượng/độ trễ |

Quy tắc triển khai:

- Tách `valid_from/valid_to`, provider-issued/available time nếu có bằng chứng, `first_seen`, `ingested_at`, `retrieval_snapshot_id`; chỉ ghi `model_run_id` khi xác định được từ nguồn. Không lấy thời điểm tải làm thời điểm phát hành.
- Polling 30–60 phút không đồng nghĩa có dữ liệu mới mỗi lần; đo source lag, polling lag và processing/serving lag riêng.
- GSMaP NOW: không cộng mọi bản cách nhau 30 phút thành tích lũy; dùng cửa sổ 1 giờ không chồng nhau hoặc phương pháp tích phân được đặc tả và kiểm thử.
- IFS precipitation trên API đã thử là tổng giờ trước mốc timestamp. Căn chỉnh interval với GSMaP, không chỉ join timestamp.
- Tạo mapping **riêng** GSMaP→basin và IFS→basin; lưu grid/geometry version, diện tích giao, mẫu số, weight và valid coverage. Mưa basin là trung bình theo diện tích; sau đó mới tích lũy theo thời gian.
- Missing không phải 0. Không đổi nguồn âm thầm; giữ source_mode, quality/data_age và provenance.
- Giữ tệp raw bất biến; Iceberg lưu metadata/bảng cần thiết, không ép toàn bộ raster toàn cầu thành rows. Nếu dùng API điểm, tải danh sách điểm lưới duy nhất theo nhóm và kiểm tra tọa độ ô trả về.
- Probe không chứng minh chất lượng dài hạn hoặc archive 2020–2026. Mục tiêu case study là 2–3 sự kiện đủ evidence. Reconstruction khác replay as-known-at-T; chỉ báo lead time khi đủ bằng chứng thời gian.

Nguồn: [JAXA NOW, 16/04/2026](https://sharaku.eorc.jaxa.jp/GSMaP/document/new/DataFormatDescription_NOW.pdf), [Open-Meteo IFS](https://open-meteo.com/en/docs/ecmwf-api), [model metadata](https://open-meteo.com/en/docs/model-updates), [ECMWF ERA5-Land](https://confluence.ecmwf.int/pages/viewpage.action?pageId=529423258).

### 3.4. Kiến trúc và routing

Luồng mục tiêu: Python micro-batch + một scheduler → raw/MinIO + Iceberg/Polaris → state/threat → PostGIS exposure → durable change log/worker → Neo4j/accessibility → routing + FastAPI/Streamlit.

Lakehouse là system of record; PostGIS/Neo4j là processing/serving có thể dựng lại. Tái sử dụng Airflow đã cài; chốt một client ghi bảng sau kiểm thử (PyIceberg là ứng viên). Không bắt buộc Kafka/Flink/Spark/OpenMetadata/Great Expectations cho luồng nền.

Routing **giữ trong phạm vi phải làm**: baseline theo thời gian đi lại và phương án có hazard/uncertainty penalty, trên khu vực/mạng đủ kiểm chứng. Chốt phương tiện, đơn vị cost, evidence đóng đường và xử lý UNKNOWN. Kết quả là lower-risk route theo chỉ số đã chọn, không phải tuyến an toàn. Thư viện routing chưa chốt; không cài đồng thời nhiều giải pháp chỉ để thử công nghệ.

## 4. Tổng quan 6 giai đoạn

Sáu nhóm dưới đây tổ chức lại timeline theo đề cương mới; không giữ lịch tuần của tracking cũ.

| Giai đoạn | Tuần | Đầu ra | Trạng thái |
|---|---|---|---|
| 1. Phạm vi, nguồn và AOI | 01–02 | Đề cương, kiến trúc sơ bộ, probe, scale assessment, evidence | Một phần |
| 2. Lakehouse, static và dynamic state | 03–05 | Schema, versioning, profile, pixel weights, ingest | Static/hạ tầng có trước; dynamic chưa triển khai |
| 3. Threat và direct exposure | 06–07 | Baseline/full, threat có giải thích, exposure lifecycle | Chưa làm |
| 4. KG và incremental | 08–09 | Queries, accessibility rules, retraction, equivalence | Chưa làm |
| 5. Routing, API và demo đầu cuối | 10–11 | Baseline/lower-risk routing, API/dashboard, replay/failure tests | Chưa làm |
| 6. Thực nghiệm và bàn giao | 12–13 | Benchmark, case study, báo cáo, demo, release | Chưa làm |

## 5. Tracking 52 task

Task lớn chỉ tick khi đủ đầu ra/tiêu chí. Thành phần đã xong được giữ tại mục 2, không bị mất ghi nhận khi chuyển sang kế hoạch mới.

### Tuần 01 — Phản biện đề xuất, chốt bài toán và thử nguồn

- [ ] **01.1 — Một phần:** Có đề xuất/đề cương; lập ma trận giữ/sửa/thu hẹp/mở rộng, chốt bài toán basin đáng chú ý, phơi nhiễm, accessibility và routing của project. **Đầu ra:** `review_matrix.md`, `scope.md`; quyết định gắn sản phẩm/kiểm thử.
- [ ] **01.2 — Chưa làm:** Chốt ba competency queries và protocol RQ1/RQ3/RQ4; RQ2 ứng dụng, RQ5 routing giữ theo quyết định project. **Đầu ra:** requirements/evaluation protocol; không cam kết thiệt hại/xác suất/tuyến an toàn.
- [ ] **01.3 — Một phần:** IFS đã probe; còn GSMaP thật, archive cùng loại, access/license log và raw forecast lưu bền vững. **Đầu ra:** script/notebook, mẫu raw, access log; đọc được đơn vị, nguồn bị chặn có phương án duyệt.
- [ ] **01.4 — Một phần:** Repo/runtime/tests/README/sơ đồ đã có; còn kiến trúc P0 thống nhất, issue board, test tự động và thông tin máy. **Nghiệm thu:** người khác chạy được test đầu tiên; không secrets trong Git.

**G1 — Chưa ghi nhận đạt:** GVHD duyệt phạm vi và kết quả thử nguồn; ưu tiên truy cập mưa trước mở thêm dịch vụ.

### Tuần 02 — AOI và bằng chứng

- [ ] **02.1 — Một phần:** Có 4 AOI và khảo sát L10/L12/IFS; còn báo cáo tái lập, GSMaP pixels/valid coverage và quyết định cụm thử nghiệm liên thông. **Đầu ra:** AOI, basin inventory, scale assessment; lý do chọn cấp/topology/thượng nguồn rõ.
- [ ] **02.2 — Một phần:** Có static catalog/crosswalk; bổ sung catalog sản phẩm dynamic: unit/CRS/resolution/chu kỳ/lag/archive/version/license. **Nghiệm thu:** nguồn dự kiến khác nguồn có mẫu thật, chưa coi crosswalk hoàn toàn giải quyết.
- [ ] **02.3 — Một phần:** Có evidence đã ingest; xác minh/gộp 2–3 event độc lập, ngày xảy ra/đưa tin, địa điểm/citation/spatial precision/confidence. **Đầu ra:** evidence register; blank là UNKNOWN, không tạo negative giả.
- [ ] **02.4 — Một phần:** OSM đã tải/trích/map; còn gap log/QA vùng thử nghiệm cho đường/cầu/y tế/khu dân cư. **Nghiệm thu:** snapshot date và thiếu thuộc tính rõ, không mặc định facility là shelter xác nhận.

**G2 — Chưa đạt:** nguồn tải thật, AOI thử nghiệm và case đủ evidence; không bắt buộc toàn tỉnh/toàn bộ 2020–2026.

### Tuần 03 — Lakehouse và data contracts

- [ ] **03.1 — Một phần:** Compose/runtime và Spark roundtrip đã có; còn kiểm tra môi trường sạch, chốt client ghi P0, thử PyIceberg write/upsert. **Đầu ra:** setup/smoke; một đường ghi–đọc tương thích, version ghim.
- [ ] **03.2 — Chưa làm:** Schema/key cho source_object, basin_feature, observation, forecast_snapshot, basin_state, threat_state, exposure, evidence, change_event, run_manifest. **Nghiệm thu:** tách object/version, model run/lần tải, issued/ingested time.
- [ ] **03.3 — Một phần:** Raw file-first/checksum có trước; thêm object-storage loader và metadata/canonical Iceberg. **Nghiệm thu:** truy ngược URI/checksum/CRS/extent/interval, không ghi đè raw cũ.
- [ ] **03.4 — Chưa làm:** Append/upsert, snapshot cũ, rerun, quarantine, manifest snapshot map của mọi bảng. **Đầu ra:** integration tests/snapshot demo; idempotent, run lỗi không công bố hoàn tất.

**G3 — Chưa đạt:** versioning/provenance nghiệp vụ thật; GeoTIFF/Parquet hoặc smoke table chưa đủ.

### Tuần 04 — Không gian và hồ sơ basin

- [ ] **04.1 — Một phần:** Có CRS/QA static; xử lý polygon 4101033820, kiểm tra trùng/chồng lấn/IDs/ngoài AOI. **Đầu ra:** spatial DQ tái lập; sửa/quarantine có dấu vết và đơn vị đo.
- [ ] **04.2 — Một phần:** Static features đã tính; chọn 4–6 feature có lý do từ DEM/BasinATLAS/đất/land cover. **Đầu ra:** feature dictionary với unit/depth/coverage/provenance; không suy storage deficit từ dữ liệu không tương thích.
- [ ] **04.3 — Một phần:** Có basin–xã/hạ tầng/sông và NEXT_DOWN; còn DRAINS_TO chuẩn hóa, GSMaP/IFS pixel weights riêng. **Nghiệm thu:** mẫu số overlap rõ, biên/coverage và không nhân đôi.
- [ ] **04.4 — Chưa làm:** Road network, bridge–segment–facility; một chiều, nút giao, layer/cầu vượt, connectivity, IDs ổn định phục vụ routing. **Đầu ra:** topology tests; không nối mọi đường giao hình học.

**G4 — Chưa đạt đầy đủ:** profile/mapping tái lập và QA thủ công ít nhất 10 đối tượng; static mapping không thay road topology/pixel weights.

### Tuần 05 — Ingest động và temporal state

- [ ] **05.1 — Chưa làm:** Ingest mưa 30–60 phút theo nguồn, raw, basin windows 1h/3h/6h/24h/72h. **Nghiệm thu:** fixture tính tay đúng mm/h/interval, không cộng trùng NOW, missing không thành 0.
- [ ] **05.2 — Chưa làm:** Archive forecast model cố định, valid interval, issued/available nếu có căn cứ, retrieval snapshot/revision và selector tại T. **Nghiệm thu:** không bịa run ID, không mất bản cũ/data leakage.
- [ ] **05.3 — Một phần khảo sát:** Đã thử IFS soil moisture/kiểm tra ERA5-Land; còn wetness proxy live, basin_state/availability policy. **Nghiệm thu:** source_mode/data_age/STALE, mô hình khác quan trắc, không đổi nguồn âm thầm.
- [ ] **05.4 — Chưa làm:** DQ schema/missing/duplicate/mưa âm/time/out-of-order/correction, state history/checkpoint/replay. **Đầu ra:** tests/quarantine/metrics; tách source/polling/processing latency.

**G5 — Chưa đạt:** dynamic state phủ AOI thử nghiệm; ingest hai lần không tạo hai state hiệu lực. Dừng công nghệ mở rộng nếu chưa đạt.

### Tuần 06 — Threat và full recomputation

- [ ] **06.1 — Chưa làm:** Đặc tả AW/RF/BS; UF chỉ khi giả định topology đủ rõ. **Đầu ra:** threat method/feature registry; chuẩn hóa/tương quan/không tính trùng mưa recent và antecedent.
- [ ] **06.2 — Chưa làm:** Score/level, VALID/DEGRADED/NO_DATA, rule version/components/reasons/input versions. **Nghiệm thu:** deterministic, NO_DATA khác LOW.
- [ ] **06.3 — Chưa làm:** Baseline mưa, missing tests, sensitivity trọng số/ngưỡng. **Nghiệm thu:** cấu hình version trước đánh giá, không gọi score là xác suất.
- [ ] **06.4 — Chưa làm:** Full state/threat trên dữ liệu đóng băng, fixture NO_DATA/HIGH→LOW. **Đầu ra:** reference outputs; không chọn sai phiên bản forecast tại T.

**G6 — Chưa đạt:** threat giải thích được tại basin; heuristic nghiên cứu, không phải FFG đầy đủ.

### Tuần 07 — Direct exposure và vòng đời tác động

- [ ] **07.1 — Chưa làm:** Giao threat basin với xã/đường/cầu/facility; lưu count/length/ratio. **Đầu ra:** exposure engine/table; candidate exposure không phải ngập/thiệt hại, population mở rộng.
- [ ] **07.2 — Chưa làm:** MAY_AFFECT/MAY_DISRUPT và OBSERVED_AFFECTED có evidence, interval/rule/provenance. **Nghiệm thu:** tách observed/predicted/scenario, không đặt probability tùy ý.
- [ ] **07.3 — Chưa làm:** Add/update/expiry khi threat/forecast đổi, kể cả level giữ nguyên nhưng score/nguồn/thời hạn đổi. **Đầu ra:** lifecycle regression tests; không giữ quan hệ hết điều kiện.
- [ ] **07.4 — Chưa làm:** QA ít nhất 10 giao cắt, full reference và summary xã kèm UNKNOWN/coverage. **Nghiệm thu:** không đếm trùng entity hoặc cộng sai overlap ratios.

**G7 — Chưa đạt:** exposure truy được về threat, raw/snapshot và geometry.

### Tuần 08 — KG và cascading impact

- [ ] **08.1 — Chưa làm:** Neo4j/schema/constraints/stable IDs/full loader cho basin/xã/khu dân cư/đường/cầu/facility/state/evidence. **Nghiệm thu:** rerun không nhân đôi; graph dựng lại từ Lakehouse.
- [ ] **08.2 — Chưa làm:** Ba queries: cầu→đường, khu dân cư→facility mất tiếp cận, giải thích MAY_ISOLATE; ít nhất một SQL/NetworkX đối chứng. **Đầu ra:** Cypher/baseline/rule catalog.
- [ ] **08.3 — Chưa làm:** Reachability trước/sau đóng đường/cầu; ONLY_ACCESS_TO kiểm tra mọi tuyến hợp lệ trong graph. **Nghiệm thu:** case đường thay thế/mạng thiếu/giao không nối, kết luận có điều kiện.
- [ ] **08.4 — Chưa làm:** Explanation/dependency version/validity/retraction khi đường mở hoặc threat giảm. **Đầu ra:** traces/expiry tests/full graph reference; không suy cô lập thực địa từ thiếu đường OSM.

**G8 — Chưa đạt:** basin→cầu/đường→accessibility có giải thích và đối chứng.

### Tuần 09 — Incremental và đồng bộ serving

- [ ] **09.1 — Chưa làm:** Change event theo key/version/time/scope/checksum/reason, dirty basin/time windows. **Nghiệm thu:** không chỉ phát hiện LOW/HIGH.
- [ ] **09.2 — Chưa làm:** Dirty set qua rolling windows, UF nếu dùng, exposure/accessibility/routing dependencies. **Nghiệm thu:** correction tại τ xét state đã có tới τ+72h khi phụ thuộc 72h; có dependency liên basin.
- [ ] **09.3 — Chưa làm:** Durable log, retry/idempotency/checkpoint worker, manifest snapshot map và serving versions. **Nghiệm thu:** recovery sau commit không mất/nhân đôi, sync lag rõ; không giả định atomic xuyên Iceberg–PostGIS–Neo4j.
- [ ] **09.4 — Chưa làm:** Full/incremental equivalence cùng snapshot/rule/time, tolerance và effective entities/edges. **Nghiệm thu:** không mismatch chưa giải thích; full fallback khi scope lớn.

**G9 — Chưa đạt:** correctness trước benchmark tốc độ và triển khai routing.

### Tuần 10 — Accessibility, API và routing phải làm

- [ ] **10.1 — Chưa làm:** Accessibility khu dân cư→y tế bình thường/gián đoạn/UNKNOWN. **Đầu ra:** service/scenarios/explanation; không coi thiếu tuyến dữ liệu là cô lập thực tế.
- [ ] **10.2 — Chưa làm:** FastAPI health/freshness/threat/exposure/explanation và contract routing của project. **Nghiệm thu:** analysis_run_id/valid_time/input versions/serving status; không trộn version không cảnh báo.
- [ ] **10.3 — Phải làm theo project; P1 trong đề cương nguồn:** Chốt một thư viện, routing baseline travel time và hazard/uncertainty penalty trên mạng nhỏ đã QA. **Đầu ra:** routing service/cost config; phương tiện/đơn vị rõ, CLOSED có evidence khác MAY_BLOCK trong scenario, không trộn km/phút tùy ý.
- [ ] **10.4 — Phải làm theo project; P1 trong đề cương nguồn:** Ít nhất hai cặp đi–đến, distance/travel time/exposure/penalty; test không có tuyến/cùng tuyến/điểm không nối. **Nghiệm thu:** lower-risk theo chỉ số, có giải thích; không tuyên bố an toàn.

**G10 — Chưa đạt:** accessibility + API + baseline/lower-risk routing theo phạm vi project. Nếu thiếu thời gian, thu nhỏ vùng/số kịch bản routing hoặc xin điều chỉnh, không tự bỏ routing.

### Tuần 11 — Dashboard, replay và vận hành đầu cuối

- [ ] **11.1 — Chưa làm:** Streamlit threat/exposure/mưa/KG/routing, source/version/health. **Nghiệm thu:** NO_DATA khác màu LOW; observed/forecast/simulated rõ; route có cost breakdown và thời điểm hiệu lực.
- [ ] **11.2 — Chưa làm:** Live/replay riêng; as-known theo availability có căn cứ, reconstruction nhãn riêng. **Đầu ra:** runner/manifest/mode tests; không lấy thời điểm tải hôm nay giả làm lịch sử.
- [ ] **11.3 — Chưa làm:** Mục tiêu chạy ít nhất 24h, source/polling/processing/graph lag và restart. **Nghiệm thu:** log thực, không suy chất lượng cảnh báo từ nhịp UI.
- [ ] **11.4 — Chưa làm:** Fault injection mất mưa/forecast hết hạn/tệp lỗi/Neo4j dừng; degraded/retry/recovery, route stale/UNKNOWN rõ. **Đầu ra:** report/release candidate; không mất raw hoặc giữ edge hết hạn; freeze features.

**G11 — Chưa đạt:** demo source→state→KG→route/dashboard, replay/failure tests; thực/hồi cứu/mô phỏng phân biệt được.

### Tuần 12 — Benchmark và case study

- [ ] **12.1 — Chưa làm:** Full/incremental với 1 basin, 5 basin, 10%/25%/50%/100%; bỏ cấu hình trùng. **Nghiệm thu:** ≥5 lần/cấu hình, median/biến thiên, cùng máy/input/rule/cache, tính detection/I/O/commit/graph writes.
- [ ] **12.2 — Chưa làm:** Correctness/late/correction/revision/outage/retraction/sync/rule-topology changes. **Đầu ra:** tests/mismatch/profiling; không mismatch chưa giải thích, công bố incremental không có lợi.
- [ ] **12.3 — Chưa làm:** 2–3 event độc lập đủ evidence, rank/Top-K/coverage/explanation/baseline. **Nghiệm thu:** không nhân bản tin thành event, không Accuracy/F1 thiếu negative tin cậy; tách hiệu chỉnh/đánh giá nếu đủ.
- [ ] **12.4 — Chưa làm:** Tổng hợp RQ, sensitivity/sai số/chi phí/giới hạn; đánh giá routing baseline so với lower-risk cùng thước đo/giả định. **Đầu ra:** results/limitations/reproducibility manifest; không đặt trước speedup/kỹ năng cảnh báo.

**G12 — Chưa đạt:** số liệu gốc/scripts/config và kết quả bất lợi; ảnh demo không thay thực nghiệm.

### Tuần 13 — Hoàn thiện và bàn giao

- [ ] **13.1 — Chưa làm:** Lỗi chặn, regression môi trường sạch, ghim version/sample hợp pháp/secrets/license/path review. **Đầu ra:** release/compose/sample/log; người khác chạy được từ README.
- [ ] **13.2 — Chưa làm:** Báo cáo 7 chương, ma trận phản hồi→quyết định, kết quả routing. **Nghiệm thu:** mục tiêu có sản phẩm/giải trình, tách số đo/giả định/chưa làm, đủ citation/provenance.
- [ ] **13.3 — Chưa làm:** Slide/demo dữ liệu mới, correction/late, HIGH giảm/retraction, outage, full/incremental và route đổi theo scenario; replay offline. **Đầu ra:** script/video chứng minh correctness/recovery.
- [ ] **13.4 — Chưa làm:** Code/dictionary/KG rules/routing cost/manifests/snapshots/metrics/tests/giới hạn; rubric self-assessment. **Nghiệm thu:** không có phần cam kết chỉ trong mô tả, không thêm tính năng tuần cuối.

**G13 — Chưa đạt:** prototype tái lập, routing trong phạm vi đã chốt, báo cáo và bằng chứng; không nghiệm thu theo số công nghệ.

## 6. Kiểm thử bắt buộc và điều kiện nghiệm thu

| Mã đề cương | Kịch bản | Điều kiện |
|---|---|---|
| E1 | Ingest lặp | Không nhân đôi rows/node/edge hiệu lực |
| E2 | Mưa trễ/correction | Đúng rolling windows tới 72h, lịch sử còn, không leakage |
| E3 | Forecast revision | Đúng bản khả dụng tại T, không bịa issued time |
| E4 | HIGH→LOW/NO_DATA | Retraction đúng; NO_DATA không là LOW |
| E5 | Outage/tệp lỗi | STALE/DEGRADED/NO_DATA, quarantine/retry/source-switch log |
| E6 | Neo4j lỗi sau commit | Idempotent recovery, serving lag rõ, rebuild được |
| E7 | Dependency liên basin | Dirty set đủ windows/hạ lưu nếu có UF/accessibility |
| E8 | Full/incremental | Cùng effective entities/edges và numeric tolerance; không so operational timestamps |
| E9 | Rule/topology/100% đổi | Phát hiện scope toàn cục, full fallback khi hợp lý, đo chi phí thật |

Bổ sung routing: CLOSED có evidence không nằm trên tuyến; MAY_BLOCK chỉ loại theo policy/scenario rõ; có UNKNOWN/no-route/disconnected origin-destination; cập nhật/retraction làm invalidation đúng route phụ thuộc; baseline và lower-risk dùng cùng network/input version, phương tiện và đơn vị cost.

## 7. Ưu tiên tiếp theo và rủi ro

1. Lưu probe IFS có raw/access log; tải/đọc GSMaP thật và kiểm tra archive/version/quota (01.3).
2. Sửa hoặc quarantine geometry lỗi; xuất scale assessment GSMaP/IFS; chốt cụm thử nghiệm và phạm vi road network đủ routing, không tự đổi L12/H3 (02.1, 04.1).
3. Xác minh 2–3 event. QA hiện ghi 12 tên sự kiện chưa resolve và 28 downstream scope exits; cần xử lý/giải thích, không coi topology/evidence hoàn chỉnh (02.3).
4. Chốt kiến trúc/schema/client ghi, thử write/upsert/snapshot và manifest trước ingest lớn (01.4, 03.1–03.4).
5. Xây pixel weights rồi ingest/state; chuẩn bị road topology theo task 04.4. Không mở Kafka/Flink/OpenMetadata thay cho các việc này.

Rủi ro: archive/quota nguồn; footprint/valid coverage chưa xác minh; sai khác ranh xã; OSM thiếu kết nối và travel-time attributes; dữ liệu stale/serving khác version; hạn 13 tuần. Không cam kết đủ mọi giờ 2020–2026 hoặc lead time khi thiếu availability lịch sử.

Ưu tiên cắt phần chưa cam kết: streaming/metadata platform → storage deficit/UF định lượng → population/facility phụ. Có thể thu nhỏ AOI/features hoặc vùng/kịch bản routing sau khi chốt lại phạm vi. **Không tự bỏ routing, provenance, temporal correctness, observed/predicted distinction, retraction hoặc full/incremental equivalence.**

## 8. Cách cập nhật

- Ghi ngày, task ID, artifact/commit và test result sau mỗi phiên; không tự suy tuần kế hoạch từ ngày lịch.
- Tick task khi đủ đầu ra/tiêu chí; giữ các phần đã làm ở mục 2.
- G1–G13 ghi nghiệm thu GVHD riêng khi có; không suy từ commit/smoke.
- Quyết định AOI/nguồn/kiến trúc/routing cập nhật scope/contracts/tracking cùng nhau; thảo luận chưa đồng nghĩa triển khai.
- File này không thay đổi code/raw/cấu hình AOI/lựa chọn L10. Không cập nhật DOCX/XLSX trong phiên này.
