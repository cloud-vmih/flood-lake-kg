# Nhật ký tối ưu hiệu năng pipeline

File này ghi lại nút thắt, thay đổi và kết quả đo của từng DAG. Khi tối ưu DAG khác, thêm một
mục mới theo mẫu ở cuối file. Các số đo là quan sát trên môi trường local và run được nêu rõ,
không phải cam kết thời gian cho mọi máy hoặc mọi tập dữ liệu.

## `gsmap_standard_ingest` — cập nhật 28/10/2026

**Run dùng để đo:** `scheduled__2026-10-27T06:27:00+00:00`. Cấu hình run cho phép tối đa 2.160
object giờ; Raw/Meta xử lý 24 object/task, Bronze xử lý 96 object/task. Các số dưới đây là trung
vị `total_seconds` và `phase_seconds` từ hai log `weather_raw_publish_timings` và
`weather_bronze_phase_timings`. Nhóm trước/sau thuộc cùng run nhưng chạy với hai phiên bản code;
tải hệ thống có thể khác nhau giữa hai thời điểm.

| Task và chỉ số | Trước | Sau | Mẫu trước/sau |
| --- | ---: | ---: | ---: |
| `landing_raw.register_raw_and_meta`: toàn task | 158,4 giây | 17,5 giây | 12 / 53 task |
| ↳ `attempts_commit` | 140,9 giây | 4,5 giây | 12 / 53 task |
| `bronze.parse_bronze`: toàn task | 352,5 giây | 126,9 giây | 14 / 4 task |
| ↳ `audit_lifecycle_evidence` | 316,0 giây | 84,2 giây | 14 / 4 task |

### 1. Raw/Meta: ghi đè `meta.ingest_attempts` ở mỗi batch

- **Nguyên nhân:** `commit_fetch_attempts` đã ghi kết quả tải thành công với trạng thái `running`.
  Sau đó mỗi task `register_raw_and_meta` đổi 24 dòng của mình thành `succeeded`, khiến Iceberg
  phải overwrite các dòng đã có. Đo phase cho thấy `attempts_commit` chiếm phần lớn thời gian.
- **Đã sửa:** fetch thành công được chốt `succeeded` trước khi commit audit tập trung. Khi publish
  Raw gặp attempt `succeeded` cùng request fingerprint, upsert giữ nguyên dòng hiện có; bằng chứng
  publish nằm ở `meta.source_objects` và lifecycle. Nếu publish thất bại, attempt vẫn có thể được
  cập nhật thành `failed`. Batch dùng bộ lọc theo danh sách `asset_id`, thay vì OR từng khóa bốn cột.
- **Run đang dở:** 1.272 attempt còn `running` của đúng run trên được cập nhật trong một commit sau
  khi xác nhận cả 90 task fetch và 90 task scope thành công; tổng 2.160 attempt của run được kiểm
  tra lại. Nhờ đó các batch Raw/Meta còn lại của run cũ cũng dùng đường ghi nhanh.
- **Đọc lifecycle:** `register_many` tra các `object_id` trong một batch, rồi ghi một commit,
  tránh đọc riêng từng object trước khi đăng ký Raw.
- **Code:** `pipelines/weather_ingest/{airflow,fetch_audit,landing,lifecycle}.py` và
  `storage/iceberg_tables.py` dưới `src/flashflood_data/`.

### 2. Bronze: đọc lifecycle từng object trước khi ghi batch

- **Nguyên nhân:** `mark_bronze_evidence_many` đã ghi 96 object trong một commit nhưng vẫn gọi
  `_load` cho từng object. Đo phase cho thấy audit lifecycle mất trung vị 316 giây/task; phần parse
  raster của các batch này chỉ khoảng 0,1 giây.
- **Đã sửa:** đọc 96 `object_id` bằng một truy vấn batch, kiểm tra object thiếu, trùng, hết hạn và
  trạng thái quality trong bộ nhớ, rồi ghi evidence bằng một commit. Điều kiện chuyển trạng thái
  lifecycle của từng object được giữ nguyên.
- **Code:** `src/flashflood_data/pipelines/weather_ingest/lifecycle.py`; phase timing được ghi bởi
  `src/flashflood_data/pipelines/weather_ingest/bronze.py`.

### Các thay đổi hỗ trợ và giới hạn còn lại

- Fetch GSMaP Standard gom 24 giờ/task và tái sử dụng một FTP session trong task; pool fetch có
  bốn slot. Raw/Meta và Bronze dùng writer pool một slot để tránh các commit Iceberg xung đột.
- Bronze gom 96 object/task, tạo một snapshot cho batch nhưng giữ DQ, lineage và lifecycle theo
  từng `object_id`. Log đo riêng inventory, tải Raw, checksum, parse, quality, commit Bronze và
  từng phần audit. **Chưa có phép đo tách riêng** tác động của việc gom batch hoặc tái sử dụng FTP
  session, nên không quy mức giảm trong bảng cho các thay đổi này.
- `audit_lifecycle_evidence` vẫn mất trung vị 84,2 giây/task sau khi bỏ đọc từng object. Phần ghi
  đè lifecycle trong Iceberg và các audit khác còn cần phân tích nếu tiếp tục tối ưu. Cỡ mẫu
  Bronze sau sửa hiện là bốn task; cần đo thêm ở các run tiếp theo.

## `silver_weather_aggregation` — cập nhật 07/11/2026

**Phạm vi kiểm chứng:** snapshot Bronze `2428879406265105875` có khoảng 303 nghìn
raster slice, trải trên 55 tháng; basin Silver có 179 basin. Đã smoke riêng tháng
01 và 02/2020, chưa benchmark toàn bộ backfill.

| Quan sát trên local | Kết quả |
| --- | --- |
| Chạy toàn bộ snapshot trong một Spark application | Chưa hoàn tất; dừng khi shuffle `/tmp` của worker lên khoảng 11 GB |
| Checkpoint normalized của toàn snapshot | Khoảng 471 MB |
| Checkpoint normalized batch 01/2020 | Khoảng 7 MB |
| Batch 01/2020 | Publish 1.065.229 dòng, 2 file Iceberg |
| Batch 02/2020, MERGE vào bảng đã có tháng 01 | 132,52 giây; thêm 996.672 dòng, tổng bảng 2.061.901 dòng/4 file |
| Chạy lại 02/2020 bằng v2 sửa revision đầu tháng | 133,44 giây; vẫn 2.061.901 dòng và 2.061.901 business key khác nhau |
| Tái sử dụng manifest tháng 02 trong cùng build v2 | 0,003 giây; không submit Spark |
| Tìm danh sách tháng, chỉ đọc cột `valid_time` của snapshot pin | 7,15 giây trong lần đo 06/11 |

Hai phép đo checkpoint dùng phạm vi khác nhau để cho thấy giới hạn tài nguyên
của batch. Không suy ra hệ số tăng tốc toàn pipeline từ các số này.

### Nguyên nhân và bản sửa

- **MERGE trọng số không lập được plan:** frame nguồn anti-join chính bảng target
  Iceberg, gây `No plan for TableReference`. Materialize tập trọng số còn thiếu
  trước MERGE để cắt dependency đọc target trong source plan.
- **DQ coverage lỗi do làm tròn:** tổng trọng số có thể là `1.0000000000006783`.
  Chặn giá trị coverage xuất ra ở 1; phép tính trung bình vẫn chia tổng trọng số
  thực. Cờ `weight_sum_exceeds_one` giữ khả năng phát hiện sai lệch vượt tolerance.
- **Driver hết heap khi lập plan ERA5:** self-join giờ trước làm plan MERGE mở
  rộng. Materialize normalized slice trước aggregation bằng reliable checkpoint
  trên thư mục driver và worker cùng mount. `localCheckpoint` từng làm mất block
  khi executor rời, nên không dùng nó cho normalized weather.
- **Toàn snapshot tạo shuffle quá lớn:** thay một application toàn bảng bằng
  application theo tháng. Source scan đọc tháng đó và thêm một giờ trước tháng
  để ERA5 vẫn tính được increment lúc 00 UTC ngày đầu tháng. Checkpoint được dọn
  khi application kết thúc bình thường.
- **Retry phải tính lại phần đã xong:** mỗi tháng có manifest sau khi publish,
  identity gắn với build signature và tháng. Runtime kiểm tra manifest rồi bỏ qua
  application đã hoàn tất trong cùng build. Nếu commit xong nhưng chưa có manifest,
  retry vẫn MERGE bằng business key để không tạo dòng trùng.
- **ERA5 đầu tháng null dù Bronze có giờ trước:** `source_revision` là hash
  checksum file, nên mẫu 23 UTC tháng trước và 00 UTC tháng sau có revision khác
  nhau. Join cũ yêu cầu bằng nhau làm mất predecessor. Bản v2 cho phép khác
  revision giữa hai cycle tại 00 UTC ngày đầu tháng; cùng cycle vẫn phải khớp
  revision. Build signature có algorithm version v2 để không tái sử dụng manifest
  v1. Regression Spark đã tái hiện lỗi trước sửa và xác nhận sai phân đúng sau sửa.
- **Khó thấy tiến độ trong một task Airflow:** log bắt đầu/kết thúc từng batch có
  `batch_month`, `reused`, `total_seconds` và output snapshot. Gateway giữ traceback
  gốc khi Spark lỗi, kể cả khi phần log sau traceback dài hơn giới hạn tail.

### Kiểm chứng và giới hạn

Unit/contract/integration test kiểm tra chọn tháng, giữ predecessor ERA5, DQ,
manifest, audit, retry giữa hai tháng và skip cùng build đã thành công. Smoke
Spark/Polaris/MinIO đã publish hai tháng. Airflow image được rebuild để nhận code mới.
Scan sau hai tháng thấy 2.061.901 business key khác nhau, không có key null và
coverage trong [0, 1]. Số đo 132,52 giây ở trên thuộc bản trước sửa revision;
batch v2 đo riêng được 133,44 giây. Tại 00 UTC 01/02, cả precipitation, surface
runoff và subsurface runoff có giá trị cho 178/179 basin sau sửa; không tăng số
dòng khi MERGE lại. Regression fixture Spark kiểm tra phép trừ có giá trị đúng
và chặn predecessor khác revision trong cùng cycle.

Runtime vẫn xét mọi tháng khi input snapshot/config đổi; chưa có dirty-window
discovery. Tại thời điểm smoke hai tháng, publisher MERGE còn quét target ngày càng lớn;
bản sửa giới hạn target theo tháng được ghi riêng bên dưới. Chưa đo tổng
thời gian 55 tháng và chưa khẳng định toàn bộ backfill đạt hiệu năng tương tự.
Meta chỉ ghi run thành công sau khi mọi tháng trong plan hoàn tất; dữ liệu các
tháng đã commit có thể đọc được trước thời điểm đó.

### Policy ưu tiên GSMaP — algorithm v3, 07/11/2026

Silver chỉ giữ ERA5 precipitation làm fallback khi không có GSMaP hợp lệ cho
cùng basin/version/window/layer/unit/value kind. Giá trị 0 mm vẫn hợp lệ; null,
âm, NaN/vô cực hoặc coverage dưới ngưỡng không loại ERA5. NOW và Standard cùng
window vẫn là hai quan sát riêng; IFS, runoff và soil moisture được giữ nguyên.

`preference.py` chọn các dòng được publish và tạo danh sách business key ERA5
dư. Application publish trước, rồi MERGE DELETE theo danh sách đã checkpoint,
giới hạn trong tháng hiện tại. Manifest và output snapshot được ghi sau cleanup;
ngắt giữa hai commit có thể còn ERA5 dư tạm thời, retry tiếp tục dọn. Không expire
snapshot và không xóa dữ liệu Raw/Bronze trong bước này.

Trong lần thử đầu, lọc nguồn trực tiếp trên aggregation/DQ lazy gây assertion
`weather unit is not canonical`, dù kiểm tra đơn vị và DQ trên cùng dữ liệu đã
qua. Chẩn đoán chỉ đọc ba giờ tái hiện lỗi; materialize accepted rows trước lọc
nguồn thì qua. Bản sửa thêm reliable checkpoint ở ranh giới DQ → source policy,
và checkpoint sau lọc để cắt dependency đọc target trước MERGE. Fixture Spark
trên bảng tạm đã tái hiện lỗi trước sửa và qua với luồng aggregate/DQ/publish
sau sửa, đồng thời kiểm tra late arrival và cleanup idempotent.

Lần chuyển đổi dữ liệu đã có dùng job audit riêng
`silver_weather_preference_migration`, chỉ xét các tháng đang có trong Silver;
không đánh dấu toàn bộ backfill của `silver_weather_aggregation` đã thành công.
Baseline trước đổi policy có 257.760 dòng mưa ERA5 trong tổng 2.061.901 dòng;
preview độc lập bằng PyArrow xác định 256.142 dòng có GSMaP hợp lệ thay thế.

Kết quả chuyển đổi thực tế:

| Phạm vi | Thời gian | Kết quả |
| --- | ---: | --- |
| 01/2020 | 145,56 giây | Dọn 132.254 dòng mưa ERA5 dư |
| 02/2020 | 131,51 giây | Dọn 123.888 dòng mưa ERA5 dư |
| Tổng bảng sau chuyển đổi | — | 1.805.759 dòng, gồm 1.618 dòng mưa ERA5 fallback |

PyArrow kiểm tra lại với snapshot trước chuyển đổi: không có business key trùng,
không còn ERA5 precipitation bị GSMaP hợp lệ phủ trùng, tập khóa fallback khớp
hoàn toàn với tập mong đợi. Số dòng GSMaP vẫn 257.581; soil moisture 1.031.040;
surface và subsurface runoff mỗi biến 257.760. Migration run
`silver-gsmap-migration-5d5d34721503e8b940cf4995` có audit
`succeeded_with_warning` và `retry_count=1`; warning do coverage, không có fatal DQ.

Rà soát còn bổ sung việc loại dòng GSMaP cũ sắp bị ghi đè khỏi tập chọn nguồn.
Nếu kết quả tính lại cùng business key trở thành null/coverage thấp, bản hợp lệ
cũ không được làm mất ERA5 fallback. Fixture Spark tái hiện trước sửa và kiểm tra
selection, publish, cleanup, replay sau sửa. Các snapshot cũ vẫn được giữ; số dòng
hiện hành giảm không đồng nghĩa đã giải phóng toàn bộ dung lượng lịch sử.

### Tháng xử lý càng về sau càng chậm — 07/11/2026

Run `manual__2026-11-07T11:05:55.785057+00:00` cho thấy tổng thời gian application
tăng theo lượng dữ liệu đã publish, dù mỗi application vẫn chỉ xử lý một tháng:

| Tháng | Tổng thời gian trước sửa |
| --- | ---: |
| 03/2020 | 113,75 giây |
| 09/2020 | 172,98 giây |
| 07/2021 | 288,48 giây |
| 10/2021 | 327,12 giây |
| 11/2021 | 321,00 giây |

**Nguyên nhân xác nhận từ code và metadata:** `silver.basin_weather_value` chưa có
partition. `_change_count` và `MERGE` của shared publisher chỉ join bằng hash,
chưa có điều kiện thời gian trên target. Vì vậy source giới hạn một tháng nhưng
hai bước này không có điều kiện tháng để loại file của các tháng khác.
Tại một snapshot đã đo, toàn bảng có 66 file/932,42 MB, còn bộ lọc 07/2021 chọn
3 file/47,81 MB theo metadata Iceberg. Đây là phạm vi file có thể loại bỏ,
không phải phép đo số byte thực đọc hoặc hệ số tăng tốc application.

**Bản sửa:** `shared/spark/publish.py` thêm `TimestampMergeScope` tùy chọn, dùng
cùng khoảng UTC nửa mở cho change detection và MERGE. Publisher kiểm tra mọi
dòng source thuộc khoảng đó trước khi ghi; cột thời gian phải nằm trong khóa
MERGE. Weather truyền `valid_time` của tháng và dùng khóa vật lý
`(business_key_hash, valid_time)`; business key logic vẫn là hash đã bao gồm thời
gian. Các pipeline không truyền scope giữ hành vi cũ. Preference/cleanup đã có
bộ lọc tháng, không đổi chính sách chọn nguồn, schema hay algorithm version v3.

**Kiểm chứng:** 86 unit/contract/integration test qua; Ruff qua. Fixture Spark với
Iceberg tạm xác nhận bounded MERGE giữ nguyên tháng khác, replay không sinh
trùng, source ngoài tháng bị chặn trước commit, và policy GSMaP vẫn đúng. Fixture
qua với driver heap 512 MB; lần chạy heap 1 GB trước đó bị Docker OOM. Máy local
đồng thời dùng gần 8 GB swap, có swap-in/out và I/O wait khoảng 59–61%, nên áp lực
bộ nhớ/đĩa cũng góp phần làm chậm. Chưa xác định toàn bộ nguồn tiêu thụ RAM trên host.

Hai file source được thay nguyên tử lúc khoảng 12:23:48 UTC, publisher trước rồi
weather caller. Không restart DAG/application đang chạy; tháng 12/2021 đã khởi
động trước mốc này nên vẫn dùng code cũ. Application mới đọc source qua bind mount.
Hash hai file nhìn từ container gateway đã khớp source host.

**Số đo sau sửa trong cùng run**, lấy từ log `weather_silver_batch_finished`:

| Tháng | Tổng thời gian | Bản code |
| --- | ---: | --- |
| 12/2021 | 329,01 giây | Cũ, application đã nạp trước cập nhật |
| 01/2022 | 104,20 giây | Có scope tháng |
| 02/2022 | 91,67 giây | Có scope tháng |
| 03/2022 | 93,31 giây | Có scope tháng |
| 04/2022 | 90,94 giây | Có scope tháng |
| 05/2022 | 99,85 giây | Có scope tháng |
| 06/2022 | 110,42 giây | Có scope tháng |

Sáu application mới hoàn tất trong khoảng 91–110 giây/tháng, so với 321–329 giây
của ba tháng ngay trước sửa. Đây là so sánh các tháng khác nhau trong cùng run,
không phải benchmark A/B trên cùng input; không suy ra mức tăng tốc cố định cho
cả 55 tháng. Run tiếp tục bình thường; toàn bộ backfill chưa hoàn tất tại thời
điểm ghi nhận. Không đổi partition hay expire snapshot trong bản sửa này.

## Mẫu bổ sung cho DAG tiếp theo

Sao chép mục này khi có tối ưu mới; ghi rõ baseline và thay đổi nào thực sự được đo.

### `<dag_id>` — `<ngày cập nhật>`

- **Run/môi trường:** `<run_id>`, phạm vi dữ liệu, cấu hình batch và phiên bản code.
- **Triệu chứng và nguyên nhân:** task/phase nào chậm, log hoặc phép đo xác nhận nguyên nhân.
- **Thay đổi:** file code/config đã sửa, hành vi cũ và hành vi mới; cách xử lý run đang dở.
- **Kết quả:** thời gian trước/sau, thống kê dùng (ví dụ trung vị), số mẫu và nguồn log.
- **Kiểm chứng:** test, lint, smoke hoặc chạy DAG thực tế; nêu rõ phần chưa kiểm chứng.
- **Còn lại:** nút thắt mới hoặc rủi ro cần theo dõi.
