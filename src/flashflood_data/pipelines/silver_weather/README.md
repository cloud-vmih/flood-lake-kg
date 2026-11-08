# Silver Weather: Bronze raster slice → giá trị theo basin L12

**Trạng thái 07/11/2026:** đã smoke và chuyển tháng 01–02/2020 sang policy v3
ưu tiên GSMaP. Có 1.817 trọng số và 1.805.759 dòng `basin_weather_value`, gồm
1.618 dòng mưa ERA5 fallback; đã dọn 256.142 dòng mưa ERA5 dư. Kiểm tra độc lập
không thấy business key trùng, ERA5 dư hoặc fallback bị mất. Audit chuyển đổi
riêng ghi `succeeded_with_warning` do coverage thấp ở một basin. Toàn bộ backfill
chỉ hoàn tất khi DAG publish đủ các tháng và ghi Meta audit thành công.

## Luồng dữ liệu

```text
bronze.weather_raster_slice + silver.source_grid + silver.dim_basin
  → pin ba Iceberg snapshot → build signature → build/repair/skip
  → Spark/Sedona tính silver.grid_basin_weight còn thiếu
  → lần lượt xử lý từng tháng: chuẩn hóa biến, đơn vị, window
  → tổng hợp cell hợp lệ theo diện tích giao
  → DQ fatal/reject/warning → checkpoint accepted → ưu tiên GSMaP theo basin/window
  → MERGE silver.basin_weather_value → dọn ERA5 precipitation dư trong tháng
  → quarantine.rejected_rows nếu có reject
  → manifest → Meta run/snapshot/quality/lineage
```

Các quan sát được giữ vẫn có source/revision riêng. Mưa ERA5 chỉ được giữ làm
fallback khi không có GSMaP hợp lệ cho đúng basin/version/window/layer/unit.
GSMaP 0 mm vẫn hợp lệ; null, âm, NaN/vô cực hoặc coverage dưới ngưỡng không loại
ERA5. NOW và Standard vẫn giữ riêng, kể cả khi hai product có window khớp nhau;
các window NOW lệch 30 phút không làm mất ERA5 ở window khác. IFS, runoff và
soil moisture giữ nguyên chính sách lưu nhiều nguồn.

`value` chỉ được tính khi
`valid_coverage_fraction` đạt ngưỡng trong YAML; cell thiếu không được tính thành 0.
ERA5-Land được đổi từ accumulation sang lượng theo giờ; GSMaP rate được nhân với độ dài window.
ERA5 đầu tháng vẫn đọc thêm giờ cuối tháng trước để tính sai phân. Mỗi tháng là
một Spark application và có manifest riêng trong `staging/silver/`; retry dùng
manifest để bỏ qua tháng đã hoàn tất. MERGE theo business key giữ chạy lại không
tạo hàng trùng. Khi Bronze hoặc basin snapshot đổi, build signature đổi. Chạy
lại cùng ba snapshot và config đã thành công trả về `skip`.

Manifest chỉ được tái sử dụng trong cùng build signature. Khi snapshot hoặc
config đổi, runtime xét lại các tháng; hiện chưa chọn riêng dirty slice/window.
Log `weather_silver_batch_started` và `weather_silver_batch_finished` ghi tháng,
`reused`, thời gian và snapshot output. DQ có scope tháng để kết quả các batch
không ghi đè nhau trong Meta.

Change detection và MERGE giá trị thời tiết giới hạn target theo `valid_time`
của tháng đang xử lý để tránh đọc lại file của toàn bộ lịch sử. Shared publisher
nhận `TimestampMergeScope`, chặn source ngoài tháng trước commit. Khóa MERGE vật
lý là `(business_key_hash, valid_time)`; business key logic vẫn là hash. Bảng chưa
partition, nên khả năng loại file dựa vào thống kê thời gian của Iceberg.

`source_revision` ERA5 được tạo từ checksum file, nên hai file tháng có revision
khác nhau. Tại 00 UTC ngày đầu tháng, normalization cho phép lấy predecessor từ
cycle tháng trước; trong cùng cycle vẫn phải khớp revision. Trước khi trừ vẫn
kiểm tra đúng giờ, cùng source/product/grid/scope/biến/layer và cell index khớp.
Thuật toán `weather-normalize-aggregate-v3` giữ bản sửa đầu tháng của v2 và thêm
policy ưu tiên GSMaP. Build signature đổi để không dùng lại manifest của policy cũ.

Publish và cleanup là hai bước commit tuần tự của cùng application: ghi GSMaP
thành công trước, rồi MERGE DELETE các business key ERA5 đã có GSMaP hợp lệ thay
thế. Nếu ngắt giữa hai bước, ERA5 dư có thể còn tạm thời; retry tiếp tục dọn.
Manifest thành công và output snapshot chỉ được ghi sau cleanup. Snapshot cũ
vẫn được giữ theo retention; policy này không tự expire snapshot hoặc xóa Raw/Bronze.

Khi tính lại, những dòng GSMaP cũ có business key sắp được ghi đè không tham gia
chọn nguồn. Điều này giữ ERA5 fallback nếu bản GSMaP tính lại thành null/coverage
thấp. Kết quả accepted được checkpoint trước policy để tách plan aggregation/DQ;
kết quả chọn nguồn được checkpoint tiếp trước MERGE vì có đọc target hiện tại.

## Đọc code theo thứ tự

| File | Vai trò |
| --- | --- |
| `config/pipelines/silver/weather_aggregation.yaml` | Bảng đầu vào/ra, CRS, coverage và đơn vị đích. |
| `airflow/dags/silver_weather_aggregation.py` | Entrypoint DAG mỏng. |
| `airflow.py` | Hai task: lập plan, điều phối các Spark application tuần tự qua pool `silver_writer`. |
| `runtime.py` | Pin snapshot, tìm tháng trong Bronze, submit từng tháng, kiểm manifest và ghi Meta audit cuối. |
| `pipeline.py` | Build signature, quyết định build/repair/skip và khóa trọng số. |
| `spark_job.py` | Mỗi application đọc snapshot, xử lý trọng số hoặc một tháng thời tiết, DQ, MERGE và ghi manifest. |
| `weights.py` | Sedona overlay cell–basin trong CRS diện tích bằng nhau. |
| `normalize.py` | Đổi tên biến, đơn vị và cửa sổ thời gian theo source. |
| `aggregate.py` | Căn mảng cell index/value, tính trung bình có trọng số và coverage. |
| `preference.py` | Lọc mưa ERA5 dư theo GSMaP hợp lệ; dọn fallback khi GSMaP đến muộn, giới hạn theo tháng. |
| `quality.py` | Rule fatal, reject và warning cho hai output. |
| `revisions.py` | Business key có source, cycle, window và revision. |
| `models.py` | Contract dòng kết quả dùng cho kiểm thử và trao đổi. |
| `reader.py` | Helper đọc input pin snapshot; Spark job dùng trực tiếp snapshot từ Airflow. |
| `shared/spark/manifest.py` | Ghi result manifest MinIO có chữ ký SigV4, dùng chung được cho domain Silver. |

## Kiểm tra

Kiểm tra offline, không chạm DAG đang chạy:

```bash
.venv/bin/pytest tests/unit/pipelines/silver_weather \
  tests/contract/storage/test_silver_weather_schema.py \
  tests/contract/infra/test_silver_weather_dag.py \
  tests/integration/silver/test_weather_aggregation_pipeline.py -q
```

Regression Spark/Iceberg trên bảng tạm, không ghi bảng lakehouse thật:

```bash
docker compose --profile spark run --rm --no-deps -T \
  --entrypoint /opt/spark/bin/spark-submit spark-submit \
  --driver-memory 512m --master 'local[1]' /opt/spark/jobs/smoke_weather_preference.py
```

Khi không còn run khác cần giữ image hiện tại, build lại Airflow/Spark rồi chạy DAG manual
`silver_weather_aggregation`. DAG mặc định paused, không có lịch tự động. Trước khi trigger,
kiểm tra `silver.dim_basin`, `silver.source_grid`, `bronze.weather_raster_slice` đều có snapshot.
Sau lần chạy đầu, kiểm tra `meta.pipeline_runs`, `meta.table_snapshot_ref`,
`meta.quality_results` và count hai bảng Silver bằng Trino.
