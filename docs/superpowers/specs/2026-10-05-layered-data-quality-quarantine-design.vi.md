# Thiết kế Data Quality và quarantine đa tầng

**Ngày:** 2026-10-05
**Trạng thái:** Chốt để lập kế hoạch triển khai
**Phạm vi:** Raw/Landing, Bronze, Silver và Gold của lakehouse.

## 1. Mục tiêu

Xây dựng một framework DQ dùng chung nhưng để từng pipeline sở hữu rule nghiệp vụ. Mỗi lỗi phải dẫn đến đúng một trong ba cách xử lý:

1. `block_object`: lỗi làm toàn bộ object không đáng tin; không publish row của object vào bảng đích.
2. `reject_row`: chỉ row sai bị loại; row hợp lệ vẫn được publish và row sai được ghi vào quarantine.
3. `accept_with_warning`: row vẫn được publish với `quality_status=warning` và có bằng chứng rule-level trong Meta.

Framework phải chạy lại không tạo trùng, không yêu cầu tải lại Raw đã hợp lệ và dùng được cho Silver/Gold khi các pipeline đó được triển khai.

## 2. Nguyên tắc theo tầng

| Tầng | Trách nhiệm DQ |
| --- | --- |
| Raw/Landing | Identity nguồn, checksum, kích thước, file đọc được, container/CRS tối thiểu và manifest bất biến. |
| Bronze | Schema nguồn, row identity, geometry/grid/temporal envelope và khả năng giữ nguyên provenance. |
| Silver | Unit chuẩn, chuẩn hóa thời gian, spatial join, coverage, topology và semantic constraint. |
| Gold | Window completeness, freshness, source priority, cân bằng nước, range vật lý, model/routing output và `NO_DATA`. |

Không lặp checksum ở Silver/Gold. Mỗi tầng chỉ kiểm invariant do chính tầng đó tạo ra và điều kiện đầu vào cần thiết cho transform.

## 3. Vocabulary thống nhất

### 3.1 Trạng thái row được publish

`quality_status` chỉ nhận:

- `passed`: mọi rule bắt buộc đều đạt và không có warning.
- `warning`: row dùng được nhưng có một hoặc nhiều finding cảnh báo.

Row bị reject không tồn tại trong bảng đích. Trạng thái `failed` thuộc quality finding, pipeline run, object lifecycle hoặc quarantine record; không ghi một row `failed` vào bảng nghiệp vụ.

### 3.2 Finding và disposition

Mỗi `QualityFinding` có `rule_id`, `rule_version`, `status`, `severity`, `scope_key`, observed/expected value và số row lỗi. Severity nhận:

- `fatal`: dẫn đến `block_object` hoặc block toàn build nếu rule có scope dataset.
- `reject`: dẫn đến `reject_row`.
- `warning`: dẫn đến `accept_with_warning`.
- `info`: bằng chứng audit, không thay đổi disposition.

Status tiếp tục dùng `passed`, `failed`, `error`, `skipped` để tương thích `meta.quality_results`.

## 4. Quarantine contract

Tạo namespace Iceberg `quarantine` với hai bảng.

### 4.1 `quarantine.rejected_objects`

Một dòng cho một `(object_id/build_input, target_dataset_id, quality_policy_id, rule_id)` bị block:

```text
rejection_id            string PK
pipeline_run_id         string
layer                   string       # raw|bronze|silver|gold
target_dataset_id       string
source_id               string?
object_id               string?
input_table             string?
input_snapshot_id       long?
scope_key               string
quality_policy_id       string
rule_id                  string
rule_version             string
reason_code              string
detail_json              json
evidence_uri             string?
rejected_at              timestamp
```

Raw object dùng `object_id`; Silver/Gold dataset-level block dùng `input_table + input_snapshot_id`. `rejection_id` là SHA-256 của identity logic, không chứa run ID, nên retry cùng policy upsert cùng một rejection.

### 4.2 `quarantine.rejected_rows`

Một dòng cho một row bị loại khỏi bảng đích:

```text
rejection_id            string PK
pipeline_run_id         string
layer                   string       # bronze|silver|gold
target_dataset_id       string
object_id               string?
input_table             string?
input_snapshot_id       long?
source_record_key       string
quality_policy_id       string
rule_ids                list<string>
reason_codes_json        json
record_json              json?
record_uri               string?
parser_version           string?
rejected_at              timestamp
```

Phải có ít nhất một trong `record_json` hoặc `record_uri`. Geometry binary, raster array lớn và payload nhạy cảm không nhét vào JSON; ghi batch Parquet trong MinIO rồi trỏ `record_uri`. Quarantine chỉ giữ bằng chứng cần review, không sao chép toàn bộ source nếu `object_id` đã đủ truy ngược Raw.

## 5. Ranh giới framework và domain

Package dùng chung `orchestration/dq/` chịu trách nhiệm:

- model finding/evaluation;
- tính disposition xác định;
- tạo rejection ID;
- validate policy/version;
- ghi quarantine idempotent;
- chuyển finding thành row `meta.quality_results`;
- điều phối publish/audit có khả năng repair.

Package domain chịu trách nhiệm:

- định nghĩa rule và threshold;
- tạo `scope_key`/`source_record_key`;
- chuyển row nguồn thành accepted row hoặc rejected evidence;
- xác định warning có cho phép downstream sử dụng hay không.

Framework không import parser/domain. Domain chỉ import public interface từ `orchestration.dq`.

## 6. Luồng publish

### 6.1 Object fatal

```text
validate object
  -> findings fatal
  -> ghi meta.quality_results pre_commit
  -> upsert quarantine.rejected_objects
  -> đánh pipeline run failed_data_quality
  -> không publish row của object
```

Nếu object từng có Bronze row từ policy cũ, migration được phép xóa slice cũ chỉ khi failure là DQ xác định. Lỗi hạ tầng, timeout hoặc exception không phân loại không được xóa snapshot tốt trước đó.

### 6.2 Row reject và warning

```text
parse object
  -> partition accepted/warning/rejected
  -> upsert rejected rows
  -> replace accepted rows theo object/build key
  -> ghi Meta quality + snapshot + lineage
  -> run succeeded hoặc succeeded_with_warning
```

Iceberg không có transaction xuyên bảng. Mỗi bước phải idempotent; run chỉ `succeeded` sau khi quarantine, output snapshot, quality, snapshot ref và lineage đều tồn tại. Retry cùng identity thực hiện repair phần thiếu.

Nếu object có zero accepted row sau khi reject, framework nâng thành `no_accepted_rows` severity `fatal`; không tạo object slice rỗng.

### 6.3 Lỗi hạ tầng

Network, MinIO, Polaris, catalog commit và worker crash là operational failure:

- Airflow retry theo policy;
- `pipeline_runs.status=failed` với `error_code`;
- không tạo DQ rejection giả;
- không xóa output tốt đã publish trước đó.

## 7. Weather semantics

Weather Bronze giữ một raster slice gồm arrays `cell_indices[]` và `values[]`. Đợt đầu không quarantine từng cell:

- cấu trúc array/grid/unit/window sai: block slice/object;
- một số cell null hoặc ngoài soft range: giữ slice, chuẩn hóa thành null nếu policy cho phép và đánh warning;
- tỷ lệ cell lỗi vượt threshold: reject cả slice;
- giá trị mưa/runoff âm vượt tolerance: reject slice hoặc block object theo policy nguồn.

Silver mới thực hiện de-accumulate ERA5, source priority và basin aggregation; các lỗi này không thuộc Bronze.

## 8. Version và idempotency

Mỗi dataset registry trỏ tới `quality_policy_id`. Policy identity gồm:

```text
policy_name + policy_version + canonical rule config hash
```

`parser_version` thay đổi khi cấu trúc row hoặc cách parse thay đổi. Chỉ tăng `quality_policy_version` khi rule/threshold thay đổi mà output row shape không đổi.

Discovery phải xem xét cả mapping/parser version và quality policy version. Một output chỉ được skip khi có run publish thành công với đúng cả hai version và row/snapshot còn tồn tại.

## 9. Migration dữ liệu hiện có

Không xóa lakehouse và không download lại mặc định.

1. Audit-only current Bronze snapshots bằng policy mới.
2. Phân loại object thành `passed`, `warning`, `requires_rewrite`, `requires_raw_reprocess`, `raw_unavailable`.
3. Rule dùng được cột Bronze sẽ ghi quality/quarantine trực tiếp và rewrite đúng object nếu cần loại row.
4. Rule cần pixel/source member sẽ đọc Raw từ MinIO rồi reprocess; không gọi provider.
5. Raw transient đã hết hạn được đánh `raw_unavailable`, không tự tải lại toàn lịch sử.
6. Migration checkpoint theo `(dataset_id, snapshot_id, quality_policy_id)` để resume.

## 10. Quan sát và governance

- Airflow log/retry giữ operational evidence.
- `meta.pipeline_runs` giữ run status và count accepted/rejected/warning.
- `meta.quality_results` giữ rule-level evidence.
- `quarantine.*` giữ object/row evidence để review.
- `table_snapshot_ref` và `lineage_edges` nối input/output snapshot.
- Dashboard sau này theo dõi reject ratio, warning ratio, fatal object count, freshness và unresolved quarantine.

Quarantine dùng cùng access policy với Raw trong giai đoạn local. Không đưa credential, signed URL hoặc full provider URL có secret vào detail JSON.

## 11. Ngoài phạm vi đợt đầu

- UI review/approve quarantine.
- Tự động sửa mọi geometry.
- Quarantine từng weather grid cell.
- Alert service và dashboard DQ.
- Tự động tải lại Raw đã hết retention.

Các phần này chỉ được bổ sung sau khi Bronze migration và Silver foundation hoạt động ổn định.
