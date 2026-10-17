# Kế hoạch triển khai Flood News Landing

> **Dành cho agent triển khai:** KỸ NĂNG BẮT BUỘC: dùng `subagent-driven-development` (khuyến nghị) hoặc `executing-plans` để thực hiện lần lượt từng công việc. Các bước dùng checkbox (`- [ ]`) để theo dõi.

**Mục tiêu:** Chuyển pilot tải bài báo từ script local thành DAG Landing idempotent, lưu HTTP body bất biến trong MinIO Raw và đăng ký đầy đủ Meta mà chưa bóc text hoặc event.

**Kiến trúc:** Giữ `flashflood_data.news` là package thuần cho URL/fetch/robots; vertical slice `pipelines/flood_news` sở hữu toàn bộ Landing và Bronze của tin lũ. DAG chỉ truyền metadata nhỏ, mapped fetch ghi attempt spool, một writer task commit Raw inventory/lifecycle/audit theo batch.

**Công nghệ:** Python 3.11, Airflow 3 TaskFlow, Pydantic 2, urllib/HTTP primitives hiện có, PyArrow S3, PyIceberg/Polaris, MinIO, pytest.

**Spec:** `docs/superpowers/specs/2026-10-09-flood-news-to-events-design.vi.md`

## Ràng buộc chung

- `source_id` của nguồn web là `flood_news_web`; không đưa lại vào `static_source_landing`.
- Raw giữ response body sau transport content decoding và trước charset decode/HTML parse.
- Cùng canonical article + cùng payload checksum phải reuse; content đổi tạo object mới, không overwrite.
- Không lưu cookie, authorization header hoặc secret trong manifest/log.
- Không bypass robots, login, paywall hoặc anti-bot.
- DAG ban đầu `schedule=None`, `catchup=False`, `max_active_runs=1`.
- Fetch lỗi mạng/HTTP là operational failure; không tạo Raw object giả.
- Mapped fetch không commit trực tiếp đồng thời vào cùng bảng Iceberg.
- DQ/quarantine dùng contract trong `2026-10-05-layered-data-quality-quarantine.vi.md` nếu P1 đã triển khai; plan này không tạo framework DQ thứ hai.
- Không sửa/xóa các file tài liệu đang bị đánh dấu deleted trong working tree.

---

## Bản đồ file và trách nhiệm

```text
config/pipelines/flood_news/landing.yaml
config/news_crawl/domains.yaml
config/news_crawl/seeds.csv
config/shared/metadata.yaml

src/flashflood_data/news/
  fetcher.py                 HTTP body/headers, retry, conditional request
  robots.py                  robots policy có cache và version
  seeds.py                   canonical URL và seed loading

src/flashflood_data/pipelines/flood_news/
  __init__.py
  README.md                 nhiệm vụ, luồng và thứ tự đọc
  models.py                  DTO dùng qua task boundaries
  config.py                  load/validate policy không chứa secret
  landing_plan.py            candidates - committed inventory = fetch plan
  landing_spool.py           attempt/payload metadata staging theo mapped task
  landing.py                 publish Raw + manifest + Meta
  runtime.py                 production dependencies cho cả hai DAG
  airflow.py                 khai báo DAG; wrapper chỉ import DAG

airflow/dags/flood_news_landing.py

tests/unit/news_crawl/
  test_fetcher.py
  test_robots.py
tests/unit/pipelines/flood_news/
  test_config.py
  test_landing_plan.py
  test_landing_spool.py
  test_landing.py
tests/contract/infra/test_flood_news_dags.py
tests/integration/news/test_news_landing.py
```

## Công việc 1: Chốt config, DTO và article identity

**Các file:**

- Tạo: `config/pipelines/flood_news/landing.yaml`
- Tạo: `config/shared/metadata.yaml`
- Tạo: `src/flashflood_data/pipelines/flood_news/__init__.py`
- Tạo: `src/flashflood_data/pipelines/flood_news/models.py`
- Tạo: `src/flashflood_data/pipelines/flood_news/config.py`
- Tạo: `tests/unit/pipelines/flood_news/test_config.py`
- Sửa: `tests/contract/storage/test_meta_bronze_schemas.py`

**Giao diện:**

- `load_news_landing_config(path: Path) -> NewsLandingConfig`.
- `canonical_article_key(source_id: str, publisher_id: str, canonical_url: str) -> str`.
- DTO: `NewsCandidate`, `PlannedNewsFetch`, `FetchedNewsPayload`, `PublishedNewsObject`.

- [ ] **Bước 1: Viết test fail cho config và identity**

```python
def test_article_key_is_stable_after_tracking_params_are_removed():
    a = canonical_article_key("flood_news_web", "bao-son-la", canonicalize_url(URL_A))
    b = canonical_article_key("flood_news_web", "bao-son-la", canonicalize_url(URL_B))
    assert a == b


def test_config_has_no_secret_values():
    config = load_news_landing_config(CONFIG)
    assert config.source_id == "flood_news_web"
    assert config.schedule is None
    assert config.user_agent_env == "NEWS_CRAWL_USER_AGENT"
```

`URL_A` và `URL_B` chỉ khác tracking query. Thêm test reject config có `password`, `cookie`, `token` hoặc `authorization`.

- [ ] **Bước 2: Chạy test để xác nhận fail**

Chạy:

```bash
pytest tests/unit/pipelines/flood_news/test_config.py tests/contract/storage/test_meta_bronze_schemas.py -q
```

Mong đợi: FAIL vì config/models chưa tồn tại và registry chưa có `flood_news_web`.

- [ ] **Bước 3: Triển khai model và config tối thiểu**

`landing.yaml` phải chứa:

```yaml
contract_version: "1.0"
source_id: flood_news_web
source_version: http-news-v1
schedule: null
seeds_path: config/news_crawl/seeds.csv
domains_path: config/news_crawl/domains.yaml
refresh_after_hours: 168
max_urls_per_run: 100
per_host_min_interval_seconds: 2
max_body_size_bytes: 8388608
user_agent_env: NEWS_CRAWL_USER_AGENT
```

`NewsCandidate` giữ seed metadata; `PlannedNewsFetch` thêm `article_key`, `asset_id`, conditional headers và request fingerprint; `FetchedNewsPayload` giữ `body_path`, response metadata và checksum, không đặt raw bytes vào XCom.

- [ ] **Bước 4: Chạy test trọng tâm**

```bash
pytest tests/unit/pipelines/flood_news/test_config.py tests/unit/news_crawl/test_seeds.py tests/contract/storage/test_meta_bronze_schemas.py -q
```

Mong đợi: PASS.

- [ ] **Bước 5: Commit contract Landing**

```bash
git add config/pipelines/flood_news/landing.yaml config/shared/metadata.yaml src/flashflood_data/pipelines/flood_news tests/unit/pipelines/flood_news/test_config.py tests/contract/storage/test_meta_bronze_schemas.py
git commit -m "feat(news): define flood news landing contract"
```

## Công việc 2: Nâng fetcher để giữ body, headers và tuân thủ host policy

**Các file:**

- Sửa: `src/flashflood_data/news/fetcher.py`
- Tạo: `src/flashflood_data/news/robots.py`
- Tạo: `tests/unit/news_crawl/test_fetcher.py`
- Tạo: `tests/unit/news_crawl/test_robots.py`

**Giao diện:**

```python
@dataclass(frozen=True)
class HttpPayload:
    status_code: int
    requested_url: str
    final_url: str
    body: bytes
    content_type: str | None
    charset: str | None
    etag: str | None
    last_modified: str | None
    retrieved_at: datetime
    elapsed_seconds: float


class NewsHttpFetcher:
    def fetch(self, request: PlannedNewsFetch) -> HttpPayload | NotModified: ...
```

- [ ] **Bước 1: Viết test fail cho HTTP behavior**

Test bằng local fake opener/server, không gọi internet:

```python
def test_gzip_is_transport_decoded_before_payload_checksum(): ...
def test_conditional_headers_return_not_modified_on_304(): ...
def test_body_over_limit_is_rejected_without_publication(): ...
def test_rate_limit_is_tracked_per_host_when_hosts_are_interleaved(): ...
def test_retry_after_is_honoured_for_429(): ...
def test_robots_denial_prevents_http_article_request(): ...
```

- [ ] **Bước 2: Chạy test để xác nhận hành vi cũ fail**

```bash
pytest tests/unit/news_crawl/test_fetcher.py tests/unit/news_crawl/test_robots.py -q
```

Mong đợi: FAIL vì `FetchResult` cũ chỉ giữ decoded HTML và limiter chỉ nhớ host gần nhất.

- [ ] **Bước 3: Triển khai fetcher mới và compatibility wrapper**

- Lưu body bytes sau giải nén `Content-Encoding`.
- Giới hạn size trong lúc stream, không đọc vô hạn rồi mới kiểm tra.
- Giữ headers allowlist; bỏ cookie/auth.
- Rate limiter dùng map `host -> last_request_monotonic`.
- Robots cache theo `(scheme, host, user_agent_policy_version)`.
- Giữ hàm `fetch()`/`PoliteFetcher.get()` compatibility cho pilot CLI nhưng implement qua primitive mới.

- [ ] **Bước 4: Chạy test fetch và regression pilot**

```bash
pytest tests/unit/news_crawl/test_fetcher.py tests/unit/news_crawl/test_robots.py tests/unit/news_crawl/test_extractor.py tests/unit/news_crawl/test_keywords.py -q
```

Mong đợi: PASS và không gọi mạng.

- [ ] **Bước 5: Commit HTTP acquisition**

```bash
git add src/flashflood_data/news/fetcher.py src/flashflood_data/news/robots.py tests/unit/news_crawl/test_fetcher.py tests/unit/news_crawl/test_robots.py
git commit -m "feat(news): preserve verified HTTP article payloads"
```

## Công việc 3: Lập kế hoạch incremental và spool mapped fetch

**Các file:**

- Tạo: `src/flashflood_data/pipelines/flood_news/landing_plan.py`
- Tạo: `src/flashflood_data/pipelines/flood_news/landing_spool.py`
- Tạo: `tests/unit/pipelines/flood_news/test_landing_plan.py`
- Tạo: `tests/unit/pipelines/flood_news/test_landing_spool.py`

**Giao diện:**

```python
def plan_news_fetches(
    candidates: Sequence[NewsCandidate],
    inventory: Sequence[SourceObjectRow],
    *,
    now: datetime,
    refresh_after: timedelta,
    limit: int,
) -> NewsFetchPlan: ...


class NewsFetchSpool:
    def write_result(self, run_id: str, map_index: int, result: FetchTaskResult) -> Path: ...
    def read_run(self, run_id: str) -> tuple[FetchTaskResult, ...]: ...
```

- [ ] **Bước 1: Viết test fail cho missing/revision plan**

```python
def test_new_url_is_planned(): ...
def test_recent_committed_article_is_skipped(): ...
def test_stale_article_uses_etag_and_last_modified(): ...
def test_same_canonical_url_is_planned_once(): ...
def test_spool_retry_replaces_same_map_attempt_without_duplicate(): ...
```

- [ ] **Bước 2: Chạy test để xác nhận fail**

```bash
pytest tests/unit/pipelines/flood_news/test_landing_plan.py tests/unit/pipelines/flood_news/test_landing_spool.py -q
```

- [ ] **Bước 3: Triển khai pure planner và atomic spool**

Planner không gọi mạng. Nó đọc latest inventory/provider metadata để tạo conditional request. `304` là outcome hợp lệ và không publish object; URL lỗi không làm cursor giả vì source này chưa dùng watermark thời gian.

Spool nằm dưới staging run directory, dùng `atomic_target` và không ghi Iceberg từ mapped fetch task.

- [ ] **Bước 4: Chạy test planner/spool**

```bash
pytest tests/unit/pipelines/flood_news/test_landing_plan.py tests/unit/pipelines/flood_news/test_landing_spool.py -q
```

Mong đợi: PASS.

- [ ] **Bước 5: Commit planner**

```bash
git add src/flashflood_data/pipelines/flood_news/landing_plan.py src/flashflood_data/pipelines/flood_news/landing_spool.py tests/unit/pipelines/flood_news/test_landing_plan.py tests/unit/pipelines/flood_news/test_landing_spool.py
git commit -m "feat(news): plan incremental article fetches"
```

## Công việc 4: Publish Raw bất biến và Meta theo batch

**Các file:**

- Tạo: `src/flashflood_data/pipelines/flood_news/landing.py`
- Tạo: `src/flashflood_data/pipelines/flood_news/runtime.py`
- Tạo: `tests/unit/pipelines/flood_news/test_landing.py`
- Tạo: `tests/integration/news/test_news_landing.py`

**Giao diện:**

```python
class FloodNewsLandingService:
    def plan(self, run_id: str, now: datetime) -> NewsFetchPlan: ...
    def fetch_to_spool(self, planned: PlannedNewsFetch, run_id: str, map_index: int) -> dict: ...
    def publish_spooled(self, run_id: str) -> NewsLandingResult: ...
    def cleanup(self, run_id: str) -> CleanupResult: ...


def build_flood_news_landing_service(root: Path | None = None) -> FloodNewsLandingService: ...
```

- [ ] **Bước 1: Viết test fail cho immutable publication**

```python
def test_same_article_body_reuses_raw_object(): ...
def test_changed_article_body_creates_new_object_and_keeps_old(): ...
def test_publish_registers_manifest_checksum_and_http_metadata(): ...
def test_failed_fetch_records_attempt_without_source_object(): ...
def test_batch_retry_does_not_duplicate_inventory_rows(): ...
```

- [ ] **Bước 2: Chạy test để xác nhận fail**

```bash
pytest tests/unit/pipelines/flood_news/test_landing.py tests/integration/news/test_news_landing.py -q
```

- [ ] **Bước 3: Triển khai service qua storage primitives hiện có**

- Dùng `ObjectPublisher` và `SourceObjectInventory`, không viết S3 client mới.
- `asset_id = news-<article_key>`.
- Selection chứa `publisher_id`, `canonical_url`, `acquisition_method` và policy version.
- Raw key chứa publisher/article/date/checksum.
- Manifest lưu `etag`, `last_modified`, final URL, content type/charset, robots decision/version và seed metadata đã sanitize.
- Commit inventory/attempts theo batch qua writer task duy nhất.
- Raw dùng retention durable trong milestone đầu.

- [ ] **Bước 4: Chạy unit/integration service**

```bash
pytest tests/unit/pipelines/flood_news/test_landing.py tests/integration/news/test_news_landing.py -q
```

Mong đợi: PASS.

- [ ] **Bước 5: Commit Landing service**

```bash
git add src/flashflood_data/pipelines/flood_news/landing.py src/flashflood_data/pipelines/flood_news/runtime.py tests/unit/pipelines/flood_news/test_landing.py tests/integration/news/test_news_landing.py
git commit -m "feat(news): publish article responses to Raw"
```

## Công việc 5: Airflow DAG, CLI compatibility và runbook

**Các file:**

- Tạo: `airflow/dags/flood_news_landing.py`
- Tạo: `tests/contract/infra/test_flood_news_dags.py`
- Sửa: `tools/news_crawl/crawl.py`
- Sửa: `docs/news-crawl-pilot.md`
- Sửa: `docs/pipeline_architecture_and_roadmap.md`
- Sửa: `README.md`

**Task graph:**

```text
register_news_registry
  -> plan_article_fetches
  -> fetch_articles.expand
  -> commit_fetch_attempts
  -> publish_raw_batch
  -> audit_landing
  -> cleanup_staging
```

- [ ] **Bước 1: Viết contract test fail cho DAG**

```python
def test_flood_news_landing_is_manual_and_single_run(dag_bag):
    dag = dag_bag.get_dag("flood_news_landing")
    assert dag.schedule is None
    assert dag.max_active_runs == 1


def test_mapped_fetch_does_not_use_raw_writer_pool(dag_bag): ...
def test_only_publish_and_meta_tasks_use_news_raw_writer_pool(dag_bag): ...
```

- [ ] **Bước 2: Chạy test để xác nhận DAG chưa có**

```bash
pytest tests/contract/infra/test_flood_news_dags.py -q
```

- [ ] **Bước 3: Triển khai DAG mỏng và giữ CLI pilot**

DAG chỉ import factory/service DTO. CLI tiếp tục xuất JSON/CSV có text cho review thủ công nhưng ghi rõ đây không phải lakehouse inventory. Không import Airflow vào `src/flashflood_data/news`.

- [ ] **Bước 4: Chạy toàn bộ suite news và DAG parse**

```bash
pytest tests/unit/news_crawl tests/integration/news/test_news_landing.py tests/contract/infra/test_flood_news_dags.py -q
```

Mong đợi: PASS.

- [ ] **Bước 5: Chạy pilot lakehouse 5 URL**

Trigger với conf giới hạn 5 URL. Kiểm tra bằng Trino/MinIO:

- năm Raw payload + manifest hoặc số nhỏ hơn nếu `304`/reuse;
- checksum khớp `meta.source_objects`;
- không có text parsing ở Landing;
- rerun không tạo inventory row trùng.

- [ ] **Bước 6: Commit DAG và tài liệu**

```bash
git add airflow/dags/flood_news_landing.py tests/contract/infra/test_flood_news_dags.py tools/news_crawl/crawl.py docs/news-crawl-pilot.md docs/pipeline_architecture_and_roadmap.md README.md
git commit -m "feat(news): add flood news landing DAG"
```

## Điểm kiểm tra trước plan kế tiếp

- Raw payload đọc lại được thành HTML đúng charset.
- `object_id`, checksum, canonical URL và HTTP validators có provenance đầy đủ.
- Rerun không tải/publish lại bài chưa đến hạn refresh.
- HTTP revision mới tồn tại song song bản cũ.
- Bronze plan có thể discover Raw object chỉ bằng `meta.source_objects`.
