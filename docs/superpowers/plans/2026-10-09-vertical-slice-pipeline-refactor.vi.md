# Vertical Slice Pipeline Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Gom code runtime theo từng pipeline, làm mỏng Airflow DAG và cập nhật mọi import/config/documentation mà không đổi contract dữ liệu.

**Architecture:** Ba vertical slice `static_landing`, `static_bronze` và `weather_ingest` sở hữu toàn bộ orchestration tương ứng. Metadata và quality chuyển vào `shared`; storage/catalog/core tiếp tục là hạ tầng dùng chung.

**Tech Stack:** Python 3.11, Airflow 3 TaskFlow, Typer, PyIceberg, PyArrow, pytest, Ruff.

**Spec:** `docs/superpowers/specs/2026-10-09-vertical-slice-pipeline-architecture.vi.md`

## Global Constraints

- Giữ nguyên DAG ID, task ID, schema, table name, object ID và MinIO key.
- Không chạy lại ingest do refactor module path.
- Không giữ compatibility namespace `flashflood_data.orchestration` sau khi caller đã chuyển.
- Không thay đổi nội dung cấu hình khi di chuyển YAML.
- Không sửa hoặc khôi phục các tài liệu đang bị xóa sẵn trong working tree.

---

### Task 1: Khóa contract kiến trúc mới

**Files:**
- Create: `tests/contract/test_pipeline_layout.py`

**Interfaces:**
- Consumes: module tree hiện tại và sáu DAG wrapper.
- Produces: contract bắt buộc import public API mới, cấm namespace orchestration cũ và giới hạn DAG wrapper 12 dòng.

- [x] Viết test import các package `flashflood_data.pipelines.*` và `flashflood_data.shared.metadata`.
- [x] Viết test quét AST để DAG wrapper chỉ import pipeline tương ứng.
- [x] Viết test quét source/DAG không còn import `flashflood_data.orchestration`.
- [x] Chạy `pytest tests/contract/test_pipeline_layout.py -q` và xác nhận FAIL vì package mới chưa tồn tại.

### Task 2: Di chuyển shared metadata/quality và ba pipeline package

**Files:**
- Move: `src/flashflood_data/orchestration/meta/` → `src/flashflood_data/shared/metadata/`
- Move: `src/flashflood_data/orchestration/quality.py` → `src/flashflood_data/shared/quality.py`
- Move: `src/flashflood_data/orchestration/landing/` → `src/flashflood_data/pipelines/static_landing/`
- Move: `src/flashflood_data/orchestration/bronze/` → `src/flashflood_data/pipelines/static_bronze/`
- Move: `src/flashflood_data/orchestration/weather/` → `src/flashflood_data/pipelines/weather_ingest/`
- Move: `src/flashflood_data/static/sources/` → `src/flashflood_data/pipelines/static_landing/adapters/`
- Modify: all imports in `src/`, `tests/`, `tools/`, `airflow/`.

**Interfaces:**
- Produces: public API liệt kê trong spec; runtime behavior giữ nguyên.

- [x] Dùng `git mv` để giữ lịch sử file.
- [x] Đổi `service.py` thành `pipeline.py`, `factory.py` thành `runtime.py`, `meta_audit.py` thành `audit.py`, và Landing `sources.py` thành `acquisition.py`.
- [x] Thay toàn bộ import theo mapping mới, bao gồm lazy adapter registry string.
- [x] Cập nhật CLI để compose static Landing qua runtime package thay vì định nghĩa factory trong `cli/app.py`.
- [x] Chạy unit tests theo từng package cho đến khi pass.

### Task 3: Làm mỏng DAG và gom config runtime

**Files:**
- Create: `src/flashflood_data/pipelines/static_landing/airflow.py`
- Create: `src/flashflood_data/pipelines/static_bronze/airflow.py`
- Rename: `src/flashflood_data/pipelines/weather_ingest/airflow_factory.py` → `airflow.py`
- Modify: `airflow/dags/*.py`
- Move: Landing/Bronze/weather/meta YAML sang `config/pipelines/` và `config/shared/`.

**Interfaces:**
- DAG wrappers import object static DAG hoặc `build_project_weather_dag` duy nhất.

- [x] Di chuyển nguyên task implementation static vào package `airflow.py` để giữ task ID.
- [x] Thu gọn mỗi DAG wrapper còn tối đa 12 dòng.
- [x] Cập nhật config path trong runtime/DAG/test.
- [x] Chạy contract DAG tests và layout tests.

### Task 4: Thêm bản đồ đọc code và cập nhật tài liệu hiện hành

**Files:**
- Create: `src/flashflood_data/pipelines/{static_landing,static_bronze,weather_ingest}/README.md`
- Modify: `README.md`
- Modify: `docs/pipeline_architecture_and_roadmap.md`
- Modify: `docs/raw_to_bronze_workflow.md`
- Modify: `docs/implementation_priority_todo.md`
- Modify: plans/specs hiện hành có đường dẫn namespace cũ.

**Interfaces:**
- Produces: một thứ tự đọc cụ thể cho từng pipeline và bản đồ path chính xác.

- [x] Ghi nhiệm vụ, input/output, task flow, config và thứ tự đọc vào từng package README.
- [x] Thay đường dẫn code/config cũ trong README, roadmap, workflow và plan ngày 05/10–09/10.
- [x] Quét repository để không còn chỉ dẫn hiện hành tới namespace cũ.

### Task 5: Verification và cleanup

**Files:**
- Delete: `src/flashflood_data/orchestration/` sau khi trống.
- Modify: `.gitignore` nếu test sinh artifact mới.

**Interfaces:**
- Produces: source tree chỉ còn namespace mới.

- [x] Chạy `ruff check src tests airflow`.
- [x] Chạy toàn bộ test suite khả dụng; báo riêng failure do file tài liệu bị xóa sẵn nếu còn.
- [x] Import sáu DAG bằng contract tests và chạy `flashflood-data --help`.
- [x] Chạy `git diff --check` và quét import/path cũ.
