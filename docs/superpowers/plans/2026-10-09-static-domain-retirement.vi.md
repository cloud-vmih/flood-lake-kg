# Static Domain Retirement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xóa package `flashflood_data.static` và gán từng phần code còn dùng cho đúng pipeline hoặc shared package mà không đổi logic dữ liệu.

**Architecture:** AOI trở thành vertical slice `study_area`; geospatial primitive và source-object DTO chuyển sang `shared`; feature kernel thuộc `silver_static_features`. Workflow/QA file-first và CLI tương ứng bị loại bỏ.

**Tech Stack:** Python 3.11, Typer, GeoPandas, Rasterio, PyArrow, PyIceberg, pytest, Ruff.

**Spec:** `docs/superpowers/specs/2026-10-09-static-domain-retirement-design.vi.md`

## Global Constraints

- Không đổi DAG ID, task ID, schema, object ID, MinIO key hoặc dữ liệu đã ingest.
- `build-aoi` phải giữ bootstrap hai nguồn admin trên máy mới.
- Không giữ compatibility package `flashflood_data.static`.
- `storage` không được import `pipelines`.

---

### Task 1: Khóa layout mới bằng contract test

- [x] Thêm test yêu cầu import `study_area`, `shared.geospatial`, `silver_static_features`.
- [x] Thêm test cấm `flashflood_data.static` và cấm `storage → pipelines`.
- [x] Thêm CLI/Make contract cho `build-aoi` và xác nhận test fail.

### Task 2: Di chuyển shared geospatial và source-object contracts

- [x] Chuyển raster/vector primitive sang `shared/geospatial` và cập nhật import/test.
- [x] Chuyển `PublishedObject`, `SourceObjectRow`, `RegisteredBatch` sang `shared/contracts`.
- [x] Cập nhật storage để chỉ phụ thuộc `core`, `catalog`, `shared`; chạy test trọng tâm.

### Task 3: Tạo pipeline `study_area`

- [x] Chuyển AOI geometry và basin selection sang package mới.
- [x] Tạo bootstrap admin + `StudyAreaPipeline.run()` và CLI `build-aoi`.
- [x] Đổi Makefile, cập nhật integration test và xác nhận đủ năm AOI L12.

### Task 4: Gán helper/kernel cho đúng owner

- [x] Chuyển admin crosswalk và WorldPop helper vào static Landing adapters.
- [x] Chuyển event legacy parser vào `flood_events`.
- [x] Chuyển feature kernel vào `silver_static_features` và cập nhật test/config lookup.

### Task 5: Xóa workflow file-first và cập nhật tài liệu

- [x] Xóa `static/workflow`, `static/qa`, mapping/profile file-first và CLI/Make target cũ.
- [x] Xóa test chỉ kiểm workflow cũ; giữ/move test thuật toán còn giá trị.
- [x] Cập nhật README, roadmap, workflow và package README.

### Task 6: Verification

- [x] Chạy Ruff và contract tests.
- [x] Chạy toàn bộ pytest.
- [x] Chạy CLI help, `docker compose config --quiet`, `git diff --check` và quét import cũ.
