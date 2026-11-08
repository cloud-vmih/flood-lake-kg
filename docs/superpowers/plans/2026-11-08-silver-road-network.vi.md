# Kế hoạch triển khai Silver Road Network

> **Dành cho người triển khai:** dùng `executing-plans`, làm theo thứ tự file và
> review contract/topology trước khi wire DAG. Checkbox chưa đánh dấu là chưa làm;
> không tự commit hoặc merge branch khác.

**Mục tiêu:** xây graph đường có version, giữ thuộc tính OSM và quan hệ road–basin.

**Kiến trúc:** DAG `silver_road_network` chỉ sở hữu node, edge và road–basin.
Spark/Sedona đọc snapshot OSM/Basin, dựng graph theo policy bảo thủ, chạy DQ và
publish bundle ba bảng. Crossing đọc output này bằng DAG độc lập.

**Công nghệ:** Spark/Sedona, Iceberg, Airflow, Pydantic, shared Spark quality/audit.

**Thiết kế:** [Exposure tách domain](../specs/2026-11-08-silver-exposure-split-design.vi.md).

## Ràng buộc

- Input `bronze.osm_feature_raw`, Meta revision và `silver.dim_basin`; pin snapshot.
- Chọn whole way giao exposure AOI, không cắt đứt line trước dựng graph.
- Output `silver.dim_road_node`, `silver.dim_road_edge`, `silver.road_basin`.
- Bronze đang lọc critical transport, chưa giữ way node-reference sequence; bản
  đầu là graph suy từ geometry có method version, không tuyên bố đủ routing/sơ tán.
- Endpoint/shared vertex cùng level mới được dùng làm junction; không planarize
  mọi giao 2D. `layer`, bridge, tunnel mâu thuẫn → không nối tự động.
- Không lấy ID node OSM từ tọa độ suy đoán. `osm_node_id` chỉ có khi có bằng chứng.
- `oneway=yes/1/true`, `oneway=-1`, `oneway=no` và roundabout xử lý bằng mapping
  cụ thể. Một dòng edge là một segment có hướng from→to chuẩn; edge two-way giữ
  `oneway=false`, consumer graph mới tạo hai cung, không nhân đôi geometry ở Silver.
- `base_travel_time_s` chỉ suy từ speed policy có version; thiếu tốc độ hợp lệ để
  null/warning, không tốc độ ngẫu định. Routing động thuộc Gold.
- Dùng EPSG:4326 để lưu, metric CRS để tính; mốc/độ chính xác snap nằm trong config.
- Road version gồm input revision, graph/tag policy và scope; node/edge ID ổn định
  trong version, không từ partition index, `monotonically_increasing_id` hay run ID.

## Flow và thứ tự đọc file

```text
DAG → airflow.py → runtime.py → pipeline.py
 → Spark spark_job.py → reader.py → normalize.py → topology.py
 → basin_overlay.py → quality.py → publish node/edge/relation → manifest → Meta
```

File domain tạo trong `src/flashflood_data/pipelines/silver_road/`:

| Thứ tự | File | Vai trò | Caller / dependency |
| ---: | --- | --- | --- |
| 1 | `config.py` | `RoadNetworkConfig`, `load_road_config(path)`, class/speed/tag/snap/CRS policy | Runtime, transforms |
| 2 | `models.py` | `RoadNodeRow`, `RoadEdgeRow`, `RoadBasinRow`, `RoadFrames(nodes, edges, relations)` | Transform/job/tests |
| 3 | `reader.py` | `read_road_inputs(spark, request, config)`; chọn authoritative OSM và basin | Spark job; pinned snapshots |
| 4 | `normalize.py` | `normalize_roads(osm, config)`; tags, oneway, grade/evidence, whole geometry | Topology |
| 5 | `topology.py` | `build_road_graph(roads, config)` → node/edge frames | Spark job; geometry/node policy |
| 6 | `basin_overlay.py` | `link_roads_to_basins(edges, basins, config)` → relation frame | Spark job; Sedona metric overlay |
| 7 | `quality.py` | `road_node_rules`, `road_edge_rules`, `road_basin_rules`, FK/graph checks | Spark job; shared quality |
| 8 | `pipeline.py` | Signature, output bundle, build/repair/skip | Runtime |
| 9 | `spark_job.py` | `run(request)`; DQ và publish ba bảng theo version | Shared CLI |
| 10 | `runtime.py`, `airflow.py` | Factory `build_road_runtime(root=None)`, submit/audit và factory DAG | Entry DAG |
| 11 | `__init__.py`, `README.md` | Import nhẹ, ghi rõ graph coverage/limitations | Người đọc |

Tạo `config/pipelines/silver/road_network.yaml`, `config/quality/silver_road_network.yaml`,
`airflow/dags/silver_road_network.py`. Không dùng package `silver_exposure`.

## Công việc 1 — Preflight và contract

**File:** tạo config/model/init, hai YAML;
sửa `src/flashflood_data/storage/iceberg_schemas.py`, `config/shared/metadata.yaml`;
tạo `tests/unit/pipelines/silver_road/test_config.py`,
`tests/contract/storage/test_silver_road_schema.py`.

- [ ] Đối chiếu Bronze policy và fixture geometry/tags thật. Ghi rõ class nào
  đã có/thiếu, node refs chưa có; yêu cầu local roads đầy đủ chuyển sang plan
  Bronze riêng, không âm thầm đọc PBF trong job Silver.
- [ ] Viết test schema gồm `bridge`, `tunnel`, `ford`, `osm_layer` và config
  từ chối snap tolerance âm/zero, speed không dương, mapping grade rỗng.

```python
def test_road_contract_preserves_crossing_evidence():
    from flashflood_data.storage.iceberg_schemas import table_schema
    names = set(table_schema(("silver", "dim_road_edge")).names)
    assert {"bridge", "tunnel", "ford", "osm_layer", "osm_way_id"} <= names
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_road/test_config.py tests/contract/storage/test_silver_road_schema.py -q`; xác nhận fail.
- [ ] Khai báo contract/registry; `ford`/`osm_layer` là bổ sung nullable có version
  contract. Chạy lại pass; review hướng edge và identity trước transform.

## Công việc 2 — Normalize và dựng node/edge

**File:** tạo reader/normalize/topology;
`tests/unit/pipelines/silver_road/test_normalize.py`,
`tests/integration/silver/test_road_topology.py`.

**Giao diện:** `normalize_roads` trả frame có source way/object, canonical direction,
geometry và grade evidence; `build_road_graph` trả `(nodes, edges)` theo models.

- [ ] Fixture bắt buộc: hai đường shared vertex, hai line cắt không shared vertex,
  bridge qua đường layer khác, T-junction không đủ evidence, oneway ngược và
  self-crossing/multipart. Không có raw node ID thì output `osm_node_id=null`.

```python
def test_bridge_and_ground_road_do_not_share_junction(bridge_graph):
    nodes, edges = bridge_graph
    road_a = edges.where("osm_way_id = 101").first()
    road_b = edges.where("osm_way_id = 202").first()
    assert {road_a.from_node_id, road_a.to_node_id}.isdisjoint(
        {road_b.from_node_id, road_b.to_node_id})
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_road/test_normalize.py tests/integration/silver/test_road_topology.py -q`; fixture đặt way IDs như trên, xác nhận fail trước transform.
- [ ] Parse tag một lần; normalize direction trước split, snap có tolerance và
  canonicalization xác định. Candidate spatial buckets chỉ phục vụ tìm vertex,
  không quyết định kết nối thay evidence. ID segment từ source way, endpoint và
  geometry fingerprint để input order không ảnh hưởng. Tính metric length.
- [ ] Chạy test pass, đảo row/partition order vẫn cùng graph. Ghi những junction
  chưa xác minh thành DQ findings; không tạo kết nối để ép graph hết component.

## Công việc 3 — Road–basin, DQ và Spark publish

**File:** tạo basin_overlay/quality/spark_job;
`tests/integration/silver/test_road_overlay.py`,
`tests/unit/pipelines/silver_road/test_quality.py`,
`tests/integration/silver/test_road_publish.py`.

- [ ] Test trước: metric length fixture kiểm tay, edge đi hai basin, endpoint
  reference thiếu, duplicate segment, length không dương, null travel time.
- [ ] Chạy `.venv/bin/pytest tests/integration/silver/test_road_overlay.py tests/unit/pipelines/silver_road/test_quality.py tests/integration/silver/test_road_publish.py -q`; xác nhận fail.
- [ ] Bbox/spatial join rồi exact intersection. `intersection_ratio` theo chiều
  dài edge đầy đủ; tổng phần giao không vượt edge ngoài tolerance khi basin không
  overlap. Không dùng degree làm mét, không coi touch-only là chiều dài dương.
- [ ] DQ unique/FK/geometry trước publish; road cycle bình thường không bị cấm
  như directed river cycle. Chỉ chặn invalid segment/self-reference vô nghĩa theo
  policy. Publish nodes→edges→relations, manifest sau đủ snapshot/quarantine.
- [ ] Test retry sau từng commit và replay không trùng; chạy lại pass.

## Công việc 4 — Runtime/DAG và gate consumer

**File:** tạo pipeline/runtime/airflow/README và DAG entrypoint;
`tests/unit/pipelines/silver_road/test_pipeline.py`,
`tests/contract/infra/test_silver_road_dag.py`.

- [ ] Test trước build/skip/repair, downstream chỉ thấy bundle đã hoàn tất;
  DAG không yêu cầu River Network, không import eager Spark.

```python
def test_road_dag_is_independent_from_river(dag):
    assert dag.dag_id == "silver_road_network"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
```

- [ ] Chạy `.venv/bin/pytest tests/unit/pipelines/silver_road tests/contract/infra/test_silver_road_dag.py -q`; xác nhận fail phần orchestration mới.
- [ ] Wire shared runtime/submit/audit; DAG manual paused, writer pool; log
  input count, rejected count, component count, phase timing và output snapshot.
- [ ] Test pass, smoke bảng tạm; production AOI sau review. Trino đối chiếu
  edge–node FK/version, road–basin length, bridge tags, replay skip. README ghi
  giới hạn critical-road coverage và geometry-derived topology trước Gold routing.
