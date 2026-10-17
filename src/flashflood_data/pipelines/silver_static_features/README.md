# Silver static feature kernels

Các hàm thuần để tổng hợp DEM, SoilGrids, WorldCover và WorldPop theo basin. Package này chưa có
DAG; `pipeline.py`, `runtime.py` và `airflow.py` sẽ được bổ sung khi triển khai Bronze → Silver.
Code ở đây chưa đọc các snapshot Bronze/Meta và chưa publish Iceberg. Kernel terrain cũ vẫn còn
giữ một DEM metric trong `dataset/derived`; hành vi này phải được thay bằng publish MinIO/Meta khi
triển khai DAG lakehouse.

Luồng ETL, task graph, contract input/output và trạng thái từng file được mô tả tại
[`docs/silver_basin_static_features_etl.md`](../../../../docs/silver_basin_static_features_etl.md).
