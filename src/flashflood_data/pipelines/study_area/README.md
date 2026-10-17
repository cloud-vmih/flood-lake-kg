# Study area pipeline

Dựng năm AOI L12 trước khi chạy các DAG. Pipeline tái sử dụng adapter admin để tạo
`core_aoi` và biên Việt Nam khi máy mới chưa có, sau đó chọn basin giao AOI cùng một hop
thượng nguồn.

Đọc theo thứ tự: `pipeline.py` → `aoi.py` → `hydro.py`. Entrypoint là
`flashflood-data build-aoi --root .` hoặc `make lakehouse-aoi`.
