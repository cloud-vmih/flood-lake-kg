# Flood event pipeline

Package này là owner của dữ liệu sự kiện lũ và hiện mới chứa code có thể tái sử dụng cho pipeline
tương lai:

- `bronze.py`: parse Excel/CSV thành record Bronze source-faithful.
- `legacy_input.py`: đọc workbook bằng chứng cũ và resolve địa giới theo thời gian hiệu lực.

Hai static DAG không import package này và không còn xử lý flood event. Các file `airflow.py`,
`runtime.py`, Landing service và Bronze writer sẽ được bổ sung khi triển khai milestone P2 trong
`docs/implementation_priority_todo.md`.
