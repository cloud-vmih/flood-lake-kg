# Pilot crawl keyword tin lũ Sơn La

Ngày chốt: **04/10/2026**. Trạng thái: **code triage đã vào repo, chưa DAG, chưa lịch chạy.**

## 1. Mục tiêu và phạm vi

Mở rộng bằng chứng sự kiện lũ ngoài file Excel `Lu_Son_La_2020_2026.xlsx`
(30 dòng, 2020–14/08/2026). Pilot chỉ làm **triage giữ/loại + gắn địa danh**,
chưa trích số liệu có cấu trúc (số chết, thiệt hại).

## 2. Kết quả đo được (42 URL seed)

| Phiên bản | Giữ | Bài học |
|---|---|---|
| v1 (24 từ, cả trang) | 40/40 | Đúng hết trên tập chọn tay nhưng mù với tin chỉ ghi tên xã |
| v2 (110 từ + auto-loại) | 23/40 | Gazetteer có tác dụng (Bó Sinh, Pá Kìm, Ít Ong bắt được) nhưng rule loại trừ lỗi 17 bài vì dính menu/sidebar |
| v3 khuyến nghị (110 từ + article-scope + match có dấu + exclude làm cờ) | **37/41 tải được** | `Co Mạ` hết nhầm `có mà`; 4 bài lọt do template từng báo |

Ba lỗi đã gặp và cách sửa: server nén gzip mà client không giải nén;
sidebar tin-liên-quan xoay vòng làm kết quả thiếu ổn định; trang Tiền Phong
có 40 thẻ `<article>` giả, VnExpress render bằng JS.

Fetch ổn định 40–41/42 qua nhiều lần chạy. Hai lỗi đã biết mặt: SSL
`anninhthudo.vn` (chuỗi chứng chỉ thiếu) và 503 `vneconomy.vn` (tạm thời,
chạy lại được).

## 3. Quyết định đã chốt (đừng làm ngược khi chưa bàn lại)

1. Quy tắc giữ bài: `flood AND place AND NOT exclude`; exclusion chỉ làm
   **cờ review** (`--no-exclude`), không auto-loại — vì `dự báo thời tiết`,
   `xả lũ`, `giá vàng` đều xuất hiện hợp lệ trong tin lũ thật.
2. Địa danh match trên **chữ có dấu gốc**, từ lũ match không dấu.
3. Gazetteer 75 xã/phường sinh tự động từ
   `dataset/harmonized/admin/admin_commune_2025.geoparquet`, không chép tay.
4. Chưa DAG, chưa lịch: code ở trạng thái thư viện + test. Data crawl không
   commit (tái tạo từ seeds + keywords); chỉ fixture mẫu vào git.
5. `src/` không bao giờ import airflow để DAG sau này chỉ bọc mỏng, không
   viết lại logic.

## 4. Bản đồ file

- Logic: `src/flashflood_data/news/` (`fetcher`, `extractor`, `keywords`,
  `seeds`) — stdlib only, không thêm dependency.
- Config: `config/news_crawl/` (`seeds.csv` 42 URL, `keywords_v2.yaml`,
  `domains.yaml` để khung selector từng báo cho vòng sau).
- Sinh keyword: `tools/news_crawl/build_keywords_v2.py`.
- Chạy tay: `.venv/bin/python tools/news_crawl/crawl.py --limit 5`
  (`--all` để chạy hết; kết quả vào `dataset/news_crawl/outputs/` —
  gitignore theo cả `dataset/`, máy nào cũng có, ngoài state Docker).
- Test offline: `pytest tests/unit/news_crawl -q` (không cần mạng/Docker).

## 5. Việc còn lại (theo thứ tự)

1. Selector boc than bai rieng cho 6 domain chính (VTV, Nhân Dân, Tuổi Trẻ,
   VnExpress, VietnamPlus, Báo Sơn La) vào `domains.yaml`.
2. Trích số liệu có cấu trúc (ngày, xã, số nhà, thiệt hại) → tiền đề cho
   `bronze.historical_event_raw`.
3. DAG `flood_event_landing` manual (chưa lịch), rồi mới tính lịch quét mới.
