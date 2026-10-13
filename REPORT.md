# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Mai Xuân Phúc` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602552` |
| GitHub | `Wrxhard` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-03365` |

## Kết quả cuối

- `dataset_version` của bạn: `e6294fd425b2`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `train: 707, val: 130, test: 182 (tổng 1019)`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt rõ rệt giữa telemetry nguồn A và nguồn B quan sát được ở tầng Bronze:
1. **Timestamp**: Nguồn A dùng cột `ts_utc` định dạng chuỗi chuẩn ISO 8601 UTC (`2025-10-07T08:00:00.014Z`), trong khi Nguồn B dùng cột `timestamp_ms` dạng số nguyên Unix epoch milliseconds (`1759933801509`).
2. **Vận tốc (Speed)**: Nguồn A lưu ở cột `speed_mps` với đơn vị mét trên giây (m/s), trong khi Nguồn B lưu ở cột `spd_kmh` với đơn vị kilômét trên giờ (km/h).
3. **Bàn đạp & Góc lái**: Nguồn A đặt tên là `gas`, `brake` (giá trị float từ 0 đến 1) và `steer`; Nguồn B đặt tên là `throttle_pct`, `brake_pct` (đơn vị phần trăm %) và `steering`.
4. **Tọa độ, độ cao & Độ lệch đồng hồ**: Nguồn A dùng tên ngắn gọn `lat`, `lon`, `alt_m`, `heading_deg` và có độ lệch đồng hồ bằng 0 (`telemetry_clock_offset_ms = 0`); Nguồn B dùng `latitude`, `longitude`, `altitude`, `hdg` và bị lệch đồng hồ 1500 ms so với video (`telemetry_clock_offset_ms = 1500`).

Pipeline giữ mỗi nguồn một bảng Bronze riêng và lưu tất cả các cột dưới dạng chuỗi (text/string) vì:
- **Ngăn ngừa lỗi Schema Drift & Bảo toàn nguyên bản (Raw Fidelity)**: Nguồn A và B đến từ hai thiết bị/cảm biến với cấu trúc và đơn vị khác nhau. Ép kiểu ngay từ đầu sẽ khiến hệ thống dễ bị crash khi gặp dữ liệu dị thường (chuỗi rỗng, format lạ, null) hoặc âm thầm làm mất độ chính xác ban đầu.
- **Tính bất biến và khả năng Replay (Idempotency)**: Bronze đóng vai trò là "Single Source of Truth" bất biến (append-only). Giữ nguyên dạng text cho phép toàn bộ logic làm sạch, chuẩn hóa và ép kiểu ở Silver có thể sửa đổi và chạy lại bất cứ lúc nào từ dữ liệu gốc mà không cần thu thập lại dữ liệu từ xe.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

(trả lời ở đây)

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

(trả lời ở đây)

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
