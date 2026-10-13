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

Pipeline sử dụng các mô hình dbt staging (`stg_telemetry_a.sql` và `stg_telemetry_b.sql`) để chuẩn hóa hai nguồn về cùng một schema Silver:
1. **Đơn vị vận tốc**: Nguồn A giữ nguyên `speed_mps` (m/s). Nguồn B chia cho 3.6 (`spd_kmh / 3.6`) để quy đổi từ km/h sang m/s.
2. **Kiểu thời gian**: Nguồn A parse chuỗi ISO UTC thành timestamp. Nguồn B chuyển đổi số nguyên epoch millisecond thành timestamp UTC.
3. **Độ lệch đồng hồ (Clock Offset)**: Nguồn B được cấu hình trong `sources.json` bị lệch 1500 ms so với video, pipeline đã tự động trừ 1500 ms (`- interval '1500 millisecond'`) cho tất cả các bản ghi của drive B.

**Bằng chứng cho thấy đã đúng**:
- Mốc thời gian bắt đầu của drive B sau khi chuẩn hóa là `2025-10-08 14:30:00.009000+00:00`, khớp hoàn toàn với mốc bắt đầu của video (`2025-10-08 14:30:00`), độ lệch chỉ 9 ms (đạt bài test tự động `within 0.5 s`). Vận tốc tối đa của hai drive đều hợp lý (drive A: 71 km/h, drive B: 100 km/h).

**Bảng Quarantine (dòng lỗi cách ly)**:
| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

**Vì sao dòng lỗi vào `rejects` thay vì bị xóa**:
- Trong Data Engineering chuyên nghiệp, việc âm thầm xóa dữ liệu lỗi (silent drop) vi phạm nguyên tắc kiểm toán (auditability). Dòng lỗi được chuyển vào `telemetry_rejects` giúp bảo toàn phương trình cân bằng dữ liệu: `dòng sạch + dòng reject = tổng nhận về` (drive A: 2968 + 53 = 3021 dòng; drive B: 3181 + 53 = 3234 dòng). Nhờ đó, kỹ sư vận hành có thể giám sát tỷ lệ lỗi theo thời gian, phát hiện sớm các cảm biến xe bị hỏng hoặc mất xung, đồng thời có thể phân tích nguyên nhân gốc rễ và xử lý lại khi cần.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

1. **Phân biệt `warn` và `error`**:
- Test `max_gap_ms` được cấu hình là `warn` (cảnh báo) vì việc mất tín hiệu telemetry tạm thời trong vài giây có thể xảy ra khi xe đi qua vùng khuất sóng, đường hầm hoặc độ trễ mạng ngắn. Đây là biến động ngoại cảnh cho phép, cảnh báo nhắc nhở kỹ sư nhưng không nên làm dừng toàn bộ pipeline xử lý của cả chuyến xe.
- Ngược lại, test `in_range` (tốc độ trong khoảng 0..70 m/s ~ 252 km/h) bắt buộc là `error` (dừng pipeline) vì các giá trị tốc độ âm hoặc vượt quá giới hạn vật lý là dữ liệu rác nghiêm trọng. Nếu lọt xuống tầng Gold, chúng sẽ làm sai lệch mô hình huấn luyện xe tự lái và gây nguy cơ mất an toàn thực tế.

2. **Kết quả thí nghiệm `break-gate` (`break_gate.json`)**:
- Khi nới luật làm sạch từ 70 m/s lên 1000 m/s, 6 dòng tốc độ dị thường lọt qua bước làm sạch sơ bộ vào Silver.
- Cổng chất lượng dbt test lập tức phát hiện và chặn lại: test `in_range_silver_telemetry_speed_mps__70__0` báo FAIL với đúng 6 dòng lỗi vi phạm (`PASS=5 WARN=1 ERROR=1 SKIP=0`). Sau khi khôi phục luật gốc, hệ thống trở lại bình thường (`PASS=6 WARN=1 ERROR=0 SKIP=0`).

3. **Vai trò của cổng chất lượng**:
- Cổng chất lượng đóng vai trò như một cơ chế phòng thủ chiều sâu (defense-in-depth). Ngay cả khi logic code biến đổi ở thượng nguồn bị sửa sai sót hoặc nới lỏng quá mức, Quality Gate đặt tại ranh giới tầng dữ liệu sẽ tự động chặn đứng dữ liệu xấu, đảm bảo dữ liệu bẩn không bao giờ được phép đi tiếp sang tầng Gold và downstream consumers.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

**Số liệu từ `idempotency.json` (thí nghiệm trên trip-00003)**:
- Số dòng trước khi chạy lại: 1246 dòng, dấu vân tay (fingerprint): `6c7480dfde7e`.
- Số dòng sau 2 lần chạy lại: 1246 dòng, dấu vân tay: `6c7480dfde7e` (giữ nguyên tuyệt đối, không đổi).
- Số dòng nếu ghi kiểu `append` sai lầm (chạy 2 lần cho 1 trip có 60 khung): tăng gấp đôi lên 120 dòng (nhân đôi 2x dữ liệu).

**Vì sao Delta `overwrite` theo partition an toàn hơn xoá rồi ghi lại**:
- Delta Lake đảm bảo tính nguyên tử (Atomicity) theo chuẩn ACID: Thao tác ghi đè theo phân vùng (`replaceWhere = "trip_id = '...'"` ) sẽ ghi các file Parquet mới vào storage trước, sau đó mới cập nhật metadata trong transaction log (`_delta_log`) trong đúng một commit duy nhất.
- Nếu quá trình trích xuất bị sập giữa chừng (crash, mất điện, tràn đĩa), transaction chưa được commit sẽ bị hủy (rollback), toàn bộ dữ liệu cũ của partition đó vẫn nguyên vẹn. Ngược lại, nếu thực hiện "Xóa rồi Ghi lại" (Delete-then-Write): nếu bước xóa đã xong mà bước ghi bị sập giữa chừng, partition đó sẽ bị mất dữ liệu vĩnh viễn (data loss); đồng thời người dùng đang truy vấn đồng thời (concurrent queries) sẽ nhìn thấy bảng bị rỗng hoặc thiếu dữ liệu.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Khác biệt giữa khung đen và khung tối, cách xử lý**:
- Khung hình **đen** (`black`): Do camera mất tín hiệu, che ống kính hoặc hỏng cảm biến (độ sáng trung bình $\le 5$ hoặc toàn đen). Chúng không chứa bất kỳ thông tin ngữ cảnh nào nên pipeline **loại bỏ hoàn toàn** (drive A có 6 khung, drive B có 4 khung).
- Khung hình **tối** (`low_light`): Là cảnh lái xe thực tế trong điều kiện thiếu sáng (ban đêm, chui qua hầm, bóng râm gầm cầu). Mặc dù độ sáng thấp nhưng vẫn chứa thông tin vật thể, làn đường và biển báo. Pipeline **gắn cờ cảnh báo nhưng vẫn giữ lại** (drive B có 61 khung tối được giữ lại, drive A có 4).
- **Đánh giá**: Tôi hoàn toàn đồng ý với thiết kế này. Mô hình xe tự lái cần dữ liệu ánh sáng yếu để học cách thích nghi với các điều kiện môi trường bất lợi trong thế giới thực, chỉ loại bỏ các khung hỏng vật lý vô giá trị.

**Bảng thống kê ngưỡng dHash từ `threshold.json`**:
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**Lựa chọn ngưỡng và sự phụ thuộc vào vận tốc xe**:
- **Chọn ngưỡng 2** (ngưỡng pipeline sử dụng):
  + Ngưỡng 0 quá nghiêm ngặt, chỉ bắt được ảnh giống nhau 100% từng pixel băm, bỏ sót các khung hình thực tế đứng yên nhưng bị nhiễu hạt cảm biến (sensor noise) hoặc ánh sáng thay đổi vi lượng.
  + Ngưỡng 6 lại quá lỏng lẻo: loại bỏ quá nhiều khung (363 khung ở A, 244 khung ở B, tức hơn một nửa dữ liệu), dễ xóa nhầm cả các khung hình có phương tiện di chuyển chậm hoặc góc lái đang thay đổi.
  + Ngưỡng 2 vừa đủ để loại bỏ các đoạn đứng hình (như camera đứng hình 6 giây ở A) mà vẫn bảo tồn thông tin chuyển động thực tế.
- **Phụ thuộc vào tốc độ xe**: Xe di chuyển càng chậm thì sự thay đổi hình ảnh giữa 2 khung hình liên tiếp càng nhỏ (khoảng cách bit dHash thấp $\le 2$), dẫn tới số lượng khung trùng càng nhiều (ở drive A tốc độ trung bình thấp 34 km/h có tới 149 khung trùng ở ngưỡng 2; drive B chạy nhanh 74 km/h chỉ có 53 khung trùng). Khi xe chạy tốc độ cao trên cao tốc, cảnh quan thay đổi nhanh nên dHash cách biệt lớn, số khung trùng sẽ rất thấp.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

**Số liệu từ `leakage.json` (1019 khung hình hợp lệ thuộc 26 trip)**:
- **Chia THEO KHUNG HÌNH (ngẫu nhiên 70/15/15)**: Toàn bộ **26/26 trip bị chia cắt** (`per_frame_split_trips_straddling = 26`), và **cả 26 trip đều có khung hình xuất hiện đồng thời ở cả train lẫn test** (`per_frame_split_trips_in_both_train_and_test = 26`).
- **Chia THEO TRIP (Gold của pipeline)**: **0 trip bị chia cắt** (`per_trip_split_trips_straddling = 0`), mỗi trip thuộc về duy nhất một split.

**Vì sao điểm kiểm thử đẹp giả tạo (Data Leakage)**:
- Trong dữ liệu video xe tự lái (10 fps / 2 fps), các khung hình liên tiếp trong cùng một chuyến đi chỉ cách nhau từ 0.1 đến 0.5 giây. Chúng có bối cảnh cảnh quan, góc camera, thời tiết, ánh sáng và mặt đường gần như giống hệt nhau.
- Nếu chia ngẫu nhiên theo khung hình, mô hình ML đã "học thuộc lòng" bối cảnh môi trường ở tập train. Khi đánh giá trên tập test (vốn chứa các khung hình chỉ cách khung train một khoảng thời gian cực ngắn), mô hình dễ dàng đạt độ chính xác rất cao vì đã thấy đề thi trước, chứ không phải vì có khả năng khái quát hóa (generalization). Khi triển khai xe thật trên cung đường mới lạ, mô hình sẽ thất bại hoàn toàn.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

1. **Vì sao dùng băm nội dung (Content Hash)**:
- Đảm bảo tính xác thực và tính tái lập (Reproducibility): Nếu đánh số tăng dần (v1, v2) hoặc dùng timestamp, mỗi lần chạy pipeline sẽ tạo ra một phiên bản mới dù dữ liệu và logic không thay đổi, gây trùng lặp và khó quản lý. Băm nội dung đảm bảo tính Idempotent: cùng một tập khung hình và siêu dữ liệu đầu vào sẽ luôn cho ra đúng một mã băm duy nhất (`e6294fd425b2`). Hai lần chạy liên tiếp sẽ cho ra kết quả mã version giống hệt nhau.
2. **Vì sao khác bạn bè dù cùng dữ liệu**:
- Trong mô hình `gold_dataset_frames.sql`, danh sách trip được sắp xếp ngẫu nhiên có muối (salt) bằng công thức:
  `order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`
- Vì `LAB_STUDENT_ID` là MSSV của tôi (`2A202602552`), chuỗi băm MD5 tạo ra thứ tự sắp xếp trip riêng biệt, dẫn đến phân bổ các trip vào các tập train, val, test khác với các sinh viên khác. Do nội dung các tập split khác nhau nên mã băm `dataset_version` của tôi là duy nhất.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

**Dataset này được tạo từ nội dung `lineage` trong `manifest.json`**:
- Bảng Delta upstream:
  + `bronze.raw_telemetry_a` (version 0)
  + `bronze.raw_telemetry_b` (version 0)
  + `bronze.trips_raw` (version 0)
  + `bronze.video_files` (version 1)
  + `silver.frames` (version 27)
  + `silver.telemetry` (version 0)
  + `silver.frames_flagged` (version 0)
  + `silver.frame_telemetry` (version 0)
- Nguồn video gốc theo SHA-256:
  + `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  + `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`

**Thông tin giúp tái tạo lại đúng dataset sau 3 tháng**:
1. Hai file video gốc có mã băm SHA-256 trùng khớp hoàn toàn với mã trên.
2. Git commit hash của mã nguồn pipeline (chứa toàn bộ logic dbt SQL, các luật lọc chất lượng: loại bỏ black, blurry, near_dup, sync_ok).
3. Giá trị `LAB_STUDENT_ID = 2A202602552` dùng làm salt để chia split.
4. Phiên bản chính xác của các bảng Delta ở trên (sử dụng Delta Time Travel `VERSION AS OF` tương ứng).

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
