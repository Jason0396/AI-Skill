# Rubric chi tiết – [DE-LV2] Final Project (10đ) + Bonus (1đ)

Nguồn: `Slide bài giảng/tieu_chi.xlsx` (sheet "Phiếu điểm", "Bonus"). Đề bài: `L12/FINAL PROJECT_Đề bài.docx`.
Kiến thức đối chiếu: L10 (ETL), L11 (Airflow/Docker/DAG), L12 (tư duy pipeline, lỗi phổ biến, debug).

## Công thức điểm (lấy nguyên từ file Excel, KHÔNG tự đổi)

| # | Tiêu chí | Tối đa | Trung bình | Khá | Tốt |
|---|---|---|---|---|---|
| 1 | Pipeline tự động hoá | 3 | 1.5 | 2 | 3 |
| 2 | Data Cleaning & Validation | 2 | 1 | 1.5 | 2 |
| 3 | Logging & Monitoring | 2 | 1 | 1.5 | 2 |
| 4 | Kiểm tra kết quả | 1 | 0.5 | 0.8 | 1 |
| 5 | Hồ sơ nộp project | 1 | 0.5 | 0.8 | 1 |
| 6 | Tài liệu & Git | 1 | 0.5 | 0.7 | 1 |

Mỗi tiêu chí chọn đúng 1 mức. Không đạt mức Trung bình = **0 điểm** (file Excel không có ô "Yếu"; xem mục "Lỗ hổng của thang điểm").
Tổng = cộng 6 tiêu chí (tối đa 10).

---

## Cách phân mức từng tiêu chí

Quy tắc chung: mức **Tốt** = đạt TẤT CẢ ý "Bắt buộc" + ≥ 1 ý "Nâng cao". **Khá** = đạt tất cả ý "Bắt buộc" nhưng thiếu nâng cao hoặc có 1 lỗi nhỏ. **Trung bình** = có làm nhưng thiếu 1 ý bắt buộc hoặc chạy lại phải sửa tay. **0** = không có hoặc không chạy được.

### 1. Pipeline tự động hoá (3đ) – L11, L12 slide 7, 10
Bắt buộc (đúng lời của rubric: "ingest → clean → load → check", "chạy lại nhiều lần không cần sửa code"):
- File DAG parse được, có đủ 4 giai đoạn thành task riêng, thứ tự phụ thuộc đúng (`>>`).
- Chạy được trên Docker bằng `docker compose up` + trigger từ Airflow UI (đề bài).
- **Idempotent**: chạy lại không tạo duplicate (`ON CONFLICT DO UPDATE`, hoặc TRUNCATE/DELETE trước khi load, hoặc MERGE). Slide 7: "chạy lại không hỏng data".
- **Thông tin kết nối DB không nằm trong code**: dùng **Airflow Connection** (`postgres_conn_id`, `PostgresHook`, `BaseHook.get_connection`; connection tạo sẵn bằng `AIRFLOW_CONN_*`, `airflow connections add` hoặc hướng dẫn UI trong README) **hoặc** user/password đọc từ file env (`.env` + `.env.example` + hướng dẫn README, `.env` trong `.gitignore`). Tuyệt đối không có user/password trong code, kể cả làm giá trị mặc định của `os.getenv` (slide 8: "hard-code" là lỗi phổ biến).
- **Task `ingest` và `transform` tách riêng** (L11: một task một việc).
Nâng cao: `retries` + `retry_delay`; `schedule` có ý nghĩa (không bắt buộc nếu đề không yêu cầu); `catchup=False`; healthcheck/`depends_on` trong compose; `on_failure_callback`; DAG dùng tham số `{{ ds }}` để chạy theo ngày.
Lỗi thường gặp → Trung bình: gộp cả ETL vào 1 task; phải sửa path/đổi tên file mới chạy; chạy lần 2 bị lỗi PK hoặc nhân đôi dòng; thiếu `airflow-init`; volume dags không mount.

### 2. Data Cleaning & Validation (2đ) – L10, L12 slide 8
Bắt buộc (4 loại lỗi rubric nêu): xử lý **null/rỗng**, **sai kiểu** (ngày/số), **giá trị âm** (quantity, price), **payment_method không hợp lệ**; và **remove duplicate** (theo khoá nghiệp vụ, giữ bản mới nhất là tốt). Dữ liệu bẩn không làm pipeline fail bất ngờ (đọc raw dạng TEXT rồi ép kiểu an toàn, không `CAST` trực tiếp).
Nâng cao: chuẩn hoá (`TRIM/LOWER`), bảng reject/quarantine lưu dòng lỗi + lý do, ghi số dòng bị loại vào log, ràng buộc `CHECK`/`PRIMARY KEY` ở bảng đích, validate sau load (đếm trùng, đối soát số dòng raw = clean + reject).
Lỗi thường gặp: `::INT` thẳng từ text → pipeline chết vì `'abc'`; chỉ lọc `> 0` mà quên `payment_method`; xoá dòng lỗi im lặng không ghi đếm; dedup không có `ORDER BY` nên giữ dòng ngẫu nhiên.

### 3. Logging & Monitoring (2đ) – L10, L11, L12 slide 9
Bắt buộc: **bảng log trong DB** (có `CREATE TABLE` + `INSERT` trong repo) (ví dụ `etl.etl_log`) có `run_id`, `step_name`, `status`, `message`, `created_at`; ghi đủ các bước **ingest, transform, load, validate, pipeline success/fail**; nhánh `except` ghi `FAIL` kèm lỗi (và ghi sau `rollback`, nếu không bản ghi FAIL mất theo transaction).
"Biết xem và giải thích log nếu lỗi": học viên (trong báo cáo/README/thuyết trình) chỉ ra được cách tra log: `SELECT ... FROM etl.etl_log ORDER BY created_at DESC` và log task trên Airflow UI.
Nâng cao: log số dòng (rows in / out / rejected); thời gian chạy từng bước; `run_id` thống nhất xuyên suốt 3 task (hoặc dùng Airflow `run_id`); dùng module `logging` song song với bảng log.
Lỗi thường gặp: chỉ `print`; mỗi script sinh `RUN_ID` riêng nên không nối được 1 lần chạy; bản ghi FAIL nằm trong transaction đã rollback; thiếu bước VALIDATE trong log.

### 4. Kiểm tra kết quả (1đ) – L12 slide 13, 15
Bắt buộc: có bằng chứng truy vấn đầu ra (file SQL/ảnh chụp/README): `COUNT(*)` raw vs DW, `SELECT * LIMIT`, xem `etl_log`.
Nâng cao (Tốt): tổng doanh thu/đối soát, kiểm tra duplicate = 0, kiểm tra không còn giá trị âm/null, so khớp số dòng raw − reject = DW; task `post_run_check` tự động fail khi bất thường (không chỉ `print`).
Khá: có truy vấn nhưng chỉ đếm dòng, không giải thích kết quả.

### 5. Hồ sơ nộp project (1đ)
Bắt buộc "đúng cấu trúc: file DAG, scripts, docker-compose.yaml". Cấu trúc chuẩn:
```
repo/
├─ docker-compose.yaml
├─ dags/
│  ├─ <ten>_dag.py
│  ├─ scripts/*.py
│  └─ data/<file>.csv
├─ sql/ (init schema, bảng)
├─ README.md   .gitignore
```
Tốt: clone về → `docker compose up airflow-init` → `up -d` chạy ngay, không thiếu file (SQL init, CSV, requirements nếu dùng thư viện ngoài như pandas/psycopg2).
Trừ mức: thiếu SQL tạo schema/bảng; CSV nằm sai chỗ so với `CSV_PATH`; thiếu thư viện trong image (pandas không có trong image `apache/airflow` mặc định có thể ổn, nhưng phải kiểm tra); lỗi cú pháp Python; nộp 2 bản project lồng nhau.

### 6. Tài liệu & Git (1đ)
Bắt buộc: `README.md` có cách chạy (lệnh, URL, tài khoản), **ảnh chụp màn hình** (Airflow DAG success, bảng dữ liệu/log); `.gitignore` chuẩn; repo sạch (không track `logs/`, `__pycache__/`, `.DS_Store`, `.env`); commit message rõ ràng.
Khá (0.7): thiếu một trong: ảnh chụp, .gitignore hợp lý, hoặc commit message mơ hồ (`update`, `fix`).
Nâng cao: mô tả kiến trúc/luồng dữ liệu, cấu trúc thư mục, troubleshooting (xung đột port 5432 – slide 14), nhiều commit có ý nghĩa theo từng bước.

---

## Bonus (tối đa +1đ) – `tieu_chi.xlsx` sheet "Bonus"
Bonus là bài đăng mạng xã hội, **AI không kiểm chứng được** nếu chỉ có repo. Chỉ chấm khi học viên cung cấp link/ảnh/nội dung bài đăng. Nếu không có: ghi "Chưa có dữ liệu để chấm Bonus", không cho điểm.

| Mục | Không làm | Yếu | Khá | Tốt |
|---|---|---|---|---|
| Câu chuyện & nội dung | 0 | 0.1 | 0.2 | 0.4 |
| Độ lan toả (≥30 tương tác thật) | 0 | 0.1 | 0.2 | 0.3 |
| Hình ảnh/video | 0 | 0.075 | 0.15 | 0.2 |
| Hashtag & tag, Public | 0 | 0.025 | 0.05 | 0.1 |

Hashtag bắt buộc: `#MindX #MindXStudentProject #LearnBuildShare #DemoDay #MindXContest2025`, dòng "DEMO sản phẩm cuối khoá Lớp (tên lớp)", "MindX Technology School", link website khoá, bài Public.

---

## Nguyên tắc từ slide (căn cứ để review; trích dẫn slide khi nhận xét)

| Bài | Nguyên tắc | Dùng để review |
|---|---|---|
| L2 | Chọn kiểu dữ liệu đúng: tiền = `NUMERIC` (không FLOAT), ngày = `DATE`, mã = `TEXT/VARCHAR` | Bảng DW/staging, tính tiêu chí 2 |
| L3 | Khoá chính duy nhất, không NULL; xử lý NULL (`IS NULL`, `COALESCE`) | PK ở bảng DW (cũng là điều kiện cho upsert), tiêu chí 1–2 |
| L7 | Tư duy **set-based**; raw giữ nguyên (`UNION ALL`, không loại trùng ở raw); `EXCEPT` phát hiện thiếu/dư, `INTERSECT` xác nhận đã load; không dùng `DISTINCT` để "che" lỗi: phải biết trùng bao nhiêu, vì sao; luồng nguồn → staging → warehouse | Tiêu chí 2, 4 |
| L8 | Tối ưu có đo lường (`EXPLAIN ANALYZE`), index chỉ ở cột dùng `WHERE/JOIN/ORDER BY`/khoá; **không index bảng raw ingestion**; tối ưu không theo cảm giác | Nâng cao của tiêu chí 2–4 |
| L9 | Python điều khiển SQL, không thay SQL; `commit/rollback`, `try/except`, đóng connection/cursor trong `finally`; không hard-code mật khẩu | Tiêu chí 1, 3 |
| L10 | Pipeline **thành công toàn bộ hoặc thất bại toàn bộ**; transform **bằng SQL**; load trong transaction; **validation trước commit**; log tối thiểu: thời gian, tên bước, trạng thái (START/SUCCESS/FAIL), thông tin lỗi; cấu trúc script: config, khai báo SQL, hàm từng bước, `main`, `try/except/finally`; 4 lỗi: commit trước validation, load xong không kiểm tra, không log, chạy được nhưng không lặp lại | Tiêu chí 1, 2, 3, 4 |
| L11 | Airflow chỉ **điều phối**, không xử lý dữ liệu; mỗi task 1 việc; task trước fail thì task sau không chạy; retry + thời gian chờ; exit code ≠ 0 = FAIL; lịch chạy; Docker: `airflow-init` → `up -d`; đọc log task trên UI | Tiêu chí 1, 3, 5, 6 |
| L12 | Từ "chạy được" đến "chạy tốt": idempotent, log rõ, validate tự động, retry đúng chỗ, cảnh báo khi lỗi; lỗi hay gặp: null/duplicate/sai kiểu, thiếu điều kiện join/filter, Docker mount volume/ENV/port, hard-code task name; debug: xác định lỗi ở data/code/Docker/Postgres → log ETL trong DB → log Airflow UI → volume/port/ENV | Tất cả tiêu chí |
| Đề bài | Chọn **1 bảng trong data bank**; Airflow đầy đủ: CSV → raw → clean/validate → DW → logging & kiểm tra; chạy trên Docker; đưa lên git; thuyết trình (chạy Docker + trigger UI, giải thích file & workflow, kết quả query) | Tiêu chí 1, 4, 5, 6 |

### Hệ quả cho cách chấm (đã áp dụng ở các mục trên)
- Làm sạch dữ liệu bằng vòng lặp từng dòng trong Python (`iterrows`, `for row in df`) vi phạm L7/L9/L10 → tối đa **Trung bình** ở tiêu chí 2. Dùng pandas để đọc CSV vào raw thì chấp nhận; clean/validate/load phải bằng SQL để đạt **Khá** trở lên. Clean bằng pandas vector hoá (không loop) nhưng không lưu số dòng bị loại → tối đa **Khá**.
- `DISTINCT`/`drop_duplicates` im lặng không đếm số trùng → không đạt **Tốt** ở tiêu chí 2 (L7 slide 16).
- `commit` trước bước validate, hoặc không có `rollback` khi lỗi → tối đa **Trung bình** ở tiêu chí 1 và 3 (L10 slide 21).
- Task sau vẫn chạy khi task trước fail (`trigger_rule=all_done`/`always` không có lý do) → trừ một mức ở tiêu chí 1 (L11 slide 18).
- Index trên bảng raw → ghi chú lỗi tư duy (L8 slide 13), không trừ điểm trực tiếp nếu không ảnh hưởng chạy.
- **Gộp ingest + transform vào 1 task**: không đạt yêu cầu của giảng viên → tiêu chí 1 tối đa **Khá**. Gộp transform + load vào 1 task: chấp nhận (một transaction, L10 "toàn bộ hoặc không"); tách riêng cả load và check là điểm cộng nâng cao.
- **Dùng Airflow Connection hoặc user/password từ file env**: cả hai đều đạt. **User/password nằm trong code** (DAG/script, kể cả giá trị mặc định của `getenv`) → tiêu chí 1 tối đa **Trung bình**. Dùng file env mà thiếu `.env.example`/hướng dẫn README → clone không chạy được, trừ ở TC5 B3, không trừ TC1.
- **Không có bảng log trong DB** (chỉ `print`) → tiêu chí 3 tối đa **Trung bình**.

---

## Lỗ hổng của thang điểm (cần giảng viên xác nhận trước khi chấm hàng loạt)
1. **Không có mức "Yếu/0"** ở phiếu chính: học viên làm dở không đạt "Trung bình" thì AI cho 0. Gợi ý bổ sung ô "Yếu" (ví dụ 0.5 × điểm Trung bình).
2. **Ô checkbox không loại trừ nhau** (có thể tick cả Trung bình, Khá, Tốt → điểm cộng dồn vượt tối đa). AI chỉ chọn 1 mức.
3. **Hệ số không đồng nhất**: tiêu chí 5 Khá = 0.8, tiêu chí 6 Khá = 0.7, tiêu chí 4 Khá = 0.8. Giữ nguyên theo Excel, nhưng nên thống nhất.
4. **Tiêu chí 5 và 6 chồng nhau** (cấu trúc thư mục vs README/Git) và tiêu chí 4 chỉ 1đ dù là bằng chứng chính cho cả pipeline.
5. **Bonus mâu thuẫn**: bản ngắn ghi "+1đ nếu ≥200 tương tác" nhưng tổng bonus tối đa 1đ, và bản chi tiết không có mục này. AI không cộng khoản +1đ này.
6. Rubric không nêu "Tốt" cần có gì cụ thể (idempotent, reject table…); file này đã làm rõ dựa trên slide L10–L12, cần giảng viên duyệt.
7. Mục "Biết xem và giải thích log" chỉ kiểm chứng được qua thuyết trình; với repo tĩnh, AI chấm theo README/ghi chú, đánh dấu "cần hỏi vấn đáp".
