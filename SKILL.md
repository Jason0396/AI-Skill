---
name: de_final_project_grader
description: Chấm điểm Final Project DE-LV2 (Airflow + Postgres + Docker, pipeline CSV → raw → clean → DW) từ repo git học viên đã clone. Bám thang điểm tieu_chi.xlsx và nội dung bài L10–L12. Trả về điểm từng tiêu chí, lỗi sai kèm cách sửa, và điểm cần cải thiện. Dùng khi được yêu cầu chấm/nhận xét bài final project.
---

# Skill: Chấm Final Project DE-LV2

## Đầu vào
Một thư mục repo đã `git clone` (hoặc URL git để clone về thư mục tạm). Nếu người dùng không đưa đường dẫn, hỏi. Không cần Docker để chấm; quét tĩnh là mặc định.

## Quy trình

1. **Thu thập bằng chứng** (chỉ đọc, không chạy code của học viên):
   ```
   python3 .kiro/skills/de_final_project_grader/scripts/inspect_repo.py <repo>
   python3 .kiro/skills/de_final_project_grader/scripts/inspect_repo.py --clone <git_url>
   ```
   Kết quả JSON gồm: cấu trúc, DAG (task, thứ tự, retries, schedule), tín hiệu SQL/Python (upsert, dedup, kiểm tra kiểu/âm/payment_method), tín hiệu log, docker-compose, README (lệnh, ảnh, sai lệch cổng), git (`.gitignore`, file rác bị track, commit message).
2. **Đọc trực tiếp** DAG, các script, SQL init, README của học viên để xác nhận những gì script chỉ đoán bằng regex. Regex cho tín hiệu, không phải kết luận: một chỗ `ON CONFLICT` bị comment hoặc nằm trong code không được gọi thì không tính.
3. **Đọc** `references/rubric.md` (có bảng nguyên tắc từ slide L2–L12) và chọn **đúng 1 mức** cho mỗi tiêu chí (Không đạt / Trung bình / Khá / Tốt). Với mỗi mức, nêu bằng chứng cụ thể (file:dòng hoặc tên task).
4. **Tính điểm** bằng công thức trong rubric (lấy từ Excel). Không làm tròn trước khi cộng.
5. **Viết báo cáo** theo mẫu dưới. Mỗi lỗi phải có: chỗ sai, vì sao sai (gắn với bài học), **các bước sửa đánh số**, cách kiểm tra đã sửa đúng, và điểm được thêm. Dùng `references/fix_guides.md` (G1–G12) làm khuôn, thay tên file/bảng/cột của học viên. Lỗi chưa có trong thư viện thì tự viết cùng cấu trúc.

## Nguyên tắc chấm
- Chỉ chấm theo bằng chứng thấy được trong repo. Điều không kiểm chứng được (chạy thật trên Docker, thuyết trình, bài đăng Bonus) ghi rõ "chưa kiểm chứng" và đề xuất giảng viên hỏi vấn đáp. Không đoán.
- Không cộng điểm cho việc thiếu lỗi. Không trừ điểm cho khác biệt về công cụ miễn đạt yêu cầu (ví dụ dùng `PostgresHook` thay `psycopg2`). Riêng việc clean/validate/load phải bằng SQL (set-based), đúng nội dung L7–L10; xem "Hệ quả cho cách chấm" trong `rubric.md`.
- Mức Tốt đòi hỏi mọi ý "Bắt buộc" và ít nhất một ý "Nâng cao" của tiêu chí đó.
- Nếu repo là bản sao của code mẫu trên lớp (L11/L12 `Sales_Airflow_E2E_Project`) gần như nguyên vẹn (cùng bảng `sales`, cùng script), ghi nhận và báo giảng viên. Đề bài yêu cầu chọn **1 bảng khác trong data bank** nên cần đối chiếu chủ đề dữ liệu; không tự kết luận gian lận.
- Không để lộ hoặc sao chép secret có trong repo vào báo cáo; chỉ nói "có secret hard-code ở file X".
- Nếu thang điểm mâu thuẫn hoặc thiếu (xem mục cuối `rubric.md`), vẫn chấm theo Excel và nêu rõ chỗ đó trong phần "Lưu ý cho giảng viên".
- Điểm mạnh phải có bằng chứng thấy được trong repo (file, task hoặc dòng code). Không khen chung chung, không khen việc ai cũng có sẵn từ code mẫu của lớp mà học viên không tự làm. Mục này đặt trước phần lỗi để học viên biết giữ lại gì.
- **Không nhận xét chung chung.** Câu như "thêm ảnh vào README", "cải thiện commit", "sửa idempotent" mà không nói làm ở đâu, bước nào, lệnh nào, kiểm tra ra sao là không đạt yêu cầu của skill. Người đọc là học viên mới học: viết sao cho làm theo được mà không cần hỏi lại.
- Giọng nhận xét dành cho học viên: cụ thể, lịch sự, tiếng Việt, ưu tiên điều có tác động điểm lớn nhất trước.

## Tiêu chí review (đối chiếu từng mục, ghi Đạt / Một phần / Không đạt kèm bằng chứng)

Mỗi tiêu chí lấy từ `tieu_chi.xlsx` + đề bài + slide L1–L12. Mã [Bx] là mục bắt buộc, [Nx] là nâng cao (cần để lên mức Tốt). Chi tiết phân mức và điểm: `references/rubric.md`.

### TC1. Pipeline tự động hoá – 3đ (L11, L12)
- [B1] File DAG parse được; DAG có `dag_id`, `start_date`, `schedule`/trigger thủ công, `catchup`.
- [B2] Đủ luồng ingest → clean → load → check, thứ tự `>>` đúng, không vòng lặp (L11 s12–18). **Task `ingest` và task `transform` (clean/validate) phải là hai task riêng** (yêu cầu bổ sung của giảng viên; L11 s17 "một task một việc"). Gộp ingest + transform trong một task: không đạt B2. Load có thể nằm chung hoặc riêng với transform, nếu giữ một transaction thì ghi rõ lý do.
- [B3] Task trước fail thì task sau không chạy (không `trigger_rule` đảo ngược); script thoát `exit 1`/`raise` khi lỗi để Airflow đánh dấu FAIL (L11 s20).
- [B4] Chạy lại nhiều lần không đổi kết quả, không nhân đôi dòng, không sửa code (idempotent: `ON CONFLICT`, `TRUNCATE`/`DELETE` trước load) (L10 s21, L12 s7).
- [B5] **Thông tin kết nối DB không được nằm trong code.** Chấp nhận **một trong hai** cách:
  - (a) **Airflow Connection** (`postgres_conn_id`/`PostgresHook`/`BaseHook.get_connection`), được tạo sẵn và có hướng dẫn: biến `AIRFLOW_CONN_<ID>` trong compose, lệnh `airflow connections add`, hoặc các bước trên UI (Admin → Connections) ghi trong README.
  - (b) **User/password lấy từ file env** (`.env` qua `env_file:` hoặc `${VAR}` trong compose; code đọc bằng `os.getenv`/`os.environ`). Repo phải có `.env.example` (không chứa mật khẩu thật) và README hướng dẫn `cp .env.example .env`; `.env` thật phải nằm trong `.gitignore`.
  - **Tuyệt đối không** để user/password trong code (DAG, script Python/SQL): kể cả làm giá trị mặc định như `os.getenv("DB_PASS", "airflow")` hoặc `env("DB_USER", "airflow")`. Thiếu biến thì phải `raise`. Vi phạm: **không đạt B5**, TC1 tối đa **Trung bình**. Mật khẩu viết cứng trong `docker-compose.yaml` (không qua file env) ghi nhận là lỗi nhẹ và nên chuyển sang `.env`. Host/port/tên DB có giá trị mặc định thì chấp nhận. (L9, L12 s8; yêu cầu bổ sung của giảng viên)
- [B6] Chạy được bằng `docker compose up airflow-init` → `up -d` → trigger trên UI (đề bài, L11 s27–30).
- [N1] `retries` + `retry_delay`; [N2] `on_failure_callback`/cảnh báo; [N3] healthcheck/`depends_on`; [N4] tham số theo ngày (`{{ ds }}`); [N5] tách riêng cả load và check (mỗi task một việc).

### TC2. Data Cleaning & Validation – 2đ (L7, L9, L10)
- [B1] Raw lưu nguyên dạng TEXT, giữ đủ dòng (L7 s9: không loại trùng ở raw).
- [B2] Xử lý: null/rỗng, sai kiểu (ngày, số), giá trị âm, `payment_method` (hoặc cột phân loại tương đương của bảng được chọn) không hợp lệ, trùng khoá.
- [B3] Ép kiểu an toàn (regex/`CASE`/`NULLIF`), dữ liệu bẩn không làm pipeline chết (L12 s8).
- [B4] Clean bằng SQL set-based, Python chỉ gọi SQL, không loop từng dòng (L10 s8–9, L9 s21).
- [B5] Có validation sau load: so `COUNT`, trùng khoá, thiếu khoá (`EXCEPT`) (L10 s13–15), và dừng pipeline khi sai.
- [N1] Bảng reject/quarantine kèm lý do; [N2] log số dòng vào/ra/bị loại; [N3] `PRIMARY KEY`/`CHECK`/kiểu `NUMERIC` cho tiền (L2–L3); [N4] dedup có `ORDER BY` rõ ràng và đếm số trùng (L7 s16); [N5] chuẩn hoá `TRIM/LOWER`.

### TC3. Logging & Monitoring – 2đ (L10 s16–17, L11 s32, L12 s9)
- [B1] **Phải có bảng log trong DB** (vd `etl.etl_log`), có lệnh `CREATE TABLE` trong repo (file SQL init/script) và các script `INSERT` vào bảng này. Chỉ `print`/log file của Airflow: không đạt B1 (tối đa Trung bình ở TC3). Bảng có thời gian, `run_id`, tên bước, trạng thái START/SUCCESS/FAIL, thông điệp lỗi.
- [B2] Log đủ ingest, transform/clean, load, validate, pipeline success/fail.
- [B3] Khi lỗi: `rollback` rồi mới ghi FAIL (nếu không, bản ghi FAIL bị mất theo transaction).
- [B4] Học viên giải thích được cách đọc log: bảng `etl_log` + log task trên Airflow UI (cần vấn đáp/README).
- [N1] Log số dòng và thời lượng từng bước; [N2] `run_id` nhất quán xuyên task; [N3] dùng `logging` song song; [N4] có câu truy vấn mẫu xem log mới nhất.

### TC4. Kiểm tra kết quả (Data Verification) – 1đ (L12 s13–15, L7 s11–12)
- [B1] Có truy vấn kết quả thật: `COUNT` raw vs DW, `SELECT … LIMIT`, xem `etl_log`.
- [B2] Có bằng chứng (SQL file, ảnh, README) và kết quả khớp với dữ liệu.
- [N1] Đối soát raw − reject = DW; kiểm duplicate = 0, không âm/null; tổng doanh thu/chỉ số; [N2] task `post_run_check` tự fail khi bất thường; [N3] giải thích kết quả.

### TC5. Hồ sơ nộp project – 1đ (đề bài, L11 s26)
- [B1] Có: file DAG (`dags/`), scripts (`dags/scripts/`), `docker-compose.yaml`.
- [B2] Có SQL khởi tạo schema/bảng (`raw`, `etl`, `dw`) mount vào Postgres hoặc script tạo bảng; có CSV dữ liệu đúng đường dẫn `CSV_PATH`.
- [B3] Clone về chạy được ngay: không thiếu file, volume mount khớp, thư viện cần thiết có sẵn, port không xung đột (L12 s14).
- [B4] Dùng **1 bảng từ data bank** (không phải bản sao nguyên vẹn dữ liệu mẫu của lớp).
- [N1] Một cấu trúc thư mục gọn, không nộp 2 project lồng nhau; [N2] `requirements.txt`/Dockerfile khi cần.

### TC6. Tài liệu & Git – 1đ
- [B1] `README.md` có: mô tả bài toán, kiến trúc luồng, cấu trúc thư mục, lệnh chạy, URL + tài khoản Airflow, cách kiểm tra kết quả.
- [B2] Có ảnh chụp màn hình (DAG success/Graph, dữ liệu DW, log) và link ảnh không hỏng.
- [B3] `.gitignore` chuẩn (`logs/`, `__pycache__/`, `.env`, `.DS_Store`, venv); repo không track file rác.
- [B4] Commit message rõ nghĩa, nhiều commit theo bước; không lộ secret thật trong lịch sử.
- [B5] Thông tin trong README khớp thực tế (port DB, tên DAG).
- [N1] Phần troubleshooting (xung đột port 5432, reset `down -v`).

### Bonus – 1đ (chỉ chấm khi có link/ảnh/nội dung bài đăng)
Câu chuyện & CTA (0.4), độ lan toả ≥30 tương tác thật (0.3), ảnh/video thật (0.2), hashtag + tag + Public (0.1). Không có dữ liệu → "Chưa chấm Bonus".

### Thuyết trình (đề bài, AI không chấm được từ repo)
Chạy Docker + trigger UI; giải thích cấu trúc file và workflow; kết quả query. AI soạn câu hỏi vấn đáp để giảng viên hỏi.

## Các kiểm tra bắt buộc đối chiếu bài học (gợi ý nguồn lỗi hay gặp, slide L12 #8–#9)
- DAG không parse được, task name sai, đường dẫn script không khớp mount volume (`./dags:/opt/airflow/dags`).
- Thiếu truyền ENV cho container; hard-code host/port/pass trong script.
- README ghi cổng Postgres khác với `ports:` trong compose (xung đột 5432 – slide 14).
- Ép kiểu thẳng từ text (`::INT`, `::DATE`) làm pipeline chết khi gặp `abc`.
- Dedup thiếu `ORDER BY` (giữ dòng ngẫu nhiên); xoá dòng lỗi không log số lượng.
- Bản ghi `FAIL` ghi trong transaction đã rollback; `RUN_ID` khác nhau giữa các task.
- `post_run_check` chỉ `print`, không `raise` khi bất thường.
- Repo track `logs/`, `__pycache__/`, `.DS_Store`, `.env`; không có `.gitignore`.
- Chạy lần 2 phải cho kết quả giống lần 1 (idempotent): kiểm tra `TRUNCATE`/`ON CONFLICT`.

## Trả kết quả cho người dùng
Luôn lưu báo cáo đầy đủ vào `cham_diem_<ten>.md` **và** in toàn bộ nội dung báo cáo trong câu trả lời cuối (đủ các mục của mẫu dưới). Không chỉ tóm tắt hay chỉ đưa đường dẫn file. Sau báo cáo, ghi đường dẫn file đã lưu.

## Mẫu báo cáo (trả về cho người dùng và có thể lưu `cham_diem_<ten_hoc_vien>.md`)

```
# Kết quả chấm Final Project – <học viên / repo>
Tổng điểm: X.X / 10   (Bonus: Y / 1 hoặc "chưa có dữ liệu")
Độ tin cậy: Cao / Trung bình / Thấp  (nêu phần chưa chạy thật, chưa vấn đáp)

## Bảng điểm
| Tiêu chí | Mức | Điểm | Bằng chứng chính |
|---|---|---|---|
| 1. Pipeline tự động hoá | Khá | 2/3 | ... |
... 

## Checklist theo tiêu chí
TC1 B1 Đạt | B2 Đạt | B3 Một phần (lý do) | ...  (liệt kê mọi mục B/N ở phần "Tiêu chí review")

## Điểm mạnh & việc đã làm được
| Tiêu chí | Việc học viên đã làm tốt | Bằng chứng (file/task/dòng) | Liên hệ bài học |
|---|---|---|---|
| TC2 | Dedup giữ bản ghi mới nhất theo `raw_id` | `etl_sales_transform_load.py:SQL_BUILD_STAGING` (`DISTINCT ON … ORDER BY`) | L7 s16 |
| ... | ... | ... | ... |

Ghi mỗi tiêu chí TC1–TC6 ít nhất một dòng nếu có điểm mạnh. Tiêu chí không có điểm mạnh thì ghi "chưa có điểm nổi bật", không bỏ trống.

## Điểm chưa đúng & cách sửa
### [Mức độ: Nghiêm trọng/Trung bình/Nhẹ] <tên lỗi> – Tiêu chí N
- **Chỗ sai:** `dags/scripts/x.py:45` (và trích đoạn code/ảnh thiếu/số liệu chứng minh)
- **Vì sao sai:** ... (L12 slide 7 – idempotent)
- **Cách sửa, làm theo thứ tự:**
  1. Mở file `...`, tìm `...`.
  2. Thay/thêm đoạn sau: ```code```
  3. Chạy lệnh: ```cmd```
  4. Commit: `git commit -m "fix: ..."`
- **Cách kiểm tra đã sửa đúng:** lệnh + kết quả mong đợi.
- **Được thêm:** tiêu chí N từ <mức> lên <mức> (+0.5đ). Ghi "Cần chạy thật để xác nhận" nếu chưa chạy Docker.

## Điểm cần cải thiện cho học viên (xếp theo lợi ích điểm)
1. ... (+x đ)
2. ...
3. Kiến thức nên ôn: ... (L10 ...; L11 ...)

## Câu hỏi vấn đáp gợi ý (phần AI chưa kiểm chứng được)
- Nếu pipeline lỗi ở bước LOAD, em xem log ở đâu?
- Chạy DAG lần 2 thì dữ liệu thay đổi thế nào? Vì sao?

## Lưu ý cho giảng viên
- Chỗ rubric mơ hồ/mâu thuẫn ảnh hưởng điểm bài này.
```

## Tệp trong skill
- `scripts/inspect_repo.py`: quét tĩnh repo, in JSON bằng chứng.
- `references/fix_guides.md`: hướng dẫn sửa từng bước (G1–G12) cho các lỗi hay gặp, kèm cách kiểm tra.
- `references/rubric.md`: thang điểm, tiêu chí phân mức, Bonus, lỗ hổng của thang điểm.
