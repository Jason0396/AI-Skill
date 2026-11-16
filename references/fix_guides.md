# Thư viện hướng dẫn sửa lỗi (dùng khi viết mục "Điểm chưa đúng & cách sửa")

Cách dùng: với mỗi lỗi tìm thấy, chép mục tương ứng dưới đây vào báo cáo và thay tên file/bảng/cột bằng của học viên. Không viết kiểu "thêm ảnh vào README" mà không nói làm thế nào. Mỗi lỗi phải có đủ: **làm ở đâu, làm những bước nào, kết quả đúng trông thế nào, cách tự kiểm tra**.

Quy tắc viết hướng dẫn:
- Đánh số bước, mỗi bước một hành động. Lệnh để trong khối code, chạy được ngay (dùng `docker compose`, không dùng `docker-compose`).
- Ghi rõ đường dẫn file cần sửa và nội dung cần có sau khi sửa (đoạn trước/sau nếu ngắn).
- Kết thúc bằng "Cách kiểm tra đã sửa đúng" có lệnh và kết quả mong đợi.
- Ghi "Cần chạy thật để xác nhận" nếu chưa chạy Docker.
- Không yêu cầu `git push --force` hoặc viết lại lịch sử. Nếu cần, hỏi giảng viên.

---

## G1. Pipeline không idempotent (DW nhân đôi khi chạy lại) – TC1 B4

Dấu hiệu: `ON CONFLICT (<khoá tự tăng>)` hoặc không có `ON CONFLICT`; sau lần chạy 2, `dw_cnt` lớn hơn `staging_cnt`.

Cách A (nhanh, phù hợp khi bảng DW được nạp lại toàn bộ mỗi lần):
1. Mở file script load (ví dụ `dags/scripts/etl_bank_transform_load.py`).
2. Trong `main()`, ngay trước `cur.execute(SQL_UPSERT_DW)`, thêm:
   ```python
   cur.execute("TRUNCATE TABLE dw.bank_fact;")
   ```
   Lệnh này nằm cùng transaction nên nếu lỗi thì rollback, DW không bị rỗng.
3. Lưu file. Compose đã mount `./dags`, không cần build lại.

Cách B (upsert đúng nghĩa, khi bảng không có khoá tự nhiên):
1. Thêm cột khoá nghiệp vụ vào `sql/02_init_schema.sql`: `row_hash TEXT UNIQUE NOT NULL` trong `dw.bank_fact` (và trong staging).
2. Trong câu `INSERT … SELECT`, tạo hash từ toàn bộ cột nghiệp vụ: `md5(concat_ws('|', col1, col2, …))`.
3. Đổi `ON CONFLICT (record_id)` thành `ON CONFLICT (row_hash) DO UPDATE SET …`.
4. Reset DB để chạy lại script init: `docker compose down -v && docker compose up airflow-init && docker compose up -d`.

Cách kiểm tra:
```bash
# Trigger DAG 2 lần liên tiếp rồi chạy:
docker compose exec postgres psql -U airflow -d bank_db -c "SELECT COUNT(*) FROM dw.bank_fact;"
```
Kết quả đúng: số dòng sau lần 2 bằng số dòng sau lần 1.

## G2. Validate không bắt được lỗi – TC2 B5, TC4

Dấu hiệu: chỉ kiểm tra trùng trên khoá tự tăng; chỉ kiểm tra `count > 0`; không so các tầng với nhau.
1. Mở hàm `main()` ở bước VALIDATE.
2. Thay đoạn kiểm tra bằng đối soát ba tầng (raw → staging → DW):
   ```python
   cur.execute("""SELECT (SELECT COUNT(*) FROM raw.<bảng_raw>),
                         (SELECT COUNT(*) FROM etl.<bảng_staging>),
                         (SELECT COUNT(*) FROM dw.<bảng_dw>)""")
   raw_cnt, stg_cnt, dw_cnt = cur.fetchone()
   if dw_cnt != stg_cnt:
       raise RuntimeError(f"dw_cnt={dw_cnt} khác staging_cnt={stg_cnt}")
   log(cur, "VALIDATE", "SUCCESS", f"raw={raw_cnt}, staging={stg_cnt}, dw={dw_cnt}, rejected={raw_cnt-stg_cnt}")
   ```
3. Thêm kiểm tra thiếu khoá (L7 s12):
   ```sql
   SELECT <cột_khoá> FROM etl.<bảng_staging>
   EXCEPT
   SELECT <cột_khoá> FROM dw.<bảng_dw>;
   ```
   Nếu có dòng trả về thì `raise RuntimeError(...)`.
4. Đặt toàn bộ bước này **trước** `conn.commit()` (L10 s21).

Cách kiểm tra: tạm đổi một giá trị trong staging (hoặc xoá một dòng trong DW trong phiên test) rồi chạy; task phải FAIL và `etl_log` có dòng `FAIL`.

## G3. User/password nằm trong code hoặc chưa tách khỏi code – TC1 B5

Dấu hiệu: `os.getenv("DB_PASS", "airflow")`, `env("DB_USER", "airflow")`, mật khẩu viết cứng trong DAG/script, hoặc `DB_PASS: airflow` viết thẳng trong `docker-compose.yaml`.
Chọn **một trong hai** cách (cả hai đều được chấm đạt).

### Cách A: file env (đơn giản, giữ nguyên script)
1. Tạo `.env.example` (commit) với giá trị mẫu, không dùng mật khẩu thật:
   ```
   DB_HOST=postgres
   DB_PORT=5432
   DB_NAME=bank_db
   DB_USER=change_me
   DB_PASS=change_me
   ```
2. Trong `docker-compose.yaml`, bỏ các dòng `DB_USER: airflow`, `DB_PASS: airflow` khỏi `environment`; giữ `env_file: - .env` (Postgres: `POSTGRES_USER: ${DB_USER}`, `POSTGRES_PASSWORD: ${DB_PASS}`).
3. Trong mỗi script, bỏ giá trị mặc định cho user/password, thiếu thì lỗi:
   ```python
   DB = {"host": os.environ["DB_HOST"], "port": int(os.environ["DB_PORT"]),
         "database": os.environ["DB_NAME"],
         "user": os.environ["DB_USER"], "password": os.environ["DB_PASS"]}
   ```
4. Trong DAG, `BashOperator(env=...)` thay thế toàn bộ env của tiến trình con; thêm `append_env=True` hoặc truyền `DB_*` từ `os.environ` đã có trong container.
5. README: thêm bước `cp .env.example .env` rồi sửa giá trị; `.env` phải có trong `.gitignore`.

### Cách B: Airflow Connection
1. Khai báo trong `docker-compose.yaml`, mục `environment` của `x-airflow-common`, lấy giá trị từ `.env`:
   ```yaml
   AIRFLOW_CONN_BANK_DB: postgresql://${DB_USER}:${DB_PASS}@postgres:5432/bank_db
   ```
2. Trong script: `c = BaseHook.get_connection("bank_db")` rồi `DB = {"host": c.host, "port": c.port, "database": c.schema, "user": c.login, "password": c.password}`; hoặc `PostgresHook(postgres_conn_id="bank_db").get_conn()`.
3. Thêm `append_env=True` vào `BashOperator` (hoặc bỏ `env=`).
4. README mục "Kết nối DB": tên `conn_id`, cách xem trên UI (Admin → Connections).

Cách kiểm tra: `grep -rnE "getenv\(.*(PASS|USER).*,|password *= *['\"]" dags/` không ra kết quả. Cách B: `docker compose exec airflow-webserver airflow connections get bank_db` hiển thị connection. Chạy DAG xanh.
Lưu ý: chưa chạy thật thì ghi "Cần chạy thật để xác nhận".

## G4. Không dedup / không đếm dòng bị loại / không có reject – TC2 B2, N1–N2

1. Dedup theo khoá nghiệp vụ (hoặc toàn bộ cột nếu bảng không có khoá), giữ dòng mới nhất:
   ```sql
   SELECT DISTINCT ON (<cột_khoá>) <các_cột>, raw_id
   FROM parsed
   ORDER BY <cột_khoá>, raw_id DESC
   ```
2. Tạo bảng reject trong `sql/02_init_schema.sql`:
   ```sql
   CREATE TABLE etl.<bảng>_reject (raw_id INT, reason TEXT, rejected_at TIMESTAMP DEFAULT NOW());
   ```
3. Sau khi build staging, chèn các `raw_id` không vào staging:
   ```sql
   INSERT INTO etl.<bảng>_reject(raw_id, reason)
   SELECT r.raw_id, 'invalid or duplicate'
   FROM raw.<bảng_raw> r
   WHERE NOT EXISTS (SELECT 1 FROM etl.<bảng_staging> s WHERE s.raw_id = r.raw_id);
   ```
   (Cần giữ `raw_id` trong staging.)
4. Ghi số dòng vào log: `log(cur, "TRANSFORM", "SUCCESS", f"raw={raw_cnt}, clean={stg_cnt}, rejected={raw_cnt-stg_cnt}")`.

Cách kiểm tra: tạo một bản CSV test có 3 dòng lỗi (tuổi `abc`, giá trị âm, dòng trùng) → chạy DAG → `etl_log` ghi `rejected=3` và bảng reject có 3 dòng.

## G5. `.env` bị gitignore nhưng compose cần – TC5 B3, TC6 B5

1. Nếu mọi biến đã nằm trong `environment:` của compose, **xoá** đoạn
   ```yaml
   env_file:
     - .env
   ```
2. Nếu cần giữ `.env`: tạo file `.env.example` (không chứa mật khẩu thật), commit file này, và thêm vào README: `cp .env.example .env`.
3. Sửa cây thư mục trong README cho khớp thực tế.

Cách kiểm tra: clone repo vào thư mục mới, chạy `docker compose up airflow-init`. Không được báo "env file not found".

## G6. README thiếu ảnh chụp – TC6 B2

Làm ở đâu: thư mục `docs/` ở gốc repo; README nhúng ảnh.
1. Chạy project: `docker compose up airflow-init` rồi `docker compose up -d`. Mở http://localhost:8080 (admin/admin), bật DAG và bấm Trigger.
2. Chụp **3 ảnh tối thiểu**:
   - Ảnh 1, `docs/01_dag_graph.png`: tab **Graph** của DAG, tất cả task màu xanh lá (success). Thấy rõ tên các task.
   - Ảnh 2, `docs/02_dw_table.png`: kết quả `SELECT * FROM dw.<bảng_dw> LIMIT 10;` và `SELECT COUNT(*) …` trong pgAdmin hoặc psql.
   - Ảnh 3, `docs/03_etl_log.png`: kết quả `SELECT * FROM etl.etl_log ORDER BY created_at DESC LIMIT 20;` thấy các bước INGEST, TRANSFORM, LOAD, VALIDATE, PIPELINE.
   Cách chụp: macOS `Cmd+Shift+4`, Windows `Win+Shift+S`. Lưu PNG vào `docs/`.
3. Kiểm tra ảnh không chứa mật khẩu hay thông tin cá nhân. Che nếu có.
4. Thêm vào README (đúng mục "Kiểm tra kết quả"):
   ```markdown
   ### Kết quả
   ![DAG chạy thành công](docs/01_dag_graph.png)
   ![Dữ liệu trong DW](docs/02_dw_table.png)
   ![Bảng log](docs/03_etl_log.png)
   ```
5. Commit: `git add docs README.md && git commit -m "docs: add screenshots of DAG run and query results"`.

Cách kiểm tra: mở README trên GitHub, ảnh hiển thị; đường dẫn không bắt đầu bằng `C:\` hoặc `/Users/`.

## G7. Repo track file rác (`logs/`, `__pycache__`, `.DS_Store`…) – TC6 B3

Làm ở đâu: gốc repo, file `.gitignore`.
1. Thêm các dòng còn thiếu vào `.gitignore` (mở file, dán vào cuối):
   ```
   logs/
   __pycache__/
   *.pyc
   .DS_Store
   .env
   ```
2. Bỏ file đã lỡ track ra khỏi git (file trên máy vẫn còn, chỉ ngừng theo dõi):
   ```bash
   git rm -r --cached logs
   git status        # kiểm tra: logs/ hiện là "deleted" trong staged changes
   git commit -m "chore: stop tracking Airflow logs"
   git push
   ```
3. Nếu thư mục `logs/` cần tồn tại khi chạy compose, Docker sẽ tự tạo; không cần commit nội dung.

Cách kiểm tra: `git ls-files | grep -c "^logs"` ra `0`; `git status` sạch sau khi chạy lại DAG.
Lưu ý: log cũ vẫn còn trong lịch sử commit. Không cần xoá lịch sử trừ khi log chứa secret thật (khi đó hỏi giảng viên).

## G8. Commit message mơ hồ – TC6 B4

Làm ở đâu: các commit **từ giờ trở đi**; không cần sửa commit cũ đã nộp.
1. Mỗi commit chỉ làm một việc. Chia việc thành các commit nhỏ, ví dụ:
   ```bash
   git add dags/bank_e2e_csv_to_dw_dag.py && git commit -m "feat: add DAG with ingest, transform_load and check tasks"
   git add sql/ && git commit -m "feat: create raw, etl, dw schemas and etl_log table"
   git add dags/scripts/etl_bank_transform_load.py && git commit -m "fix: make DW load idempotent with TRUNCATE before insert"
   git add README.md docs/ && git commit -m "docs: add run guide and screenshots"
   ```
2. Dạng message: `<loại>: <việc đã làm, bằng tiếng Anh hoặc Việt rõ nghĩa>`. Loại thường dùng: `feat`, `fix`, `docs`, `chore`, `refactor`. Dài từ 10 ký tự trở lên, không dùng "commit", "update", "fix" đứng một mình.
3. Xem lại: `git log --oneline` phải đọc được như danh sách việc đã làm.

Cách kiểm tra: không còn message nào thuộc "commit", "next commit", "update", "test".
Nếu muốn sửa message commit cũ: cần `git rebase -i` rồi `git push --force`; chỉ làm khi giảng viên cho phép vì viết lại lịch sử.

## G9. README bỏ bước `airflow-init` – TC1 B6

1. Mở README, mục "Cách chạy". Thay bằng:
   ```bash
   docker compose down -v            # chỉ khi chạy lại từ đầu
   docker compose up airflow-init    # khởi tạo DB Airflow và user admin (chờ đến khi thoát)
   docker compose up -d              # chạy Airflow + Postgres
   ```
2. Thêm dòng chờ ~1–2 phút cho UI sẵn sàng tại http://localhost:8080.

Cách kiểm tra: chạy đúng các lệnh trong README trên máy sạch; UI mở được.

## G10. `post_run_check` chỉ in, không fail và không log – TC4 N2, TC3

1. Sau khi tính các số đếm, thêm điều kiện và `raise`:
   ```python
   if dw_cnt == 0 or dw_cnt > raw_cnt:
       raise RuntimeError(f"CHECK FAIL raw={raw_cnt} dw={dw_cnt}")
   ```
2. Ghi kết quả vào `etl.etl_log` bằng `INSERT` như các script khác (step `CHECK`).
3. Cuối script `sys.exit(1)` khi lỗi để Airflow đánh dấu task FAIL.

Cách kiểm tra: tạm sửa điều kiện để sai → task `post_run_check` đỏ trên UI.

## G11. `RUN_ID` khác nhau giữa các task – TC3 N2

1. Trong DAG, truyền run id của Airflow vào script qua biến môi trường:
   ```python
   bash_command=f'{PYTHON} "{INGEST_SCRIPT}"',
   env={"RUN_ID": "{{ run_id }}", ...},   # nhớ append_env=True
   ```
2. Trong script thay `RUN_ID = f"RUN_{uuid...}"` bằng `RUN_ID = os.getenv("RUN_ID") or f"RUN_{uuid.uuid4().hex[:8]}"`.

Cách kiểm tra: `SELECT DISTINCT run_id FROM etl.etl_log ORDER BY 1;` sau một lần trigger chỉ ra một giá trị.

## G12. File thừa / sai chỗ (`debug/` trong `dags/`, CSV hai bản, file backup) – TC5 N1

1. Chuyển script debug ra thư mục ngoài `dags/`: `git mv dags/scripts/debug tools/debug` (scheduler không còn quét).
2. Giữ một bản CSV tại đường dẫn `CSV_PATH`: `git rm data/Bank.csv` (nếu `dags/data/Bank.csv` là bản dùng).
3. Xoá hoặc chú thích rõ file không dùng (`sql/Data.sql.backup`); cập nhật README cho khớp.
4. Commit: `git commit -m "chore: remove unused files and move debug scripts out of dags"`.

Cách kiểm tra: cây thư mục trong README khớp với `git ls-files`.
