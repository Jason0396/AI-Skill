# de_final_project_grader

Skill Kiro chấm Final Project DE-LV2 (Airflow + Postgres + Docker, CSV → raw → clean → DW). Skill quét tĩnh repo của học viên và trả về điểm theo `tieu_chi.xlsx`, lỗi sai kèm cách sửa từng bước.

## Cài đặt
1. Cài [Kiro CLI](https://kiro.dev) và `python3` (không cần thư viện ngoài).
2. Clone repo này vào thư mục skills của workspace:
   ```bash
   mkdir -p .kiro/skills
   git clone <URL_REPO_NAY> .kiro/skills/de_final_project_grader
   ```

## Cách dùng
1. Clone bài của bạn về máy (ví dụ `./my-final-project`).
2. Trong Kiro, gọi:
   ```
   /de_final_project_grader hãy chấm bài ./my-final-project
   ```
3. Skill in báo cáo và lưu `cham_diem_<ten>.md` (điểm từng tiêu chí, điểm mạnh, lỗi và cách sửa, câu hỏi vấn đáp).
4. Sửa theo hướng dẫn, commit, rồi chấm lại.

Chạy riêng script thu thập bằng chứng:
```bash
python3 .kiro/skills/de_final_project_grader/scripts/inspect_repo.py ./my-final-project
```

## Quy định kết nối DB
Dùng Airflow Connection hoặc lấy user/password từ file `.env` (kèm `.env.example`). Không để user/password trong code, kể cả giá trị mặc định của `os.getenv`.

## Cấu trúc
- `SKILL.md`: quy trình và tiêu chí chấm
- `references/rubric.md`: thang điểm; `references/fix_guides.md`: hướng dẫn sửa lỗi
- `scripts/inspect_repo.py`: quét tĩnh repo, không chạy code học viên
