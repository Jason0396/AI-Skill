#!/usr/bin/env python3
"""Thu thap bang chung (evidence) tu repo hoc vien da clone ve, phuc vu cham Final Project DE-LV2.

Dung:
    python3 inspect_repo.py <duong_dan_repo>
    python3 inspect_repo.py --clone <git_url>     # clone vao thu muc tam roi quet

Script CHI DOC (static). Khong chay docker, khong chay code cua hoc vien.
Ket qua la bang chung de AI doi chieu rubric; khong tu cham diem.
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

SKIP_DIRS = {".git", "node_modules", ".venv", "venv"}
JUNK_PATTERNS = [
    (r"(^|/)logs/", "thu muc logs/ cua Airflow"),
    (r"(^|/)__pycache__/", "__pycache__/"),
    (r"\.pyc$", "file .pyc"),
    (r"(^|/)\.DS_Store$", ".DS_Store"),
    (r"(^|/)\.env$", ".env (co the lo secret)"),
    (r"(^|/)(venv|\.venv)/", "virtualenv"),
    (r"(^|/)\.idea/|(^|/)\.vscode/", "file IDE"),
]
BAD_COMMIT = re.compile(r"^(update|fix|test|abc|aaa|asd|commit|final|new|\.+|[a-z]{1,3})$", re.I)


def run(cmd: list[str], cwd: Path) -> str:
    try:
        return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=120).stdout
    except Exception:
        return ""


def read(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def list_files(root: Path) -> list[str]:
    out = []
    for p in root.rglob("*"):
        if p.is_file() and not (set(p.relative_to(root).parts) & SKIP_DIRS):
            out.append(str(p.relative_to(root)))
    return sorted(out)


def tracked_files(root: Path) -> list[str] | None:
    if not (root / ".git").exists():
        return None
    return [l for l in run(["git", "ls-files", "-z"], root).split("\0") if l]


def analyze_dag(path: Path) -> dict:
    src = read(path)
    info: dict = {"file": str(path), "parse_ok": True}
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        return {"file": str(path), "parse_ok": False, "error": str(e)}
    info["dag_id"] = (re.search(r"dag_id\s*=\s*['\"]([^'\"]+)", src) or [None, None])[1] or \
        (re.search(r"DAG_ID\s*=\s*['\"]([^'\"]+)", src) or [None, None])[1]
    info["operators"] = sorted(set(re.findall(r"\b(\w+Operator|@task)\b", src)))
    info["task_ids"] = re.findall(r"task_id\s*=\s*['\"]([^'\"]+)", src)
    info["dependency_lines"] = [l.strip() for l in src.splitlines() if ">>" in l or "<<" in l or "set_downstream" in l]
    sched = re.search(r"schedule(?:_interval)?\s*=\s*([^,\n]+)", src)
    info["schedule"] = sched.group(1).strip() if sched else None
    info["catchup"] = (re.search(r"catchup\s*=\s*(\w+)", src) or [None, None])[1]
    info["retries"] = (re.search(r"['\"]?retries['\"]?\s*[:=]\s*(\d+)", src) or [None, None])[1]
    info["has_retry_delay"] = "retry_delay" in src
    info["has_failure_callback"] = bool(re.search(r"on_failure_callback|email_on_failure|on_success_callback", src))
    info["has_trigger_rule"] = "trigger_rule" in src
    info["downstream_runs_after_fail"] = bool(re.search(r"trigger_rule\s*=\s*['\"]?(all_done|none_failed_min_one_success|always|TriggerRule\.(ALL_DONE|ALWAYS))", src))  # L11 s18: task truoc fail thi task sau khong duoc chay
    info["task_count"] = len(re.findall(r"task_id\s*=", src))
    info["uses_xcom_or_params"] = bool(re.search(r"xcom|params=|op_kwargs|\{\{\s*ds", src))
    info["start_date_uses_days_ago"] = "days_ago" in src  # deprecated o Airflow 2.x moi
    info["hardcoded_credentials_in_dag"] = bool(re.search(r"(password|PASS)['\"]?\s*[:=]\s*['\"][^'\"]+['\"]", src)) \
        and "getenv" not in src
    info["uses_env_or_connection"] = bool(re.search(r"os\.getenv|os\.environ|Variable\.get|BaseHook|PostgresHook|Connection", src))
    info["uses_airflow_connection"] = bool(re.search(
        r"conn_id|get_connection|PostgresHook|PostgresOperator|SQLExecuteQueryOperator|BaseHook", src))
    tids = [t.lower() for t in info["task_ids"]]
    info["has_ingest_task"] = any(re.search(r"ingest|extract|load_csv|csv", t) for t in tids)
    info["has_transform_task"] = any(re.search(r"transform|clean|validate", t) for t in tids)
    info["references_scripts"] = re.findall(r"/opt/airflow/[\w/\.\-]+\.(?:py|sql)", src)
    return info


SQL_SIGNALS = {
    "idempotent_upsert": r"ON\s+CONFLICT",
    "truncate_or_delete_before_load": r"\bTRUNCATE\b|\bDELETE\s+FROM\b",
    "drop_if_exists": r"DROP\s+TABLE\s+IF\s+EXISTS",
    "dedup_distinct_on": r"DISTINCT\s+ON",
    "dedup_row_number": r"ROW_NUMBER\s*\(",
    "dedup_group_by": r"GROUP\s+BY[\s\S]{0,200}HAVING\s+COUNT",
    "null_handling": r"NULLIF|IS\s+(NOT\s+)?NULL|COALESCE",
    "regex_type_check": r"~\s*'|~\*|regexp|TRY_CAST|pd\.to_numeric|pd\.to_datetime|errors\s*=\s*['\"]coerce",
    "negative_check": r"(quantity|qty|price|amount)\w*\s*(>=|>)\s*0|\bCHECK\s*\(",
    "payment_method_check": r"payment_method[\s\S]{0,80}\bIN\s*\(",
    "trim_lower_normalize": r"\bTRIM\(|\bLOWER\(|\bUPPER\(|\.str\.strip|\.str\.lower",
    "reject_table": r"reject|quarantine|error_row|bad_row|invalid",
    "transaction_control": r"conn\.commit|conn\.rollback|autocommit|BEGIN;|COMMIT;",
    "unique_constraint": r"PRIMARY\s+KEY|UNIQUE",
    "create_schema": r"CREATE\s+SCHEMA",
    # Bai L7-L10: validation bang SQL, set-based, index, transform bang SQL
    "validate_except": r"\bEXCEPT\s+(ALL\s+)?(SELECT|\()",
    "validate_count_compare": r"COUNT\s*\(\s*\*\s*\)",
    "create_index": r"CREATE\s+(UNIQUE\s+)?INDEX",
    "union_all_ingest": r"UNION\s+ALL",
    "row_by_row_python_loop": r"\.iterrows\(|\.itertuples\(|for\s+\w+\s+in\s+(df|rows|data)\b[\s\S]{0,120}(execute|insert)",
    "pandas_cleaning": r"drop_duplicates|dropna|fillna\(|astype\(|pd\.to_(numeric|datetime)",
    "finally_close_connection": r"finally\s*:[\s\S]{0,200}\.close\(\)",
    "exit_code_on_fail": r"sys\.exit\(\s*1|raise\s+\w*(Error|Exception)",
    "explain_analyze": r"EXPLAIN",
}
LOG_SIGNALS = {
    "insert_into_log_table": r"INSERT\s+INTO\s+\w+\.?\w*\s*\(?[\s\S]{0,40}log",
    "etl_log_table_defined": r"CREATE\s+TABLE[^;]*etl_log",
    "has_run_id": r"run_id",
    "has_status_values": r"START|SUCCESS|FAIL|ERROR",
    "logs_exception": r"except[\s\S]{0,300}(log|INSERT)",
    "logs_row_counts": r"(rows|cnt|count)\s*=",
    "python_logging_module": r"import\s+logging|logging\.(info|error|getLogger)",
    "print_only": r"\bprint\(",
}
STEP_NAMES = ["INGEST", "CLEAN", "TRANSFORM", "LOAD", "VALIDATE", "PIPELINE", "CHECK"]


def scan_signals(text: str, table: dict[str, str]) -> dict[str, bool]:
    return {k: bool(re.search(v, text, re.I)) for k, v in table.items()}


def analyze_scripts(root: Path, files: list[str]) -> dict:
    code_files = [f for f in files if f.endswith((".py", ".sql")) and not f.startswith(("logs/", "plugins/"))
                  and "/logs/" not in f and "__pycache__" not in f]
    blob = "\n".join(read(root / f) for f in code_files)
    per_file = {}
    for f in code_files:
        t = read(root / f)
        per_file[f] = {
            "lines": t.count("\n") + 1,
            "py_syntax_ok": _safe_parse(t) if f.endswith(".py") else None,
        }
    steps = [s for s in STEP_NAMES if re.search(rf"['\"]{s}['\"]", blob)]
    return {
        "code_files": code_files,
        "per_file": per_file,
        "sql_signals": scan_signals(blob, SQL_SIGNALS),
        "log_signals": scan_signals(blob, LOG_SIGNALS),
        "log_step_names_found": steps,
        "hardcoded_secret_hint": bool(re.search(r"password\s*[=:]\s*['\"](?!\s*['\"])[^'\"]+['\"]", blob, re.I))
                                 and not re.search(r"getenv|environ", blob),
        "sql_injection_fstring_hint": bool(re.search(r"execute\(\s*f['\"]", blob)),
    }


def _safe_parse(t: str) -> bool:
    try:
        ast.parse(t)
        return True
    except SyntaxError:
        return False


def analyze_compose(root: Path, files: list[str]) -> dict:
    cands = [f for f in files if re.search(r"docker-compose.*\.ya?ml$|compose\.ya?ml$", f)]
    if not cands:
        return {"found": False}
    t = read(root / cands[0])
    return {
        "found": True, "file": cands[0],
        "has_postgres": bool(re.search(r"image:\s*postgres", t)),
        "has_airflow": bool(re.search(r"image:\s*apache/airflow|airflow-webserver|airflow-scheduler|airflow standalone", t)),
        "mounts_dags": bool(re.search(r"\./dags:", t)),
        "mounts_sql_init": bool(re.search(r"docker-entrypoint-initdb\.d", t)),
        "has_airflow_init": "airflow-init" in t,
        "env_db_vars": bool(re.search(r"DB_HOST|DB_NAME|POSTGRES_", t)),
        "host_ports": re.findall(r"-\s*['\"]?(\d+):\d+['\"]?", t),
        "pinned_image_tag": not re.search(r"image:\s*[\w/\-]+\s*$", t, re.M),
        "has_healthcheck": "healthcheck" in t,
    }


def connection_setup(root: Path, files: list[str], dags: list[dict], scripts_blob: str) -> dict:
    """Airflow Connection co duoc dung va duoc tao o dau (compose AIRFLOW_CONN_*, `airflow connections add`, UI ghi trong README)."""
    texts = {f: read(root / f) for f in files
             if f.endswith((".yaml", ".yml", ".sh", ".md", ".py", ".env", ".txt")) and "/logs/" not in f
             and not f.startswith("logs/") and "__pycache__" not in f}
    defined_in = [f for f, t in texts.items()
                  if re.search(r"AIRFLOW_CONN_\w+|airflow\s+connections\s+add|conn_id\s*=\s*['\"][\w\-]+['\"]\s*.*uri|Connection\(", t)]
    readme_mentions = any(re.search(r"Admin\s*(>|->|→)\s*Connections|connections add|AIRFLOW_CONN", t, re.I)
                          for f, t in texts.items() if f.lower().endswith("readme.md"))
    conn_ids = sorted({m for t in texts.values() for m in re.findall(r"(?:postgres_conn_id|conn_id)\s*=\s*['\"]([\w\-]+)['\"]", t)})
    cred_defaults = sorted({f for f, t in texts.items() if f.endswith(".py") and re.search(
        r"(?i)(getenv|env)\(\s*['\"]\w*(PASS|PWD|USER|SECRET)\w*['\"]\s*,\s*['\"][^'\"]+['\"]", t)})
    compose_plain = sorted({f for f, t in texts.items() if re.search(r"compose.*\.ya?ml$", f) and re.search(
        r"(?im)^\s*\w*(PASS|PASSWORD|PWD)\w*\s*:\s*['\"]?(?!\$\{)[^\s'\"$#]+", t)})
    env_file_used = any(re.search(r"(?m)^\s*env_file\s*:|\$\{\w+\}", t) for f, t in texts.items() if re.search(r"compose.*\.ya?ml$", f))
    env_example = any(f.endswith((".env.example", "env.example")) for f in files)
    env_only = bool(re.search(r"os\.getenv\(['\"]DB_|os\.environ\[['\"]DB_", scripts_blob)) and not conn_ids
    return {"conn_ids_used": conn_ids, "connection_defined_in": defined_in,
            "readme_explains_connection": readme_mentions,
            "db_credentials_via_env_only": env_only,
            "credential_defaults_in_code": cred_defaults,
            "plaintext_password_in_compose": compose_plain,
            "compose_uses_env_file_or_vars": env_file_used,
            "has_env_example": env_example,
            "dag_uses_airflow_connection": any(d.get("uses_airflow_connection") for d in dags)}


def analyze_readme(root: Path, files: list[str]) -> dict:
    rd = [f for f in files if f.lower().endswith("readme.md") and f.count("/") <= 1]
    if not rd:
        return {"found": False}
    t = read(root / rd[0])
    imgs = re.findall(r"!\[[^\]]*\]\(([^)]+)\)|<img[^>]+src=['\"]([^'\"]+)", t)
    img_paths = [a or b for a, b in imgs]
    missing = [p for p in img_paths if not p.startswith("http") and not (root / rd[0]).parent.joinpath(p).exists()]
    return {
        "found": True, "file": rd[0], "chars": len(t),
        "mentions_docker_compose_up": bool(re.search(r"docker[ -]compose\s+up", t)),
        "mentions_airflow_ui_8080": "8080" in t,
        "mentions_credentials": bool(re.search(r"admin", t, re.I)),
        "mentions_trigger_dag": bool(re.search(r"trigger", t, re.I)),
        "mentions_verify_queries": bool(re.search(r"SELECT\s", t, re.I)),
        "mentions_project_structure": bool(re.search(r"dags/|├|└|structure|cấu trúc", t, re.I)),
        "mentions_requirements_or_prereq": bool(re.search(r"docker desktop|prerequisite|yêu cầu|requirement", t, re.I)),
        "mentions_reset_or_troubleshoot": bool(re.search(r"down -v|troubleshoot|lỗi|reset", t, re.I)),
        "mentions_architecture_or_flow": bool(re.search(r"flow|pipeline|raw|staging|warehouse|\bdw\b", t, re.I)),
        "image_count": len(img_paths), "image_paths": img_paths, "broken_image_links": missing,
        "headings": re.findall(r"^#{1,3}\s+(.+)$", t, re.M),
    }


def port_consistency(root: Path, readme: dict, compose: dict) -> dict:
    """README noi cong DB nao, compose map cong nao (loi hay gap: README 5432, compose 5433)."""
    if not readme.get("found") or not compose.get("found"):
        return {}
    t = read(root / readme["file"])
    readme_ports = set(re.findall(r"localhost:(\d{4})|[Pp]ort\W{0,5}(\d{4})", t) and
                       [a or b for a, b in re.findall(r"localhost:(\d{4})|[Pp]ort\W{0,5}(\d{4})", t)])
    host_ports = set(compose.get("host_ports", []))
    db_ports = {p for p in host_ports if p != "8080"}
    return {"readme_ports": sorted(readme_ports - {"8080"}), "compose_host_ports": sorted(host_ports),
            "mismatch": bool(readme_ports - {"8080"}) and bool(db_ports) and not ((readme_ports - {"8080"}) & db_ports)}


def analyze_git(root: Path, tracked: list[str] | None, files: list[str]) -> dict:
    gi = root / ".gitignore"
    res: dict = {"is_git_repo": (root / ".git").exists(), "gitignore_exists": gi.exists()}
    gtxt = read(gi) if gi.exists() else ""
    res["gitignore_lines"] = [l for l in gtxt.splitlines() if l.strip() and not l.startswith("#")]
    res["gitignore_covers"] = {
        "logs": bool(re.search(r"^/?logs/?", gtxt, re.M)),
        "pycache": bool(re.search(r"__pycache__|\*\.py[cod]", gtxt)),
        "env": bool(re.search(r"^\.env", gtxt, re.M)),
        "ds_store": ".DS_Store" in gtxt,
        "venv": bool(re.search(r"venv", gtxt)),
    }
    basis = tracked if tracked is not None else files
    res["junk_basis"] = "git ls-files" if tracked is not None else "filesystem (khong co .git, kem tin cay)"
    junk: dict[str, list[str]] = {}
    for f in basis:
        for pat, label in JUNK_PATTERNS:
            if re.search(pat, f):
                junk.setdefault(label, []).append(f)
    res["junk_tracked"] = {k: {"count": len(v), "examples": v[:3]} for k, v in junk.items()}
    if res["is_git_repo"]:
        log = run(["git", "log", "--pretty=%h|%an|%ad|%s", "--date=short"], root).splitlines()
        subs = [l.split("|", 3)[-1] for l in log]
        res["commit_count"] = len(log)
        res["authors"] = sorted({l.split("|")[1] for l in log})
        res["commit_messages_sample"] = subs[:15]
        res["vague_commit_messages"] = [s for s in subs if BAD_COMMIT.match(s.strip()) or len(s.strip()) < 8]
        res["remote"] = run(["git", "remote", "-v"], root).strip()
        res["branch"] = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], root).strip()
        res["secrets_in_history_hint"] = bool(run(["git", "log", "--all", "--oneline", "-S", "password=", "--", "."], root).strip())
    return res


def analyze_structure(files: list[str]) -> dict:
    def has(p): return any(re.search(p, f) for f in files)
    return {
        "dag_file": has(r"(^|/)dags/[^/]+\.py$"),
        "scripts_dir": has(r"(^|/)dags/scripts/.+\.py$|(^|/)scripts/.+\.py$"),
        "docker_compose": has(r"docker-compose.*\.ya?ml$|(^|/)compose\.ya?ml$"),
        "sql_dir_or_init": has(r"\.sql$"),
        "data_csv": has(r"\.csv$"),
        "readme": has(r"(^|/)README\.md$"),
        "gitignore": ".gitignore" in files,
        "requirements_or_dockerfile": has(r"requirements\.txt|Dockerfile"),
        "verification_artifact": has(r"verif|check|evidence|screenshots?/|\.(png|jpg|jpeg)$"),
        "screenshots": [f for f in files if re.search(r"\.(png|jpe?g|gif)$", f, re.I)][:20],
        "nested_duplicate_project_hint": len({f.split("/")[0] for f in files if f.endswith("docker-compose.yaml")}) > 1
        or sum(1 for f in files if f.endswith("docker-compose.yaml")) > 1,
    }


def sanity_check_data(root: Path, files: list[str]) -> dict:
    csvs = [f for f in files if f.endswith(".csv")]
    out = {}
    for f in csvs[:3]:
        lines = read(root / f).splitlines()
        out[f] = {"rows_excluding_header": max(len(lines) - 1, 0), "header": lines[0] if lines else ""}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", nargs="?")
    ap.add_argument("--clone", help="git URL de clone vao thu muc tam")
    a = ap.parse_args()
    if a.clone:
        tmp = Path(tempfile.mkdtemp(prefix="de_grade_"))
        r = subprocess.run(["git", "clone", "--quiet", a.clone, str(tmp / "repo")], capture_output=True, text=True)
        if r.returncode:
            print("Clone loi:", r.stderr, file=sys.stderr)
            return 2
        root = tmp / "repo"
    elif a.repo:
        root = Path(a.repo).expanduser().resolve()
    else:
        ap.error("can <repo> hoac --clone <url>")
    if not root.is_dir():
        print(f"Khong tim thay thu muc: {root}", file=sys.stderr)
        return 2

    files = list_files(root)
    tracked = tracked_files(root)
    dag_files = [f for f in files if re.search(r"(^|/)dags/[^/]+\.py$", f) and "__pycache__" not in f]
    result = {
        "repo": str(root),
        "total_files": len(files),
        "structure": analyze_structure(files),
        "dags": [analyze_dag(root / f) for f in dag_files],
        "scripts": analyze_scripts(root, files),
        "compose": analyze_compose(root, files),
        "readme": analyze_readme(root, files),
        "git": analyze_git(root, tracked, files),
        "data": sanity_check_data(root, files),
    }
    result["airflow_connection"] = connection_setup(root, files, result["dags"], "\n".join(
        read(root / f) for f in result["scripts"]["code_files"]))
    result["port_consistency"] = port_consistency(root, result["readme"], result["compose"])
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
