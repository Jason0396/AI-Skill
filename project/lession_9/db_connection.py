"""
db_connection.py
Kết nối MySQL Aiven Cloud.
Thông tin kết nối đọc từ password.txt (không hardcode).
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

# ─── Đọc thông tin kết nối từ password.txt ─────────
_PWD_FILE = os.path.join(os.path.dirname(__file__), "../../password.txt")

def _load_config():
    config = {}
    with open(os.path.abspath(_PWD_FILE), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            config[key.strip()] = val.strip().strip('"').strip("'")
    return config

_cfg    = _load_config()
DB_HOST = _cfg["DB_HOST"]
DB_PORT = int(_cfg["DB_PORT"])
DB_NAME = _cfg["DB_NAME"]
DB_USER = _cfg["DB_USER"]
DB_PASS = _cfg["DB_PASS"]
# ───────────────────────────────────────────────────


def get_engine():
    conn_str = (
        f"mysql+pymysql://{DB_USER}:{DB_PASS}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        f"?charset=utf8mb4"
    )
    return create_engine(
        conn_str,
        connect_args={"ssl": {"ssl_disabled": False}},
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def get_df(query: str, params: dict = None) -> pd.DataFrame:
    """Chạy SQL → trả pandas DataFrame."""
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


if __name__ == '__main__':
    try:
        with get_engine().connect() as conn:
            row = conn.execute(text("SELECT DATABASE(), NOW()")).fetchone()
            print(f"✅ Kết nối thành công! DB: {row[0]} | Time: {row[1]}")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
