# ─────────────────────────────────────────────────────────────────────────────
# db_configs.py  — connection factory for NES Analytics
# ─────────────────────────────────────────────────────────────────────────────
import os
import psycopg2
import psycopg2.extras
from credential import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

PG_CONFIG = {
    "host": os.environ.get("DB_HOST", "").strip(), # .strip() removes trailing/leading spaces
    "port": os.environ.get("DB_PORT", "").strip(),
    "user": os.environ.get("DB_USER", "").strip(),
    "password": os.environ.get("DB_PASSWORD", ""),
    "database": os.environ.get("DB_NAME", "").strip(),
}

def get_conn():
    """Return a new psycopg2 connection with sensible defaults."""
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '120s';")
        cur.execute("SET work_mem = '32MB';")
    return conn
