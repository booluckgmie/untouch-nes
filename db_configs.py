# ─────────────────────────────────────────────────────────────────────────────
# db_configs.py  — connection factory for NES Analytics
# ─────────────────────────────────────────────────────────────────────────────
import psycopg2
import psycopg2.extras
from credential import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

PG_CONFIG = {
    "host":     DB_HOST,
    "port":     DB_PORT,
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "connect_timeout": 10,
}

def get_conn():
    """Return a new psycopg2 connection with sensible defaults."""
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '120s';")
        cur.execute("SET work_mem = '32MB';")
    return conn
