# ─────────────────────────────────────────────────────────────────────────────
# credential.py  — NES Analytics DB credentials
# Loads from .env file or environment variables — do NOT hardcode secrets here
# ─────────────────────────────────────────────────────────────────────────────
import os
from pathlib import Path

_env = Path(__file__).parent / ".env"
if _env.exists():
    for _line in _env.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

DB_HOST     = os.environ.get("DB_HOST",     "43.217.146.116")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
DB_USER     = os.environ.get("DB_USER",     "postgres.rc1vzzw53e9mcc2luokr")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME     = os.environ.get("DB_NAME",     "postgres")
