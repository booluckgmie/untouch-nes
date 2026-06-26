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

def _require(key: str) -> str:
    v = os.environ.get(key, "").strip()
    if not v:
        raise RuntimeError(
            f"Required environment variable {key!r} is not set. "
            "Add it to your .env file or export it in your shell."
        )
    return v

DB_HOST     = _require("DB_HOST")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
DB_USER     = _require("DB_USER")
DB_PASSWORD = _require("DB_PASSWORD")
DB_NAME     = os.environ.get("DB_NAME", "postgres")
