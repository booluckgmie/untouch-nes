# ─────────────────────────────────────────────────────────────────────────────
# cache.py  — simple file-backed JSON cache with TTL
# ─────────────────────────────────────────────────────────────────────────────
import json
import re
import time
from pathlib import Path

_SAFE_KEY = re.compile(r"[^\w\-]")

CACHE_DIR = Path(__file__).parent / "data"
CACHE_DIR.mkdir(exist_ok=True)

# Default TTL = 6 hours  (set to 0 to disable)
DEFAULT_TTL = 6 * 3600


def _path(key: str) -> Path:
    safe = _SAFE_KEY.sub("_", key)
    return CACHE_DIR / f"{safe}.json"


def get(key: str) -> dict | None:
    p = _path(key)
    if not p.exists():
        return None
    meta_p = _path(key + "__meta")
    if meta_p.exists():
        meta = json.loads(meta_p.read_text())
        if DEFAULT_TTL > 0 and time.time() - meta["ts"] > DEFAULT_TTL:
            p.unlink(missing_ok=True)
            meta_p.unlink(missing_ok=True)
            return None
    return json.loads(p.read_text())


def set(key: str, value: dict):
    _path(key).write_text(json.dumps(value, default=str))
    _path(key + "__meta").write_text(json.dumps({"ts": time.time()}))


def invalidate(key: str):
    _path(key).unlink(missing_ok=True)
    _path(key + "__meta").unlink(missing_ok=True)


def invalidate_all():
    for f in CACHE_DIR.glob("*.json"):
        f.unlink(missing_ok=True)


def list_cached() -> list[dict]:
    result = []
    for f in CACHE_DIR.glob("*.json"):
        if f.name.endswith("__meta.json"):
            continue
        meta_p = CACHE_DIR / f.name.replace(".json", "__meta.json")
        ts = None
        if meta_p.exists():
            ts = json.loads(meta_p.read_text()).get("ts")
        result.append({
            "key":  f.stem,
            "size": f.stat().st_size,
            "ts":   ts,
        })
    return sorted(result, key=lambda x: x["key"])
