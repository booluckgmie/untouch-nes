#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run.sh  — NES Analytics Dashboard local startup
# ─────────────────────────────────────────────────────────────────────────────
set -e
cd "$(dirname "$0")"

echo "=== NES Analytics Dashboard ==="
echo ""

# 1. Check Python
python3 --version || { echo "ERROR: python3 not found"; exit 1; }

# 2. Install deps if needed
echo "Checking dependencies..."
python3 -c "import flask, psycopg2, pandas" 2>/dev/null || {
    echo "Installing: flask psycopg2-binary pandas"
    pip install flask psycopg2-binary pandas --break-system-packages -q
}
echo "  ✓ Dependencies OK"

# 3. Test DB connectivity
echo "Testing DB connection..."
python3 -c "
from db_configs import get_conn
conn = get_conn()
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM public.nd_site')
n = cur.fetchone()[0]
conn.close()
print(f'  ✓ DB connected — {n} sites in nd_site')
" || { echo "  ✗ DB connection failed — check credential.py"; exit 1; }

# 4. Start Flask
echo ""
echo "Starting dashboard at http://localhost:5001"
echo "Use Ctrl+C to stop"
echo ""
python3 app.py
