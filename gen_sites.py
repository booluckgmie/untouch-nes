"""Regenerate data/all_sites.json from nd_site table."""
import json
from pathlib import Path
from db_configs import get_conn

conn = get_conn()
with conn.cursor() as cur:
    cur.execute("""
        SELECT ns.refid_mcmc,
               COALESCE(nsp.sitename, 'Site-' || ns.id::text) AS nadi_name,
               COALESCE(st.name, '')        AS state,
               COALESCE(org.name, '')       AS tp,
               COALESCE(org.description, '') AS dusp
        FROM public.nd_site ns
        LEFT JOIN public.nd_site_profile nsp ON nsp.id = ns.site_profile_id
        LEFT JOIN public.nd_state st         ON st.id  = nsp.state_id
        LEFT JOIN public.organizations org   ON org.id = nsp.dusp_tp_id
        WHERE ns.refid_mcmc IS NOT NULL
        ORDER BY COALESCE(st.name, ''), COALESCE(nsp.sitename, '')
    """)
    rows = cur.fetchall()
conn.close()

sites = [[r[0], r[1], r[2], r[3], r[4]] for r in rows]
print(f"Fetched {len(sites)} sites from DB")

out = Path(__file__).parent / "data" / "all_sites.json"
out.write_text(json.dumps({"sites": sites}), encoding="utf-8")
print(f"Written -> {out}")
