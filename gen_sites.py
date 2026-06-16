"""Regenerate data/all_sites.json from nd_site table."""
import json
from pathlib import Path
from db_configs import get_conn

conn = get_conn()
with conn.cursor() as cur:
    # Diagnostic: show available columns on nd_site_profile so we can
    # identify the correct tp/dusp column names.
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'nd_site_profile'
        ORDER BY ordinal_position
    """)
    cols = cur.fetchall()
    print("nd_site_profile columns:")
    for c in cols:
        print(f"  {c[0]}  ({c[1]})")

    # TODO: replace ''::text with the correct column references once
    # column names are confirmed from the diagnostic output above.
    cur.execute("""
        SELECT ns.refid_mcmc,
               COALESCE(nsp.sitename, 'Site-' || ns.id::text) AS nadi_name,
               COALESCE(st.name, '') AS state,
               ''::text AS tp,
               ''::text AS dusp
        FROM public.nd_site ns
        LEFT JOIN public.nd_site_profile nsp ON nsp.id = ns.site_profile_id
        LEFT JOIN public.nd_state st ON st.id = nsp.state_id
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
