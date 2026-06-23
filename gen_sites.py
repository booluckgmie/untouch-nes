"""Regenerate data/all_sites.json from nd_site_profile table.

Uses nd_site_profile.refid_mcmc so that site IDs match the event_site_refid_mcmc
produced by nes_db._fetch_participants (ep.ref_id → nd_site_profile), aligning
with nadi_pipeline.py which counts by event site rather than home NADI.
"""
import json
from pathlib import Path
from db_configs import get_conn

conn = get_conn()
with conn.cursor() as cur:
    cur.execute("""
        SELECT
            nsp.refid_mcmc,
            COALESCE(nsp.sitename, 'Site-' || nsp.id::text) AS nadi_name,
            COALESCE(st.name, '')                            AS state,
            COALESCE(org.name, '')                           AS tp,
            COALESCE(org.description, '')                    AS dusp,
            COALESCE(reg.bm, '')                             AS region_bm,
            COALESCE(ph.name, '')                            AS phase_name,
            COALESCE(parl.name, '')                          AS parliament_name,
            COALESCE(dun.name, '')                           AS dun_name,
            COALESCE(muk.name, '')                           AS mukim_name
        FROM public.nd_site_profile nsp
        LEFT JOIN public.nd_state       st   ON st.id   = nsp.state_id
        LEFT JOIN public.organizations  org  ON org.id  = nsp.dusp_tp_id
        LEFT JOIN public.nd_region      reg  ON reg.id  = nsp.region_id
        LEFT JOIN public.nd_phases      ph   ON ph.id   = nsp.phase_id
        LEFT JOIN public.nd_parliaments parl ON parl.id = nsp.parliament_rfid
        LEFT JOIN public.nd_duns        dun  ON dun.id  = nsp.dun_rfid
        LEFT JOIN public.nd_mukims      muk  ON muk.id  = nsp.mukim_id
        WHERE nsp.refid_mcmc IS NOT NULL AND nsp.refid_mcmc <> ''
        ORDER BY COALESCE(st.name, ''), COALESCE(nsp.sitename, '')
    """)
    rows = cur.fetchall()
conn.close()

# [refid_mcmc, nadi_name, state, tp, dusp, region_bm, phase_name, parliament_name, dun_name, mukim_name]
sites = [list(r) for r in rows]
print(f"Fetched {len(sites)} sites from DB")

out = Path(__file__).parent / "data" / "all_sites.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps({"sites": sites}), encoding="utf-8")
print(f"Written -> {out}")
