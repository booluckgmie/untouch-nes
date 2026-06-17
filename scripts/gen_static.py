"""
Generate static JSON files for Netlify hosting.
Writes to public/data/:
  meta.json    — subcategory list
  sites.json   — 1099 NADI sites
  sub_1.json   — Entrepreneur data
  sub_2.json   — LifelongLearning data
  sub_3.json   — NADI4U Wellbeing data
  sub_4.json   — NADI4U Awareness data
  sub_8.json   — NADI2U Wellbeing data

Run: python scripts/gen_static.py
"""
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
import nes_db as DB
from db_configs import get_conn

OUT = ROOT / "public" / "data"
OUT.mkdir(parents=True, exist_ok=True)

SITES_FILE = ROOT / "data" / "all_sites.json"
if not SITES_FILE.exists():
    print("all_sites.json missing — regenerating from DB…")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                ns.refid_mcmc,
                COALESCE(nsp.sitename, 'Site-' || ns.id::text)  AS nadi_name,
                COALESCE(st.name, '')                            AS state,
                COALESCE(tpd.name, '')                           AS tp,
                COALESCE(tpd.description, '')                    AS dusp,
                COALESCE(reg.bm, '')                             AS region_bm,
                COALESCE(ph.name, '')                            AS phase_name,
                COALESCE(parl.name, '')                          AS parliament_name,
                COALESCE(dun.name, '')                           AS dun_name,
                COALESCE(muk.name, '')                           AS mukim_name
            FROM public.nd_site ns
            LEFT JOIN public.nd_site_profile       nsp  ON nsp.id  = ns.site_profile_id
            LEFT JOIN public.nd_state              st   ON st.id   = nsp.state_id
            LEFT JOIN public.nd_tech_partner_dusp  tpd  ON tpd.id  = nsp.dusp_tp_id
            LEFT JOIN public.nd_region             reg  ON reg.id  = nsp.region_id
            LEFT JOIN public.nd_phases             ph   ON ph.id   = nsp.phase_id
            LEFT JOIN public.nd_parliaments        parl ON parl.id = nsp.parliament_rfid
            LEFT JOIN public.nd_duns               dun  ON dun.id  = nsp.dun_rfid
            LEFT JOIN public.nd_mukims             muk  ON muk.id  = nsp.mukim_id
            WHERE ns.refid_mcmc IS NOT NULL
            ORDER BY COALESCE(st.name,''), COALESCE(nsp.sitename,'')
        """)
        rows = cur.fetchall()
    conn.close()
    SITES_FILE.parent.mkdir(parents=True, exist_ok=True)
    SITES_FILE.write_text(json.dumps({"sites": [list(r) for r in rows]}), encoding="utf-8")
    print(f"  Written {len(rows)} sites -> {SITES_FILE}")

sites = json.loads(SITES_FILE.read_text(encoding="utf-8"))["sites"]
_SITE_COLS = ["refid_mcmc", "nadi_name", "state", "tp", "dusp",
              "region_bm", "phase_name", "parliament_name", "dun_name", "mukim_name"]
df_sites = pd.DataFrame(sites, columns=_SITE_COLS[:len(sites[0]) if sites else len(_SITE_COLS)])

START_DATE = "2026-01-01"

# ── Write sites.json ──────────────────────────────────────────────────────────
(OUT / "sites.json").write_text(
    json.dumps({"ok": True, "sites": sites}), encoding="utf-8"
)
print(f"sites.json  ({len(sites)} sites)")

# ── Fetch each subcategory ────────────────────────────────────────────────────
meta_subs = []
for sub_id in [1, 2, 3, 4, 8]:
    info = DB.SUBCATEGORIES.get(sub_id, {})
    print(f"\n=== sub{sub_id} {info.get('label','')} ===")

    conn = get_conn()
    try:
        event_ids = DB._get_event_ids(conn, sub_id, START_DATE, None)
    finally:
        conn.close()

    if not event_ids:
        print(f"  No events — skipping")
        meta_subs.append({
            "id": sub_id, "cat": info.get("cat",""), "mod": info.get("mod",""),
            "label": info.get("label",""), "cached": False, "fetched": None,
        })
        continue

    chunks = [event_ids[i:i+DB._EVENT_CHUNK] for i in range(0, len(event_ids), DB._EVENT_CHUNK)]
    print(f"  {len(event_ids):,} events | {len(chunks)} chunks")

    ev_frames, par_frames = [], []
    conn = get_conn()
    try:
        for idx, chunk in enumerate(chunks, 1):
            print(f"  chunk {idx}/{len(chunks)}", end="\r")
            ev_frames.append(DB._fetch_events(conn, chunk))
            par_frames.append(DB._fetch_participants(conn, chunk, f"gs_{sub_id}_{idx}"))
            conn.commit()
    finally:
        conn.close()
    print()

    df_ev  = pd.concat(ev_frames,  ignore_index=True).drop_duplicates("event_id")
    df_par = pd.concat(par_frames, ignore_index=True)
    df     = df_par.merge(df_ev, on="event_id", how="left")
    df     = DB.apply_sso_remap(df, sub_id)
    print(f"  {len(df):,} participant rows | {df['event_id'].nunique():,} unique events")

    fetched = datetime.now().strftime("%d %b %Y, %H:%M")
    payload = {
        "sub_id":     sub_id,
        "fetched":    fetched,
        "start_date": START_DATE,
        "end_date":   None,
        "nadi":       DB.build_nadi_index(df, df_sites, sub_id),
        "monthly":    DB.build_monthly(df, df_sites, sub_id),
        "weekly":     DB.build_weekly(df, df_sites, sub_id),
    }

    out_path = OUT / f"sub_{sub_id}.json"
    out_path.write_text(json.dumps(payload), encoding="utf-8")
    size_kb = out_path.stat().st_size // 1024
    print(f"  -> sub_{sub_id}.json ({size_kb} KB)")

    meta_subs.append({
        "id": sub_id, "cat": info.get("cat",""), "mod": info.get("mod",""),
        "label": info.get("label",""), "cached": True, "fetched": fetched,
    })

# ── Write meta.json ───────────────────────────────────────────────────────────
(OUT / "meta.json").write_text(
    json.dumps({"ok": True, "subcategories": meta_subs}), encoding="utf-8"
)
print(f"\nmeta.json   ({len(meta_subs)} subcategories)")
print("\nDone. Files in public/data/")
