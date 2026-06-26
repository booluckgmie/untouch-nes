"""
Fetch all NES participant data across all subcategories.

Taxonomy from DB:  category_name, pillar_name, sub_pillar_name, deeper_sub_pillar
PDPA:              IC masked (first 8 digits + ****)
Site info:         full site_full CTE (district, zone, technology, mini_nadi, etc.)
Under-12:          included, participant_type column

Supports CLI filters: --from, --to, --tp, --dusp, --phase, --state, --subcategory

Usage:
    python fetch_all_data.py
    python fetch_all_data.py --from 2026-01-01 --to 2026-06-30
    python fetch_all_data.py --subcategory 1,2,3 --from 2026-01-01
    python fetch_all_data.py --tp "MAXIS" --state "Selangor"
"""

import argparse
import io
import sys
from datetime import date

from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")



import pandas as pd
import psycopg2
import psycopg2.extras
from tqdm import tqdm

PG_CONFIG = {
    "host":     "43.217.146.116",
    "port":     5432,
    "user":     "postgres.rc1vzzw53e9mcc2luokr",
    "password": "QAAjdrBjpSjuBLmMpV76",
    "database": "postgres",
}

SUBCATEGORIES = {
    1:  {"label": "NADI4U-Entrepreneur"},
    2:  {"label": "NADI4U-LifelongLearning"},
    3:  {"label": "NADI4U-Wellbeing"},
    4:  {"label": "NADI4U-Awareness"},
    5:  {"label": "NADI4U-GovInit"},
    6:  {"label": "NADI2U-Entrepreneur"},
    7:  {"label": "NADI2U-LifelongLearning"},
    8:  {"label": "NADI2U-Wellbeing"},
    9:  {"label": "NADI2U-Awareness"},
    10: {"label": "NADI2U-GovInit"},
    11: {"label": "OTHERS-Activity"},
    12: {"label": "OTHERS-Training"},
    13: {"label": "OTHERS-Services"},
}

_BATCH       = 5_000
_EVENT_CHUNK = 100


def _arr(ids) -> str:
    return ",".join(f"'{i}'" for i in ids)


def _get_event_ids(conn, subcategory_id: int, start_date, end_date) -> list:
    date_filter = ""
    if start_date:
        date_filter += f"\n  AND e.start_datetime::date >= '{start_date}'"
    if end_date:
        date_filter += f"\n  AND e.end_datetime::date   <= '{end_date}'"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT e.id
            FROM public.nd_event e
            WHERE e.site_id IS NOT NULL
              AND jsonb_typeof(e.site_id) = 'array'
              AND e.subcategory_id = {subcategory_id}
              AND e.program_id IS NOT NULL
              AND e.status_id NOT IN (1, 7, 9)
              {date_filter}
            ORDER BY e.id
        """)
        return [row[0] for row in cur.fetchall()]


def _fetch_chunk(conn, event_ids: list, cur_name: str) -> pd.DataFrame:
    sql = f"""
    WITH site_full AS (
        SELECT
            a.id                                    AS site_id,
            a.refid_mcmc,
            a.refid_tp,
            a.sitename,
            a.fullname                              AS site_fullname,
            d.name                                  AS dusp,
            c.name                                  AS tp,
            e.name                                  AS state,
            f.name                                  AS parliament,
            g.name                                  AS dun,
            j.name                                  AS district,
            COALESCE(i.address1, '') || ' ' || COALESCE(i.address2, '') AS address,
            i.postcode,
            a.latitude,
            a.longtitude,
            k.name                                  AS backhaul,
            l.name                                  AS technology,
            m.area                                  AS zone,
            n.name                                  AS phase,
            a.remark                                AS operation_status,
            a.is_mini                               AS mini_nadi
        FROM nd_site_profile a
        LEFT JOIN nd_region        b ON b.id = a.region_id
        LEFT JOIN organizations    c ON c.id = a.dusp_tp_id AND c.type = 'tp'
        LEFT JOIN organizations    d ON d.id = c.parent_id  AND d.type = 'dusp'
        LEFT JOIN nd_state         e ON e.id = a.state_id
        LEFT JOIN nd_parliaments   f ON f.id = a.parliament_rfid
        LEFT JOIN nd_duns          g ON g.id = a.dun_rfid
        LEFT JOIN nd_site_status   h ON h.id = a.active_status
        LEFT JOIN nd_site_address  i ON i.site_id = a.id
        LEFT JOIN nd_district      j ON j.id = i.district_id
        LEFT JOIN nd_bandwidth     k ON k.id = a.bandwidth
        LEFT JOIN nd_technology    l ON l.id = a.technology
        LEFT JOIN nd_zone          m ON m.id = a.zone_id
        LEFT JOIN nd_phases        n ON n.id = a.phase_id
    )
    SELECT
        -- ===== EVENT TAXONOMY =====
        ev.id                                                               AS event_id,
        cat.name                                                            AS category_name,
        subcat.name                                                         AS pillar_name,
        prog.name                                                           AS sub_pillar_name,
        CASE
            WHEN p.user_type = 'sso_admin' THEN md.name_bi
            ELSE NULL
        END                                                                 AS deeper_sub_pillar,
        ev.program_name,
        -- ===== EVENT TIMING =====
        TO_CHAR(ev.start_datetime, 'FMMM/FMDD/YYYY HH24:MI')               AS event_start,
        TO_CHAR(ev.end_datetime,   'FMMM/FMDD/YYYY HH24:MI')               AS event_end,
        ev.start_datetime::date                                             AS event_startdate,
        EXTRACT(YEAR    FROM ev.start_datetime)::int                        AS event_year,
        EXTRACT(QUARTER FROM ev.start_datetime)::int                        AS event_quarter,
        EXTRACT(MONTH   FROM ev.start_datetime)::int                        AS event_month,
        TO_CHAR(ev.start_datetime, 'FMMonth')                               AS event_month_name,
        TO_CHAR(ev.start_datetime, 'YYYY-MM')                               AS event_year_month,
        TO_CHAR(ev.start_datetime, 'YYYY-"Q"Q')                             AS event_year_quarter,
        ROUND(
            EXTRACT(EPOCH FROM (ev.end_datetime - ev.start_datetime)) / 3600.0,
            2
        )                                                                   AS event_duration_hours,
        TO_CHAR(ev.created_at, 'FMMM/FMDD/YYYY HH24:MI:SS')                AS event_created_at,
        p.full_name                                                         AS event_created_by_name,
        p.user_type                                                         AS event_created_by_type,
        -- ===== PARTICIPATION =====
        ep.id                                                               AS participant_id,
        ep.attendance,
        ep.member_id,
        ep.participant_under_twelve_id,
        CASE
            WHEN ep.member_id IS NOT NULL                   THEN 'Member'
            WHEN ep.participant_under_twelve_id IS NOT NULL THEN 'Under 12'
            ELSE 'Unknown'
        END                                                                 AS participant_type,
        -- ===== PARTICIPANT DETAILS =====
        COALESCE(nmp.fullname, u12.fullname)                                AS participant_name,
        CASE
            WHEN nmp.identity_no IS NOT NULL
            THEN SUBSTRING(LPAD(nmp.identity_no::text, 12, '0') FROM 1 FOR 8) || '****'
            ELSE NULL
        END                                                                 AS identity_no,
        COALESCE(g.bm, '')                                                  AS gender_name,
        TO_CHAR(nmp.join_date, 'FMMM/FMDD/YYYY')                           AS join_date,
        -- ===== SITE INFO =====
        ep.ref_id                                                           AS site_id,
        sf.sitename                                                         AS site_name,
        sf.site_fullname,
        sf.refid_tp                                                         AS site_refid_tp,
        sf.refid_mcmc                                                       AS site_refid_mcmc,
        sf.dusp                                                             AS site_dusp,
        sf.tp                                                               AS site_tp,
        sf.state                                                            AS site_state,
        sf.parliament                                                       AS site_parliament,
        sf.dun                                                              AS site_dun,
        sf.district                                                         AS site_district,
        sf.address                                                          AS site_address,
        sf.postcode                                                         AS site_postcode,
        sf.phase                                                            AS site_phase,
        sf.zone                                                             AS site_zone,
        sf.technology                                                       AS site_technology,
        sf.backhaul                                                         AS site_backhaul,
        sf.latitude                                                         AS site_latitude,
        sf.longtitude                                                       AS site_longitude,
        sf.mini_nadi                                                        AS site_mini_nadi,
        sf.operation_status                                                 AS site_operation_status
    FROM nd_event ev
    INNER JOIN nd_event_participant            ep     ON ep.event_id  = ev.id
    LEFT JOIN  profiles                        p      ON p.id         = ev.created_by
    LEFT JOIN  nd_event_category               cat    ON cat.id       = ev.category_id
    LEFT JOIN  nd_event_subcategory            subcat ON subcat.id    = ev.subcategory_id
    LEFT JOIN  nd_event_program                prog   ON prog.id      = ev.program_id
    LEFT JOIN  nd_sso_profile                  sso    ON sso.user_id  = p.id
    LEFT JOIN  master_data                     md     ON md.id        = sso.sso_type_id
    LEFT JOIN  nd_member_profile               nmp    ON nmp.id       = ep.member_id
    LEFT JOIN  nd_event_participant_under_twelve u12  ON u12.id       = ep.participant_under_twelve_id
    LEFT JOIN  site_full                       sf     ON sf.site_id   = ep.ref_id
    LEFT JOIN  nd_genders                      g      ON g.id         = COALESCE(nmp.gender, u12.gender)
    WHERE ev.id = ANY(ARRAY[{_arr(event_ids)}]::uuid[])
    """
    rows = []
    with conn.cursor(cur_name, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = _BATCH
        cur.execute(sql)
        while True:
            batch = cur.fetchmany(_BATCH)
            if not batch:
                break
            rows.extend(batch)
    return pd.DataFrame(rows)


def fetch_subcategory(sub_id: int, start_date=None, end_date=None) -> pd.DataFrame:
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    try:
        with conn.cursor() as cfg:
            cfg.execute("SET statement_timeout = 0;")
            cfg.execute("SET work_mem = '64MB';")

        print(f"  Getting event IDs ...")
        event_ids = _get_event_ids(conn, sub_id, start_date, end_date)
        if not event_ids:
            print(f"  No events found — skipping.")
            return pd.DataFrame()

        chunks = [event_ids[i:i + _EVENT_CHUNK] for i in range(0, len(event_ids), _EVENT_CHUNK)]
        print(f"  {len(event_ids):,} events -> {len(chunks)} chunks")

        frames = []
        with tqdm(total=len(event_ids), desc=f"  sub{sub_id}", unit="evt", dynamic_ncols=True) as pbar:
            for idx, chunk in enumerate(chunks, 1):
                df = _fetch_chunk(conn, chunk, f"nes_{sub_id}_{idx}")
                conn.commit()
                frames.append(df)
                pbar.update(len(chunk))

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)
    finally:
        conn.close()


def apply_filters(df: pd.DataFrame, tp=None, dusp=None, phase=None, state=None) -> pd.DataFrame:
    if tp:
        df = df[df["site_tp"].str.contains(tp, case=False, na=False)]
    if dusp:
        df = df[df["site_dusp"].str.contains(dusp, case=False, na=False)]
    if phase:
        df = df[df["site_phase"].str.contains(phase, case=False, na=False)]
    if state:
        df = df[df["site_state"].str.contains(state, case=False, na=False)]
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch all NES participant data.")
    parser.add_argument("--from",        dest="date_from",   default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--to",          dest="date_to",     default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--tp",          dest="tp",          default=None, help="Filter by TP (partial)")
    parser.add_argument("--dusp",        dest="dusp",        default=None, help="Filter by DUSP (partial)")
    parser.add_argument("--phase",       dest="phase",       default=None, help="Filter by phase (partial)")
    parser.add_argument("--state",       dest="state",       default=None, help="Filter by state (partial)")
    parser.add_argument("--subcategory", dest="subcategory", default=None, help="Comma-separated IDs e.g. 1,2,3")
    parser.add_argument("--out",         dest="out",         default=None, help="Output CSV filename")
    args = parser.parse_args()

    if args.subcategory:
        try:
            sub_ids = [int(x.strip()) for x in args.subcategory.split(",")]
            invalid = [s for s in sub_ids if s not in SUBCATEGORIES]
            if invalid:
                sys.exit(f"Invalid subcategory IDs: {invalid}. Valid: {sorted(SUBCATEGORIES)}")
        except ValueError:
            sys.exit("--subcategory must be comma-separated integers e.g. 1,2,3")
    else:
        sub_ids = sorted(SUBCATEGORIES.keys())

    print(f"\nFetching subcategories: {sub_ids}")
    if args.date_from or args.date_to:
        print(f"Date range: {args.date_from or 'any'} to {args.date_to or 'any'}")
    print()

    all_frames = []
    for sub_id in sub_ids:
        label = SUBCATEGORIES[sub_id]["label"]
        print(f"[{sub_id}/13] {label}")
        df = fetch_subcategory(sub_id, args.date_from, args.date_to)
        if not df.empty:
            all_frames.append(df)
            print(f"  -> {len(df):,} rows\n")
        else:
            print()

    if not all_frames:
        print("No data returned.")
        return

    print("Combining ...")
    combined = pd.concat(all_frames, ignore_index=True)

    if any([args.tp, args.dusp, args.phase, args.state]):
        before = len(combined)
        combined = apply_filters(combined, args.tp, args.dusp, args.phase, args.state)
        print(f"Filters applied: {before:,} -> {len(combined):,} rows")

    combined.sort_values(
        ["category_name", "pillar_name", "site_state", "site_name", "event_startdate"],
        inplace=True, ignore_index=True,
    )

    print(f"\n{'='*60}")
    print(f"  Total rows         : {len(combined):,}")
    print(f"  Unique participants: {combined['participant_id'].nunique():,}")
    print(f"  Unique events      : {combined['event_id'].nunique():,}")
    print(f"  Unique NADI        : {combined['site_name'].nunique():,}")
    print(f"  Categories         : {sorted(combined['category_name'].dropna().unique())}")
    print(f"  Pillars            : {sorted(combined['pillar_name'].dropna().unique())}")
    if combined['deeper_sub_pillar'].notna().any():
        print(f"  Deeper sub-pillars : {sorted(combined['deeper_sub_pillar'].dropna().unique())}")
    print(f"  Date range         : {combined['event_startdate'].min()} to {combined['event_startdate'].max()}")
    print(f"{'='*60}\n")

    today = date.today().strftime("%Y%m%d")
    date_tag = ""
    if args.date_from or args.date_to:
        date_tag = f"_{args.date_from or 'start'}_{args.date_to or 'end'}"
    out_path = Path(args.out) if args.out else Path(f"nes_all_data{date_tag}_{today}.csv")

    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Saved -> {out_path}  ({len(combined):,} rows, {len(combined.columns)} columns)")


if __name__ == "__main__":
    main()
