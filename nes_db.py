import re
from datetime import date
import psycopg2
import psycopg2.extras
import pandas as pd
from tqdm import tqdm

_ILLEGAL_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

PG_CONFIG = {
    "host": "43.217.146.116",
    "port": 5432,
    "user": "postgres.rc1vzzw53e9mcc2luokr",
    "password": "QAAjdrBjpSjuBLmMpV76",
    "database": "postgres",
}

# ── Subcategory IDs ────────────────────────────────────────────────────────────
# NADI4U  : 1 Entrepreneur | 2 Lifelong Learning | 3 Wellbeing | 4 Awareness | 5 Gov Init
# NADI2U  : 6 Entrepreneur | 7 Lifelong Learning | 8 Wellbeing | 9 Awareness | 10 Gov Init
# OTHERS  : 11 Activity    | 12 Training          | 13 Services

# ── Optional date filters (set to None to disable) ────────────────────────────
START_DATE = None
END_DATE   = None

SUBCATEGORIES = {
    1:  "NADI4U-Entrepreneur",
    2:  "NADI4U-LifelongLearning",
    3:  "NADI4U-Wellbeing",
    4:  "NADI4U-Awareness",
    5:  "NADI4U-GovInit",
    6:  "NADI2U-Entrepreneur",
    7:  "NADI2U-LifelongLearning",
    8:  "NADI2U-Wellbeing",
    9:  "NADI2U-Awareness",
    10: "NADI2U-GovInit",
    11: "OTHERS-Activity",
    12: "OTHERS-Training",
    13: "OTHERS-Services",
}

_BATCH       = 5_000
_EVENT_CHUNK = 100   # events per chunk; lower if server still OOMs


def _arr(ids) -> str:
    return ",".join(f"'{i}'" for i in ids)


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Get event IDs for a given subcategory
# ─────────────────────────────────────────────────────────────────────────────

def _get_event_ids(conn, subcategory_id: int) -> list:
    date_filter = ""
    if START_DATE:
        date_filter += f"\n  AND e.start_datetime::date >= '{START_DATE}'"
    if END_DATE:
        date_filter += f"\n  AND e.end_datetime::date   <= '{END_DATE}'"
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


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Fetch event taxonomy + event-site geographic/admin columns
#
#   DISTINCT ON (e.id): if an event has multiple sites in its JSONB array,
#   we take the first (lowest site_id integer) to avoid row explosion.
#
#   New columns vs old version:
#     event_site_profile_id, event_site_name, event_site_refid_mcmc,
#     event_site_tp, event_site_dusp, event_site_region, event_site_phase,
#     event_site_parliament, event_site_dun, event_site_mukim, event_site_state
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_events(conn, event_ids: list) -> pd.DataFrame:
    sql = f"""
    SELECT DISTINCT ON (e.id)
        e.id                                                AS event_id,
        COALESCE(ec.name,  '')                              AS event_category,
        COALESCE(esc.name, '')                              AS program_module,
        COALESCE(ep.name,  '')                              AS program,
        COALESCE(e.program_name, ep.name, '')               AS program_name,
        e.start_datetime::date                              AS event_startdate,
        e.end_datetime::date                                AS event_lastdate,
        COALESCE(pm.name, '')                               AS program_method,
        COALESCE(e.description, '')                         AS event_description,
        e.requester_id,
        -- ── Event-site geographic / admin columns ──────────────────────────
        ev_nsp.id                                           AS event_site_profile_id,
        COALESCE(ev_nsp.sitename, '')                       AS event_site_name,
        COALESCE(ev_nsp.refid_mcmc, '')                     AS event_site_refid_mcmc,
        COALESCE(ev_org.name, '')                           AS event_site_tp,
        COALESCE(ev_org.description, '')                    AS event_site_dusp,
        COALESCE(ev_reg.bm, '')                             AS event_site_region,
        COALESCE(ev_ph.name, '')                            AS event_site_phase,
        COALESCE(ev_parl.name, '')                          AS event_site_parliament,
        COALESCE(ev_dun.name, '')                           AS event_site_dun,
        COALESCE(ev_muk.name, '')                           AS event_site_mukim,
        COALESCE(ev_st.name, '')                            AS event_site_state
    FROM public.nd_event e
    LEFT JOIN public.nd_event_program     ep   ON ep.id  = e.program_id
    LEFT JOIN public.nd_event_subcategory esc  ON esc.id = ep.subcategory_id
    LEFT JOIN public.nd_event_category    ec   ON ec.id  = esc.category_id
    LEFT JOIN public.nd_program_method    pm   ON pm.id  = e.program_method
    -- ── Expand event site_id JSONB array → first valid site_profile ────────
    LEFT JOIN LATERAL jsonb_array_elements_text(e.site_id) AS j(site_id_txt) ON true
    LEFT JOIN public.nd_site_profile  ev_nsp  ON ev_nsp.id = j.site_id_txt::int
    LEFT JOIN public.organizations    ev_org  ON ev_org.id  = ev_nsp.dusp_tp_id
    LEFT JOIN public.nd_region        ev_reg  ON ev_reg.id  = ev_nsp.region_id
    LEFT JOIN public.nd_phases        ev_ph   ON ev_ph.id   = ev_nsp.phase_id
    LEFT JOIN public.nd_parliaments   ev_parl ON ev_parl.id = ev_nsp.parliament_rfid
    LEFT JOIN public.nd_duns          ev_dun  ON ev_dun.id  = ev_nsp.dun_rfid
    LEFT JOIN public.nd_mukims        ev_muk  ON ev_muk.id  = ev_nsp.mukim_id
    LEFT JOIN public.nd_state         ev_st   ON ev_st.id   = ev_nsp.state_id
    WHERE e.id = ANY(ARRAY[{_arr(event_ids)}]::uuid[])
    ORDER BY e.id, j.site_id_txt::int ASC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall())


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Fetch participants + member's home NADI site
#
#   Home NADI columns (unchanged from v1):
#     refid_mcmc, nadi_name, state, standard_code
#
#   These are distinct from event_site_* columns — a participant's home NADI
#   may differ from the site where the event was held.
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_participants(conn, event_ids: list, cur_name: str) -> pd.DataFrame:
    sql = f"""
    SELECT
        ep.event_id,
        ep.id                                               AS participant_id,
        ep.ref_id                                           AS participant_ref_id,
        ep.attendance,
        ep.verified,
        ep.registered_as_new_member,
        mp.id                                               AS member_id,
        mp.ref_id                                           AS member_ref_id,
        COALESCE(mp.fullname, '')                           AS fullname,
        COALESCE(mp.identity_no, '')                        AS ic_number,
        COALESCE(mp.identity_no_type::text, '')             AS ic_type,
        COALESCE(mp.mobile_no, '')                          AS mobile_no,
        COALESCE(mp.email, '')                              AS email,
        mp.dob,
        mp.age,
        COALESCE(g.bm,  '')                                 AS gender,
        COALESCE(r.bm,  '')                                 AS race,
        COALESCE(mp.status_membership::text, '')            AS membership_status,
        COALESCE(mp.oku_status::text, 'false')              AS oku_status,
        COALESCE(mp.education_level::text, '')              AS education_level,
        COALESCE(mp.income_range::text, '')                 AS income_range,
        COALESCE(mp.nationality_id::text, '')               AS nationality_id,
        COALESCE(mp.registration_status::text, '')          AS registration_status,
        -- ── Member's home NADI site ────────────────────────────────────────
        ns.refid_mcmc,
        COALESCE(nsp.sitename, 'Site-' || ns.id::text)     AS nadi_name,
        COALESCE(st.name, '')                               AS state,
        ns.standard_code,
        -- ── Under-12 companion ────────────────────────────────────────────
        u12.id                                              AS under12_id,
        COALESCE(u12.fullname,  '')                         AS under12_name,
        COALESCE(u12.mobile_no, '')                         AS under12_phone,
        COALESCE(g2.bm, '')                                 AS under12_gender
    FROM public.nd_event_participant ep
    LEFT JOIN public.nd_member_profile              mp  ON mp.id  = ep.member_id
    LEFT JOIN public.nd_site                        ns  ON ns.id  = mp.ref_id
    LEFT JOIN public.nd_site_profile                nsp ON nsp.id = ns.site_profile_id
    LEFT JOIN public.nd_state                       st  ON st.id  = nsp.state_id
    LEFT JOIN public.nd_genders g   ON g.id  = mp.gender
    LEFT JOIN public.nd_races   r   ON r.id  = mp.race_id
    LEFT JOIN public.nd_event_participant_under_twelve u12
                                    ON u12.id = ep.participant_under_twelve_id
    LEFT JOIN public.nd_genders g2  ON g2.id = u12.gender
    WHERE ep.event_id = ANY(ARRAY[{_arr(event_ids)}]::uuid[])
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


# ─────────────────────────────────────────────────────────────────────────────
# Main fetch — iterates chunks, merges events + participants
# ─────────────────────────────────────────────────────────────────────────────

def fetch(subcategory_id: int) -> pd.DataFrame:
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    try:
        with conn.cursor() as cfg:
            cfg.execute("SET statement_timeout = 0;")
            cfg.execute("SET work_mem = '32MB';")

        print(f"Getting event IDs for subcategory_id={subcategory_id} ...")
        event_ids = _get_event_ids(conn, subcategory_id)
        chunks    = [event_ids[i:i + _EVENT_CHUNK] for i in range(0, len(event_ids), _EVENT_CHUNK)]
        print(f"{len(event_ids):,} events → {len(chunks)} chunks of {_EVENT_CHUNK}")

        if not event_ids:
            print("No events found — skipping.")
            return pd.DataFrame()

        ev_frames:  list = []
        par_frames: list = []

        with tqdm(total=len(event_ids), desc=f"sub{subcategory_id}", unit="evt", dynamic_ncols=True) as pbar:
            for idx, chunk in enumerate(chunks, 1):
                df_ev  = _fetch_events(conn, chunk)
                df_par = _fetch_participants(conn, chunk, f"nes_par_{idx}")
                conn.commit()
                ev_frames.append(df_ev)
                par_frames.append(df_par)
                pbar.update(len(chunk))
                pbar.set_postfix(
                    evrows=f"{sum(len(f) for f in ev_frames):,}",
                    parrows=f"{sum(len(f) for f in par_frames):,}",
                    chunk=f"{idx}/{len(chunks)}",
                )

        df_events = pd.concat(ev_frames,  ignore_index=True).drop_duplicates(subset=["event_id"])
        df_pars   = pd.concat(par_frames, ignore_index=True)

        print(f"\nMerging {len(df_events):,} events × {len(df_pars):,} participants ...")
        df = df_pars.merge(df_events, on="event_id", how="left")

        if df.empty:
            print("No data returned.")
            return df

        df.sort_values(
            ["event_site_state", "event_site_name", "event_startdate", "event_id", "fullname"],
            inplace=True, ignore_index=True,
        )
        print(
            f"Rows        : {len(df):,}\n"
            f"Event sites : {df['event_site_name'].nunique():,}\n"
            f"Events      : {df['event_id'].nunique():,}\n"
            f"Participants: {df['participant_id'].nunique():,}"
        )
        return df
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# All-sites reference table  (all 1099 NADI — independent of subcategory)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_all_sites() -> pd.DataFrame:
    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    ns.refid_mcmc,
                    ns.standard_code,
                    COALESCE(nsp.sitename, 'Site-' || ns.id::text) AS nadi_name,
                    COALESCE(st.name, '')                           AS state
                FROM public.nd_site            ns
                LEFT JOIN public.nd_site_profile nsp ON nsp.id = ns.site_profile_id
                LEFT JOIN public.nd_state         st  ON st.id  = nsp.state_id
                ORDER BY st.name, nsp.sitename
            """)
            df = pd.DataFrame(cur.fetchall())
    finally:
        conn.close()
    print(f"All sites loaded: {len(df):,} rows")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Site-level pivot  (programs × events/pax)
#
#   Pivot index uses member's home NADI (refid_mcmc, nadi_name, state) so that
#   the pivot matches the all-sites reference table for a full 1099-site view.
#   all_programs — fixed program list for consistent columns across monthly sheets.
# ─────────────────────────────────────────────────────────────────────────────

def build_site_pivot(
    df: pd.DataFrame,
    df_sites: pd.DataFrame,
    all_programs: list | None = None,
) -> pd.DataFrame:
    all_idx = (
        df_sites[["refid_mcmc", "nadi_name", "state"]]
        .drop_duplicates()
        .set_index(["refid_mcmc", "nadi_name", "state"])
        .index
    )

    df_pax  = df[df["participant_id"].notna()].copy()
    df_prog = df_pax[df_pax["program"].notna() & (df_pax["program"] != "")].copy()

    agg = (
        df_prog
        .groupby(["refid_mcmc", "nadi_name", "state", "program"])
        .agg(
            events=("event_id",       "nunique"),
            pax   =("participant_id", "nunique"),
        )
        .reset_index()
    )

    fixed_progs = all_programs if all_programs is not None else (
        sorted(agg["program"].unique()) if not agg.empty else []
    )
    ordered = []
    for p in fixed_progs:
        ordered += [(p, "Sum of events"), (p, "Sum of pax")]
    ordered += [("Total", "Sum of events"), ("Total", "Sum of pax")]

    if agg.empty:
        empty = pd.DataFrame(0, index=all_idx, columns=pd.MultiIndex.from_tuples(ordered))
        return empty

    pivot = agg.pivot_table(
        index     =["refid_mcmc", "nadi_name", "state"],
        columns   ="program",
        values    =["events", "pax"],
        aggfunc   ="sum",
        fill_value=0,
    )
    pivot = pivot.swaplevel(axis=1).sort_index(axis=1, level=0)
    pivot.columns = pd.MultiIndex.from_tuples(
        [(prog, "Sum of events" if metric == "events" else "Sum of pax")
         for prog, metric in pivot.columns]
    )

    # True totals — avoids double-counting; captures no-program events too
    true_ev  = df_pax.groupby(["refid_mcmc", "nadi_name", "state"])["event_id"].nunique()
    true_pax = df_pax.groupby(["refid_mcmc", "nadi_name", "state"])["participant_id"].nunique()
    pivot[("Total", "Sum of events")] = true_ev
    pivot[("Total", "Sum of pax")]    = true_pax

    # Fill any missing fixed-program columns (months with no data for a program)
    for col in ordered:
        if col not in pivot.columns:
            pivot[col] = 0

    pivot = pivot[[c for c in ordered if c in pivot.columns]]
    return pivot.reindex(all_idx, fill_value=0).fillna(0).astype(int)


# ─────────────────────────────────────────────────────────────────────────────
# Monthly pivot sheets  (one pivot per calendar month + "All" summary)
#
#   Columns are fixed to the full program list so all sheets are identical
#   in structure — months with no data for a program show zero columns.
# ─────────────────────────────────────────────────────────────────────────────

def build_monthly_sheets(df: pd.DataFrame, df_sites: pd.DataFrame) -> dict:
    df = df.copy()
    df["_period"] = pd.to_datetime(df["event_startdate"], errors="coerce").dt.to_period("M")

    # Derive full program list from ALL months so columns stay fixed per sheet
    all_programs = sorted(
        df.loc[df["program"].notna() & (df["program"] != ""), "program"].unique()
    )

    sheets = {}
    for period in sorted(df["_period"].dropna().unique()):
        df_m  = df[df["_period"] == period]
        sheet = period.strftime("%b %Y")   # e.g. "Jan 2026"
        sheets[sheet] = build_site_pivot(df_m, df_sites, all_programs)

    sheets["All"] = build_site_pivot(df, df_sites, all_programs)
    return sheets


# ─────────────────────────────────────────────────────────────────────────────
# Save helpers
# ─────────────────────────────────────────────────────────────────────────────

def save_raw(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)
    print(f"Raw saved  → {path}  ({len(df):,} rows)")


def _sanitize(frame: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters that cause openpyxl to reject cells."""
    frame = frame.copy()
    for col in frame.select_dtypes(include="object").columns:
        frame[col] = frame[col].apply(
            lambda v: _ILLEGAL_CHARS.sub("", v) if isinstance(v, str) else v
        )
    return frame


def save_to_excel(path: str, sheets: dict):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, frame in sheets.items():
            _sanitize(frame).to_excel(
                writer,
                sheet_name=sheet_name[:31],
                index=isinstance(frame.index, pd.MultiIndex) or bool(frame.index.name),
            )
    print(f"Excel saved → {path}  ({len(sheets)} sheets)")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df_all_sites = fetch_all_sites()

    for sub_id, sub_label in SUBCATEGORIES.items():
        print(f"\n{'='*60}")
        print(f"Subcategory {sub_id}: {sub_label}")
        print(f"{'='*60}")

        df = fetch(sub_id)
        if df.empty:
            print("No data — skipping.")
            continue

        prefix = f"nes_sub{sub_id:02d}_{sub_label}_{date.today().strftime('%Y%m%d')}"

        # 1. Raw participant-level CSV (includes all event_site_* columns)
        save_raw(df, f"{prefix}_raw.csv")

        # 2. Monthly pivot Excel (one sheet per month + All)
        sheets = build_monthly_sheets(df, df_all_sites)
        save_to_excel(f"{prefix}.xlsx", sheets)
