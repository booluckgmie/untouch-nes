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
    # id: {cat, mod, label}
    # cat   = top-level category  (NADI4U / NADI2U / OTHERS)
    # mod   = pillar / module name
    # label = combined slug used for filenames and display
    1:  {"cat": "NADI4U", "mod": "Entrepreneur",     "label": "NADI4U-Entrepreneur"},
    2:  {"cat": "NADI4U", "mod": "LifelongLearning", "label": "NADI4U-LifelongLearning"},
    3:  {"cat": "NADI4U", "mod": "Wellbeing",        "label": "NADI4U-Wellbeing"},
    4:  {"cat": "NADI4U", "mod": "Awareness",        "label": "NADI4U-Awareness"},
    5:  {"cat": "NADI4U", "mod": "GovInit",          "label": "NADI4U-GovInit"},
    6:  {"cat": "NADI2U", "mod": "Entrepreneur",     "label": "NADI2U-Entrepreneur"},
    7:  {"cat": "NADI2U", "mod": "LifelongLearning", "label": "NADI2U-LifelongLearning"},
    8:  {"cat": "NADI2U", "mod": "Wellbeing",        "label": "NADI2U-Wellbeing"},
    9:  {"cat": "NADI2U", "mod": "Awareness",        "label": "NADI2U-Awareness"},
    10: {"cat": "NADI2U", "mod": "GovInit",          "label": "NADI2U-GovInit"},
    11: {"cat": "OTHERS", "mod": "Activity",         "label": "OTHERS-Activity"},
    12: {"cat": "OTHERS", "mod": "Training",         "label": "OTHERS-Training"},
    13: {"cat": "OTHERS", "mod": "Services",         "label": "OTHERS-Services"},
}

_BATCH       = 5_000
_EVENT_CHUNK = 100   # events per chunk; lower if server still OOMs


def _arr(ids) -> str:
    return ",".join(f"'{i}'" for i in ids)


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Get event IDs for a given subcategory
#
#   start_date / end_date: explicit args take priority over module globals.
#   This lets gen_static.py pass its own date window without touching globals.
# ─────────────────────────────────────────────────────────────────────────────

def _get_event_ids(
    conn,
    subcategory_id: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list:
    _start = start_date if start_date is not None else START_DATE
    _end   = end_date   if end_date   is not None else END_DATE
    date_filter = ""
    if _start:
        date_filter += f"\n  AND e.start_datetime::date >= '{_start}'"
    if _end:
        date_filter += f"\n  AND e.end_datetime::date   <= '{_end}'"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT e.id
            FROM public.nd_event e
            WHERE e.site_id IS NOT NULL
              AND jsonb_typeof(e.site_id) = 'array'
              AND e.subcategory_id = {subcategory_id}
              AND e.program_id IS NOT NULL
              AND e.status_id NOT IN (1, 7, 10)
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
#   Event-site columns (new vs original nes_db):
#     event_site_profile_id, event_site_name, event_site_refid_mcmc,
#     event_site_tp, event_site_dusp, event_site_region, event_site_phase,
#     event_site_parliament, event_site_dun, event_site_mukim, event_site_state
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_events(conn, event_ids: list) -> pd.DataFrame:
    """Event taxonomy metadata only — one row per event, no site expansion."""
    sql = f"""
    SELECT DISTINCT
        e.id                                                AS event_id,
        COALESCE(ec.name,  '')                              AS event_category,
        COALESCE(esc.name, '')                              AS program_module,
        COALESCE(ep.name,  '')                              AS program,
        COALESCE(e.program_name, ep.name, '')               AS program_name,
        e.start_datetime::date                              AS event_startdate,
        e.end_datetime::date                                AS event_lastdate,
        COALESCE(pm.name, '')                               AS program_method,
        COALESCE(e.description, '')                         AS event_description,
        e.requester_id
    FROM public.nd_event e
    LEFT JOIN public.nd_event_program     ep  ON ep.id  = e.program_id
    LEFT JOIN public.nd_event_subcategory esc ON esc.id = ep.subcategory_id
    LEFT JOIN public.nd_event_category    ec  ON ec.id  = esc.category_id
    LEFT JOIN public.nd_program_method    pm  ON pm.id  = e.program_method
    WHERE e.id = ANY(ARRAY[{_arr(event_ids)}]::uuid[])
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall())


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Fetch participants + member's home NADI site
#
#   Home NADI columns (refid_mcmc, nadi_name, state, standard_code) come from
#   the member's registered site — distinct from event_site_* columns which
#   describe where the event was physically held.
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_participants(conn, event_ids: list, cur_name: str) -> pd.DataFrame:
    """Participants + event site + member demographic columns."""
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
        -- ── Event site (where the event was held) — aligns with pipeline ──
        COALESCE(esp.refid_mcmc, '')                        AS event_site_refid_mcmc,
        COALESCE(esp.sitename, '')                          AS event_site_name,
        COALESCE(es_state.name, '')                         AS event_site_state,
        COALESCE(es_org.name, '')                           AS event_site_tp,
        COALESCE(es_org.description, '')                    AS event_site_dusp,
        COALESCE(es_ph.name, '')                            AS event_site_phase,
        COALESCE(es_parl.name, '')                          AS event_site_parliament,
        COALESCE(es_dun.name, '')                           AS event_site_dun,
        -- ── Keep home-NADI cols for backward compat ───────────────────────
        ns.refid_mcmc,
        COALESCE(nsp.sitename, 'Site-' || ns.id::text)     AS nadi_name,
        COALESCE(st.name, '')                               AS state,
        ns.standard_code,
        COALESCE(org.name, '')                              AS tp,
        COALESCE(org.description, '')                       AS dusp,
        COALESCE(reg.bm, '')                                AS region_bm,
        COALESCE(ph.name, '')                               AS phase_name,
        COALESCE(parl.name, '')                             AS parliament_name,
        COALESCE(dun.name, '')                              AS dun_name,
        COALESCE(muk.name, '')                              AS mukim_name,
        -- ── Under-12 companion ────────────────────────────────────────────
        u12.id                                              AS under12_id,
        COALESCE(u12.fullname,  '')                         AS under12_name,
        COALESCE(u12.mobile_no, '')                         AS under12_phone,
        COALESCE(g2.bm, '')                                 AS under12_gender
    FROM public.nd_event_participant ep
    -- Event site: ep.ref_id → nd_site_profile (same join as nadi_pipeline.py)
    LEFT JOIN public.nd_site_profile                esp     ON esp.id    = ep.ref_id
    LEFT JOIN public.nd_state                       es_state ON es_state.id = esp.state_id
    LEFT JOIN public.organizations                  es_org   ON es_org.id   = esp.dusp_tp_id
    LEFT JOIN public.nd_phases                      es_ph    ON es_ph.id    = esp.phase_id
    LEFT JOIN public.nd_parliaments                 es_parl  ON es_parl.id  = esp.parliament_rfid
    LEFT JOIN public.nd_duns                        es_dun   ON es_dun.id   = esp.dun_rfid
    -- Member profile + home NADI (kept for demographics/backward compat)
    LEFT JOIN public.nd_member_profile              mp   ON mp.id  = ep.member_id
    LEFT JOIN public.nd_site                        ns   ON ns.id  = mp.ref_id
    LEFT JOIN public.nd_site_profile                nsp  ON nsp.id = ns.site_profile_id
    LEFT JOIN public.nd_state                       st   ON st.id  = nsp.state_id
    LEFT JOIN public.organizations                  org  ON org.id = nsp.dusp_tp_id
    LEFT JOIN public.nd_region                      reg  ON reg.id = nsp.region_id
    LEFT JOIN public.nd_phases                      ph   ON ph.id  = nsp.phase_id
    LEFT JOIN public.nd_parliaments                 parl ON parl.id = nsp.parliament_rfid
    LEFT JOIN public.nd_duns                        dun  ON dun.id = nsp.dun_rfid
    LEFT JOIN public.nd_mukims                      muk  ON muk.id = nsp.mukim_id
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
            ["state", "nadi_name", "event_startdate", "event_id", "fullname"],
            inplace=True, ignore_index=True,
        )
        print(
            f"Rows        : {len(df):,}\n"
            f"Sites       : {df['nadi_name'].nunique():,}\n"
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
    """
    All NADI site profiles — uses nd_site_profile.refid_mcmc so it matches
    the event_site_refid_mcmc produced by _fetch_participants (ep.ref_id → nd_site_profile).
    """
    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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
                ORDER BY st.name, nsp.sitename
            """)
            df = pd.DataFrame(cur.fetchall())
    finally:
        conn.close()
    print(f"All sites loaded: {len(df):,} rows")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Site-level pivot  (programs × events/pax)  — used by __main__ Excel export
#
#   Index: member's home NADI (refid_mcmc, nadi_name, state) so it aligns
#   with fetch_all_sites() for a guaranteed 1099-site view.
#   all_programs: fixed column list so all monthly sheets stay structurally
#   identical — months with no data for a program show zero columns.
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

    # Fill any missing fixed-program columns (months with no data for that program)
    for col in ordered:
        if col not in pivot.columns:
            pivot[col] = 0

    pivot = pivot[[c for c in ordered if c in pivot.columns]]
    return pivot.reindex(all_idx, fill_value=0).fillna(0).astype(int)


# ─────────────────────────────────────────────────────────────────────────────
# Monthly pivot sheets  (one pivot per calendar month + "All" summary)
#   Used by __main__ Excel export only.
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
# JSON output helpers  (consumed by gen_static.py → Netlify frontend)
# ─────────────────────────────────────────────────────────────────────────────

def apply_sso_remap(df: pd.DataFrame, sub_id: int) -> pd.DataFrame:
    """
    Hook for SSO / username remapping before JSON serialisation.
    NES participant data does not require SSO remapping — this is a no-op
    passthrough so gen_static.py can call it unconditionally without branching.
    """
    return df


def _ll_canonical(pn: str) -> str:
    """
    Map raw Lifelong Learning program_name to one of 6 canonical sub-program keys.
    Priority order matches pivot_shared._ll_classifier (last-applied wins → reversed here):
      skill/skillforge > cyber/siber/security > ekelas > tuisyen > mahir > esport > tiny/techie > default CYBERSECURITY
    """
    p = (pn or "").lower()
    if "skillforge" in p or "skill" in p:                     return "ESPORT"
    if "cyber" in p or "siber" in p or "security" in p:       return "CYBERSECURITY"
    if "ekelas" in p or "e-kelas" in p:                       return "EKELAS"
    if "tuisyen" in p or "tuition" in p or "guidance" in p:   return "TUISYEN RAKYAT"
    if "mahir" in p:                                           return "MAHIR"
    if "esport" in p:                                          return "ESPORT"
    if "tiny" in p or "techie" in p:                          return "TINYTECHIES"
    return "CYBERSECURITY"


def _safe_int(v) -> int:
    """Coerce numpy int64 / NaN / None to a plain Python int safely."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _participant_counts(df_pax: pd.DataFrame) -> dict:
    """
    Standard demographic breakdown for a participant slice.
    All values are plain Python ints (JSON-serialisable).

    Keys: total, women, youth (15-24), children (<12), oku
    """
    if df_pax.empty:
        return {"total": 0, "women": 0, "youth": 0, "children": 0, "oku": 0}

    age = pd.to_numeric(df_pax["age"], errors="coerce")
    return {
        "total":    _safe_int(df_pax["participant_id"].nunique()),
        "women":    _safe_int((df_pax["gender"] == "Perempuan").sum()),
        "youth":    _safe_int(age.between(15, 24, inclusive="both").sum()),
        "children": _safe_int((age < 12).sum()),
        "oku":      _safe_int((df_pax["oku_status"].str.lower() == "true").sum()),
    }


def _week_label(week_period) -> str:
    """
    Human-readable ISO-week label: "19 Jan – 25 Jan 2026".
    Uses cross-platform %-free formatting (lstrip to drop leading zero).
    """
    s = week_period.start_time
    e = week_period.end_time
    return (
        f"{s.strftime('%d %b').lstrip('0')} – "
        f"{e.strftime('%d %b %Y').lstrip('0')}"
    )


def build_nadi_index(
    df: pd.DataFrame,
    df_sites: pd.DataFrame,
    sub_id: int,
) -> list[dict]:
    """
    Per-NADI summary — one entry per site, covering all time in the dataset.

    Schema per entry:
      {
        "refid":    "N0001",
        "name":     "NADI Kampung Baru",
        "state":    "Selangor",
        "events":   12,
        "pax":      {"total": 340, "women": 120, "youth": 80, "children": 5, "oku": 2},
        "programs": {"NADI-Preneur": {"events": 4, "pax": 120}}
      }

    All 1099 sites from df_sites are included; sites with no activity get zeros.
    """
    df_pax = df[df["participant_id"].notna()].copy()

    _geo = ["nadi_name", "state", "tp", "dusp", "region_bm",
            "phase_name", "parliament_name", "dun_name", "mukim_name"]
    avail = ["refid_mcmc"] + [c for c in _geo if c in df_sites.columns]
    site_map = (
        df_sites[avail]
        .drop_duplicates("refid_mcmc")
        .set_index("refid_mcmc")
    )

    # Use event site for counting (aligns with nadi_pipeline.py)
    site_col = "event_site_refid_mcmc" if "event_site_refid_mcmc" in df_pax.columns else "refid_mcmc"

    result = []
    for refid, site_row in site_map.iterrows():
        s = df_pax[df_pax[site_col] == refid]

        programs: dict = {}
        if not s.empty:
            if sub_id == 2:
                # Lifelong Learning: classify by program TEMPLATE name (ep.name) so that
                # free-form custom titles like "TEAMWORK & PERANAN DALAM PASUKAN E-SUKAN"
                # still resolve to the right canonical (via template "SkillForge Basketball").
                # Vectorised groupby avoids pax double-counting when a participant attends
                # multiple events under the same canonical category.
                sc = s.copy()
                sc["_canon"] = sc["program"].apply(_ll_canonical)
                for canon, cgrp in sc.groupby("_canon"):
                    programs[canon] = {
                        "events": _safe_int(cgrp["event_id"].nunique()),
                        "pax":    _safe_int(cgrp["participant_id"].nunique()),
                    }
            else:
                for prog, grp in s.groupby("program"):
                    if not prog:
                        continue
                    programs[prog] = {
                        "events": _safe_int(grp["event_id"].nunique()),
                        "pax":    _safe_int(grp["participant_id"].nunique()),
                    }

        # Compact per-event list: [{n, m, p, a, sp}]
        ev_list: list = []
        if not s.empty:
            for eid, eg in s.groupby("event_id"):
                name = str(eg["program_name"].iloc[0] or eg["program"].iloc[0] or "")[:60]
                # Use template name for canonical classification (same logic as programs block above)
                subp = (_ll_canonical(str(eg["program"].iloc[0] or "")) if sub_id == 2
                        else str(eg["program"].iloc[0] or "")[:40])
                try:
                    mlbl = pd.Timestamp(eg["event_startdate"].iloc[0]).strftime("%b %Y")
                except Exception:
                    mlbl = ""
                att = eg["attendance"].astype(str).str.lower().isin(["true", "1", "t", "yes"])
                ev_list.append({
                    "n":  name,
                    "m":  mlbl,
                    "p":  _safe_int(eg["participant_id"].nunique()),
                    "a":  _safe_int(att.sum()),
                    "sp": subp,
                })

        result.append({
            "refid":      refid,
            "name":       str(site_row.get("nadi_name", "") or ""),
            "state":      str(site_row.get("state", "") or ""),
            "tp":         str(site_row.get("tp", "") or ""),
            "dusp":       str(site_row.get("dusp", "") or ""),
            "region":     str(site_row.get("region_bm", "") or ""),
            "phase":      str(site_row.get("phase_name", "") or ""),
            "parliament": str(site_row.get("parliament_name", "") or ""),
            "dun":        str(site_row.get("dun_name", "") or ""),
            "mukim":      str(site_row.get("mukim_name", "") or ""),
            "events":     _safe_int(s["event_id"].nunique()) if not s.empty else 0,
            "pax":        _participant_counts(s),
            "programs":   programs,
            "ev":         ev_list,
        })

    return result


def build_monthly(
    df: pd.DataFrame,
    df_sites: pd.DataFrame,
    sub_id: int,
) -> list[dict]:
    """
    Monthly rollup — one entry per calendar month, chronologically ordered.

    Schema per entry:
      {
        "period": "2026-01",
        "label":  "Jan 2026",
        "events": 45,
        "pax":    {"total": 1200, "women": 400, "youth": 300, "children": 50, "oku": 10},
        "nadi":   {"N0001": {"events": 3, "pax": 80}, ...}
      }
    """
    df = df.copy()
    df["_period"] = pd.to_datetime(df["event_startdate"], errors="coerce").dt.to_period("M")
    df_pax = df[df["participant_id"].notna()]

    # Use event site for counting (aligns with nadi_pipeline.py)
    site_col = "event_site_refid_mcmc" if "event_site_refid_mcmc" in df_pax.columns else "refid_mcmc"

    result = []
    for period in sorted(df["_period"].dropna().unique()):
        m = df_pax[df_pax["_period"] == period]
        nadi_map: dict = {}
        for refid, grp in m.groupby(site_col):
            if not refid:
                continue
            entry: dict = {
                "events": _safe_int(grp["event_id"].nunique()),
                "pax":    _safe_int(grp["participant_id"].nunique()),
            }
            if sub_id == 2:
                # Classify by template name (ep.name / "program" col) — same rationale as build_nadi_index
                gc = grp.copy()
                gc["_canon"] = gc["program"].apply(_ll_canonical)
                pr: dict = {
                    canon: {
                        "events": _safe_int(cgrp["event_id"].nunique()),
                        "pax":    _safe_int(cgrp["participant_id"].nunique()),
                    }
                    for canon, cgrp in gc.groupby("_canon")
                }
                if pr:
                    entry["pr"] = pr
            nadi_map[refid] = entry
        result.append({
            "period": str(period),                     # "2026-01"
            "label":  period.strftime("%b %Y"),         # "Jan 2026"
            "events": _safe_int(m["event_id"].nunique()),
            "pax":    _participant_counts(m),
            "nadi":   nadi_map,
        })

    return result


def build_weekly(
    df: pd.DataFrame,
    df_sites: pd.DataFrame,
    sub_id: int,
) -> list[dict]:
    """
    Weekly rollup — one entry per ISO week (Mon–Sun), chronologically ordered.

    Schema per entry:
      {
        "period": "2026-W03",
        "label":  "12 Jan – 18 Jan 2026",
        "events": 12,
        "pax":    {"total": 300, "women": 100, "youth": 80, "children": 10, "oku": 2},
        "nadi":   {"N0001": {"events": 1, "pax": 25}, ...}
      }
    """
    df = df.copy()
    df["_week"] = pd.to_datetime(df["event_startdate"], errors="coerce").dt.to_period("W")
    df_pax = df[df["participant_id"].notna()]

    # Use event site for counting (aligns with nadi_pipeline.py)
    site_col = "event_site_refid_mcmc" if "event_site_refid_mcmc" in df_pax.columns else "refid_mcmc"

    result = []
    for week in sorted(df["_week"].dropna().unique()):
        w = df_pax[df_pax["_week"] == week]
        w_start = week.start_time
        nadi_map: dict = {}
        for refid, grp in w.groupby(site_col):
            if not refid:
                continue
            nadi_map[refid] = {
                "events": _safe_int(grp["event_id"].nunique()),
                "pax":    _safe_int(grp["participant_id"].nunique()),
            }
        result.append({
            "period": f"{w_start.year}-W{w_start.isocalendar()[1]:02d}",
            "label":  _week_label(week),
            "events": _safe_int(w["event_id"].nunique()),
            "pax":    _participant_counts(w),
            "nadi":   nadi_map,
        })

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Save helpers  (CSV + Excel for __main__ CLI export)
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
# Main — CLI export: CSV + monthly pivot Excel per subcategory
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df_all_sites = fetch_all_sites()

    for sub_id, info in SUBCATEGORIES.items():
        sub_label = info["label"]
        print(f"\n{'='*60}")
        print(f"Subcategory {sub_id}: {sub_label}  [{info['cat']} / {info['mod']}]")
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
