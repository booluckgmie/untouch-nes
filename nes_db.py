# ─────────────────────────────────────────────────────────────────────────────
# nes_db.py  — DB query layer for NES Analytics Dashboard
# ─────────────────────────────────────────────────────────────────────────────
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras

from db_configs import get_conn

_log = logging.getLogger(__name__)

# ── SSO requester_id → program label override ─────────────────────────────────
# When matched, OVERRIDES the program column (not just a display tag).
SSO_MAP = {
    "afc1c08c-b487-42d4-a2d1-e8d2e83b4ab5": "MAHIR",
    "4078f98f-1a06-4c59-8d7b-a303d43a689b": "CYBERSECURITY",
    "b0aed14c-8bec-47b7-94cb-dd5a03225787": "NADI TUISYEN RAKYAT",
    "a15e546a-cda0-4451-a9ce-61afa15021c7": "ESPORT",
    "2232e98a-5050-4fcd-918b-672c1a97dadd": "EKELAS",
}

# Programs superseded by SSO remap — dropped per subcategory after remap is applied.
DROPPED_PROGRAMS: dict[int, set] = {
    2: {"NADI-Nurture", "NADI-SkillForge"},
}

def sso_label(uid) -> str:
    """Resolve requester_id UUID to SSO label (for display in weekly view)."""
    if not uid or str(uid) in ("nan", "None", "—"):
        return "—"
    uid_str = str(uid).strip()
    if uid_str in SSO_MAP:
        return SSO_MAP[uid_str]
    _log.warning("Unknown SSO requester_id: %s", uid_str)
    return uid_str[:8]


def apply_sso_remap(df: pd.DataFrame, sub_id: int) -> pd.DataFrame:
    """Override 'program' col where requester_id matches SSO_MAP; then drop DROPPED_PROGRAMS rows."""
    if "requester_id" not in df.columns:
        return df
    df = df.copy()
    norm = df["requester_id"].astype(str).str.strip().str.lower()
    sso_lower = {k.lower(): v for k, v in SSO_MAP.items()}
    mask = norm.isin(sso_lower)
    if mask.any():
        df.loc[mask, "program"] = norm[mask].map(sso_lower)
        _log.info("SSO remap: %d rows relabelled → %s", mask.sum(),
                  df.loc[mask, "program"].value_counts().to_dict())
    if sub_id in DROPPED_PROGRAMS:
        drop_mask = df["program"].isin(DROPPED_PROGRAMS[sub_id])
        if drop_mask.any():
            _log.info("Dropping %d rows with superseded programs: %s", drop_mask.sum(),
                      df.loc[drop_mask, "program"].value_counts().to_dict())
            df = df[~drop_mask].reset_index(drop=True)
    return df

_BATCH       = 5_000
_EVENT_CHUNK = 100

# ── Subcategory map ───────────────────────────────────────────────────────────
SUBCATEGORIES = {
    1:  {"cat": "NADI4U", "mod": "Entrepreneur",     "label": "NADI4U-Entrepreneur"},
    2:  {"cat": "NADI4U", "mod": "LifelongLearning", "label": "NADI4U-LifelongLearning"},
    3:  {"cat": "NADI4U", "mod": "Wellbeing",        "label": "NADI4U-Wellbeing"},
    8:  {"cat": "NADI2U", "mod": "Wellbeing",        "label": "NADI2U-Wellbeing"},
}


def _arr(ids) -> str:
    return ",".join(f"'{i}'" for i in ids)


def _msort(m: str):
    try:    return datetime.strptime(m, "%b %Y")
    except: return datetime.min


# ─────────────────────────────────────────────────────────────────────────────
# 1. Event IDs for a subcategory
# ─────────────────────────────────────────────────────────────────────────────
def _get_event_ids(conn, subcategory_id: int,
                   start_date: Optional[str] = None,
                   end_date:   Optional[str] = None) -> list:
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


# ─────────────────────────────────────────────────────────────────────────────
# 2. Event metadata
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_events(conn, event_ids: list) -> pd.DataFrame:
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
        df = pd.DataFrame(cur.fetchall())
    if not df.empty:
        for col in ("event_id", "requester_id"):
            if col in df.columns:
                df[col] = df[col].astype(str)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. Participants (chunked server-side cursor)
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_participants(conn, event_ids: list, cur_name: str) -> pd.DataFrame:
    # NOTE: requester_id comes from nd_event via _fetch_events merge — NOT repeated here
    sql = f"""
    SELECT
        ep.event_id,
        ep.id                                               AS participant_id,
        ep.attendance,
        ep.verified,
        mp.id                                               AS member_id,
        ns.refid_mcmc,
        COALESCE(nsp.sitename, 'Site-' || ns.id::text)     AS nadi_name,
        COALESCE(st.name, '')                               AS state
    FROM public.nd_event_participant ep
    LEFT JOIN public.nd_member_profile  mp  ON mp.id  = ep.member_id
    LEFT JOIN public.nd_site            ns  ON ns.id  = mp.ref_id
    LEFT JOIN public.nd_site_profile    nsp ON nsp.id = ns.site_profile_id
    LEFT JOIN public.nd_state           st  ON  st.id = nsp.state_id
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
    df = pd.DataFrame(rows)
    if not df.empty:
        for col in ("event_id", "participant_id", "member_id"):
            if col in df.columns:
                df[col] = df[col].astype(str)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 4. Build NADI-index data (for "NADI Index" tab)
# ─────────────────────────────────────────────────────────────────────────────
def build_nadi_index(df: pd.DataFrame, df_sites: pd.DataFrame, sub_id: int) -> dict:
    """
    Returns:
      {
        'cat': 'NADI4U', 'mod': 'Entrepreneur',
        'e': total_events, 'p': total_pax,
        'a': active_nadi,  'u': untouched_nadi,
        'progs': ['NADI-EmpowerHER', ...],
        'rows': [[nadi_idx, total_ev, total_pax, {prog_key:[ev,pax]}], ...]
      }
    """
    info = SUBCATEGORIES.get(sub_id, {})

    ev = (df.groupby("event_id")
            .agg(refid_mcmc  =("refid_mcmc",    "first"),
                 nadi_name   =("nadi_name",      "first"),
                 state        =("state",          "first"),
                 program      =("program",        "first"),
                 program_name =("program_name",   "first"),
                 total_pax   =("participant_id",  "nunique"),
                 attended    =("attendance",      lambda x: int(x.sum())))
            .reset_index())

    progs    = sorted(ev["program"].dropna().unique().tolist())
    prog_key = {p: str(k) for k, p in enumerate(progs)}

    sites_list = df_sites[["refid_mcmc","nadi_name","state"]].drop_duplicates().values.tolist()
    ref_to_i   = {r[0]: i for i, r in enumerate(sites_list)}

    total_ev    = ev["event_id"].nunique()
    total_pax   = df["participant_id"].nunique() if "participant_id" in df else 0
    active_refs = set(ev["refid_mcmc"].dropna().unique())

    nadi_agg = {}
    for _, r in ev.iterrows():
        ref = r["refid_mcmc"]
        if pd.isna(ref):
            continue
        if ref not in nadi_agg:
            nadi_agg[ref] = {"te": 0, "tp": 0, "pr": {}}
        prog = r["program"]
        k    = prog_key.get(prog, "?")
        if k not in nadi_agg[ref]["pr"]:
            nadi_agg[ref]["pr"][k] = [0, 0]
        nadi_agg[ref]["pr"][k][0] += 1
        nadi_agg[ref]["pr"][k][1] += int(r["total_pax"])
        nadi_agg[ref]["te"] += 1
        nadi_agg[ref]["tp"] += int(r["total_pax"])

    rows = []
    for ref, agg in nadi_agg.items():
        i = ref_to_i.get(ref)
        if i is None:
            continue
        rows.append([i, agg["te"], agg["tp"], agg["pr"]])

    return {
        "cat":   info.get("cat", ""),
        "mod":   info.get("mod", ""),
        "e":     total_ev,
        "p":     total_pax,
        "a":     len(active_refs),
        "u":     len(sites_list) - len(active_refs),
        "progs": progs,
        "rows":  rows,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5. Build monthly data (for "Monthly Update" tab)
# ─────────────────────────────────────────────────────────────────────────────
def build_monthly(df: pd.DataFrame, df_sites: pd.DataFrame, sub_id: int) -> dict:
    """
    Returns:
      {
        'months': ['Oct 2025', ...],
        'progs':  ['NADI-EmpowerHER', ...],
        'rows':   [[nadi_idx,
                    [ev_tot_m0, ...],   # total events per month
                    [px_tot_m0, ...],   # total pax per month
                    {prog_key: {'ev': [ev_m0,...], 'px': [px_m0,...]}}
                   ], ...]
      }
    """
    sites_list = df_sites[["refid_mcmc","nadi_name","state"]].drop_duplicates().values.tolist()
    ref_to_i   = {r[0]: i for i, r in enumerate(sites_list)}

    has_pax = "participant_id" in df.columns

    df2 = df.copy()
    df2["_month"] = pd.to_datetime(df2["event_startdate"], errors="coerce").dt.to_period("M")

    months_sorted = sorted(
        df2["_month"].dropna().dt.strftime("%b %Y").unique(), key=_msort
    )

    progs    = sorted(df2.loc[df2["program"].notna() & (df2["program"] != ""), "program"].unique().tolist())
    prog_key = {p: str(k) for k, p in enumerate(progs)}
    nm       = len(months_sorted)

    nadi_data: dict = {}

    for mi, month_str in enumerate(months_sorted):
        period = pd.Period(month_str, freq="M")
        sub    = df2[df2["_month"] == period]
        for ref, grp in sub.groupby("refid_mcmc"):
            i = ref_to_i.get(ref)
            if i is None:
                continue
            if i not in nadi_data:
                nadi_data[i] = {"tot": [0]*nm, "pax": [0]*nm, "pr": {}}
            nadi_data[i]["tot"][mi] = grp["event_id"].nunique()
            nadi_data[i]["pax"][mi] = grp["participant_id"].nunique() if has_pax else 0
            for prog, pgrp in grp.groupby("program"):
                pk = prog_key.get(prog)
                if pk is None:
                    continue
                if pk not in nadi_data[i]["pr"]:
                    nadi_data[i]["pr"][pk] = {"ev": [0]*nm, "px": [0]*nm}
                nadi_data[i]["pr"][pk]["ev"][mi] = pgrp["event_id"].nunique()
                nadi_data[i]["pr"][pk]["px"][mi] = pgrp["participant_id"].nunique() if has_pax else 0

    rows = [[i, d["tot"], d["pax"], d["pr"]] for i, d in sorted(nadi_data.items())]
    return {"months": months_sorted, "progs": progs, "rows": rows}


# ─────────────────────────────────────────────────────────────────────────────
# 6. Build weekly data (for "Weekly Drill-Down" dashboard)
# ─────────────────────────────────────────────────────────────────────────────
def build_weekly(df: pd.DataFrame, df_sites: pd.DataFrame, sub_id: int) -> dict:
    """
    Returns:
      {
        'cat', 'mod',
        'weeks': [{k, l, ev, si, z, c, p}, ...],
        'nw':    {week_key: {ref: [{pn,pr,tp,at,st,sso}]}},
        'zero':  [{ref,nm,s,pr,pn,wk,wl,tp,sso}],
        'active_refs': [...]
      }
    """
    info = SUBCATEGORIES.get(sub_id, {})

    if "requester_id" not in df.columns:
        df = df.copy()
        df["requester_id"] = None

    ev = (df.groupby("event_id")
            .agg(refid_mcmc   =("refid_mcmc",    "first"),
                 nadi_name    =("nadi_name",      "first"),
                 state        =("state",          "first"),
                 program      =("program",        "first"),
                 program_name =("program_name",   "first"),
                 event_start  =("event_startdate","first"),
                 requester_id =("requester_id",   "first"),
                 total_pax    =("participant_id",  "nunique"),
                 attended     =("attendance",      lambda x: int(x.sum())))
            .reset_index())

    ev["event_start"] = pd.to_datetime(ev["event_start"], errors="coerce")
    ev["wstart"]      = ev["event_start"].apply(
        lambda d: d - timedelta(days=d.weekday()) if pd.notna(d) else pd.NaT)
    ev["wk"] = ev["wstart"].dt.strftime("W%V %Y")
    ev["wl"] = ev["wstart"].apply(
        lambda d: f"{d.strftime('%d %b')}–{(d+timedelta(days=6)).strftime('%d %b %Y')}"
        if pd.notna(d) else "")
    ev["st"] = ev.apply(
        lambda r: "c" if r["attended"] >= r["total_pax"] and r["total_pax"] > 0
                  else ("z" if r["attended"] == 0 else "p"), axis=1)

    weeks_meta = []
    for wk, grp in ev.dropna(subset=["wk"]).groupby("wk"):
        wl     = grp["wl"].iloc[0]
        wstart = grp["wstart"].iloc[0]
        weeks_meta.append({
            "k":  wk,
            "l":  wl,
            "ws": str(wstart.date()) if pd.notna(wstart) else "",
            "ev": len(grp),
            "si": grp["refid_mcmc"].nunique(),
            "z":  int((grp["st"] == "z").sum()),
            "c":  int((grp["st"] == "c").sum()),
            "p":  int((grp["st"] == "p").sum()),
        })
    weeks_meta.sort(key=lambda w: w.get("ws", ""))

    nadi_week = {}
    for _, r in ev.dropna(subset=["wk", "refid_mcmc"]).iterrows():
        wk  = r["wk"]
        ref = str(r["refid_mcmc"])
        if wk not in nadi_week:
            nadi_week[wk] = {}
        if ref not in nadi_week[wk]:
            nadi_week[wk][ref] = []
        nadi_week[wk][ref].append({
            "pn":  str(r["program_name"])[:65],
            "pr":  str(r["program"]),
            "tp":  int(r["total_pax"]),
            "at":  int(r["attended"]),
            "st":  r["st"],
            "sso": sso_label(r["requester_id"]),
        })

    zero_list = []
    for _, r in ev[ev["st"] == "z"].iterrows():
        zero_list.append({
            "ref": str(r["refid_mcmc"]) if pd.notna(r["refid_mcmc"]) else "—",
            "nm":  str(r["nadi_name"])  if pd.notna(r["nadi_name"])  else "—",
            "s":   str(r["state"])      if pd.notna(r["state"])       else "—",
            "pr":  str(r["program"]),
            "pn":  str(r["program_name"])[:65],
            "wk":  r["wk"],
            "wl":  r["wl"],
            "tp":  int(r["total_pax"]),
            "sso": sso_label(r["requester_id"]),
        })

    active_refs = list(ev["refid_mcmc"].dropna().unique())

    return {
        "cat":         info.get("cat", ""),
        "mod":         info.get("mod", ""),
        "weeks":       weeks_meta,
        "nw":          nadi_week,
        "zero":        zero_list,
        "active_refs": active_refs,
    }
