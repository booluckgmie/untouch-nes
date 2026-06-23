"""
pivot_shared.py
---------------
Shared constants and helpers consumed by pivot_monthly.py.

Defines PILLARS (one entry per NADI4U sub-category), site helpers,
and the pivot-table builder used to produce Excel sheets.
"""

from pathlib import Path

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Lifelong Learning sub-program classifier
# ─────────────────────────────────────────────────────────────────────────────

def _ll_classifier(df: pd.DataFrame) -> pd.Series:
    """Map raw program_name → one of 6 canonical LL sub-program names."""
    p = df["program_name"].str.lower().fillna("")
    res = pd.Series("CYBERSECURITY", index=df.index)
    res[p.str.contains(r"tiny|techie",           regex=True, na=False)] = "TINYTECHIES"
    res[p.str.contains("esport",                              na=False)] = "ESPORT"
    res[p.str.contains("mahir",                               na=False)] = "MAHIR"
    res[p.str.contains(r"tuisyen|tuition|guidance", regex=True, na=False)] = "TUISYEN RAKYAT"
    res[p.str.contains(r"ekelas|e-kelas",        regex=True, na=False)] = "EKELAS"
    res[p.str.contains(r"cyber|siber|security",  regex=True, na=False)] = "CYBERSECURITY"
    res[p.str.contains(r"skillforge|skill",      regex=True, na=False)] = "ESPORT"
    return res


LL_GROUPS = ["CYBERSECURITY", "EKELAS", "TUISYEN RAKYAT", "ESPORT", "MAHIR", "TINYTECHIES"]

# ─────────────────────────────────────────────────────────────────────────────
# PILLARS: list of (pillar_name, file_label, group_col, classifier_fn, fixed_groups)
#
#   pillar_name   — value in parquet "pillar_name" column
#   file_label    — suffix used in output Excel filename
#   group_col     — column used as pivot column axis
#   classifier_fn — if set, called as fn(df) → Series to (re)assign group_col
#   fixed_groups  — explicit ordered list of groups (None = derive from data)
# ─────────────────────────────────────────────────────────────────────────────

PILLARS = [
    ("Entrepreneur",           "entrepreneur",      "sub_pillar_name", None,           None),
    ("Lifelong Learning",      "lifelong_learning", "program_name",    _ll_classifier, LL_GROUPS),
    ("Wellbeing",              "wellbeing",         "sub_pillar_name", None,           None),
    ("Awareness",              "awareness",         "sub_pillar_name", None,           None),
    ("Government Initiatives", "gov_init",          "sub_pillar_name", None,           None),
]


# ─────────────────────────────────────────────────────────────────────────────
# Parquet finder
# ─────────────────────────────────────────────────────────────────────────────

def find_parquet(directory: Path) -> Path:
    files = sorted(directory.glob("nadi_consolidated_*.parquet"), reverse=True)
    if not files:
        raise FileNotFoundError(f"No nadi_consolidated_*.parquet in {directory}")
    return files[0]


# ─────────────────────────────────────────────────────────────────────────────
# Site reference table
# ─────────────────────────────────────────────────────────────────────────────

def build_all_sites(df: pd.DataFrame) -> pd.DataFrame:
    """Unique (refid, name, state) from event_site_* columns in the parquet."""
    return (
        df[["event_site_refid_mcmc", "event_site_name", "event_site_state"]]
        .drop_duplicates()
        .rename(columns={
            "event_site_refid_mcmc": "refid_mcmc",
            "event_site_name":       "nadi_name",
            "event_site_state":      "state",
        })
        .dropna(subset=["refid_mcmc"])
        .query("refid_mcmc != ''")
        .sort_values(["state", "nadi_name"])
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pivot builder
# ─────────────────────────────────────────────────────────────────────────────

def build_pivot(
    df: pd.DataFrame,
    all_sites: pd.DataFrame,
    group_col: str,
    groups: list,
) -> pd.DataFrame:
    """
    Build an (n_sites × (n_groups*2 + 2)) summary pivot.

    Columns: MultiIndex (group, "Events") / (group, "Pax") … (Total, …)
    Index  : refid_mcmc | nadi_name | state
    """
    idx_cols = ["refid_mcmc", "nadi_name", "state"]
    all_idx  = all_sites.set_index(idx_cols).index

    ordered = [(g, m) for g in groups for m in ("Events", "Pax")]
    ordered += [("Total", "Events"), ("Total", "Pax")]
    mi_cols = pd.MultiIndex.from_tuples(ordered)

    empty = pd.DataFrame(0, index=all_idx, columns=mi_cols)

    dfx = df.rename(columns={
        "event_site_refid_mcmc": "refid_mcmc",
        "event_site_name":       "nadi_name",
        "event_site_state":      "state",
    })
    dfx = dfx[dfx["refid_mcmc"].notna() & (dfx["refid_mcmc"] != "")]
    dfx = dfx[dfx[group_col].notna() & (dfx[group_col] != "")]
    if groups:
        dfx = dfx[dfx[group_col].isin(groups)]

    if dfx.empty:
        return empty

    agg = (
        dfx.groupby(idx_cols + [group_col])
        .agg(Events=("event_id", "nunique"), Pax=("participant_id", "nunique"))
        .reset_index()
    )

    pivot = agg.pivot_table(
        index=idx_cols, columns=group_col,
        values=["Events", "Pax"], aggfunc="sum", fill_value=0,
    ).swaplevel(axis=1).sort_index(axis=1, level=0)
    pivot.columns = pd.MultiIndex.from_tuples(list(pivot.columns))

    tot = dfx.groupby(idx_cols).agg(
        Events=("event_id", "nunique"), Pax=("participant_id", "nunique")
    )
    pivot[("Total", "Events")] = tot["Events"]
    pivot[("Total", "Pax")]    = tot["Pax"]

    for col in mi_cols:
        if col not in pivot.columns:
            pivot[col] = 0

    pivot = pivot[[c for c in mi_cols if c in pivot.columns]]
    return empty.add(pivot, fill_value=0).fillna(0).astype(int)
