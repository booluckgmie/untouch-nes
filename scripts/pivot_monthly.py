"""
pivot_monthly.py
----------------
Reads latest nadi_consolidated_*.parquet → one Excel file per NADI4U pillar,
one sheet per calendar month (MMM YYYY), quarterly (Q# YYYY), weekly
(MMM YYYY W#, latest 3 months) + "All" summary sheet.

Index  : refid_mcmc | nadi_name | state
Columns: sub_pillar_name or deep program (Lifelong Learning) × events/pax + Total
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd

from pivot_shared import (
    PILLARS,
    build_all_sites,
    build_pivot,
    find_parquet,
)

LOAD_COLS = [
    "event_site_refid_mcmc", "event_site_name", "event_site_state",
    "event_category", "pillar_name", "sub_pillar_name",
    "program_name", "deeper_sub_pillar",
    "event_id", "participant_id", "event_start_datetime",
]

WEEK_BINS   = [0, 7, 14, 21, 31]
WEEK_LABELS = ["W1", "W2", "W3", "W4"]


def main():
    try:
        parquet = find_parquet(Path(__file__).parent.parent)
    except FileNotFoundError as e:
        sys.exit(str(e))

    print(f"Loading {parquet.name} ...")
    df_full = pd.read_parquet(parquet, columns=LOAD_COLS)
    print(f"  Total rows  : {len(df_full):,}")

    all_sites = build_all_sites(df_full)
    print(f"  Unique sites: {len(all_sites):,}")

    df_n4u = df_full[df_full["event_category"].str.contains("NADI4U", na=False)].copy()
    _ts = pd.to_datetime(df_n4u["event_start_datetime"], utc=True, errors="coerce").dt.tz_localize(None)
    df_n4u["_period"]  = _ts.dt.to_period("M")
    df_n4u["_quarter"] = _ts.dt.to_period("Q")
    df_n4u["_day"]     = _ts.dt.day
    df_n4u["_week"]    = pd.cut(df_n4u["_day"], bins=WEEK_BINS, labels=WEEK_LABELS)

    months        = sorted(df_n4u["_period"].dropna().unique())
    quarters      = sorted(df_n4u["_quarter"].dropna().unique())
    weekly_months = months[-3:]
    print(f"  NADI4U rows : {len(df_n4u):,}  |  months: {len(months)}  |  quarters: {len(quarters)}")

    today   = date.today().strftime("%Y%m%d")
    out_dir = parquet.parent

    for pillar_name, label, group_col, classifier, fixed_groups in PILLARS:
        df_pil = df_n4u[df_n4u["pillar_name"] == pillar_name].copy()
        if classifier is not None:
            df_pil[group_col] = classifier(df_pil)
        groups = fixed_groups if fixed_groups is not None else sorted(df_pil[group_col].dropna().unique())

        print(f"\n[{pillar_name}]  {len(df_pil):,} rows  |  groups: {groups}")
        out = out_dir / f"nadi4u_{label.replace(' ', '_').lower()}_{today}_w.xlsx"

        with pd.ExcelWriter(out, engine="openpyxl") as writer:

            # Monthly sheets
            for period in months:
                df_m   = df_pil[df_pil["_period"] == period]
                sheet  = period.strftime("%b %Y")
                result = build_pivot(df_m, all_sites, group_col, groups)
                result.to_excel(writer, sheet_name=sheet, merge_cells=True)
                print(f"  {sheet:10s}  {len(df_m):>8,} rows  {(result > 0).any(axis=1).sum():>5,} active sites")

            # Quarterly sheets  (format: "Q2 2026" not "2026Q2")
            for qtr in quarters:
                df_q   = df_pil[df_pil["_quarter"] == qtr]
                sheet  = f"Q{qtr.quarter} {qtr.start_time.year}"
                result = build_pivot(df_q, all_sites, group_col, groups)
                result.to_excel(writer, sheet_name=sheet, merge_cells=True)
                print(f"  {sheet:10s}  {len(df_q):>8,} rows  {(result > 0).any(axis=1).sum():>5,} active sites")

            # Weekly sheets for latest 3 months (_week pre-computed on df_n4u)
            for period in weekly_months:
                month_label = period.strftime("%b %Y")
                for wk in WEEK_LABELS:
                    df_w   = df_pil[(df_pil["_period"] == period) & (df_pil["_week"] == wk)]
                    sheet  = f"{month_label} {wk}"
                    result = build_pivot(df_w, all_sites, group_col, groups)
                    result.to_excel(writer, sheet_name=sheet, merge_cells=True)
                    print(f"  {sheet:14s}  {len(df_w):>8,} rows  {(result > 0).any(axis=1).sum():>5,} active sites")

            # All-time summary
            result_all = build_pivot(df_pil, all_sites, group_col, groups)
            result_all.to_excel(writer, sheet_name="All", merge_cells=True)
            print(f"  {'All':10s}  {len(df_pil):>8,} rows  {(result_all > 0).any(axis=1).sum():>5,} active sites")

        print(f"  Saved -> {out.name}  ({out.stat().st_size / 1024:.0f} KB)")

    print("\nDone.")


if __name__ == "__main__":
    main()
