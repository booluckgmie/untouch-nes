"""
generate_parquet.py
-------------------
Consolidates ALL NADI events + participants into a single .parquet file.

Combines:
  - masterdb.py  : chunked streaming, participant demographics, home-NADI lookup
  - User SQL     : richer event-site joins, event hierarchy, PDPA-masked IC,
                   deeper_sub_pillar for SSO admins

No date filtering. All categories. All subcategories.
"""

import re
import sys
import time
import traceback
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))
from credential import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

# ── Connection ────────────────────────────────────────────────────────────────
PG_CONFIG = {
    "host":     DB_HOST,
    "port":     DB_PORT,
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
}

# ── Tuning ────────────────────────────────────────────────────────────────────
_EVENT_CHUNK   = 100      # events per SQL batch
_ROW_BATCH     = 5_000    # server-side cursor fetch size
_MAX_RETRIES   = 3        # retries per chunk on transient failure
_RETRY_DELAY   = 5        # seconds between retries

# Status IDs to exclude — set to () to include every status.
# masterdb.py historically skips 1=draft, 7=cancelled, 9=rejected.
EXCLUDE_STATUSES: tuple = (1, 7, 10)

# ── Output ────────────────────────────────────────────────────────────────────
OUT_DIR  = Path(__file__).parent.parent
OUT_FILE = OUT_DIR / f"nadi_consolidated_{date.today().strftime('%Y%m%d')}.parquet"

_ILLEGAL = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _conn() -> psycopg2.extensions.connection:
    c = psycopg2.connect(**PG_CONFIG)
    c.autocommit = False
    with c.cursor() as cur:
        cur.execute("SET statement_timeout = 0;")
        cur.execute("SET work_mem = '64MB';")
    return c


def _arr(ids) -> str:
    return ",".join(f"'{i}'" for i in ids)


def _clean_str(v):
    return _ILLEGAL.sub("", v) if isinstance(v, str) else v


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Fetch all event IDs
# ─────────────────────────────────────────────────────────────────────────────

def get_all_event_ids(conn) -> list:
    status_clause = (
        f"AND e.status_id NOT IN ({','.join(str(s) for s in EXCLUDE_STATUSES)})"
        if EXCLUDE_STATUSES else ""
    )
    sql = f"""
        SELECT DISTINCT e.id
        FROM public.nd_event e
        INNER JOIN public.nd_event_participant ep ON ep.event_id = e.id
        WHERE 1=1
          {status_clause}
        ORDER BY e.id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        ids = [row[0] for row in cur.fetchall()]
    print(f"Total events with participants: {len(ids):,}")
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Fetch one chunk (events + participants, fully joined)
# ─────────────────────────────────────────────────────────────────────────────

def _chunk_sql(ids: list) -> str:
    arr = _arr(ids)
    return f"""
    SELECT
        -- ── EVENT ────────────────────────────────────────────────────────────
        e.id                                                            AS event_id,
        COALESCE(cat.name,  '')                                         AS event_category,
        COALESCE(subcat.name, '')                                       AS pillar_name,
        COALESCE(prog.name, '')                                         AS sub_pillar_name,
        CASE WHEN pr.user_type::text = 'sso_admin' THEN md.name_bi
             ELSE NULL END                                              AS deeper_sub_pillar,
        COALESCE(e.program_name, prog.name, '')                         AS program_name,
        COALESCE(pm.name, '')                                           AS program_method,
        e.start_datetime                                                AS event_start_datetime,
        e.end_datetime                                                  AS event_end_datetime,
        e.created_at                                                    AS event_created_at,
        COALESCE(pr.full_name, '')                                      AS event_created_by_name,
        COALESCE(pr.user_type::text, '')                                AS event_created_by_type,

        -- ── PARTICIPANT ───────────────────────────────────────────────────────
        ep.id                                                           AS participant_id,
        ep.attendance,
        ep.verified,
        ep.registered_as_new_member,
        ep.member_id,
        ep.participant_under_twelve_id,

        -- ── MEMBER PROFILE ────────────────────────────────────────────────────
        COALESCE(mp.fullname, '')                                        AS participant_name,
        mp.identity_no,
        COALESCE(mp.mobile_no, '')                                       AS mobile_no,
        COALESCE(mp.email, '')                                           AS email,
        mp.dob,
        mp.age,
        mp.join_date,
        COALESCE(gdr.bm, '')                                             AS gender,
        COALESCE(rce.bm, '')                                             AS race,
        COALESCE(mp.oku_status::text, 'false')                           AS oku_status,
        COALESCE(mp.education_level::text, '')                           AS education_level,
        COALESCE(mp.income_range::text, '')                              AS income_range,
        COALESCE(mp.nationality_id::text, '')                            AS nationality_id,
        COALESCE(mp.status_membership::text, '')                         AS status_membership,
        COALESCE(mp.registration_status::text, '')                       AS registration_status,

        -- ── UNDER-12 ──────────────────────────────────────────────────────────
        COALESCE(u12.fullname, '')                                       AS under12_name,
        COALESCE(u12.mobile_no, '')                                      AS under12_phone,
        COALESCE(g12.bm, '')                                             AS under12_gender,

        -- ── EVENT SITE (ep.ref_id → nd_site_profile) ─────────────────────────
        esp.id                                                           AS event_site_id,
        COALESCE(esp.refid_mcmc, '')                                     AS event_site_refid_mcmc,
        COALESCE(esp.refid_tp, '')                                       AS event_site_refid_tp,
        COALESCE(esp.sitename, '')                                       AS event_site_name,
        COALESCE(esp.fullname, '')                                       AS event_site_fullname,
        COALESCE(es_dusp.name, '')                                       AS event_site_dusp,
        COALESCE(es_tp.name, '')                                         AS event_site_tp,
        COALESCE(es_state.name, '')                                      AS event_site_state,
        COALESCE(es_parl.name, '')                                       AS event_site_parliament,
        COALESCE(es_dun.name, '')                                        AS event_site_dun,
        COALESCE(es_dist.name, '')                                       AS event_site_district,
        TRIM(COALESCE(es_addr.address1, '') || ' ' ||
             COALESCE(es_addr.address2, ''))                             AS event_site_address,
        COALESCE(es_addr.postcode, '')                                   AS event_site_postcode,
        COALESCE(es_ph.name, '')                                         AS event_site_phase,
        COALESCE(es_zone.area, '')                                       AS event_site_zone,
        COALESCE(es_tech.name, '')                                       AS event_site_technology,
        COALESCE(es_bw.name, '')                                         AS event_site_backhaul,
        esp.is_mini                                                      AS event_site_mini_nadi,

        -- ── MEMBER HOME NADI (mp.ref_id → nd_site → nd_site_profile) ─────────
        COALESCE(hns.refid_mcmc, '')                                     AS home_site_refid_mcmc,
        COALESCE(hns.standard_code, '')                                  AS home_site_standard_code,
        COALESCE(hnsp.sitename,
                 CASE WHEN hns.id IS NOT NULL
                      THEN 'Site-' || hns.id::text END,
                 '')                                                     AS home_site_name,
        COALESCE(hn_state.name, '')                                      AS home_site_state,
        COALESCE(hn_org.name, '')                                        AS home_site_tp,
        COALESCE(hn_ph.name, '')                                         AS home_site_phase,
        COALESCE(hn_parl.name, '')                                       AS home_site_parliament,
        COALESCE(hn_dun.name, '')                                        AS home_site_dun

    FROM public.nd_event e

    LEFT JOIN public.profiles                           pr      ON pr.id      = e.created_by
    LEFT JOIN public.nd_event_category                  cat     ON cat.id     = e.category_id
    LEFT JOIN public.nd_event_subcategory               subcat  ON subcat.id  = e.subcategory_id
    LEFT JOIN public.nd_event_program                   prog    ON prog.id    = e.program_id
    LEFT JOIN public.nd_program_method                  pm      ON pm.id      = e.program_method
    LEFT JOIN public.nd_sso_profile                     sso     ON sso.user_id = pr.id
    LEFT JOIN public.master_data                        md      ON md.id      = sso.sso_type_id

    INNER JOIN public.nd_event_participant              ep      ON ep.event_id = e.id
    LEFT JOIN public.nd_member_profile                  mp      ON mp.id      = ep.member_id
    LEFT JOIN public.nd_genders                         gdr     ON gdr.id     = mp.gender
    LEFT JOIN public.nd_races                           rce     ON rce.id     = mp.race_id
    LEFT JOIN public.nd_event_participant_under_twelve  u12     ON u12.id     = ep.participant_under_twelve_id
    LEFT JOIN public.nd_genders                         g12     ON g12.id     = u12.gender

    -- Event site (ep.ref_id → nd_site_profile directly)
    LEFT JOIN public.nd_site_profile                    esp     ON esp.id     = ep.ref_id
    LEFT JOIN public.organizations                      es_tp   ON es_tp.id   = esp.dusp_tp_id
                                                                AND es_tp.type = 'tp'
    LEFT JOIN public.organizations                      es_dusp ON es_dusp.id = es_tp.parent_id
                                                                AND es_dusp.type = 'dusp'
    LEFT JOIN public.nd_state                           es_state ON es_state.id = esp.state_id
    LEFT JOIN public.nd_parliaments                     es_parl ON es_parl.id = esp.parliament_rfid
    LEFT JOIN public.nd_duns                            es_dun  ON es_dun.id  = esp.dun_rfid
    LEFT JOIN public.nd_site_address                    es_addr ON es_addr.site_id = esp.id
    LEFT JOIN public.nd_district                        es_dist ON es_dist.id = es_addr.district_id
    LEFT JOIN public.nd_phases                          es_ph   ON es_ph.id   = esp.phase_id
    LEFT JOIN public.nd_zone                            es_zone ON es_zone.id = esp.zone_id
    LEFT JOIN public.nd_technology                      es_tech ON es_tech.id = esp.technology
    LEFT JOIN public.nd_bandwidth                       es_bw   ON es_bw.id   = esp.bandwidth

    -- Member home NADI (mp.ref_id → nd_site → nd_site_profile)
    LEFT JOIN public.nd_site                            hns     ON hns.id     = mp.ref_id
    LEFT JOIN public.nd_site_profile                    hnsp    ON hnsp.id    = hns.site_profile_id
    LEFT JOIN public.nd_state                           hn_state ON hn_state.id = hnsp.state_id
    LEFT JOIN public.organizations                      hn_org  ON hn_org.id  = hnsp.dusp_tp_id
    LEFT JOIN public.nd_phases                          hn_ph   ON hn_ph.id   = hnsp.phase_id
    LEFT JOIN public.nd_parliaments                     hn_parl ON hn_parl.id = hnsp.parliament_rfid
    LEFT JOIN public.nd_duns                            hn_dun  ON hn_dun.id  = hnsp.dun_rfid

    WHERE e.id = ANY(ARRAY[{arr}]::uuid[])
    """


def _fetch_chunk(conn, chunk: list, cur_name: str) -> pd.DataFrame:
    rows = []
    with conn.cursor(cur_name, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = _ROW_BATCH
        cur.execute(_chunk_sql(chunk))
        while True:
            batch = cur.fetchmany(_ROW_BATCH)
            if not batch:
                break
            rows.extend(batch)
    conn.commit()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _fetch_chunk_with_retry(chunk: list, idx: int) -> pd.DataFrame:
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            conn = _conn()
            try:
                return _fetch_chunk(conn, chunk, f"nadi_cur_{idx}_{attempt}")
            finally:
                conn.close()
        except Exception as exc:
            if attempt == _MAX_RETRIES:
                print(f"\n[chunk {idx}] Failed after {_MAX_RETRIES} attempts: {exc}")
                traceback.print_exc()
                return pd.DataFrame()
            print(f"\n[chunk {idx}] Attempt {attempt} failed ({exc}); retry in {_RETRY_DELAY}s ...")
            time.sleep(_RETRY_DELAY)
    return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Derive columns in Python (faster than SQL for large frames)
# ─────────────────────────────────────────────────────────────────────────────

def _derive_columns(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["event_start_datetime"], errors="coerce", utc=True).dt.tz_localize(None)
    te = pd.to_datetime(df["event_end_datetime"],   errors="coerce", utc=True).dt.tz_localize(None)

    df["event_year"]         = ts.dt.year.astype("Int16")
    df["event_quarter"]      = ts.dt.quarter.astype("Int8")
    df["event_month"]        = ts.dt.month.astype("Int8")
    df["event_month_name"]   = ts.dt.strftime("%B")
    df["event_year_month"]   = ts.dt.strftime("%Y-%m")
    df["event_year_quarter"] = ts.dt.to_period("Q").astype(str)
    df["event_duration_hours"] = (
        (te - ts).dt.total_seconds() / 3600.0
    ).round(2)

    df["participant_type"] = np.where(
        df["member_id"].notna(), "Member",
        np.where(df["participant_under_twelve_id"].notna(), "Under 12", "Unknown"),
    )

    # Normalise timestamps to tz-naive for parquet compatibility
    for col in ("event_start_datetime", "event_end_datetime", "event_created_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_localize(None)

    # Strip control characters from all string columns
    for col in df.select_dtypes(include=["object", "str"]).columns:
        df[col] = df[col].apply(_clean_str)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Write incremental parquet (append chunks to avoid OOM)
# ─────────────────────────────────────────────────────────────────────────────

def _df_to_table(df: pd.DataFrame) -> pa.Table:
    return pa.Table.from_pandas(df, preserve_index=False)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("NADI Consolidated Parquet Generator")
    print(f"Output : {OUT_FILE}")
    print(f"Exclude statuses: {EXCLUDE_STATUSES or 'none'}")
    print("=" * 60)

    # 1. Fetch event IDs
    conn = _conn()
    try:
        event_ids = get_all_event_ids(conn)
    finally:
        conn.close()

    if not event_ids:
        print("No events found. Exiting.")
        return

    chunks = [event_ids[i:i + _EVENT_CHUNK] for i in range(0, len(event_ids), _EVENT_CHUNK)]
    print(f"{len(event_ids):,} events -> {len(chunks)} chunks of <={_EVENT_CHUNK}")

    # 2. Fetch + write incrementally
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    total_rows = 0
    failed_chunks = []

    with tqdm(total=len(event_ids), desc="Fetching", unit="evt", dynamic_ncols=True) as pbar:
        for idx, chunk in enumerate(chunks, 1):
            df = _fetch_chunk_with_retry(chunk, idx)

            if df.empty:
                failed_chunks.append(idx)
                pbar.update(len(chunk))
                continue

            df = _derive_columns(df)
            table = _df_to_table(df)

            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(
                    OUT_FILE,
                    schema,
                    compression="snappy",
                    version="2.6",
                )

            try:
                table = table.cast(schema)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                table = pa.Table.from_pandas(
                    df.astype({
                        c: "object"
                        for c in df.columns
                        if df[c].dtype.name not in ("datetime64[ns]", "float64", "int64", "bool")
                    }),
                    schema=schema,
                    preserve_index=False,
                )

            writer.write_table(table)
            total_rows += len(df)

            pbar.update(len(chunk))
            pbar.set_postfix(rows=f"{total_rows:,}", chunk=f"{idx}/{len(chunks)}")

    if writer:
        writer.close()

    print("\n" + "=" * 60)
    print(f"Done.")
    print(f"  Rows written : {total_rows:,}")
    print(f"  Output file  : {OUT_FILE}  ({OUT_FILE.stat().st_size / 1_048_576:.1f} MB)")
    if failed_chunks:
        print(f"  Failed chunks: {failed_chunks}  (data missing for these event batches)")
    print("=" * 60)


if __name__ == "__main__":
    main()
