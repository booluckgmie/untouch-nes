# NES Analytics Dashboard

Combined NADI Index + Weekly Drill-Down · Live PostgreSQL fetch · Flask backend

---

## Quick start

```bash
cd nes_dashboard
bash run.sh          # installs deps, tests DB, starts server
```

Open **http://localhost:5001** in your browser.

---

## Manual steps (if run.sh fails)

```bash
pip install flask psycopg2-binary pandas

# Verify DB
python3 -c "from db_configs import get_conn; c=get_conn(); print('OK')"

# Start server
python3 app.py
```

---

## First use workflow

1. Open http://localhost:5001 — you'll see the Overview page
2. In the sidebar, click **⬇ fetch from DB** for each subcategory
3. A background thread fetches from PostgreSQL and caches locally (JSON in `data/`)
4. Green dot appears when ready — click the subcategory to explore

**Fetch order recommendation** (fastest → slowest):
- sub05 GovInit → sub06 Entrepreneur → sub07 Lifelong Learning (smallest)
- sub01–04, sub09 (medium)
- sub08 Wellbeing (largest — ~11k events, takes ~2–3 min)

Cache TTL = 6 hours. Re-fetch any time via sidebar button.

---

## File structure

```
nes_dashboard/
├── app.py              Flask server + API routes
├── nes_db.py           All DB query functions (fetch, build_nadi_index, build_monthly, build_weekly)
├── cache.py            File-backed JSON cache (6h TTL)
├── db_configs.py       psycopg2 connection factory
├── credential.py       DB host/user/password — DO NOT COMMIT
├── run.sh              One-shot startup script
├── templates/
│   └── index.html      Combined single-page dashboard UI
└── data/               Auto-created cache directory (JSON blobs)
```

---

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Dashboard HTML |
| GET | `/api/sites` | 1,099 NADI index (cached) |
| GET | `/api/subcategories` | All 10 subs + cache status |
| POST | `/api/fetch/<sub_id>` | Trigger background DB fetch |
| GET | `/api/progress/<sub_id>` | Poll fetch progress |
| GET | `/api/data/<sub_id>` | Full cached payload |
| POST | `/api/cache/invalidate/<sub_id>` | Clear one sub's cache |
| POST | `/api/cache/invalidate_all` | Clear all cache |

---

## Dashboard tabs (per subcategory)

### NADI Index
- 1,099 NADI table with status pills (active / partial / untouched)
- Program coverage dots per row
- Filter by state, status, text search
- Click any row → slide panel: program breakdown + monthly sparkline + zero-events list

### Monthly Update
- Heat-map table: rows = NADI, columns = months, cells = event count
- Frozen NADI ref/name/state columns
- Monthly totals footer
- Colour scale: grey=0, light green=1–5, mid=6–20, dark=21–99, teal=100+

### Weekly (SSO + NADI officer view)
Stacked bar chart (red/amber/green) across all weeks

**By week** — click any bar → NADI list for that week → click NADI → right panel shows every `program_name` event with attendance status + SSO ID. Zero events shown with ⚠ banner.

**By NADI** — full 1,099 table with across-all-weeks totals. Filter: has-zero / all-complete / untouched. Click → right panel shows per-week event breakdown, zero events highlighted red.

**⚠ Events (0 attended)** — flat list of all zero-attendance events. Filter by week / state / program / NADI. Shows full `program_name` column. Pre-filtered when bar is clicked.

**Untouched NADI block** — collapsible list of all 1,099 NADI minus active refs = sites with 0 events this subcategory. Grid layout, state + ref shown.

---

## DB schema used

```
nd_event                → event metadata, program_id, subcategory_id, site_id, status_id
nd_event_participant    → participant_id, member_id, attendance
nd_member_profile       → member ref_id (→ nd_site.id)
nd_site                 → refid_mcmc, site_profile_id
nd_site_profile         → sitename, state_id
nd_state                → state name
nd_event_program        → program name, subcategory_id
nd_event_subcategory    → subcategory name, category_id
nd_event_category       → category name (NADI4U / NADI2U)
nd_program_method       → delivery method
```

Status filter: `status_id NOT IN (1, 7, 9)` — excludes draft / cancelled / rejected.
