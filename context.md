# KC Streetcar — Live Load Tracker · Project Context

A real-time passenger-occupancy dashboard for the **KC Streetcar**. It polls the
Swiftly APC (Automatic Passenger Counter) API, computes per-vehicle load and a
live activity feed, and serves a web dashboard. Deployed on Railway.

This document is a handoff so a fresh session can continue the work without
re-deriving the design.

---

## 1. What it does

- Polls Swiftly's APC raw-events endpoint on a background thread (~every 30s).
- Computes **current passengers onboard per vehicle** from the raw ons/offs.
- Builds a **live activity feed** of stop visits (boardings/alightings).
- Infers **direction of travel** (Northbound / Southbound).
- Resolves vehicle positions to **GTFS stop names**.
- Serves everything as JSON at `/api/state`, rendered by a thin HTML dashboard.

---

## 2. Files

| File | Role |
|------|------|
| `swiftly_apc_tracker.py` | Core library: API fetch, occupancy walk, `StopIndex`. Importable; also runs standalone as a logger via `python swiftly_apc_tracker.py`. |
| `app.py` | FastAPI service. Background poller, in-memory live state, `/api/state` JSON, serves `index.html`. **All app logic lives here.** |
| `index.html` | Thin frontend. Polls `/api/state` every ~4s and renders. No business logic. |
| `gps.html` | On-demand GPS-diagnostics page (served at `/gps`). Calls `/api/gps-diagnostics`. |
| `reports.html` | Ridership reports page (served at `/reports`). Cumulative counter, daily trend, sortable directional by-stop table. Calls `/api/reports/*`. |
| `db.py` | Postgres persistence (Phase 1: raw event capture). No-op unless `DATABASE_URL` is set. |
| `schema.sql` | Reference DDL. Phase 1 (`apc_events`) is auto-created by `db.init_schema()`; Phase 2 rollups are commented. |
| `requirements.txt` | `requests`, `fastapi`, `uvicorn[standard]`, `tzdata`. |
| `railway.json` | Railway deploy config (Railpack builder, start command, single replica). |
| `stops.txt` | GTFS stops. **Currently the full agency feed — includes bus stops.** |
| `filter_streetcar_stops.py` | One-time local utility: writes a streetcar-only stops file from a full GTFS folder (route_type 0). Stdlib only. |
| `nearest_stop.py` | Local diagnostic: distance from a coordinate to the nearest stop in a stops file. Stdlib only. |

---

## 3. Architecture & data flow

```
Swiftly APC API ──poll──> poller thread ──> compute ──> LiveState (in memory)
   (full day per request)   (app.py)         (pure)         │
                                                            ├──> /api/state (JSON)
                                                            └──> index.html (renders)
```

- **Frontend is a dumb display layer.** `/api/state` is the contract between the
  two halves; anything else (a map, an alerter, a logger) can read the same
  endpoint. It is the real API, not a debug route.
- **State is in memory only.** Each poll overwrites the last; nothing is
  persisted. Occupancy is recomputed from the API each cycle, so a restart
  reconstructs it immediately; the feed simply repopulates.

---

## 4. The Swiftly API (important quirks)

- Endpoint: `GET https://api.goswift.ly/ridership/kcata/apc-raw-events?date=YYYY-MM-DD`
  with header `Authorization: <API_KEY>`.
- **Returns the WHOLE day** for a date — there is no time parameter.
- Buckets events into a date by the event's own timestamp (calendar date, not
  GTFS service-date). After-midnight events land in the new date's payload.
- Each event has a unique, monotonically increasing `id`, plus `vehicle_id`,
  `time` (agency-local, no tz), `latitude`, `longitude`, `ons`, `offs`.
- Emits occasional **heartbeat records** with `ons:0, offs:0`.
- Reports are **per door**, so one stop visit produces several records seconds apart.
- Rate limit: **1500 requests / 15 min**. We do 1–2 requests per cycle — far under.

---

## 5. Core algorithms & the reasoning behind them

### Occupancy (`occupancy_since_last_gap` in `swiftly_apc_tracker.py`)
- Occupancy is a **pure function of the event set**, recomputed from scratch each
  poll and keyed on the unique `id`. Recompute (not accumulate) ⇒ an event can
  never be double-counted no matter how often we poll.
- Walk a vehicle's events in time order, sum `ons − offs`. **Reset to 0** at:
  - a **gap > `GAP_RESET_HOURS`** (3h) → vehicle was at the depot;
  - a **terminal stop** (everyone must exit). On arrival the count zeroes; during
    the terminal dwell only **boardings** are counted (the mass-deboarding offs
    are ignored because the reset already accounts for them). This re-anchors to
    ground truth and corrects accumulated APC drift.
- **Floored at 0 at every step** (not just the end). Critical: a mass deboarding
  (e.g. an unconfigured turnback) must not drive the running total negative and
  mask the boardings that follow it. This was a real bug — cars with obvious
  positive activity were reading 0.
- Terminal reset is robust to long dwells / GPS blips: it re-fires only if the
  car hasn't been at a terminal within `terminal_rearrive_s` (default 900s).

### Lookback (`gather_events`)
- A run can be ~20h (pull-out 05:45 → in service until 01:30 next day; driver
  reliefs are quick swaps, not depot returns). So each poll fetches **every
  calendar date the window `[now − LOOKBACK_HOURS, now]` touches** (LOOKBACK_HOURS
  = 22) and merges them, so a long / cross-midnight run reconstructs fully.

### Activity feed (`build_feed` in `app.py`)
- Re-derived from the recent event window **every poll** (not append-only). An
  in-progress dwell shows as a live line that grows each cycle, then finalizes
  when the car leaves.
- Visits are clustered **by location**: events within `CLUSTER_RADIUS_M` of where
  the visit started are one visit (so per-door records and long terminus dwells
  collapse). A new visit starts on a location change or after `DWELL_MAX_GAP_S`.
- Per-door ons/offs are summed into one visit. The log line shows the **full
  activity** (`+ons / −offs`); the occupancy counter is the separate boardings-only
  view after a terminal reset — they're consistent by design.
- Visits with **no activity** (`ons==0 and offs==0`) are hidden (heartbeat noise).
- Visit `id` = min event id in the cluster (stable, so the frontend animates a
  new line once and updates it in place rather than re-animating).

### Direction of travel (`infer_direction` + `direction_override`)
Priority order:
1. **Couplet override** (`STOP_DIRECTION`): some stops are one-way by geometry —
   River Market & Delaware are always Southbound, City Market always Northbound
   (the north downtown couplet, where latitude can't tell direction).
2. **Actual movement**: sign of the most recent meaningful north-south change in
   position. Correct regardless of terminal detection (a southbound car reads SB
   even if the north terminus isn't matched).
3. **Recent terminal fallback**: when the car is stationary, direction it's about
   to depart in (away from the terminus). A stale terminal from a prior round trip
   is ignored.

### Stop resolution (`StopIndex`, `resolve_cluster_location`, `resolve_recent_location`)
- Nearest GTFS stop within `max_meters` (haversine, linear scan).
- A visit's stop is decided by **majority vote across the cluster**, so one
  drifted GPS fix can't blank the name.
- An active vehicle's location falls back to its **most recent resolvable** fix.

### Persistence (`db.py`, Phase 1 — raw capture)
- **Fully optional**: no-op unless `DATABASE_URL` is set, so the app is unchanged
  without a database. DB failures never crash the tracker; capture retries next poll.
- Each poll, `capture_events` inserts new raw events (id above an in-memory
  high-water mark) into `apc_events` via `execute_values` + `ON CONFLICT (id) DO
  NOTHING`. High-water advances only on a successful write. On startup the mark is
  seeded from `MAX(id)` in the table.
- `apc_events` is the immutable source of truth (see schema.sql). Every future
  metric is derived from it and rebuildable, so logic changes never rewrite history.
- Known gaps to close in Phase 2: (a) the high-water race can rarely skip an
  out-of-order-committed id — a periodic full-day reconcile fixes it; (b) events
  missed during long downtime older than the fetch lookback need an explicit
  backfill (fetch specific past dates).

### Rollups & reports (`build_stop_hourly`, Phase 2)
- **Stack is flat, not a cascade.** `apc_events` (raw) → `stop_hourly` (per stop,
  per hour, **per direction**). Coarser periods (day/week/month/year, last-4h) are
  **aggregate-on-read** `GROUP BY` queries over `stop_hourly`, not stored tables —
  it's small enough (~300k–900k rows/yr) that this is instant. `vehicle_daily`
  (planned) is a *sibling* off raw, not a child (peak-onboard needs the occupancy walk).
- `build_stop_hourly(date)` reads that date's raw, groups by vehicle and time-sorts,
  tags each door-active event with its **as-of travel direction** (same priority as
  the live board: couplet name override → movement trend → terminal anchor; `Unknown`
  if none), resolves the stop via `StopIndex`/VMF, buckets by (hour, stop_name,
  direction), and full-replaces the date's rows (idempotent, rebuildable). Unresolved
  activity → `(unmatched)` bucket (keeps totals reconciling); VMF excluded. One-way
  stops naturally only ever get one direction's rows.
- Startup: rebuild all raw dates missing from the rollup, plus today+yesterday; then
  refresh today+yesterday every `ROLLUP_INTERVAL_S` (300s). Ridership = `SUM(ons)`.
- Report endpoints: `/api/reports/summary` (today's totals), `/api/reports/by-stop?hours=`
  (per-stop NB/SB split + combined total, sorted by activity; excludes `(unmatched)`),
  `/api/reports/daily?days=|frm=|to=` (per-day totals, custom range). Degrade to empty
  without a DB.
- Served at `/gps` (page) + `/api/gps-diagnostics` (JSON). **Not** part of the poll
  loop — computed fresh per request, so it can do a full-day scan cheaply.
- Method: an event with a boarding or alighting (`ons>0 or offs>0`, excluding VMF)
  was definitely at a stop, so its offset to the **nearest** stop (unbounded) is the
  GPS drift. Aggregated by vehicle and by stop.
- Metrics: mean/max/percentile drift, match-fail % (beyond the radius), directional
  **bias** (mean offset vector → compass direction + magnitude), and **consistency**
  = |mean vector| / mean magnitude (near 1 = systematic → wrong stop coord or antenna;
  near 0 = random receiver noise), plus the largest individual offsets (outliers).
- The page also shows a **settings panel** (stop-match / VMF / terminal radii, VMF
  center, stops loaded) and a **"VMF Activity (Excluded from Tracker)"** table listing
  the door-active fixes dropped by the VMF zone, sorted farthest-from-VMF first so the
  team can watch the yard/River-Market boundary. VMF stays excluded everywhere else.

---

## 6. Configuration

**Environment variables**
- `SWIFTLY_API_KEY` — required.
- `STOPS_FILE` — path to GTFS stops (relative resolves against app dir).
- `STOP_MATCH_RADIUS_M` — stop-match radius, default **175**.
- `TERMINAL_RADIUS_M` — terminus-detection radius, default **150**.
- `CLUSTER_RADIUS_M` — feed clustering radius, default **100**.
- `VMF_RADIUS_M` — maintenance-facility non-revenue zone radius, default **230**.
- `DATABASE_URL` — Postgres connection string. If unset, persistence is fully
  disabled and the app runs as before. Set by Railway when a Postgres service is linked.
- `PORT` — set by Railway.

**Constants in `app.py` / `swiftly_apc_tracker.py`**
- `AGENCY = "kcata"`, `AGENCY_TZ = America/Chicago` (roll at local midnight).
- `CAPACITY = 150` (crowding bar; KC Streetcar approx).
- `GAP_RESET_HOURS = 3`, `LOOKBACK_HOURS = 22`, `POLL_INTERVAL_S = 30`.
- `ACTIVE_WINDOW_MIN = 30`, `FEED_WINDOW_MIN = 120`, `FEED_MAX = 120`.
- `TERMINAL_STOP_NAMES = ["UMKC", "Riverfront"]` (substring match on stop_name).
- `STOP_DIRECTION = {"Delaware": "Southbound", "Riverfront": "Southbound", "City Market": "Northbound"}` — reliable one-way anchors only (River Market's NB/SB couplet is excluded because a car can match the wrong side).
- `VMF_LAT, VMF_LON = 39.112475, -94.577264` — Vehicle Maintenance Facility. A
  non-revenue zone: a vehicle whose latest fix is within `VMF_RADIUS_M` is treated
  as out of service (excluded from the vehicle list and feed), and VMF events reset
  occupancy and are ignored (so a car pulling out starts fresh at 0).

---

## 7. `/api/state` shape

```jsonc
{
  "updated_at": "2026-06-27T12:31:09-05:00",
  "active_count": 8,
  "total_onboard": 142,
  "capacity": 150,
  "stops_loaded": true,
  "vehicles": [
    { "vehicle": "810", "count": 86, "last_time": "...",
      "direction": "Northbound", "stop_id": "1812",
      "stop": "UNION HILL (31ST & MAIN ST)", "lat": ..., "lon": ... }
  ],
  "feed": [
    { "id": 123, "vehicle": "812", "time": "...", "direction": "Southbound",
      "ons": 0, "offs": 27, "doors": 3,
      "stop_id": "...", "stop": "PLAZA ...", "lat": ..., "lon": ... }
  ],
  "error": null
}
```

---

## 8. Deployment (Railway)

- GitHub repo → Railway service. `railway.json` pins Railpack + the uvicorn start
  command + `numReplicas: 1`.
- **Single instance only** — each process runs its own poller; two would double
  API load and split the feed. Do **not** use gunicorn multi-workers.
- Use an **always-on** plan; a sleeping service stops polling and staleness the
  data (a sleep is not a restart).
- Set `SWIFTLY_API_KEY` (and `STOPS_FILE`, optional radius overrides) in Railway
  **Variables**. Env vars that have code defaults do **not** appear in the
  Variables panel unless you add them.
- Run locally: `pip install -r requirements.txt`, `export SWIFTLY_API_KEY=...`,
  `python app.py` → http://localhost:8000.

---

## 9. Testing approach

There is no test file yet. Logic has been verified by importing the modules and
feeding synthetic event dicts to the pure functions
(`occupancy_since_last_gap`, `build_feed`, `infer_direction`, `StopIndex.nearest`)
and asserting expected outputs, plus booting uvicorn against a stubbed
`core.fetch_day` and curling `/` and `/api/state`. Worth formalizing into a
`pytest` suite.

---

## 10. Known issues & recommended next steps

1. **`stops.txt` includes bus stops** (~2300 stops). Risk: a streetcar mid-block
   can match a nearby bus stop. Fix: run `filter_streetcar_stops.py <gtfs_folder>
   streetcar_stops.txt`, commit it, set `STOPS_FILE=streetcar_stops.txt`. Then the
   match/terminal radii can be widened freely.
2. **Riverfront resolves poorly** (worst offender) — likely the car reports from a
   turnback point off the platform, or the GTFS coord is off. Use `nearest_stop.py`
   to measure; fix the stop coord or, after filtering to streetcar-only stops,
   raise `STOP_MATCH_RADIUS_M` / `TERMINAL_RADIUS_M` (env vars). This also tightens
   the north-end drift reset and southbound direction calls.
3. **Feed clustering can split a stop on large GPS jitter.** Mitigated by the
   no-activity filter + `CLUSTER_RADIUS_M`. A more robust approach is to cluster by
   **resolved stop_id** (leverage the match radius for grouping too) instead of raw
   distance — recommended once stops are filtered to streetcar-only.
4. **`/api/state` is unauthenticated, unversioned, and snapshot-only** (no history).
   Before pointing heavier/public consumers (a map, alerts) at it: add a key or
   rate limit, freeze field names or add a `/api/v1/` prefix, and add a separate
   persistence layer if you want trends (safe to add alongside — the tracker is
   stateless).
5. **APC drift** is corrected *across* trips by the terminal resets, but *within*
   a single leg the APC error rides along until the next terminus. Inherent to APC;
   usually small over one leg.

---

## 11. Quick glossary of the non-obvious decisions

- *Why recompute occupancy every poll instead of accumulating?* Idempotency — the
  API returns the whole day, so re-summing can't double-count; accumulating could.
- *Why does a car read single digits right after a terminus?* Correct — everyone
  exited; the count is now just the return-trip boarders.
- *Why per-step floor?* So a mass deboarding can't push the count negative and hide
  later boardings (was causing phantom zeros).
- *Why fetch up to ~22h back?* A single continuous run can span ~20h across midnight.
- *Why majority-vote stop names?* One drifted GPS fix shouldn't blank a stop label.
