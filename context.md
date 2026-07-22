# KC Streetcar — Live Load Tracker · Project Context

A real-time passenger-occupancy dashboard for the **KC Streetcar**. It polls the
Swiftly APC (Automatic Passenger Counter) API, computes per-vehicle load and a live
activity feed, persists raw events to Postgres, derives ridership reports, and serves
three web pages. Deployed on Railway.

This document is a handoff so a fresh session can continue the work without
re-deriving the design. Everything described here is **built and live** unless a line
explicitly says "planned".

---

## 1. What it does

- Polls Swiftly's APC raw-events endpoint on a background thread (~every 30s).
- Computes **current passengers onboard per vehicle** from the raw ons/offs.
- Tracks **peak vehicle load** and **peak system load** for the current service day.
- Builds a **live activity feed** of stop visits (boardings/alightings).
- Infers **direction of travel** (Northbound / Southbound).
- Resolves vehicle positions to **GTFS stop names**.
- **Persists raw events** to Postgres and derives **directional per-stop ridership
  rollups** + report endpoints and a reports page.
- Runs an on-demand **GPS-diagnostics** page.
- Serves the live board plus JSON at `/api/state` (the contract for any consumer).

---

## 2. Files

| File | Role |
|------|------|
| `swiftly_apc_tracker.py` | Core library: API fetch, `gather_events`, the occupancy walk (`occupancy_since_last_gap`), `StopIndex`, `counts_ok`, `RateLimiter`. Importable; also runs standalone as a logger via `python swiftly_apc_tracker.py`. |
| `app.py` | FastAPI service. Background poller, in-memory `LiveState`, live/peak computation, feed, direction, stop resolution, VMF exclusion, glitch rejection, GPS diagnostics, rollups, report + rebuild endpoints, serves the pages + icons. **Most app logic lives here.** |
| `db.py` | Postgres persistence: raw `apc_events` capture + `stop_hourly` rollup read/write helpers. No-op unless `DATABASE_URL` is set; DB failures never crash the tracker. |
| `index.html` | Live board. Polls `/api/state` ~4s and renders. No business logic. |
| `gps.html` | On-demand GPS-diagnostics page (`/gps`). Calls `/api/gps-diagnostics`. |
| `reports.html` | Ridership reports page (`/reports`). Cumulative counter, daily trend, sortable directional by-stop table. Calls `/api/reports/*`. |
| `schema.sql` | Reference DDL. `apc_events` + `stop_hourly` are active (auto-created by `db.init_schema()`); `vehicle_daily` is commented (planned). |
| `site.webmanifest` + icons | PWA manifest + favicon/app-icon set (`favicon.ico/.svg`, `favicon-96x96.png`, `apple-touch-icon.png`, `web-app-manifest-{192,512}.png`). Served from `/` by app.py (looks in `favicon/` then the app root). All pages link them; index.html is the installable start_url. |
| `requirements.txt` | `requests`, `fastapi`, `uvicorn[standard]`, `psycopg2-binary`, `tzdata`. |
| `railway.json` | Railway deploy config (Railpack builder, uvicorn start command, single replica). |
| `streetcar_stops.txt` | GTFS stops, **streetcar-only** (route_type 0), pointed to by `STOPS_FILE`. Root station names (street detail trimmed) so two-sided stops share a name and aggregate. |
| `filter_streetcar_stops.py` | Local utility: writes a streetcar-only stops file from a full GTFS folder. Stdlib only. |
| `nearest_stop.py` | Local diagnostic: distance from a coordinate to the nearest stop. Stdlib only. |
| `stop_spacing.py` | Local diagnostic: nearest-neighbor spacing between stops (informs the match radius). Stdlib only. |
| `check_stop_pairs.py` | Local diagnostic: flags close-together stop pairs whose names differ (they'd double-count in reports). Stdlib only. |

---

## 3. Architecture & data flow

```
Swiftly APC API ─poll─> poller thread ─> compute ─> LiveState (in memory) ─> /api/state ─> pages
   (full day/req)        (app.py)         (pure)          │
                                                          ├─> capture_events ─> apc_events (raw, Postgres)
                                                          │                        │
                                                          │              build_stop_hourly (derive)
                                                          │                        │
                                                          └─> compute_system_peak   stop_hourly ─> /api/reports/*
```

- **Frontend is a dumb display layer.** `/api/state` and `/api/reports/*` are the
  contracts; anything else can read them.
- **Live state is in memory**; each poll overwrites the last. Occupancy is recomputed
  from the API each cycle, so a restart reconstructs it immediately.
- **Raw is the source of truth.** `apc_events` stores every event unchanged; every
  metric (rollups, reports) is *derived* and rebuildable, so logic changes never
  rewrite history — you re-derive with `POST /api/reports/rebuild`.

---

## 4. The Swiftly API (important quirks)

- Endpoint: `GET https://api.goswift.ly/ridership/kcata/apc-raw-events?date=YYYY-MM-DD`
  with header `Authorization: <API_KEY>`.
- **Returns the WHOLE day** for a date — there is no time parameter.
- Buckets events into a date by the event's own timestamp (calendar date). After-
  midnight events land in the new calendar date's payload.
- Each event has a unique, monotonically increasing `id`, plus `vehicle_id`, `time`
  (agency-local, no tz), `latitude`, `longitude`, `ons`, `offs`.
- Emits occasional **heartbeat records** (`ons:0, offs:0`).
- Reports are **per door**, so one stop visit produces several records seconds apart.
- Occasionally emits **glitch records** with impossible counts (e.g. 749 boardings in
  one record) — handled by `MAX_DOOR_COUNT` (see below).
- Rate limit: **1500 requests / 15 min**. We do 1–2 per cycle — far under.

---

## 5. Core live algorithms & the reasoning behind them

### Occupancy (`occupancy_since_last_gap` in `swiftly_apc_tracker.py`)
- A **pure function of the event set**, recomputed from scratch each poll and keyed on
  the unique `id`. Recompute (not accumulate) ⇒ an event can never be double-counted.
- Walk a vehicle's events in time order, sum `ons − offs`. **Reset to 0** at:
  - a **gap > `GAP_RESET_HOURS`** (3h) → vehicle was at the depot;
  - a **terminal stop** (everyone exits). On arrival the count zeroes; during the
    dwell only **boardings** are counted (mass-deboarding offs ignored — the reset
    already accounts for them). Re-anchors to ground truth, correcting APC drift.
  - a **VMF (non-revenue) event** → out of service, count 0.
- **Floored at 0 at every step** (not just the end): a mass deboarding must not drive
  the running total negative and mask the boardings that follow (was a real bug).
- Terminal reset is robust to long dwells / GPS blips: re-fires only if the car hasn't
  been at a terminal within `terminal_rearrive_s` (default 900s).
- **Glitch rejection**: a record whose `ons` or `offs` exceeds `MAX_DOOR_COUNT`
  (default 100, env-tunable) contributes nothing — via `core.counts_ok()`. It keeps
  its position/terminal role but its counts are ignored in every account (occupancy,
  feed, rollups, diagnostics). The raw record is still stored, so outliers stay
  available for analysis.

### Peak vehicle load today
- `occupancy_since_last_gap(..., with_peak=True, peak_since=)` (a **default-off flag**,
  so the plain live call is unchanged) also returns the highest running occupancy and
  its time, counting only moments at/after the service-day start.
- The poller takes the max across **all** vehicles (even now-idle ones that peaked
  earlier) → `peak_load` / `peak_time` / `peak_vehicle` on `/api/state`.

### Peak system load today (`compute_system_peak` in `app.py`)
- The max over the service day of the **sum of every vehicle's occupancy at one
  instant** — i.e. the running max of the same quantity the live "onboard" number
  measures right now.
- Uses the default-off `with_series` flag to get each car's occupancy step-series,
  merges all cars' step-changes onto one timeline, and sweeps once. A car contributes
  its occupancy for `ACTIVE_WINDOW_MIN` (30 min) after each report, then expires to 0
  — the same "active" rule that defines the live onboard total, evaluated at each
  historical instant. O(n log n) sort + O(n) sweep.
- Runs on a throttle (`SYSTEM_PEAK_INTERVAL_S`, 60s) **after** the live board is
  published in `poll_once`, so it can never delay the core numbers (~50ms/run at a full
  day's volume). Exposed as `system_peak` / `system_peak_time`.

### Lookback (`gather_events`)
- A run can be ~20h (pull-out ~05:45 → in service until ~01:30; reliefs are quick swaps,
  not depot returns). Each poll fetches **every calendar date the window
  `[now − LOOKBACK_HOURS, now]` touches** (22h) and merges them, so a long / cross-
  midnight run reconstructs fully.

### Activity feed (`build_feed` in `app.py`)
- Re-derived from the recent window **every poll** (not append-only). An in-progress
  dwell shows as a live line that grows, then finalizes when the car leaves.
- Visits clustered **by location** (`CLUSTER_RADIUS_M`); a new visit starts on a
  location change or after `DWELL_MAX_GAP_S`. Per-door ons/offs summed into one visit.
- No-activity visits (`ons==0 and offs==0`) hidden. VMF and glitch records excluded.
- Visit `id` = min event id in the cluster (stable, so the frontend animates once).

### Direction of travel (`infer_direction` + `direction_override`)
Priority order:
1. **One-way anchor override** (`STOP_DIRECTION`): stops that are one-way by geometry —
   Delaware & Riverfront always Southbound, UMKC & City Market always Northbound.
   River Market is deliberately **excluded** (its NB/SB couplet at 3rd & Grand sits so
   close a car can match the wrong side).
2. **Actual movement**: sign of the most recent meaningful latitude change
   (`DIR_MOVE_DEG` ≈ 55m). Correct even if a terminal isn't matched.
3. **Recent terminal fallback**: when stationary, the direction it's about to depart in
   (away from the terminus). A stale terminal from a prior round trip is ignored.

### Stop resolution (`StopIndex`, `resolve_cluster_location`, `resolve_recent_location`)
- Nearest GTFS stop within `max_meters` (`STOP_MATCH_RADIUS_M`, haversine, linear scan).
- A visit's stop is decided by **majority vote across the cluster**, so one drifted GPS
  fix can't blank the name.
- An active vehicle's location falls back to its **most recent resolvable** fix.

### VMF (Vehicle Maintenance Facility) exclusion
- `VMF_LAT, VMF_LON = 39.112475, -94.577264`; a fix within `VMF_RADIUS_M` (230m, sized
  to cover the staging yard while staying ~150m clear of River Market ~380m away) is
  non-revenue. A vehicle whose latest fix is there is hidden; VMF events reset occupancy
  and are excluded from feed/rollups/diagnostics.

---

## 6. Persistence, rollups & reports

### Raw capture (`db.py`, `capture_events`)
- Optional: no-op unless `DATABASE_URL` is set. DB failures never crash the tracker.
- Each poll inserts new events (id above an in-memory high-water mark) into `apc_events`
  via `execute_values` + `ON CONFLICT (id) DO NOTHING`. High-water advances only on a
  successful write; seeded from `MAX(id)` at startup.
- `apc_events` is the immutable source of truth. Open edge cases: the high-water race
  can rarely skip an out-of-order-committed id (a periodic full-day reconcile would fix
  it), and events missed during a long outage older than the fetch lookback need an
  explicit date-range backfill.

### Rollups (`build_stop_hourly`)
- **Flat stack, not a cascade.** `apc_events` (raw) → `stop_hourly` (per stop, per hour,
  **per direction**). Coarser periods (day/week/month/year, last-4h) are
  **aggregate-on-read** `GROUP BY` queries — the table is small (~300k–900k rows/yr).
  `vehicle_daily` (planned) would be a *sibling* off raw (peak-onboard needs the walk).
- `build_stop_hourly(date)` reads that date's raw, groups by vehicle + time-sorts, tags
  each door-active event with its **as-of travel direction** (same priority as the live
  board; `Unknown` if none), resolves the stop, buckets by (hour, stop_name, direction),
  and full-replaces the date's rows (idempotent, rebuildable). Unresolved activity →
  `(unmatched)` bucket (keeps totals reconciling); VMF + glitch records excluded.
- Poller: on startup rebuilds any raw dates missing from the rollup + today + yesterday,
  then refreshes today + yesterday every `ROLLUP_INTERVAL_S` (300s). Ridership = `SUM(ons)`.

### Service day
- Reports scope to a **service day** running `SERVICE_DAY_CUTOFF_H` (default 4am) to the
  same hour next day, so post-midnight running counts toward the day it started and the
  counter resets in the pre-dawn quiet. Applied **at query time** from `bucket_start`
  (raw stays a pure calendar fact), so the cutoff can be retuned with just a rebuild.
  `service_day_start()` computes the current one; summary + the "Day" toggle +
  busiest-today use it; daily groups by it.

### Report endpoints
- `/api/reports/summary` — current service day's boardings/alightings.
- `/api/reports/by-stop?hours=|scope=service_day` — per-stop NB/SB split + combined
  total, sorted by activity, excludes `(unmatched)`. "Day" toggle sends `scope=service_day`.
- `/api/reports/daily?days=|frm=|to=` — per-service-day totals for the trend.
- All degrade to empty without a DB.

### Rebuild (`POST /api/reports/rebuild`, token-gated by `REBUILD_TOKEN`)
- Re-derives rollups from raw after a logic/data change. **No range = ALL captured
  dates** (safe default — a partial rebuild leaves old+new logic mixed). Optional
  `frm`/`to` for the niche date-local case. Runs in a background thread (won't time out);
  `GET` the same path with the token to poll progress. 503 if no token set, 401 if
  wrong, 409 if already running. Invoke:
  `curl -X POST -H "X-Rebuild-Token: SECRET" https://<app>/api/reports/rebuild`.

---

## 7. GPS diagnostics (`/gps` + `/api/gps-diagnostics`)

- **On-demand**, computed fresh per request (not in the poll loop), so it can do a
  full-day scan cheaply.
- Method: an event with door activity (`ons>0 or offs>0`, excluding VMF + glitches) was
  definitely at a stop, so its offset to the **nearest** stop (unbounded) is the GPS
  drift. Aggregated by vehicle and by stop.
- Metrics: mean/max/percentile drift, match-fail % (beyond the match radius), directional
  **bias** (mean offset vector → compass direction + magnitude), and **consistency** =
  |mean vector| / mean magnitude (near 1 = systematic → wrong stop coord or antenna;
  near 0 = random receiver noise), plus the largest individual offsets.
- The page also shows a **settings panel** (radii, VMF center, stops loaded) and a
  **"VMF Activity (Excluded from Tracker)"** table (door-active fixes dropped by the VMF
  zone, farthest-from-VMF first) so the team can watch the yard/River-Market boundary.

---

## 8. Configuration

**Environment variables**
- `SWIFTLY_API_KEY` — required.
- `STOPS_FILE` — path to the streetcar-only GTFS stops (relative resolves against app dir).
- `STOP_MATCH_RADIUS_M` — stop-match radius (default **175**).
- `TERMINAL_RADIUS_M` — terminus-detection radius (default **150**).
- `CLUSTER_RADIUS_M` — feed clustering radius (default **100**).
- `VMF_RADIUS_M` — maintenance-facility non-revenue zone radius (default **230**).
- `VEHICLE_CAPACITY` — crowding-bar capacity (default **150**), sent to the frontend.
- `MAX_DOOR_COUNT` — single-record count above which a record is a glitch (default **100**).
- `SERVICE_DAY_CUTOFF_H` — service-day boundary hour (default **4**).
- `DATABASE_URL` — Postgres connection string. Unset ⇒ persistence fully disabled.
- `REBUILD_TOKEN` — secret enabling the rebuild endpoint. Unset ⇒ endpoint returns 503.
- `PORT` — set by Railway.

**Key constants (`app.py` / `swiftly_apc_tracker.py`)**
- `AGENCY = "kcata"`, `AGENCY_TZ = America/Chicago`.
- `GAP_RESET_HOURS = 3`, `LOOKBACK_HOURS = 22`, `POLL_INTERVAL_S = 30`, `FLOOR_AT_ZERO = True`.
- `ACTIVE_WINDOW_MIN = 30`, `FEED_WINDOW_MIN = 120`, `FEED_MAX = 120`, `DWELL_MAX_GAP_S = 900`.
- `ROLLUP_INTERVAL_S = 300`, `SYSTEM_PEAK_INTERVAL_S = 60`.
- `DIR_MOVE_DEG = 0.0005` (~55m of latitude = real movement, not jitter).
- `TERMINAL_STOP_NAMES = ["UMKC", "Riverfront"]` (substring match).
- `STOP_DIRECTION = {"Delaware": "Southbound", "Riverfront": "Southbound", "UMKC": "Northbound", "City Market": "Northbound"}` — reliable one-way anchors only (River Market excluded).
- `VMF_LAT, VMF_LON = 39.112475, -94.577264`.
- Note: raw `service_date` rolls at calendar midnight; **reports** use the 4am service day.

---

## 9. `/api/state` shape

```jsonc
{
  "updated_at": "2026-06-27T12:31:09-05:00",
  "active_count": 8,
  "total_onboard": 142,
  "capacity": 150,
  "stops_loaded": true,
  "peak_load": 96,               // highest single-vehicle load this service day
  "peak_time": "3:42 PM",
  "peak_vehicle": "810",
  "system_peak": 210,            // highest system-wide sum this service day
  "system_peak_time": "5:18 PM",
  "vehicles": [
    { "vehicle": "810", "count": 86, "last_time": "...", "direction": "Northbound",
      "stop_id": "1812", "stop": "Union Hill", "lat": 0, "lon": 0 }
  ],
  "feed": [
    { "id": 123, "vehicle": "812", "time": "...", "direction": "Southbound",
      "ons": 0, "offs": 27, "doors": 3, "stop_id": "...", "stop": "Plaza",
      "lat": 0, "lon": 0 }
  ],
  "error": null
}
```

---

## 10. Deployment (Railway)

- GitHub repo → Railway service. `railway.json` pins Railpack + the uvicorn start
  command + `numReplicas: 1`.
- **Single instance only** — each process runs its own poller; two would double API
  load and split the feed. Do **not** use gunicorn multi-workers.
- Use an **always-on** plan; a sleeping service stops polling.
- Postgres: add a Railway Postgres service and set `DATABASE_URL` on the app service as
  a **reference variable** to it. Icons live in `favicon/` (served from root URLs).
- Env vars with code defaults don't appear in the Variables panel unless you add them.
- Run locally: `pip install -r requirements.txt`, `export SWIFTLY_API_KEY=...`,
  `python app.py` → http://localhost:8000.

---

## 11. Testing approach

No test suite yet. Logic is verified by importing the modules and feeding synthetic
event dicts to the pure functions (`occupancy_since_last_gap`, `build_feed`,
`infer_direction`, `StopIndex.nearest`, `compute_system_peak`, `build_stop_hourly`) and
asserting outputs; DB paths via a pip-installable embedded Postgres (`pgserver`); and
boot smoke tests (uvicorn against a stubbed `core.fetch_day`, curl the routes). Worth
formalizing into `pytest`.

---

## 12. Known issues & recommended next steps

1. **`vehicle_daily` rollup not built** — the planned per-vehicle daily table (boardings,
   alightings, peak_onboard). It's a sibling off raw; peak_onboard needs the occupancy
   walk. Would power a per-car view on the reports page.
2. **High-water race / downtime backfill** (see §6) — a periodic full-day reconcile and
   an explicit older-than-lookback backfill remain open.
3. **`/api/state` is unauthenticated, unversioned, snapshot-only.** Before pointing
   heavier/public consumers at it, add a key/rate-limit and freeze field names (or add
   `/api/v1/`).
4. **Feed clustering can split a stop on large GPS jitter.** Mitigated by the
   no-activity filter + `CLUSTER_RADIUS_M`; clustering by resolved `stop_id` would be
   more robust.
5. **APC drift within a leg** rides along until the next terminus reset. Inherent to APC;
   usually small.

---

## 13. Glossary of non-obvious decisions

- *Why recompute occupancy every poll instead of accumulating?* Idempotency — the API
  returns the whole day, so re-summing can't double-count.
- *Why does a car read single digits right after a terminus?* Correct — everyone exited;
  the count is now just the return-trip boarders.
- *Why per-step floor?* So a mass deboarding can't push the count negative and hide later
  boardings.
- *Why fetch up to ~22h back?* A single continuous run can span ~20h across midnight.
- *Why majority-vote stop names?* One drifted GPS fix shouldn't blank a stop label.
- *Why store raw and derive everything?* So changing matching / direction / VMF / glitch
  logic never rewrites history — re-derive with the rebuild endpoint.
- *Why is River Market not a direction anchor?* Its NB/SB couplet is too close; a car can
  match the wrong side. Delaware / UMKC / Riverfront / City Market are the safe anchors.
- *Why a 4am service day?* So post-midnight running counts toward the day it started and
  the counter resets in the quiet pre-dawn window.
