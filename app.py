#!/usr/bin/env python3
"""
Web service around the Swiftly APC tracker.

Polls Swiftly on a background thread, keeps live per-vehicle occupancy and a
recent-activity feed in memory, and serves both to the frontend.

  Run:   uvicorn app:app --host 0.0.0.0 --port 8000   (single worker)
  Env:   SWIFTLY_API_KEY   required
         STOPS_FILE        optional path to a GTFS stops.txt (lights up stop names)

State is in memory only. Occupancy is recomputed from the API each cycle, so a
restart reconstructs it immediately; the activity feed is ephemeral and simply
repopulates as new events arrive.
"""
from __future__ import annotations

import logging
import math
import hmac
import os
import threading
import time
from contextlib import asynccontextmanager
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

import requests
import uvicorn
from fastapi import FastAPI, Header
from fastapi.responses import FileResponse, JSONResponse

import swiftly_apc_tracker as core
import db

ACTIVE_WINDOW_MIN = 30     # a vehicle is "active" if it reported within this many minutes
FEED_MAX = 120            # stop-visits retained for the activity ticker
CAPACITY = int(os.environ.get("VEHICLE_CAPACITY", "150"))   # nominal crowding-bar capacity (KC Streetcar ~150); env-tunable

# Per-door reporting: one stop visit emits several records (one per door), and a
# car can idle 5-10 min at a terminus while riders trickle on. So cluster by
# LOCATION, not time: events within CLUSTER_RADIUS_M are the same visit no matter
# how spread out, and a new visit starts only when the car moves to a different
# stop. The radius stays well under stop spacing (~150m+) so adjacent stops never
# merge; DWELL_MAX_GAP_S still breaks a same-location revisit a round trip later.
CLUSTER_RADIUS_M = float(os.environ.get("CLUSTER_RADIUS_M", "100"))
DWELL_MAX_GAP_S = 900     # a gap longer than this starts a new visit (same-stop revisit)
FEED_WINDOW_MIN = 120     # only cluster events from this recent a window for the feed

# Stops where every passenger must exit (turnbacks). Occupancy is re-anchored to
# zero here, correcting accumulated APC drift once per round trip. Matched by
# case-insensitive substring against GTFS stop_name. The southern terminus is the
# end of every run; add the northern terminus too for tighter drift control.
TERMINAL_STOP_NAMES = ["UMKC", "Riverfront"]
TERMINAL_RADIUS_M = float(os.environ.get("TERMINAL_RADIUS_M", "150"))

# Vehicle Maintenance Facility: a non-revenue zone. Vehicles reporting from here
# (including staging/yard moves that trigger door counts) are out of service --
# not shown, and their events don't count. Point sampled at the facility doors:
# 39 06'44.91" N, 94 34'38.15" W. Staging extends ~230m from that point; the
# nearest revenue stop (River Market, 3rd & Grand) is ~380m away, so a 230m zone
# covers the yard while staying 150m clear of that stop.
VMF_LAT, VMF_LON = 39.112475, -94.577264
VMF_RADIUS_M = float(os.environ.get("VMF_RADIUS_M", "230"))

# Stops whose direction is fixed by route geometry. Used only as reliable anchors:
# one-way stops with no nearby opposite-direction twin. The latitude trend can't
# tell direction on the north couplet, so we lean on these. River Market (3rd &
# Grand) is deliberately NOT here -- its NB and SB records sit close together, so a
# northbound car can match the SB record and get flipped wrongly. Delaware is
# SB-only with no twin, so it's the trustworthy SB anchor; the termini are one-way
# by definition -- a car only ever departs Riverfront southbound and UMKC northbound.
# Matched by case-insensitive substring of the resolved stop name; overrides inference.
STOP_DIRECTION = {
    "Delaware": "Southbound",
    "Riverfront": "Southbound",
    "UMKC": "Northbound",
    "City Market": "Northbound",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
HERE = os.path.dirname(os.path.abspath(__file__))

# Resolve a relative STOPS_FILE against this file's directory so it's found no
# matter what working directory the host runs us from.
_stops_env = os.environ.get("STOPS_FILE")
_stops_path = None
if _stops_env:
    _stops_path = _stops_env if os.path.isabs(_stops_env) else os.path.join(HERE, _stops_env)
stops = core.StopIndex(_stops_path,
                       max_meters=float(os.environ.get("STOP_MATCH_RADIUS_M", "175")))

# Resolve the configured terminal stop name(s) to coordinates once at startup, so
# per-event checks are a couple of cheap distance comparisons rather than a full
# nearest-stop scan.
TERMINALS = [(lat, lon) for sid, name, lat, lon in stops.stops
             if any(t.lower() in name.lower() for t in TERMINAL_STOP_NAMES)]
if stops.stops:
    logging.info("terminal stops matched: %d (%s)", len(TERMINALS), TERMINAL_STOP_NAMES)


def is_terminal(lat, lon) -> bool:
    if lat is None or lon is None or not TERMINALS:
        return False
    return any(core.StopIndex._haversine_m(lat, lon, tlat, tlon) <= TERMINAL_RADIUS_M
               for tlat, tlon in TERMINALS)


def at_vmf(lat, lon) -> bool:
    """True if a fix is inside the maintenance-facility (non-revenue) zone."""
    if lat is None or lon is None:
        return False
    return core.StopIndex._haversine_m(lat, lon, VMF_LAT, VMF_LON) <= VMF_RADIUS_M


# Northern / southern terminus by latitude, for direction inference.
NORTH_TERMINAL = max(TERMINALS, key=lambda c: c[0]) if TERMINALS else None
SOUTH_TERMINAL = min(TERMINALS, key=lambda c: c[0]) if TERMINALS else None
DIR_MOVE_DEG = 0.0005          # ~55m of latitude change = real movement, not jitter


def direction_override(stop_name) -> str | None:
    """Fixed direction for couplet stops that geometry can't disambiguate."""
    if not stop_name:
        return None
    for frag, d in STOP_DIRECTION.items():
        if frag.lower() in stop_name.lower():
            return d
    return None


def infer_direction(evs: list[dict]) -> str | None:
    """Northbound / Southbound for a vehicle, as of its latest event.

    Primary signal is actual movement: the sign of the most recent meaningful
    north-south change in position. This is correct regardless of whether a
    turnback was detected, so a southbound car still reads Southbound even if the
    north terminus wasn't matched. Only when the car has been essentially
    stationary (a dwell) do we fall back to a *recent* terminal to show the
    direction it's about to depart in; a stale terminal from a prior round trip
    is ignored so it can't mislabel the current leg.
    """
    pts = [e for e in evs if e["lat"] is not None]
    if pts:
        last = pts[-1]
        for e in reversed(pts[:-1]):
            if (last["_t"] - e["_t"]).total_seconds() > 900:
                break                      # gone back 15 min without real movement
            dlat = last["lat"] - e["lat"]
            if abs(dlat) > DIR_MOVE_DEG:
                return "Northbound" if dlat > 0 else "Southbound"

    if pts and NORTH_TERMINAL and SOUTH_TERMINAL and NORTH_TERMINAL != SOUTH_TERMINAL:
        last_t = pts[-1]["_t"]
        for e in reversed(evs):
            if e.get("terminal") and e["lat"] is not None:
                if (last_t - e["_t"]).total_seconds() > 1800:
                    break                  # stale terminal -> don't trust it
                dn = core.StopIndex._haversine_m(e["lat"], e["lon"], *NORTH_TERMINAL)
                ds = core.StopIndex._haversine_m(e["lat"], e["lon"], *SOUTH_TERMINAL)
                return "Southbound" if dn <= ds else "Northbound"
    return None


def resolve_cluster_location(cluster: list[dict]) -> dict:
    """Stop for a visit by majority vote across its events, so one drifted GPS fix
    that falls outside the match radius doesn't blank the name. Coordinates shown
    are the last event's (where the car is now)."""
    hits = [h for e in cluster if (h := stops.nearest(e["lat"], e["lon"]))]
    last = cluster[-1]
    if hits:
        (sid, name), _ = Counter(hits).most_common(1)[0]
        return {"stop_id": sid, "stop": name, "lat": last["lat"], "lon": last["lon"]}
    return {"stop_id": None, "stop": None, "lat": last["lat"], "lon": last["lon"]}


def resolve_recent_location(evs: list[dict]) -> dict:
    """Current location for a vehicle: the most recent of its last few fixes that
    resolves to a stop, so a single bad last fix doesn't drop it to raw coords."""
    for e in reversed(evs[-6:]):
        hit = stops.nearest(e["lat"], e["lon"])
        if hit:
            return {"stop_id": hit[0], "stop": hit[1], "lat": e["lat"], "lon": e["lon"]}
    last = evs[-1]
    return {"stop_id": None, "stop": None, "lat": last["lat"], "lon": last["lon"]}


class LiveState:
    def __init__(self):
        self.lock = threading.Lock()
        self.vehicles: list[dict] = []
        self.feed: list[dict] = []
        self.updated_at: str | None = None
        self.error: str | None = None
        self.peak_load: int = 0
        self.peak_time: str | None = None
        self.peak_vehicle: str | None = None
        self.system_peak: int = 0
        self.system_peak_time: str | None = None

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "updated_at": self.updated_at,
                "active_count": len(self.vehicles),
                "total_onboard": sum(v["count"] for v in self.vehicles),
                "capacity": CAPACITY,
                "stops_loaded": bool(stops.stops),
                "peak_load": self.peak_load,
                "peak_time": self.peak_time,
                "peak_vehicle": self.peak_vehicle,
                "system_peak": self.system_peak,
                "system_peak_time": self.system_peak_time,
                "vehicles": list(self.vehicles),
                "feed": list(self.feed),
                "error": self.error,
            }


state = LiveState()


def _make_visit(vehicle: str, cluster: list[dict], direction: str | None) -> dict:
    """Collapse a vehicle's clustered per-door records into one stop visit."""
    last = cluster[-1]
    loc = resolve_cluster_location(cluster)
    return {
        "id": min(e["id"] for e in cluster),          # stable across late-joining doors
        "vehicle": vehicle,
        "time": last["time"],
        "direction": direction_override(loc["stop"]) or direction,
        "ons": sum(e["ons"] for e in cluster),
        "offs": sum(e["offs"] for e in cluster),
        "doors": len(cluster),
        **loc,
    }


def _move_m(a: dict, b: dict):
    """Distance in meters between two events, or None if either lacks coordinates."""
    if None in (a["lat"], a["lon"], b["lat"], b["lon"]):
        return None
    return core.StopIndex._haversine_m(a["lat"], a["lon"], b["lat"], b["lon"])


def build_feed(by_vehicle: dict[str, list[dict]], since) -> list[dict]:
    """Cluster recent events into stop visits by location, newest first.

    Events for one vehicle are time-sorted. They stay in one visit while they're
    within CLUSTER_RADIUS_M of where the visit started -- so the several door
    records of one stop AND a long terminus dwell with riders trickling on all
    collapse into a single visit. A new visit starts when the car moves to a
    different stop, or after DWELL_MAX_GAP_S (a revisit to the same stop later).
    """
    visits: list[dict] = []
    for v, evs in by_vehicle.items():
        cluster: list[dict] = []
        end_idx = -1
        for idx, e in enumerate(evs):
            if e["_t"] < since:                       # feed shows only recent activity
                continue
            if e.get("nonrevenue"):                   # VMF / out of service -> not a visit
                continue
            if not core.counts_ok(e["ons"], e["offs"]):   # impossible count -> glitch, ignore
                continue
            if cluster:
                d = _move_m(cluster[0], e)
                moved = d is not None and d > CLUSTER_RADIUS_M
                stale = (e["_t"] - cluster[-1]["_t"]).total_seconds() > DWELL_MAX_GAP_S
                if moved or stale:
                    visit = _make_visit(v, cluster, infer_direction(evs[:end_idx + 1]))
                    if visit["ons"] or visit["offs"]:     # hide no-activity heartbeats
                        visits.append(visit)
                    cluster = []
            cluster.append(e)
            end_idx = idx
        if cluster:
            visit = _make_visit(v, cluster, infer_direction(evs[:end_idx + 1]))
            if visit["ons"] or visit["offs"]:
                visits.append(visit)
    visits.sort(key=lambda x: (x["time"], x["id"]), reverse=True)
    return visits[:FEED_MAX]


SYSTEM_PEAK_INTERVAL_S = 60      # recompute the system-wide peak at most this often
_last_system_peak = 0.0


def compute_system_peak(by_vehicle: dict, gap_s: float, since):
    """Peak total passengers onboard across the whole system during the current
    service day, i.e. the max over time of the sum of every vehicle's occupancy.

    Each car's occupancy is a step function (from occupancy_since_last_gap's series);
    a car contributes its occupancy for ACTIVE_WINDOW_MIN after each report, then
    expires to 0 -- the same 30-min "active" rule that defines the live onboard total,
    just evaluated at each historical instant instead of only now. We merge every
    car's step-changes onto one timeline and sweep once, carrying a running total:
    O(n log n) for the sort, O(n) for the sweep, over a few thousand points. Returns
    (peak, peak_time)."""
    stale = timedelta(minutes=ACTIVE_WINDOW_MIN)
    changes = []                                  # (time, vehicle, new_contribution)
    for v, evs in by_vehicle.items():
        _, series = core.occupancy_since_last_gap(
            evs, gap_s, core.FLOOR_AT_ZERO, with_series=True)
        prev = 0
        for i, (t, occ) in enumerate(series):
            if occ != prev:                       # only emit where the load actually changes
                changes.append((t, v, occ))
                prev = occ
            next_t = series[i + 1][0] if i + 1 < len(series) else None
            # each report refreshes staleness; expire to 0 only when the car then goes
            # quiet longer than the active window (heartbeats just push the expiry out)
            if (next_t is None or t + stale < next_t) and prev != 0:
                changes.append((t + stale, v, 0))
                prev = 0
    if not changes:
        return 0, None
    changes.sort(key=lambda c: c[0])
    contrib: dict = {}
    total = peak = 0
    peak_t = None
    entered = False
    for t, v, val in changes:
        if not entered and t >= since:            # carry-in load as we cross the boundary
            if total > peak:
                peak, peak_t = total, since
            entered = True
        total += val - contrib.get(v, 0)
        contrib[v] = val
        if t >= since and total > peak:
            peak, peak_t = total, t
    return peak, peak_t


def poll_once(session: requests.Session, limiter: core.RateLimiter) -> None:
    now = datetime.now(core.AGENCY_TZ)
    by_vehicle = core.gather_events(session, limiter, now)
    gap_s = core.GAP_RESET_HOURS * 3600
    now_naive = now.replace(tzinfo=None)
    active_cutoff = now_naive - timedelta(minutes=ACTIVE_WINDOW_MIN)
    feed_since = now_naive - timedelta(minutes=FEED_WINDOW_MIN)

    vehicles: list[dict] = []
    peak_load, peak_t, peak_veh = 0, None, None
    since_naive = service_day_start(now).replace(tzinfo=None)   # current service day start
    for v, evs in by_vehicle.items():
        for e in evs:                                    # flag turnbacks + non-revenue
            e["terminal"] = is_terminal(e["lat"], e["lon"])
            e["nonrevenue"] = at_vmf(e["lat"], e["lon"])
        count, vpeak, vpeak_t = core.occupancy_since_last_gap(
            evs, gap_s, core.FLOOR_AT_ZERO, with_peak=True, peak_since=since_naive)
        if vpeak > peak_load:                            # highest any car reached today
            peak_load, peak_t, peak_veh = vpeak, vpeak_t, v
        last = evs[-1]
        if last.get("nonrevenue"):                       # sitting at the VMF -> out of service
            continue
        if last["_t"] >= active_cutoff:                  # reported recently -> active
            loc = resolve_recent_location(evs)
            vehicles.append({
                "vehicle": v, "count": count, "last_time": last["time"],
                "direction": direction_override(loc["stop"]) or infer_direction(evs),
                **loc,
            })
    vehicles.sort(key=lambda x: (-x["count"], x["vehicle"]))   # busiest first

    feed = build_feed(by_vehicle, feed_since)
    peak_time = peak_t.strftime("%I:%M %p").lstrip("0") if peak_t else None

    with state.lock:
        state.vehicles = vehicles
        state.feed = feed
        state.updated_at = now.isoformat(timespec="seconds")
        state.peak_load = peak_load
        state.peak_time = peak_time
        state.peak_vehicle = peak_veh
        state.error = None

    capture_events(by_vehicle)                   # persist raw events (no-op without a DB)

    # System peak runs AFTER the live board is published above, on a throttle -- so it
    # can never sit in front of or slow the core live-load numbers. It's a cheap linear
    # sweep, but decoupling it keeps that guarantee structural, not just "fast enough".
    global _last_system_peak
    if time.monotonic() - _last_system_peak > SYSTEM_PEAK_INTERVAL_S:
        sp, sp_t = compute_system_peak(by_vehicle, gap_s, since_naive)
        with state.lock:
            state.system_peak = sp
            state.system_peak_time = sp_t.strftime("%I:%M %p").lstrip("0") if sp_t else None
        _last_system_peak = time.monotonic()


_captured_hw = 0                                 # largest event id already persisted


def capture_events(by_vehicle: dict[str, list[dict]]) -> None:
    """Insert new raw events into the DB, deduped by id. Advances the high-water
    mark only on a successful write, so a DB outage just retries next poll."""
    global _captured_hw
    if not db.enabled:
        return
    hw = _captured_hw
    rows = []
    new_hw = hw
    for evs in by_vehicle.values():
        for e in evs:
            if e["id"] <= hw:
                continue
            rows.append((e["id"], e["vehicle"],
                         e["_t"].replace(tzinfo=core.AGENCY_TZ), e["_t"].date(),
                         e["lat"], e["lon"], e["ons"], e["offs"]))
            if e["id"] > new_hw:
                new_hw = e["id"]
    if not rows:
        return
    inserted = db.insert_events(rows)
    if inserted is not None:                      # None = write failed -> keep hw, retry
        _captured_hw = new_hw
        if inserted:
            logging.info("captured %d new raw events (high-water id=%d)", inserted, new_hw)


# ---- Rollups (derived from raw; rebuildable) ---------------------------------
ROLLUP_INTERVAL_S = 300          # refresh recent days' rollup at most this often
UNMATCHED = "(unmatched)"        # bucket for door activity that didn't resolve to a stop
_last_rollup = 0.0


def build_stop_hourly(service_date) -> int:
    """Rebuild one service date's stop_hourly rollup from raw, tagging each event with
    its travel direction as of that moment (same priority as the live board:
    couplet override -> movement -> terminal anchor). Pure function of raw + current
    logic, so it self-heals and can be re-run when matching/direction logic changes."""
    if not db.enabled:
        return 0
    by_vehicle: dict = defaultdict(list)
    for _id, vehicle_id, event_time, lat, lon, ons, offs in db.fetch_raw_day(service_date):
        by_vehicle[vehicle_id].append({
            "id": _id,
            "_t": event_time.astimezone(core.AGENCY_TZ).replace(tzinfo=None),  # naive local
            "lat": lat, "lon": lon, "ons": ons, "offs": offs,
            "terminal": is_terminal(lat, lon),
        })

    buckets: dict = defaultdict(lambda: [0, 0, 0])       # (hour, name, direction) -> [ons,offs,n]
    for evs in by_vehicle.values():
        evs.sort(key=lambda e: (e["_t"], e["id"]))
        for i, e in enumerate(evs):
            if not (e["ons"] or e["offs"]):              # boarding/alighting activity only
                continue
            if not core.counts_ok(e["ons"], e["offs"]):  # impossible count -> glitch, ignore
                continue
            if at_vmf(e["lat"], e["lon"]):               # non-revenue -> excluded
                continue
            hit = stops.nearest(e["lat"], e["lon"])
            name = hit[1] if hit else UNMATCHED
            direction = direction_override(name) or infer_direction(evs[:i + 1]) or "Unknown"
            bt = e["_t"].replace(minute=0, second=0, microsecond=0, tzinfo=core.AGENCY_TZ)
            b = buckets[(bt, name, direction)]
            b[0] += e["ons"]
            b[1] += e["offs"]
            b[2] += 1
    rows = [(bt, service_date, name, direction, o, f, n)
            for (bt, name, direction), (o, f, n) in buckets.items()]
    db.replace_stop_hourly(service_date, rows)
    return len(rows)


def rebuild_stop_hourly(from_date, to_date) -> None:
    d = from_date
    while d <= to_date:
        build_stop_hourly(d)
        d += timedelta(days=1)


# ---- On-demand rebuild (token-gated, background) -----------------------------
# Re-derive rollups from raw after a logic/data change. Defaults to ALL captured
# dates so the whole table stays on one consistent logic version -- rebuilding only
# a slice would leave old and new logic mixed (see the WW1/WWI merge fix).
REBUILD_TOKEN = os.environ.get("REBUILD_TOKEN")
_rebuild_lock = threading.Lock()
_rebuild_status: dict = {"running": False, "started": None, "finished": None,
                         "dates_total": 0, "dates_done": 0, "from": None, "to": None,
                         "error": None}


def _run_rebuild(dates) -> None:
    try:
        for i, d in enumerate(dates, 1):
            build_stop_hourly(d)
            _rebuild_status["dates_done"] = i
        _rebuild_status["error"] = None
        logging.info("rollup rebuild done: %d dates", len(dates))
    except Exception as exc:
        logging.exception("rollup rebuild failed")
        _rebuild_status["error"] = str(exc)
    finally:
        _rebuild_status["running"] = False
        _rebuild_status["finished"] = datetime.now(core.AGENCY_TZ).isoformat(timespec="seconds")
        _rebuild_lock.release()


def _rebuild_authorized(token) -> bool:
    return bool(REBUILD_TOKEN) and bool(token) and hmac.compare_digest(token, REBUILD_TOKEN)


def refresh_rollups() -> None:
    """On startup: build any raw dates missing from the rollup, plus today+yesterday.
    Thereafter: refresh today+yesterday on a throttle (yesterday catches late uploads
    and cross-midnight runs; older days are immutable)."""
    global _last_rollup
    if not db.enabled:
        return
    today = datetime.now(core.AGENCY_TZ).date()
    yesterday = today - timedelta(days=1)
    if _last_rollup == 0.0:                              # first pass after startup
        for d in db.rollup_missing_dates():
            build_stop_hourly(d)
        build_stop_hourly(yesterday)
        build_stop_hourly(today)
        _last_rollup = time.monotonic()
        logging.info("rollups: startup rebuild complete")
    elif time.monotonic() - _last_rollup > ROLLUP_INTERVAL_S:
        build_stop_hourly(yesterday)
        build_stop_hourly(today)
        _last_rollup = time.monotonic()


def poller() -> None:
    global _captured_hw
    session = requests.Session()
    limiter = core.RateLimiter()
    if db.enabled:
        db.init_schema()
        _captured_hw = db.high_water()
        logging.info("raw capture enabled (high-water id=%d)", _captured_hw)
    else:
        logging.info("raw capture disabled (%s)", db.DISABLED_REASON)
    while True:
        try:
            poll_once(session, limiter)
            refresh_rollups()
        except Exception as exc:                         # keep the service up; surface it
            logging.exception("poll failed")
            with state.lock:
                state.error = str(exc)
        time.sleep(core.POLL_INTERVAL_S)


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=poller, daemon=True, name="poller").start()
    yield


app = FastAPI(lifespan=lifespan, title="KCATA Live Load")


# ---- GPS diagnostics (on-demand; not part of the poll loop) ------------------
# An event with a boarding or alighting was definitely at a stop, so the offset
# from where it reported to the nearest stop's coordinate is the GPS drift. We
# measure that for every door-activity event in a day and aggregate.
_diag_session = requests.Session()
_diag_limiter = core.RateLimiter()
DRIFT_OUTLIER_COUNT = 20
_COMPASS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def _compass(north_m: float, east_m: float) -> str:
    if abs(north_m) < 1 and abs(east_m) < 1:
        return "-"
    ang = (math.degrees(math.atan2(east_m, north_m)) + 360) % 360   # 0=N, 90=E
    return _COMPASS[int((ang + 22.5) // 45) % 8]


def _drift_detail(lat, lon):
    """Nearest stop (unbounded) and the offset to it, in meters.
    Returns (stop_id, stop_name, distance_m, north_m, east_m) or None."""
    if lat is None or lon is None or not stops.stops:
        return None
    best_d, best = float("inf"), None
    for sid, name, slat, slon in stops.stops:
        d = core.StopIndex._haversine_m(lat, lon, slat, slon)
        if d < best_d:
            best_d, best = d, (sid, name, slat, slon)
    sid, name, slat, slon = best
    north = (lat - slat) * 111320.0
    east = (lon - slon) * 111320.0 * math.cos(math.radians(lat))
    return sid, name, best_d, north, east


def _finalize(a: dict) -> dict:
    n = a["n"] or 1
    mean = a["sum"] / n
    bn, be = a["sn"] / n, a["se"] / n
    bias_mag = math.hypot(bn, be)
    return {
        "n": a["n"],
        "mean_offset_m": round(mean),
        "max_offset_m": round(a["max"]),
        "fail_pct": round(100 * a["fail"] / n, 1),
        "bias_dir": _compass(bn, be),
        "bias_mag_m": round(bias_mag),
        # 0 = random scatter (noisy receiver); ~1 = same direction every time
        # (systematic -> wrong stop coordinate or antenna offset)
        "consistency": round(bias_mag / mean, 2) if mean > 0 else 0.0,
    }


def compute_gps_diagnostics() -> dict:
    now = datetime.now(core.AGENCY_TZ)
    today = now.date()
    events = core.fetch_day(_diag_session, _diag_limiter, today)
    radius = stops.max_meters

    def acc():
        return {"n": 0, "sum": 0.0, "max": 0.0, "sn": 0.0, "se": 0.0, "fail": 0}
    overall = acc()
    veh: dict = defaultdict(acc)
    stp: dict = defaultdict(acc)
    stop_ids: dict = {}
    dists: list = []
    outliers: list = []
    vmf_hits: list = []

    for e in events:
        ons, offs = e.get("ons") or 0, e.get("offs") or 0
        if not (ons or offs):                    # only confirmed at-stop events
            continue
        if not core.counts_ok(ons, offs):        # impossible count -> glitch, ignore
            continue
        lat, lon = e.get("latitude"), e.get("longitude")
        if at_vmf(lat, lon):                      # non-revenue -> excluded, but recorded
            vmf_hits.append((core.StopIndex._haversine_m(lat, lon, VMF_LAT, VMF_LON),
                             e["vehicle_id"], e.get("time", ""), lat, lon, ons, offs))
            continue
        det = _drift_detail(lat, lon)
        if not det:
            continue
        sid, name, d, north, east = det
        fail = 1 if d > radius else 0
        for a in (overall, veh[e["vehicle_id"]], stp[name]):
            a["n"] += 1
            a["sum"] += d
            a["max"] = max(a["max"], d)
            a["sn"] += north
            a["se"] += east
            a["fail"] += fail
        stop_ids[name] = sid
        dists.append(d)
        outliers.append((d, e["vehicle_id"], e.get("time", ""), name, lat, lon,
                         _compass(north, east)))

    dists.sort()

    def pct(p):
        return round(dists[min(len(dists) - 1, int(p * len(dists)))]) if dists else 0

    outliers.sort(reverse=True)
    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "date": today.isoformat(),
        "match_radius_m": round(radius),
        "stops_loaded": bool(stops.stops),
        "sample_size": overall["n"],
        "settings": {
            "stop_match_radius_m": round(radius),
            "vmf_radius_m": round(VMF_RADIUS_M),
            "vmf_center": [round(VMF_LAT, 6), round(VMF_LON, 6)],
            "terminal_radius_m": round(TERMINAL_RADIUS_M),
            "stops_count": len(stops.stops),
        },
        "overall": {**_finalize(overall),
                    "p50": pct(0.50), "p90": pct(0.90), "p95": pct(0.95)},
        "by_vehicle": sorted(
            ({"vehicle": v, **_finalize(a)} for v, a in veh.items()),
            key=lambda x: -x["mean_offset_m"]),
        "by_stop": sorted(
            ({"stop": s, "stop_id": stop_ids.get(s), **_finalize(a)}
             for s, a in stp.items()),
            key=lambda x: -x["mean_offset_m"]),
        "outliers": [
            {"offset_m": round(d), "dir": bearing, "vehicle": v, "time": t,
             "nearest_stop": nm, "lat": lat, "lon": lon}
            for d, v, t, nm, lat, lon, bearing in outliers[:DRIFT_OUTLIER_COUNT]],
        "vmf_excluded_count": len(vmf_hits),
        "vmf_hits": [                              # farthest-from-VMF first (near the boundary)
            {"dist_m": round(d), "vehicle": v, "time": t,
             "ons": ons, "offs": offs, "lat": lat, "lon": lon}
            for d, v, t, lat, lon, ons, offs in sorted(vmf_hits, reverse=True)[:30]],
    }


@app.get("/api/state")
def api_state():
    return JSONResponse(state.snapshot())


# A "service day" runs from SERVICE_DAY_CUTOFF_H one morning to the same hour the
# next, so post-midnight running counts toward the day it started and the counter
# resets in the pre-dawn quiet. Applied at query time (from bucket_start), so raw
# stays a pure calendar fact and the cutoff can be retuned with just a rebuild.
SERVICE_DAY_CUTOFF_H = int(os.environ.get("SERVICE_DAY_CUTOFF_H", "4"))
TZ_NAME = getattr(core.AGENCY_TZ, "key", "America/Chicago")


def service_day_start(dt=None):
    """Start of the service day containing dt (agency-local, tz-aware)."""
    dt = dt or datetime.now(core.AGENCY_TZ)
    anchor = dt.replace(hour=SERVICE_DAY_CUTOFF_H, minute=0, second=0, microsecond=0)
    if dt < anchor:                                  # before the cutoff -> prior service day
        anchor -= timedelta(days=1)
    return anchor


def _service_day_start_for(d):
    """Service-day start for a given service-day date."""
    return datetime(d.year, d.month, d.day, SERVICE_DAY_CUTOFF_H, tzinfo=core.AGENCY_TZ)


@app.get("/api/reports/summary")
def api_report_summary():
    """Boardings/alightings for the current service day so far."""
    start = service_day_start()
    end = start + timedelta(days=1)
    rows = db.fetchall(
        "SELECT COALESCE(SUM(ons),0), COALESCE(SUM(offs),0) FROM stop_hourly "
        "WHERE bucket_start >= %s AND bucket_start < %s", (start, end))
    ons, offs = rows[0] if rows else (0, 0)
    return JSONResponse({"service_day": start.date().isoformat(),
                         "boardings": int(ons), "alightings": int(offs),
                         "db_enabled": db.enabled})


@app.get("/api/reports/by-stop")
def api_report_by_stop(hours: float = 24, scope: str | None = None, limit: int = 100):
    """Per-stop boardings/alightings split by direction plus a combined total. Either
    a rolling window (`hours`) or the current service day (`scope=service_day`)."""
    now = datetime.now(core.AGENCY_TZ)
    if scope == "service_day":
        since = service_day_start(now)
        until = since + timedelta(days=1)
        meta = {"scope": "service_day", "service_day": since.date().isoformat()}
    else:
        since = now - timedelta(hours=hours)
        until = now + timedelta(days=1)              # effectively open (no future buckets)
        meta = {"scope": "rolling", "hours": hours}
    rows = db.fetchall(
        "SELECT stop_name, "
        "  COALESCE(SUM(ons)  FILTER (WHERE direction='Northbound'),0), "
        "  COALESCE(SUM(offs) FILTER (WHERE direction='Northbound'),0), "
        "  COALESCE(SUM(ons)  FILTER (WHERE direction='Southbound'),0), "
        "  COALESCE(SUM(offs) FILTER (WHERE direction='Southbound'),0), "
        "  SUM(ons), SUM(offs) "
        "FROM stop_hourly WHERE bucket_start >= %s AND bucket_start < %s AND stop_name <> %s "
        "GROUP BY stop_name ORDER BY SUM(ons)+SUM(offs) DESC LIMIT %s",
        (since, until, UNMATCHED, limit))
    return JSONResponse({
        "since": since.isoformat(timespec="seconds"), **meta,
        "stops": [{
            "stop": s,
            "nb": {"ons": int(nbo), "offs": int(nbf)},
            "sb": {"ons": int(sbo), "offs": int(sbf)},
            "total": {"ons": int(o), "offs": int(f), "activity": int(o) + int(f)},
        } for s, nbo, nbf, sbo, sbf, o, f in rows]})


@app.get("/api/reports/daily")
def api_report_daily(days: int = 30, frm: str | None = None, to: str | None = None):
    """Per-service-day boardings/alightings. Defaults to the last `days` service days;
    pass frm/to (YYYY-MM-DD service-day dates) for a custom range."""
    cur = service_day_start()
    start = _service_day_start_for(date.fromisoformat(frm)) if frm else cur - timedelta(days=days - 1)
    end = (_service_day_start_for(date.fromisoformat(to)) if to else cur) + timedelta(days=1)
    rows = db.fetchall(
        f"SELECT ((bucket_start AT TIME ZONE %s) - interval '{SERVICE_DAY_CUTOFF_H} hours')::date "
        "AS sday, SUM(ons), SUM(offs) FROM stop_hourly "
        "WHERE bucket_start >= %s AND bucket_start < %s GROUP BY sday ORDER BY sday",
        (TZ_NAME, start, end))
    return JSONResponse({
        "from": start.date().isoformat(), "to": (end - timedelta(days=1)).date().isoformat(),
        "cutoff_hour": SERVICE_DAY_CUTOFF_H,
        "days": [{"date": d.isoformat(), "boardings": int(o), "alightings": int(f)}
                 for d, o, f in rows]})


@app.post("/api/reports/rebuild")
def api_rebuild(frm: str | None = None, to: str | None = None,
                x_rebuild_token: str | None = Header(None)):
    """Re-derive rollups from raw. No range = ALL captured dates (the safe default,
    keeps the table on one logic version). Runs in the background; poll GET for status."""
    if not REBUILD_TOKEN:
        return JSONResponse({"error": "rebuild disabled (REBUILD_TOKEN not set)"}, status_code=503)
    if not _rebuild_authorized(x_rebuild_token):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if not db.enabled:
        return JSONResponse({"error": "no database"}, status_code=503)
    if not _rebuild_lock.acquire(blocking=False):
        return JSONResponse({"status": "already running", **_rebuild_status}, status_code=409)
    try:
        if frm or to:                                    # explicit range (the niche case)
            today = datetime.now(core.AGENCY_TZ).date()
            start = date.fromisoformat(frm) if frm else today
            end = date.fromisoformat(to) if to else today
            dates, d = [], start
            while d <= end:
                dates.append(d)
                d += timedelta(days=1)
        else:                                            # default: everything captured
            dates = db.all_service_dates()
    except ValueError:
        _rebuild_lock.release()
        return JSONResponse({"error": "bad date (use YYYY-MM-DD)"}, status_code=400)

    _rebuild_status.update({
        "running": True, "started": datetime.now(core.AGENCY_TZ).isoformat(timespec="seconds"),
        "finished": None, "dates_total": len(dates), "dates_done": 0,
        "from": dates[0].isoformat() if dates else None,
        "to": dates[-1].isoformat() if dates else None, "error": None})
    threading.Thread(target=_run_rebuild, args=(dates,), daemon=True, name="rebuild").start()
    return JSONResponse({"status": "started", "dates": len(dates),
                         "from": _rebuild_status["from"], "to": _rebuild_status["to"]},
                        status_code=202)


@app.get("/api/reports/rebuild")
def api_rebuild_status(x_rebuild_token: str | None = Header(None)):
    """Progress of the most recent rebuild."""
    if not _rebuild_authorized(x_rebuild_token):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return JSONResponse(_rebuild_status)


@app.get("/api/gps-diagnostics")
def api_gps_diagnostics():
    try:
        return JSONResponse(compute_gps_diagnostics())
    except Exception as exc:
        logging.exception("gps diagnostics failed")
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "index.html"))


@app.get("/gps")
def gps_page():
    return FileResponse(os.path.join(HERE, "gps.html"))


@app.get("/reports")
def reports_page():
    return FileResponse(os.path.join(HERE, "reports.html"))


# Favicon + installable-web-app (PWA) icons, served from the app root.
_ICONS = {
    "favicon.ico": "image/x-icon",
    "favicon.svg": "image/svg+xml",
    "favicon-96x96.png": "image/png",
    "apple-touch-icon.png": "image/png",
    "web-app-manifest-192x192.png": "image/png",
    "web-app-manifest-512x512.png": "image/png",
    "site.webmanifest": "application/manifest+json",
}


def _icon_route(fname, media):
    def serve():
        path = os.path.join(HERE, "favicon", fname)
        if not os.path.exists(path):                 # fall back to the app root
            path = os.path.join(HERE, fname)
        return FileResponse(path, media_type=media)
    return serve


for _fname, _media in _ICONS.items():
    app.add_api_route(f"/{_fname}", _icon_route(_fname, _media), include_in_schema=False)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0",
                port=int(os.environ.get("PORT", "8000")))
