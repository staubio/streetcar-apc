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
import os
import threading
import time
from contextlib import asynccontextmanager
from collections import Counter
from datetime import datetime, timedelta

import requests
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

import swiftly_apc_tracker as core

ACTIVE_WINDOW_MIN = 30     # a vehicle is "active" if it reported within this many minutes
FEED_MAX = 120            # stop-visits retained for the activity ticker
CAPACITY = 150           # nominal capacity for the crowding bar (KC Streetcar ~150)

# Per-door reporting: one stop visit emits several records (one per door), and a
# car can idle 5-10 min at a terminus while riders trickle on. So cluster by
# LOCATION, not time: events within CLUSTER_RADIUS_M are the same visit no matter
# how spread out, and a new visit starts only when the car moves to a different
# stop. The radius stays well under stop spacing (~150m+) so adjacent stops never
# merge; DWELL_MAX_GAP_S still breaks a same-location revisit a round trip later.
CLUSTER_RADIUS_M = 80.0
DWELL_MAX_GAP_S = 900     # a gap longer than this starts a new visit (same-stop revisit)
FEED_WINDOW_MIN = 120     # only cluster events from this recent a window for the feed

# Stops where every passenger must exit (turnbacks). Occupancy is re-anchored to
# zero here, correcting accumulated APC drift once per round trip. Matched by
# case-insensitive substring against GTFS stop_name. The southern terminus is the
# end of every run; add the northern terminus too for tighter drift control.
TERMINAL_STOP_NAMES = ["UMKC", "Riverfront"]
TERMINAL_RADIUS_M = float(os.environ.get("TERMINAL_RADIUS_M", "80"))

# Stops whose direction is fixed by route geometry. At the one-way couplet on the
# north downtown loop the latitude trend can't tell direction, but these stops are
# only ever served one way. Matched by case-insensitive substring of the resolved
# stop name; this overrides inference. (City Market is belt-and-suspenders -- it's
# late enough in the run that movement usually has it right already.)
STOP_DIRECTION = {
    "River Market": "Southbound",
    "Delaware": "Southbound",
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
                       max_meters=float(os.environ.get("STOP_MATCH_RADIUS_M", "120")))

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

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "updated_at": self.updated_at,
                "active_count": len(self.vehicles),
                "total_onboard": sum(v["count"] for v in self.vehicles),
                "capacity": CAPACITY,
                "stops_loaded": bool(stops.stops),
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
            if cluster:
                d = _move_m(cluster[0], e)
                moved = d is not None and d > CLUSTER_RADIUS_M
                stale = (e["_t"] - cluster[-1]["_t"]).total_seconds() > DWELL_MAX_GAP_S
                if moved or stale:
                    visits.append(_make_visit(v, cluster, infer_direction(evs[:end_idx + 1])))
                    cluster = []
            cluster.append(e)
            end_idx = idx
        if cluster:
            visits.append(_make_visit(v, cluster, infer_direction(evs[:end_idx + 1])))
    visits.sort(key=lambda x: (x["time"], x["id"]), reverse=True)
    return visits[:FEED_MAX]


def poll_once(session: requests.Session, limiter: core.RateLimiter) -> None:
    now = datetime.now(core.AGENCY_TZ)
    by_vehicle = core.gather_events(session, limiter, now)
    gap_s = core.GAP_RESET_HOURS * 3600
    now_naive = now.replace(tzinfo=None)
    active_cutoff = now_naive - timedelta(minutes=ACTIVE_WINDOW_MIN)
    feed_since = now_naive - timedelta(minutes=FEED_WINDOW_MIN)

    vehicles: list[dict] = []
    for v, evs in by_vehicle.items():
        for e in evs:                                    # flag turnbacks for the walk
            e["terminal"] = is_terminal(e["lat"], e["lon"])
        count = core.occupancy_since_last_gap(evs, gap_s, core.FLOOR_AT_ZERO)
        last = evs[-1]
        if last["_t"] >= active_cutoff:                  # reported recently -> active
            loc = resolve_recent_location(evs)
            vehicles.append({
                "vehicle": v, "count": count, "last_time": last["time"],
                "direction": direction_override(loc["stop"]) or infer_direction(evs),
                **loc,
            })
    vehicles.sort(key=lambda x: (-x["count"], x["vehicle"]))   # busiest first

    feed = build_feed(by_vehicle, feed_since)

    with state.lock:
        state.vehicles = vehicles
        state.feed = feed
        state.updated_at = now.isoformat(timespec="seconds")
        state.error = None


def poller() -> None:
    session = requests.Session()
    limiter = core.RateLimiter()
    while True:
        try:
            poll_once(session, limiter)
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


@app.get("/api/state")
def api_state():
    return JSONResponse(state.snapshot())


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "index.html"))


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0",
                port=int(os.environ.get("PORT", "8000")))
