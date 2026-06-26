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
from datetime import datetime, timedelta

import requests
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

import swiftly_apc_tracker as core

ACTIVE_WINDOW_MIN = 30     # a vehicle is "active" if it reported within this many minutes
FEED_MAX = 120            # stop-visits retained for the activity ticker
CAPACITY = 150           # nominal capacity for the crowding bar (KC Streetcar ~150)

# Per-door reporting: one stop visit emits several records (one per door) a few
# seconds apart. Cluster a vehicle's events within this many seconds into one
# visit and sum ons/offs across doors. Keep it well below the travel time between
# stops (minutes) so distinct stops never merge.
AGG_TOLERANCE_S = 60
FEED_WINDOW_MIN = 120     # only cluster events from this recent a window for the feed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
HERE = os.path.dirname(os.path.abspath(__file__))

# Resolve a relative STOPS_FILE against this file's directory so it's found no
# matter what working directory the host runs us from.
_stops_env = os.environ.get("STOPS_FILE")
_stops_path = None
if _stops_env:
    _stops_path = _stops_env if os.path.isabs(_stops_env) else os.path.join(HERE, _stops_env)
stops = core.StopIndex(_stops_path)


def describe_location(lat, lon) -> dict:
    """Resolve coordinates to a stop name when possible; always keep raw coords."""
    hit = stops.nearest(lat, lon)
    return {
        "stop_id": hit[0] if hit else None,
        "stop": hit[1] if hit else None,
        "lat": lat, "lon": lon,
    }


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


def _make_visit(vehicle: str, cluster: list[dict]) -> dict:
    """Collapse a vehicle's clustered per-door records into one stop visit."""
    last = cluster[-1]
    return {
        "id": min(e["id"] for e in cluster),          # stable across late-joining doors
        "vehicle": vehicle,
        "time": last["time"],
        "ons": sum(e["ons"] for e in cluster),
        "offs": sum(e["offs"] for e in cluster),
        "doors": len(cluster),
        **describe_location(last["lat"], last["lon"]),
    }


def build_feed(by_vehicle: dict[str, list[dict]], since) -> list[dict]:
    """Cluster recent per-door events into stop visits, newest first.

    Events for one vehicle are already time-sorted; a gap larger than the
    tolerance starts a new visit. Distinct stops are minutes apart, so they never
    merge; the several door records of one stop (seconds apart) collapse into one.
    """
    visits: list[dict] = []
    for v, evs in by_vehicle.items():
        cluster: list[dict] = []
        for e in evs:
            if e["_t"] < since:                       # feed shows only recent activity
                continue
            if cluster and (e["_t"] - cluster[-1]["_t"]).total_seconds() > AGG_TOLERANCE_S:
                visits.append(_make_visit(v, cluster))
                cluster = []
            cluster.append(e)
        if cluster:
            visits.append(_make_visit(v, cluster))
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
        count = core.occupancy_since_last_gap(evs, gap_s, core.FLOOR_AT_ZERO)
        last = evs[-1]
        if last["_t"] >= active_cutoff:                  # reported recently -> active
            vehicles.append({
                "vehicle": v, "count": count, "last_time": last["time"],
                **describe_location(last["lat"], last["lon"]),
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
