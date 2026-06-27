#!/usr/bin/env python3
"""
Real-time per-vehicle passenger occupancy from Swiftly APC raw events.

OCCUPANCY = SUM SINCE THE LAST QUIET GAP
----------------------------------------
For each vehicle we walk its events in time order and sum `ons - offs`, resetting
the running total to zero whenever the gap between consecutive events exceeds
GAP_RESET_HOURS. That single rule covers everything:

  * mid-route across midnight  -> no gap -> occupancy carries through
  * returned to the depot      -> long gap -> baseline resets to empty
  * APC drift                  -> bounded to one run (hours), never across days

Because occupancy is recomputed from the event set each poll (keyed on the unique
`id`), re-polling the same day is idempotent: an event can never be counted twice.

WHY WE LOOK BACK ~20 HOURS
--------------------------
A run can span up to ~20h (pull out 05:45, in service until 01:30 next day; driver
reliefs are quick swaps, not depot returns, so they don't trip the gap reset). To
reconstruct that run we fetch every calendar-date payload the window
[now - LOOKBACK_HOURS, now] touches (Swiftly buckets each event into a calendar
date by its own timestamp) and merge them before walking. The gap-walk discards
anything before a vehicle's most recent reset, so over-fetching is harmless.
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict, deque
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests

# ---- Config -----------------------------------------------------------------
AGENCY = "kcata"
BASE_URL = "https://api.goswift.ly/ridership/{agency}/apc-raw-events"
def get_api_key() -> str:                     # rotate the pasted key; load from env
    return os.environ["SWIFTLY_API_KEY"]
AGENCY_TZ = ZoneInfo("America/Chicago")      # KCATA = US Central; events are local time

POLL_INTERVAL_S = 30
GAP_RESET_HOURS = 3.0          # quiet gap that means "back at the depot / out of service"
FLOOR_AT_ZERO = True           # clamp final occupancy to >= 0

# Longest continuous run we must be able to reconstruct. A vehicle can pull out at
# 05:45 and stay in service until 01:30 the next day (~20h) as drivers cycle in and
# out, so we must walk events back this far. Set >= your max run length + margin.
LOOKBACK_HOURS = 22.0

# Rate limit: 1500 req / 15 min = 1.667/s. We do 1-2 requests per cycle -> tiny.
MAX_REQUESTS = 1500
WINDOW_S = 900
MIN_SECONDS_BETWEEN_REQUESTS = 1.0


class RateLimiter:
    """Sliding-window cap plus a minimum spacing between requests (safety net)."""

    def __init__(self, max_requests=MAX_REQUESTS, window_s=WINDOW_S,
                 min_interval_s=MIN_SECONDS_BETWEEN_REQUESTS):
        self.max_requests = max_requests
        self.window_s = window_s
        self.min_interval_s = min_interval_s
        self._stamps: deque[float] = deque()
        self._last = 0.0

    def acquire(self) -> None:
        now = time.monotonic()
        wait = self.min_interval_s - (now - self._last)
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()

        cutoff = now - self.window_s
        while self._stamps and self._stamps[0] < cutoff:
            self._stamps.popleft()

        if len(self._stamps) >= self.max_requests:
            sleep_for = self._stamps[0] + self.window_s - now
            if sleep_for > 0:
                logging.warning("window cap reached; sleeping %.1fs", sleep_for)
                time.sleep(sleep_for)
            now = time.monotonic()
            while self._stamps and self._stamps[0] < now - self.window_s:
                self._stamps.popleft()

        self._stamps.append(now)
        self._last = now


def fetch_day(session: requests.Session, limiter: RateLimiter,
              service_date: date, max_retries: int = 4) -> list[dict]:
    """Fetch one calendar date's raw events, with retry/backoff. Single response."""
    url = BASE_URL.format(agency=AGENCY)
    params = {"date": service_date.isoformat()}
    headers = {"Accept": "application/json", "Authorization": get_api_key()}

    for attempt in range(max_retries):
        limiter.acquire()
        r = session.get(url, params=params, headers=headers, timeout=60)

        if r.status_code == 429:
            retry_after = float(r.headers.get("Retry-After", 5))
            logging.warning("429 rate limited; sleeping %.1fs", retry_after)
            time.sleep(retry_after)
            continue
        if 500 <= r.status_code < 600:
            backoff = 2 ** attempt
            logging.warning("server %d; backoff %ds", r.status_code, backoff)
            time.sleep(backoff)
            continue

        r.raise_for_status()
        return r.json().get("apcRawEvents", [])

    raise RuntimeError(f"fetch_day failed after {max_retries} retries for {service_date}")


def parse_event_time(s: str) -> datetime | None:
    """Parse 'YYYY-MM-DD HH:MM:SS[.fff]' as a naive local datetime (agency clock)."""
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    logging.warning("unparseable time: %r", s)
    return None


def occupancy_since_last_gap(events: list[dict], gap_reset_s: float,
                             floor: bool = True) -> int:
    """Sum ons - offs over a vehicle's events, resetting to 0 on each quiet gap
    and at terminal stops where every passenger must exit.

    `events` must be deduped on `id` and sorted by (time, id). An event with a
    truthy `terminal` key marks a turnback where the vehicle empties: on arrival
    the count is zeroed (known empty), and during the terminal dwell only the
    boardings (return-trip riders) are counted -- the mass deboarding offs are
    ignored because the reset already accounts for them. This re-anchors the
    count to ground truth once per round trip, correcting accumulated APC drift.
    """
    running = 0
    last_t: datetime | None = None
    prev_terminal = False
    for e in events:
        t = e["_t"]
        if last_t is not None and (t - last_t).total_seconds() > gap_reset_s:
            running = 0          # long quiet gap -> depot / out of service -> reset
            prev_terminal = False
        if e.get("terminal"):
            if not prev_terminal:
                running = 0      # arrival at turnback: everyone must exit -> empty
            running += e["ons"]  # count return-trip boardings; ignore deboarding offs
            prev_terminal = True
        else:
            running += e["ons"] - e["offs"]
            prev_terminal = False
        if floor and running < 0:
            running = 0          # occupancy can't be negative; clamping here keeps a
                                 # mass deboarding (e.g. an unconfigured turnback) from
                                 # masking the boardings that follow it
        last_t = t
    return max(0, running) if floor else running


def gather_events(session: requests.Session, limiter: RateLimiter,
                  now: datetime) -> dict[str, list[dict]]:
    """Fetch every calendar date the lookback window touches, then group deduped,
    time-sorted events per vehicle.

    A continuous run can reach LOOKBACK_HOURS into the past, so we fetch each date
    spanned by [now - LOOKBACK_HOURS, now]. For a ~20h run that is at most 2 dates
    (today, and yesterday until ~late evening). The gap-walk then discards anything
    before a vehicle's most recent reset, so over-fetching is harmless.
    """
    window_start = now - timedelta(hours=LOOKBACK_HOURS)
    days: list[date] = []
    d = window_start.date()
    while d <= now.date():
        days.append(d)
        d += timedelta(days=1)

    by_vehicle: dict[str, list[dict]] = defaultdict(list)
    seen: set[int] = set()
    for d in days:
        for e in fetch_day(session, limiter, d):
            eid = e["id"]
            if eid in seen:                       # idempotent: never count an id twice
                continue
            seen.add(eid)
            t = parse_event_time(e["time"])
            if t is None:
                continue
            by_vehicle[e["vehicle_id"]].append({
                "id": eid, "vehicle": e["vehicle_id"], "_t": t, "time": e["time"],
                "lat": e.get("latitude"), "lon": e.get("longitude"),
                "ons": e.get("ons") or 0, "offs": e.get("offs") or 0,
            })

    for evs in by_vehicle.values():
        evs.sort(key=lambda e: (e["_t"], e["id"]))
    return by_vehicle


def compute_occupancy(session: requests.Session, limiter: RateLimiter,
                      now: datetime) -> dict[str, int]:
    by_vehicle = gather_events(session, limiter, now)
    gap_s = GAP_RESET_HOURS * 3600
    return {v: occupancy_since_last_gap(evs, gap_s, FLOOR_AT_ZERO)
            for v, evs in by_vehicle.items()}


# ---- Stop resolution (the "next step": lat/lon -> stop name) -----------------
import csv
import math


class StopIndex:
    """Maps an (lat, lon) to the nearest GTFS stop within a distance threshold.

    Load from a GTFS stops.txt (columns: stop_id, stop_name, stop_lat, stop_lon).
    With no file it is a no-op: nearest() returns None and the app falls back to
    showing raw coordinates. Drop in stops.txt and stop names light up everywhere.

    Lookup is a linear haversine scan. Fine for a few thousand stops at a 30s
    cadence; if you have very many stops, swap in a KD-tree / BallTree on
    radians (sklearn) or an R-tree keyed on a small lat/lon grid.
    """

    def __init__(self, path: str | None = None, max_meters: float = 60.0):
        self.max_meters = max_meters
        self.stops: list[tuple[str, str, float, float]] = []  # (id, name, lat, lon)
        if path:
            self.load(path)

    def load(self, path: str) -> None:
        try:
            with open(path, newline="", encoding="utf-8-sig") as fh:
                for row in csv.DictReader(fh):
                    try:
                        self.stops.append((
                            row["stop_id"], row.get("stop_name", row["stop_id"]),
                            float(row["stop_lat"]), float(row["stop_lon"]),
                        ))
                    except (KeyError, ValueError):
                        continue
        except FileNotFoundError:
            logging.warning("STOPS_FILE %r not found; serving coordinates only", path)
            return
        logging.info("loaded %d stops from %s", len(self.stops), path)

    @staticmethod
    def _haversine_m(lat1, lon1, lat2, lon2) -> float:
        r = 6371000.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(math.sqrt(a))

    def nearest(self, lat, lon) -> tuple[str, str] | None:
        """Return (stop_id, stop_name) of the closest stop within max_meters, else None."""
        if lat is None or lon is None or not self.stops:
            return None
        best = None
        best_d = self.max_meters
        for sid, name, slat, slon in self.stops:
            d = self._haversine_m(lat, lon, slat, slon)
            if d <= best_d:
                best_d = d
                best = (sid, name)
        return best


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    session = requests.Session()
    limiter = RateLimiter()

    while True:
        try:
            occ = compute_occupancy(session, limiter, datetime.now(AGENCY_TZ))
            onboard = {v: c for v, c in occ.items() if c > 0}
            logging.info("vehicles=%d  onboard_now=%d  total_riders=%d",
                         len(occ), len(onboard), sum(onboard.values()))
            # TODO: publish `occ` to your datastore / dashboard / message bus here.
        except requests.HTTPError as e:
            logging.warning("HTTP error: %s", e)
        except Exception:
            logging.exception("poll failed")
        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
