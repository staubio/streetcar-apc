"""Postgres persistence — Phase 1: raw APC event capture (the source of truth).

Everything here is a no-op unless DATABASE_URL is set, so the app runs unchanged
without a database. Failures never propagate to the caller: if the DB is down, the
live tracker keeps running and capture simply retries on the next poll.

Design: raw events are immutable facts, deduped by Swiftly's event `id` (the table
PK + ON CONFLICT DO NOTHING). Every derived metric (per-stop rollups, ridership
trends) is computed from this table later and is fully rebuildable, so changing the
stop-matching or occupancy logic never invalidates stored history.
"""
from __future__ import annotations

import logging
import os
import threading

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:                              # library not installed -> stay disabled
    psycopg2 = None

DATABASE_URL = os.environ.get("DATABASE_URL")
if psycopg2 is None:
    DISABLED_REASON = "psycopg2 not installed"
elif not DATABASE_URL:
    DISABLED_REASON = "DATABASE_URL not set"
else:
    DISABLED_REASON = None
enabled = DISABLED_REASON is None

_conn = None
_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS apc_events (
    id           BIGINT PRIMARY KEY,            -- Swiftly event id: natural dedup key
    vehicle_id   TEXT        NOT NULL,
    event_time   TIMESTAMPTZ NOT NULL,          -- the agency-local instant, tz-aware
    service_date DATE        NOT NULL,          -- agency-local calendar bucket
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    ons          SMALLINT    NOT NULL,
    offs         SMALLINT    NOT NULL,
    ingested_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS apc_events_service_date_idx ON apc_events (service_date);
CREATE INDEX IF NOT EXISTS apc_events_vehicle_time_idx ON apc_events (vehicle_id, event_time);

-- Layer 2: per-stop, per-hour, PER-DIRECTION rollup. Derived from apc_events + the
-- app's stop resolution and direction inference; rebuildable at any time.
-- One-time migration: stop_hourly is a derived cache, so if an older version exists
-- without the direction column we just drop it and let the backfill rebuild it.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'stop_hourly')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name = 'stop_hourly' AND column_name = 'direction') THEN
        DROP TABLE stop_hourly;
    END IF;
END $$;
CREATE TABLE IF NOT EXISTS stop_hourly (
    bucket_start TIMESTAMPTZ NOT NULL,          -- start of the hour, agency-local instant
    service_date DATE        NOT NULL,          -- agency-local date, for daily grouping
    stop_name    TEXT        NOT NULL,          -- '(unmatched)' if activity didn't resolve
    direction    TEXT        NOT NULL,          -- 'Northbound' | 'Southbound' | 'Unknown'
    ons          INTEGER     NOT NULL,
    offs         INTEGER     NOT NULL,
    events       INTEGER     NOT NULL,          -- door-active event count in the bucket
    PRIMARY KEY (bucket_start, stop_name, direction)
);
CREATE INDEX IF NOT EXISTS stop_hourly_date_idx ON stop_hourly (service_date);
CREATE INDEX IF NOT EXISTS stop_hourly_stop_idx ON stop_hourly (stop_name);
"""

_INSERT = (
    "INSERT INTO apc_events "
    "(id, vehicle_id, event_time, service_date, latitude, longitude, ons, offs) "
    "VALUES %s ON CONFLICT (id) DO NOTHING"
)


def _get_conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(DATABASE_URL)
        _conn.autocommit = False
    return _conn


def _reset():
    global _conn
    try:
        if _conn is not None:
            _conn.close()
    except Exception:
        pass
    _conn = None


def init_schema() -> None:
    if not enabled:
        return
    with _lock:
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                cur.execute(SCHEMA)
            conn.commit()
            logging.info("db: schema ready")
        except Exception:
            logging.exception("db: init_schema failed")
            _reset()


def high_water() -> int:
    """Largest event id already stored, so we don't re-ship what we have."""
    if not enabled:
        return 0
    with _lock:
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(MAX(id), 0) FROM apc_events")
                return int(cur.fetchone()[0])
        except Exception:
            logging.exception("db: high_water failed")
            _reset()
            return 0


def insert_events(rows) -> int | None:
    """Insert raw events; returns count inserted, or None on failure (so the caller
    can hold its high-water mark and retry next poll).

    rows: iterable of (id, vehicle_id, event_time, service_date, lat, lon, ons, offs).
    """
    if not enabled:
        return 0
    rows = list(rows)
    if not rows:
        return 0
    with _lock:
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                execute_values(cur, _INSERT, rows, page_size=1000)
                inserted = cur.rowcount
            conn.commit()
            return inserted if inserted is not None and inserted >= 0 else 0
        except Exception:
            logging.exception("db: insert_events failed")
            _reset()
            return None


def fetch_raw_day(service_date):
    """All raw events for a service date, for (re)building rollups from source."""
    if not enabled:
        return []
    with _lock:
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, vehicle_id, event_time, latitude, longitude, ons, offs "
                    "FROM apc_events WHERE service_date = %s", (service_date,))
                return cur.fetchall()
        except Exception:
            logging.exception("db: fetch_raw_day failed")
            _reset()
            return []


def rollup_missing_dates():
    """Service dates present in raw but not yet in the rollup (for startup rebuild)."""
    if not enabled:
        return []
    with _lock:
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT service_date FROM apc_events "
                    "WHERE service_date NOT IN (SELECT DISTINCT service_date FROM stop_hourly) "
                    "ORDER BY service_date")
                return [r[0] for r in cur.fetchall()]
        except Exception:
            logging.exception("db: rollup_missing_dates failed")
            _reset()
            return []


def replace_stop_hourly(service_date, rows) -> bool:
    """Idempotently replace one service date's rollup: delete then insert, in one txn.
    rows: (bucket_start, service_date, stop_name, direction, ons, offs, events)."""
    if not enabled:
        return False
    with _lock:
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                cur.execute("DELETE FROM stop_hourly WHERE service_date = %s", (service_date,))
                if rows:
                    execute_values(
                        cur,
                        "INSERT INTO stop_hourly "
                        "(bucket_start, service_date, stop_name, direction, ons, offs, events) "
                        "VALUES %s",
                        list(rows), page_size=1000)
            conn.commit()
            return True
        except Exception:
            logging.exception("db: replace_stop_hourly failed")
            _reset()
            return False


def fetchall(sql, params=()):
    """Generic read helper for report queries."""
    if not enabled:
        return []
    with _lock:
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        except Exception:
            logging.exception("db: query failed")
            _reset()
            return []
