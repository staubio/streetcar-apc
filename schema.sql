-- KC Streetcar tracker — database schema
--
-- Phase 1 (active): raw event capture. The app creates this automatically at
-- startup via db.init_schema(); this file is a reference / for manual setup.
--
-- Layer 1 — immutable source of truth. Every metric is derived from this and is
-- rebuildable, so changing stop-matching / occupancy logic never rewrites history.

CREATE TABLE IF NOT EXISTS apc_events (
    id           BIGINT PRIMARY KEY,            -- Swiftly event id: natural dedup key
    vehicle_id   TEXT        NOT NULL,
    event_time   TIMESTAMPTZ NOT NULL,          -- agency-local instant, tz-aware
    service_date DATE        NOT NULL,          -- agency-local calendar bucket
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    ons          SMALLINT    NOT NULL,
    offs         SMALLINT    NOT NULL,
    ingested_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS apc_events_service_date_idx ON apc_events (service_date);
CREATE INDEX IF NOT EXISTS apc_events_vehicle_time_idx ON apc_events (vehicle_id, event_time);

-- Layer 2 (active) — per-stop, per-hour rollup. Derived from apc_events + the app's
-- stop resolution, rebuildable any time. Drives all stop/ridership reports at any
-- time granularity (coarser periods are aggregate-on-read, not stored tables).
CREATE TABLE IF NOT EXISTS stop_hourly (
    bucket_start TIMESTAMPTZ NOT NULL,          -- start of the hour, agency-local instant
    service_date DATE        NOT NULL,          -- agency-local date, for daily grouping
    stop_name    TEXT        NOT NULL,          -- '(unmatched)' if activity didn't resolve
    ons          INTEGER     NOT NULL,
    offs         INTEGER     NOT NULL,
    events       INTEGER     NOT NULL,
    PRIMARY KEY (bucket_start, stop_name)
);
CREATE INDEX IF NOT EXISTS stop_hourly_date_idx ON stop_hourly (service_date);
CREATE INDEX IF NOT EXISTS stop_hourly_stop_idx ON stop_hourly (stop_name);

-- ---------------------------------------------------------------------------
-- Phase 2 remaining (planned) — a sibling rollup off raw, not a child of stop_hourly.
--
-- CREATE TABLE vehicle_daily (              -- needs the occupancy walk for peak_onboard
--     service_date DATE NOT NULL,
--     vehicle_id   TEXT NOT NULL,
--     boardings    INTEGER NOT NULL,
--     alightings   INTEGER NOT NULL,
--     peak_onboard INTEGER,
--     PRIMARY KEY (service_date, vehicle_id)
-- );
