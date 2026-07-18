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

-- ---------------------------------------------------------------------------
-- Phase 2 (planned) — derived rollups, rebuilt from apc_events. Not created yet.
--
-- CREATE TABLE stops (
--     stop_id     TEXT PRIMARY KEY,
--     stop_name   TEXT NOT NULL,
--     latitude    DOUBLE PRECISION,
--     longitude   DOUBLE PRECISION,
--     is_terminal BOOLEAN DEFAULT false
-- );
--
-- CREATE TABLE stop_hourly (                    -- the workhorse rollup
--     service_date DATE     NOT NULL,
--     hour         SMALLINT NOT NULL,           -- 0..23 agency-local
--     stop_name    TEXT     NOT NULL,           -- name -> NB/SB variants merge
--     ons          INTEGER  NOT NULL,
--     offs         INTEGER  NOT NULL,
--     visits       INTEGER  NOT NULL,
--     PRIMARY KEY (service_date, hour, stop_name)
-- );
--
-- CREATE TABLE vehicle_daily (
--     service_date DATE NOT NULL,
--     vehicle_id   TEXT NOT NULL,
--     boardings    INTEGER NOT NULL,
--     alightings   INTEGER NOT NULL,
--     peak_onboard INTEGER,
--     PRIMARY KEY (service_date, vehicle_id)
-- );
