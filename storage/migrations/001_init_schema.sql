-- storage/migrations/001_init_schema.sql

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. radius_sessions — partitioned, surrogate PK để cho phép
--    nhiều record (Start/Stop/Interim) cùng acct_session_id
CREATE TABLE IF NOT EXISTS radius_sessions (
    id               BIGSERIAL,
    acct_session_id  TEXT NOT NULL,
    acct_status_type VARCHAR(16),
    event_timestamp  TIMESTAMPTZ NOT NULL,
    ingest_timestamp TIMESTAMPTZ DEFAULT NOW(),
    msisdn           VARCHAR(16),
    imsi             CHAR(15),
    imei             CHAR(15),
    rat_type         VARCHAR(8),
    framed_ip        INET,
    nas_ip           INET,
    mcc_mnc          CHAR(6),
    late_arrival     BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);

-- 2. swap_event
CREATE TABLE IF NOT EXISTS swap_event (
    id           BIGSERIAL PRIMARY KEY,
    msisdn       VARCHAR(16),
    old_imsi     CHAR(15),
    new_imsi     CHAR(15),
    old_imei     CHAR(15),
    new_imei     CHAR(15),
    imei         CHAR(15),
    swap_type    VARCHAR(16),
    detected_at  TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    source       VARCHAR(32)
);

-- 3. duplicate_log
CREATE TABLE IF NOT EXISTS duplicate_log (
    id          SERIAL PRIMARY KEY,
    session_id  TEXT NOT NULL,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    reason      VARCHAR(50)
);

-- 4. conflict_log
CREATE TABLE IF NOT EXISTS conflict_log (
    id            SERIAL PRIMARY KEY,
    session_id    TEXT NOT NULL,
    conflict_type VARCHAR(10) NOT NULL,
    details       TEXT,
    error_code      VARCHAR(50)
);

-- 5. invalid_log
CREATE TABLE IF NOT EXISTS invalid_log (
    id         SERIAL PRIMARY KEY,
    session_id TEXT,
    msisdn     VARCHAR(20),
    error_code VARCHAR(50) NOT NULL,
    details    TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);