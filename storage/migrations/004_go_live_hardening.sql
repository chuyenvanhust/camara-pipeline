-- Replay-safe state, history, audit and notification outbox.
ALTER TABLE msisdn_device
    ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    ADD COLUMN IF NOT EXISTS last_event_id TEXT,
    ADD COLUMN IF NOT EXISTS last_source_partition INT NOT NULL DEFAULT -1,
    ADD COLUMN IF NOT EXISTS last_source_offset BIGINT NOT NULL DEFAULT -1;

ALTER TABLE msisdn_sim
    ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    ADD COLUMN IF NOT EXISTS last_event_id TEXT,
    ADD COLUMN IF NOT EXISTS last_source_partition INT NOT NULL DEFAULT -1,
    ADD COLUMN IF NOT EXISTS last_source_offset BIGINT NOT NULL DEFAULT -1;

ALTER TABLE device_swap_history
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS source_topic TEXT,
    ADD COLUMN IF NOT EXISTS source_partition INT,
    ADD COLUMN IF NOT EXISTS source_offset BIGINT;
UPDATE device_swap_history SET event_id='legacy-device:' || id WHERE event_id IS NULL;
ALTER TABLE device_swap_history ALTER COLUMN event_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_device_swap_event ON device_swap_history(event_id);

ALTER TABLE sim_swap_history
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS source_topic TEXT,
    ADD COLUMN IF NOT EXISTS source_partition INT,
    ADD COLUMN IF NOT EXISTS source_offset BIGINT;
UPDATE sim_swap_history SET event_id='legacy-sim:' || id WHERE event_id IS NULL;
ALTER TABLE sim_swap_history ALTER COLUMN event_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_sim_swap_event ON sim_swap_history(event_id);

ALTER TABLE audit_log
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS event_time TIMESTAMPTZ;
UPDATE audit_log
SET event_id='legacy-audit:' || id, event_time=COALESCE(event_time, created_at)
WHERE event_id IS NULL OR event_time IS NULL;
ALTER TABLE audit_log ALTER COLUMN event_id SET NOT NULL;
ALTER TABLE audit_log ALTER COLUMN event_time SET NOT NULL;
DROP INDEX IF EXISTS uq_audit_event;
CREATE UNIQUE INDEX IF NOT EXISTS uq_audit_event_type
    ON audit_log(event_id, event_type);

ALTER TABLE notification_log
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;
UPDATE notification_log SET event_id='legacy-notification:' || id WHERE event_id IS NULL;
ALTER TABLE notification_log ALTER COLUMN event_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_notification_event_subscription
    ON notification_log(event_id, subscription_id);
CREATE INDEX IF NOT EXISTS idx_notification_stale_claim
    ON notification_log(status, locked_at)
    WHERE status='IN_PROGRESS';

CREATE TABLE IF NOT EXISTS radius_session_state (
    acct_session_id TEXT PRIMARY KEY,
    msisdn VARCHAR(16) NOT NULL,
    nas_identifier TEXT,
    active BOOLEAN NOT NULL,
    last_event_at TIMESTAMPTZ NOT NULL,
    last_event_id TEXT NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_radius_session_active_msisdn
    ON radius_session_state(msisdn, last_event_at DESC) WHERE active;
CREATE INDEX IF NOT EXISTS idx_radius_session_active_nas
    ON radius_session_state(nas_identifier) WHERE active;
