---storage\migrations\002_indexes.sql
-- Indexes cho radius_sessions
CREATE INDEX IF NOT EXISTS idx_msisdn_ts ON radius_sessions (msisdn, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_imsi_ts ON radius_sessions (imsi, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_imei_ts ON radius_sessions (imei, event_timestamp DESC);

-- Indexes cho swap_event
CREATE INDEX IF NOT EXISTS idx_swap_msisdn ON swap_event (msisdn, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_swap_imei ON swap_event (imei, detected_at DESC);