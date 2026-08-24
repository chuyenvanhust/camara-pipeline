-- storage/migrations/003_audit_retention_index.sql
-- F-11: Index cho audit_log queries theo event_type và msisdn
-- Hiện tại audit_log chỉ có PK — mọi query theo event_type/msisdn/created_at đều seq scan.

CREATE INDEX IF NOT EXISTS idx_audit_event_time
    ON audit_log (event_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_msisdn_time
    ON audit_log (msisdn, created_at DESC)
    WHERE msisdn IS NOT NULL;
