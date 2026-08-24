-- storage/migrations/002_notification_outbox_index.sql
-- F-03 + F-11: Index cho notification_log claim query (outbox pattern dispatcher)
-- Dùng bởi NotificationDispatcher._claim_batch():
--   WHERE status IN ('PENDING', 'FAILED') AND next_retry_at <= NOW()
--   FOR UPDATE SKIP LOCKED

CREATE INDEX IF NOT EXISTS idx_notification_claim
    ON notification_log (status, next_retry_at)
    WHERE status IN ('PENDING', 'FAILED');
