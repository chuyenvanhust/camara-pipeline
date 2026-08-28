-- Optimize Outbox join with active subscriptions
CREATE INDEX IF NOT EXISTS idx_subscription_lookup_active
    ON subscription(event_type, status, msisdn)
    WHERE status='ACTIVE';
