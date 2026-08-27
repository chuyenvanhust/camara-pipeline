-- Allow subscriptions for one MSISDN or for all UEs (msisdn IS NULL).
ALTER TABLE subscription ALTER COLUMN msisdn DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname='ck_subscription_event_type'
    ) THEN
        ALTER TABLE subscription ADD CONSTRAINT ck_subscription_event_type
            CHECK (event_type IN ('SIM_SWAP','DEVICE_SWAP'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname='ck_subscription_status'
    ) THEN
        ALTER TABLE subscription ADD CONSTRAINT ck_subscription_status
            CHECK (status IN ('ACTIVE','EXPIRED','CANCELLED'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_subscription_any_ue
    ON subscription(event_type, status)
    WHERE msisdn IS NULL AND status='ACTIVE';
