-- storage/migrations/001_init_schema.sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- 1. Bảng trạng thái hiện tại (Current State)
-- ============================================================

-- msisdn_device: Trạng thái IMEI hiện tại
CREATE TABLE IF NOT EXISTS msisdn_device (
    msisdn       VARCHAR(16) PRIMARY KEY,
    imei_current VARCHAR(15) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- msisdn_sim: Trạng thái IMSI hiện tại
CREATE TABLE IF NOT EXISTS msisdn_sim (
    msisdn       VARCHAR(16) PRIMARY KEY,
    imsi_current VARCHAR(15) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 2. Bảng lịch sử thay đổi (Swap History)
-- ============================================================

-- device_swap_history: Lịch sử đổi thiết bị
CREATE TABLE IF NOT EXISTS device_swap_history (
    id         BIGSERIAL PRIMARY KEY,
    msisdn     VARCHAR(16) NOT NULL,
    imei_old   VARCHAR(15),
    imei_new   VARCHAR(15) NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_device_swap_msisdn ON device_swap_history (msisdn, changed_at DESC);

-- sim_swap_history: Lịch sử đổi SIM
CREATE TABLE IF NOT EXISTS sim_swap_history (
    id         BIGSERIAL PRIMARY KEY,
    msisdn     VARCHAR(16) NOT NULL,
    imsi_old   VARCHAR(15),
    imsi_new   VARCHAR(15) NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sim_swap_msisdn ON sim_swap_history (msisdn, changed_at DESC);

-- ============================================================
-- 3. Bảng subscription (Open Gateway)
-- ============================================================

CREATE TABLE IF NOT EXISTS subscription (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    msisdn          VARCHAR(16) NOT NULL,
    event_type      VARCHAR(32) NOT NULL,  -- 'SIM_SWAP' | 'DEVICE_SWAP'
    callback_url    VARCHAR(2048) NOT NULL,
    status          VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',  -- 'ACTIVE' | 'EXPIRED' | 'CANCELLED'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_subscription_lookup
    ON subscription (msisdn, event_type, status)
    WHERE status = 'ACTIVE';

-- ============================================================
-- 4. Bảng Audit & Notification Tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS audit_log (
    id         BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    msisdn     VARCHAR(16),
    details    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notification_log (
    id              BIGSERIAL PRIMARY KEY,
    subscription_id UUID REFERENCES subscription(subscription_id),
    event_type      VARCHAR(32) NOT NULL,
    payload         JSONB NOT NULL,
    status          VARCHAR(16) NOT NULL DEFAULT 'PENDING',  -- PENDING | SENT | FAILED
    attempts        INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at   TIMESTAMPTZ,
    error_detail    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_notification_retry
    ON notification_log (status, next_retry_at)
    WHERE status IN ('PENDING', 'FAILED');