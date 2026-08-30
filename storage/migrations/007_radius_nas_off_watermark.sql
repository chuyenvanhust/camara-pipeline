-- Cross-partition ordering fence for NAS-wide Accounting-Off events.
-- Kafka preserves MSISDN order per partition, but a NAS spans partitions.
CREATE TABLE IF NOT EXISTS radius_nas_off_watermark (
    nas_identifier TEXT PRIMARY KEY,
    watermark_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_radius_nas_off_watermark_updated
    ON radius_nas_off_watermark(updated_at);
