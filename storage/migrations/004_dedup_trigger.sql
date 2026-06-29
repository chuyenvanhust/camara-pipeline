-- storage/migrations/004_dedup_trigger.sql

-- Bảng lưu trữ key dài hạn cho mục đích dedup — KHÔNG dùng radius_sessions
-- trực tiếp để tránh phải quét toàn bộ partition khi check trùng.
-- Đây là "long-term storage" thật sự của lớp backstop, độc lập với
-- chu kỳ sống của các partition radius_sessions (vd: nếu sau này có
-- job archive/drop partition cũ, bảng này vẫn giữ key để chống rollover).
CREATE TABLE IF NOT EXISTS dedup_seen_keys (
    acct_session_id  TEXT NOT NULL,
    acct_status_type VARCHAR(16) NOT NULL,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (acct_session_id, acct_status_type)
);

CREATE OR REPLACE FUNCTION fn_dedup_long_term_check()
RETURNS TRIGGER AS $$
DECLARE
    v_inserted BOOLEAN;
BEGIN
    -- INSERT ... ON CONFLICT DO NOTHING: check-và-set trong 1 statement
    -- nguyên tử, an toàn khi có 2 transaction insert đồng thời cùng key
    -- (tránh race condition mà cách check rồi insert riêng lẻ có thể gặp).
    INSERT INTO dedup_seen_keys (acct_session_id, acct_status_type)
    VALUES (NEW.acct_session_id, NEW.acct_status_type)
    ON CONFLICT (acct_session_id, acct_status_type) DO NOTHING
    RETURNING TRUE INTO v_inserted;

    IF v_inserted IS NULL THEN
        -- Conflict xảy ra -> key đã tồn tại từ trước, bất kể đã bao lâu
        -- -> đây là duplicate.
        INSERT INTO duplicate_log (session_id, detected_at, reason)
        VALUES (NEW.acct_session_id, NOW(), 'LATE_DUPLICATE_LONG_TERM');
        RETURN NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dedup_long_term_check ON radius_sessions;
CREATE TRIGGER trg_dedup_long_term_check
    BEFORE INSERT ON radius_sessions
    FOR EACH ROW
    EXECUTE FUNCTION fn_dedup_long_term_check();