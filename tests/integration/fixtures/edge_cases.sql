-- ============================================================================
-- DỮ LIỆU ĐẶC BIỆT PHỤC VỤ CÁC EDGE CASES & PIPELINE TESTS
-- ============================================================================

-- Biện pháp an toàn: Đảm bảo bảng luôn tồn tại trước khi nạp edge cases
CREATE TABLE IF NOT EXISTS swap_event (id SERIAL PRIMARY KEY, msisdn VARCHAR(20) NOT NULL, swap_type VARCHAR(50) NOT NULL, detected_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS radius_sessions (acct_session_id VARCHAR(100) PRIMARY KEY, acct_status_type VARCHAR(20) NOT NULL, msisdn VARCHAR(20) NOT NULL, imsi VARCHAR(20), imei VARCHAR(20), event_timestamp TIMESTAMP NOT NULL, ingest_timestamp TIMESTAMP NOT NULL);

-- [TC23 - Deduplication] Bản ghi gốc đã được lưu vào hệ thống trước đó
INSERT INTO radius_sessions (acct_session_id, acct_status_type, msisdn, imsi, imei, event_timestamp, ingest_timestamp) VALUES
('sess_dup_10001', 'Start', '+84931111111', '452010000000111', '860934042394001', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes');

-- [TC26 - Conflict Resolution] Lưu một phiên Start để test kịch bản Stop gửi lên sai IMSI (Mismatch)
INSERT INTO radius_sessions (acct_session_id, acct_status_type, msisdn, imsi, imei, event_timestamp, ingest_timestamp) VALUES
('sess_conflict_A', 'Start', '+84932222222', '452010000000222', '860934042394002', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes');

-- [TC27 - Conflict Resolution] Lưu một phiên Start đang Active nhằm test lỗi gửi tiếp một Start trùng IMSI mà chưa Stop
INSERT INTO radius_sessions (acct_session_id, acct_status_type, msisdn, imsi, imei, event_timestamp, ingest_timestamp) VALUES
('sess_conflict_B_active', 'Start', '+84933333333', '452010000000333', '860934042394003', NOW() - INTERVAL '15 minutes', NOW() - INTERVAL '15 minutes');

-- [TC28 - Conflict/Swap Event Generation] Bản ghi nền lưu lịch sử IMSI cũ của thuê bao để test xem khi có IMSI mới hệ thống có sinh Event hay không
INSERT INTO radius_sessions (acct_session_id, acct_status_type, msisdn, imsi, imei, event_timestamp, ingest_timestamp) VALUES
('sess_swap_evt_old', 'Start', '+84934444444', '452010000000444', '860934042394004', NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days');