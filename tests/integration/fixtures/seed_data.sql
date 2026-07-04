-- Dùng INSERT ... ON CONFLICT để đảm bảo không lỗi nếu data đã tồn tại
-- Lưu ý: Vì bảng radius_sessions có PK (id, event_timestamp), 
-- nếu bạn không set giá trị id cụ thể, nó sẽ tự tăng.

INSERT INTO swap_event (msisdn, swap_type, detected_at)
VALUES 
('+84901234563', 'SIM_SWAP', NOW() - INTERVAL '31 days'),
('+84911234562', 'DEVICE_SWAP', NOW() - INTERVAL '35 days')
ON CONFLICT DO NOTHING;

-- Chèn dữ liệu mẫu cho radius_sessions
-- Chỉ chèn nếu chưa tồn tại bản ghi có acct_session_id này để tránh lỗi
INSERT INTO radius_sessions
    (acct_session_id, acct_status_type, msisdn, imsi, imei, event_timestamp, ingest_timestamp)
SELECT 
    '11111111-1111-1111-1111-111111111101', 'Start', '+84921234561', '452010000000002', '860934042394121',
    NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours'
WHERE NOT EXISTS (
    SELECT 1 FROM radius_sessions WHERE acct_session_id = '11111111-1111-1111-1111-111111111101' AND acct_status_type = 'Start'
);

INSERT INTO radius_sessions
    (acct_session_id, acct_status_type, msisdn, imsi, imei, event_timestamp, ingest_timestamp)
SELECT 
    '11111111-1111-1111-1111-111111111101', 'Stop', '+84921234561', '452010000000002', '860934042394121',
    NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'
WHERE NOT EXISTS (
    SELECT 1 FROM radius_sessions WHERE acct_session_id = '11111111-1111-1111-1111-111111111101' AND acct_status_type = 'Stop'
);