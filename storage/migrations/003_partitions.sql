---storage\migrations\003_partitions.sql

-- Tạo phân vùng cho tháng 06/2026
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m06 PARTITION OF radius_sessions
    FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-07-01 00:00:00+00');

-- Phân vùng mặc định (hứng các records ngoài dải thời gian trên)
CREATE TABLE IF NOT EXISTS radius_sessions_default PARTITION OF radius_sessions DEFAULT;