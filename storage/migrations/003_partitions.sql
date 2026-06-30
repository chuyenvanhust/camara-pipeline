-- storage/migrations/003_partitions.sql

-- Tạo phân vùng cho tất cả các tháng trong năm 2026
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m01 PARTITION OF radius_sessions FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m02 PARTITION OF radius_sessions FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m03 PARTITION OF radius_sessions FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m04 PARTITION OF radius_sessions FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m05 PARTITION OF radius_sessions FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m06 PARTITION OF radius_sessions FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m07 PARTITION OF radius_sessions FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m08 PARTITION OF radius_sessions FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m09 PARTITION OF radius_sessions FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m10 PARTITION OF radius_sessions FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m11 PARTITION OF radius_sessions FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE IF NOT EXISTS radius_sessions_y2026m12 PARTITION OF radius_sessions FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- Phân vùng mặc định dự phòng
CREATE TABLE IF NOT EXISTS radius_sessions_default PARTITION OF radius_sessions DEFAULT;