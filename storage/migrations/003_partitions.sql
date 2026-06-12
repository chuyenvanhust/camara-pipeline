-- TODO 3.1: Khởi tạo sẵn các phân vùng vật lý (Dữ liệu mẫu cho hiện tại và tương lai)
-- Cú pháp: CREATE TABLE bảng_y2026m06 PARTITION OF bảng FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- Tạo phân vùng cho sim_swap_events (Tháng 06 năm 2026)
CREATE TABLE sim_swap_events_y2026m06 PARTITION OF sim_swap_events
    FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-07-01 00:00:00+00');

-- TODO 3.2: Tạo các phân vùng tương ứng cho device_swap_events
CREATE TABLE device_swap_events_y2026m06 PARTITION OF device_swap_events
    FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-07-01 00:00:00+00');