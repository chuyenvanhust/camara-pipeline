-- TODO 2.1: Tạo Composite Index cho bảng 'sim_swap_events' phục vụ API Pattern 1
-- Query: WHERE phone_number = ? ORDER BY event_timestamp DESC
-- Tối ưu loại bỏ Seq Scan, kích hoạt Index Scan/Index Only Scan trực tiếp trên phân vùng
CREATE INDEX idx_sim_swap_query ON sim_swap_events (phone_number, event_timestamp DESC);

-- TODO 2.2: Tạo Composite Index cho bảng 'device_swap_events' phục vụ API Pattern 2
-- Query: WHERE phone_number = ? AND imei = ? ORDER BY event_timestamp DESC
CREATE INDEX idx_device_swap_query ON device_swap_events (phone_number, imei, event_timestamp DESC);