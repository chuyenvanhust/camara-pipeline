-- TODO 1.1: Tạo Extension pgcrypto (nếu cần dùng UUID làm ID sự kiện)
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- TODO 1.2: Định nghĩa bảng 'sim_swap_events' sử dụng Declarative Partitioning theo thời gian (BY RANGE)
-- Bảng này bắt buộc phải chứa các trường: id, phone_number (chuẩn E.164), event_timestamp, v.v.
CREATE TABLE sim_swap_events (
    id UUID DEFAULT gen_random_uuid(),
    phone_number VARCHAR(15) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(50),
    -- Lưu ý: Trong Declarative Partitioning, mọi ràng buộc UNIQUE/PRIMARY KEY 
    -- bắt buộc phải bao gồm cả cột phân vùng (event_timestamp)
    PRIMARY KEY (id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);

-- TODO 1.3: Định nghĩa bảng 'device_swap_events' phân vùng theo thời gian (BY RANGE)
-- Bảng này bắt buộc chứa các trường tương tự và bổ sung trường 'imei' (15 chữ số).
CREATE TABLE device_swap_events (
    id UUID DEFAULT gen_random_uuid(),
    phone_number VARCHAR(15) NOT NULL,
    imei VARCHAR(15) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(50),
    PRIMARY KEY (id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);