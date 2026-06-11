# scripts/

Shell script wrappers cho các tác vụ thường dùng.
Tất cả script đều đọc config từ `.env` ở root project.

## Scripts

| Script | Tương đương `make` | Mô tả |
|--------|-------------------|-------|
| `run_simulator.sh` | `make sim` | Chạy simulator với tham số mặc định (2M records, seed=42) |
| `run_pipeline.sh` | `make pipeline` | Submit Spark job + khởi động Kafka consumer cho 5 stage |
| `run_load_test.sh` | `make load-test` | k6 load test 3 API: 100 VU, 60s, xuất kết quả JSON |
| `generate_report.sh` | `make report` | Sinh HTML Data Quality Report, mở browser |
| `reset_db.sh` | `make reset-db` | Drop + recreate toàn bộ schema (chỉ dùng khi dev) |

## Thứ tự chạy lần đầu

```bash
# 1. Khởi động toàn bộ stack (bao gồm mock services)
make up

# 2. Seed mock services (cần chạy 1 lần, hoặc sau khi reset)
python mock_services/gsma_tac/seed.py --count 2000 --seed 42
python mock_services/hlr_hss/seed.py  --count 100000 --seed 42
python mock_services/itu_e164/seed.py

# 3. Sinh dữ liệu
make sim

# 4. Chạy pipeline
make pipeline

# 5. Xem report
make report

# 6. Đo latency API
make load-test
```
