# scripts/

Wrapper cho các tác vụ vận hành pipeline qua Docker Compose. Không có `Makefile`/
`make` trong repo này — mọi thứ chạy trực tiếp bằng `bash scripts/<tên script>.sh`.

Hầu hết script tự nạp `.env` ở thư mục gốc project nếu file đó tồn tại (`DB_USER`,
`DB_PASSWORD`, `DB_NAME`, `API_KEY`, `RADIUS_SHARED_SECRET`, ...). Không bắt buộc
phải có `.env` — thiếu thì dùng default đã set sẵn trong `docker-compose.yml`
(ví dụ `API_KEY=dev-secret`, `RADIUS_SHARED_SECRET=camara-radius-dev-secret`).

## Thứ tự chạy lần đầu

```bash
# 1. Sinh dữ liệu RADIUS mẫu ra data/radius_log.csv (chạy trong container tạm,
#    không cần cài Python/deps trên host)
bash scripts/run_simulator.sh

# 2. Build & khởi động toàn bộ stack (Kafka, Postgres, Redis, migrate,
#    fastapi, pipeline, radius-ingestion, notification-dispatcher,
#    prometheus, grafana), đợi tới khi fastapi + pipeline healthy
bash scripts/run_pipeline.sh

# 3a. Nạp dữ liệu qua đường CSV (một lần, chạy xong tự thoát)
bash scripts/run_ingest_csv.sh data/radius_log.csv

# 3b. HOẶC nạp qua đường UDP (giả lập thiết bị NAS/GGSN thật gửi RADIUS
#     Accounting-Request) — cần 2 lệnh, chạy song song:
bash scripts/run_ingest_udp.sh                          # bật listener UDP/1813
bash scripts/simulate_radius_device.sh data/radius_log.csv 15000   # phát 15.000 pkt/s

# 4. Sinh báo cáo chất lượng dữ liệu (HTML, tự mở trình duyệt)
bash scripts/generate_report.sh

# 5. (Tùy chọn) Đo tải API bằng k6
bash scripts/run_load_test.sh
```

Sau bước 2, các service sau đã tự chạy nền, không cần lệnh riêng:
- `pipeline` — 3 consumer (`cg-ip-msisdn`, `cg-device-swap`, `cg-sim-swap`), Prometheus metrics ở `:9200/metrics`
- `notification-dispatcher` — gửi callback outbox, poll `notification_log`
- `fastapi` — API ở `http://localhost:8000` (Swagger: `/docs`)
- `prometheus` (`http://localhost:9090`), `grafana` (`http://localhost:3000`, mặc định admin/admin)

## Danh sách script

| Script | Dùng khi nào | Ghi chú |
|---|---|---|
| `run.sh {up\|down\|status\|logs\|ingest-csv\|simulate-radius\|reset-db}` | Điều khiển nhanh stack qua 1 lệnh gọn | `up`/`down` chỉ là alias cho `docker compose up -d --build` / `down` (không xóa volume). `logs [service]` mặc định tail `pipeline`. |
| `run_pipeline.sh` | Khởi động lần đầu, hoặc sau khi đổi code cần build lại | `docker compose up -d --build` rồi poll healthcheck của `fastapi-app` và `camara-pipeline` tối đa 120s; in log 3 service liên quan nếu timeout. |
| `run_simulator.sh` | Cần dữ liệu RADIUS giả lập mới | Chạy trong container `python:3.11-slim` tạm (`docker run --rm`), tự cài `requests/pydantic/altair/pandas/asyncpg`, xuất `data/radius_log.csv` (mặc định 2,000,000 record / 100,000 subscriber / seed=42). Sửa tham số bằng cách sửa trực tiếp lệnh trong script (`--records`, `--subscribers`, `--days`, `--duplicate-rate`, `--late-arrival-rate`, `--invalid-imei-rate`, `--conflict-rate`, `--missing-field-rate`, `--sim-swap-rate`, `--device-swap-rate`). |
| `run_ingest_csv.sh FILE.csv` | Nạp 1 lần toàn bộ file CSV vào Kafka | `docker compose run --rm --no-deps` container `pipeline` tạm thời, mount file CSV read-only, chạy `pipeline.ingestion.producer --file`. Container tự thoát sau khi ingest xong. |
| `run_ingest_udp.sh` | Muốn test đường UDP thật (RADIUS Accounting-Request nhị phân) | Chỉ bật service `radius-ingestion` (đã định nghĩa sẵn trong compose, lắng nghe UDP/1813, có `restart: unless-stopped`) — không tự tắt, cần `docker compose stop radius-ingestion` khi xong. |
| `simulate_radius_device.sh FILE.csv [rate] [--loop]` | Giả lập 1 thiết bị NAS/GGSN gửi gói tin UDP thật tới listener ở trên | Chạy bằng Python trên host. Sender pre-encode song song, cache AVP lặp và pace theo micro-burst; `rate` mặc định 50, đã benchmark ở 15.000 pkt/s. Thêm `--loop` để lặp file. |
| `generate_report.sh` | Cần xem báo cáo chất lượng dữ liệu dạng HTML | Chạy `reporting/quality_report.py` bên trong container `camara-pipeline` (tránh phải cài `psycopg2`/`jinja2` trên host), copy report ra `reports/quality_report_<timestamp>.html`, tự mở bằng trình duyệt mặc định (Windows/macOS/Linux). Nếu Postgres không kết nối được, report vẫn được tạo nhưng có **banner cảnh báo đỏ** báo đây là dữ liệu mock, không phải số thật. |
| `run_load_test.sh` | Đo latency/throughput 3 API (sim-swap, device-swap, number-verification) | Chạy `grafana/k6` qua Docker (`docker run --rm`), cần biến `BASE_URL` (mặc định `http://host.docker.internal:8000`) và `API_KEY` (mặc định `dev-secret`, phải khớp `.env`). Kết quả JSON ghi vào `reports/*.json`. |
| `run_dispatcher.sh {start\|stop\|restart\|status\|logs\|local}` | Cần quản lý riêng notification-dispatcher mà không đụng cả stack | Container `notification-dispatcher` tự chạy khi `docker compose up -d` nhưng **không có `restart:` policy trong compose** — nếu nó crash, sẽ nằm chết cho tới khi chủ động `start`/`restart` lại bằng script này. `local` chạy trực tiếp bằng `python3 -m pipeline.dispatcher.notification_dispatcher` (debug, cần `.env` có `DB_HOST` mà host resolve được, ví dụ `localhost` nếu Postgres đã publish port ra ngoài). |
| `reset.sh` | Nghi ngờ dữ liệu bị ingest trùng, hoặc cần baseline sạch để đo lại benchmark | **Có xác nhận `y/n`.** Kill process pipeline cũ còn sót trong container, xóa + tạo lại Kafka topic đúng số partition (`KAFKA_PARTITIONS`, mặc định 8; việc xóa vật lý có độ trễ nên script tự retry tối đa 90s), `TRUNCATE` các bảng nghiệp vụ (`sim_swap_history`, `device_swap_history`, `msisdn_sim`, `msisdn_device`, `audit_log`), và `FLUSHALL` Redis. **Không đụng tới `subscription`** (danh sách đăng ký webhook vẫn giữ nguyên) và không đụng container/volume ngoài project. |
| `reset_db.sh` | Cần xóa sạch TOÀN BỘ schema Postgres (kể cả `subscription`, `notification_log`) để chạy lại migration từ đầu | **Có xác nhận `y/n`.** `DROP SCHEMA public CASCADE; CREATE SCHEMA public;` — sau khi chạy, phải `docker compose up -d migrate` (hoặc restart stack) để migration tạo lại toàn bộ bảng trước khi dùng tiếp. |

## Lưu ý quan trọng

- **`run.sh up` / `down` không xóa volume** (không có `-v`) — dữ liệu Postgres/Redis giữa các lần `up`/`down` được giữ nguyên. Muốn xóa sạch, dùng `reset.sh` (dữ liệu nghiệp vụ) hoặc `reset_db.sh` (toàn bộ schema).
- **`reset.sh` và `reset_db.sh` khác nhau về phạm vi**: `reset.sh` giữ lại schema + subscription/notification, chỉ xóa dữ liệu nghiệp vụ để chạy lại benchmark sạch; `reset_db.sh` xóa cả schema, dùng khi migration bị hỏng hoặc cần trạng thái DB hoàn toàn mới.
- **Thứ tự bắt buộc cho đường UDP**: phải `run_ingest_udp.sh` (bật listener) trước rồi mới `simulate_radius_device.sh` (bên gửi) — chạy ngược thứ tự sẽ mất gói tin do UDP không retry.
- Tuning sender: `RADIUS_SENDER_QUEUE_SIZE` (mặc định `50000`),
  `RADIUS_SENDER_PACING_WINDOW_MS` (`2`), `RADIUS_SENDER_MAX_CATCHUP_MS` (`100`) và
  `RADIUS_SENDER_MAX_PACKETS` (`0`, gửi hết file). Ví dụ benchmark hữu hạn:
  `RADIUS_SENDER_MAX_PACKETS=150000 bash scripts/simulate_radius_device.sh data/radius_log.csv 15000`.
- Mọi script `docker exec`/`docker compose exec` giả định container đã chạy qua `run_pipeline.sh` hoặc `run.sh up` trước đó; nếu container chưa tồn tại, script sẽ báo lỗi thay vì tự khởi động stack.
