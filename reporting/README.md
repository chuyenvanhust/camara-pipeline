# reporting/

Sinh Data Quality Report dạng HTML sau mỗi lần chạy pipeline.

## Files

| File | Vai trò |
|------|---------|
| `quality_report.py` | Query PostgreSQL → tính tỷ lệ swap → render HTML qua Jinja2 |
| `templates/report.html.jinja2` | HTML template với biểu đồ Chart.js |

## Chạy

```bash
# Cần PostgreSQL đang chạy (docker compose up postgres migrate)
python reporting/quality_report.py --output reports/quality_report.html

# Offline template test (không cần DB)
python reporting/quality_report.py --allow-mock --output reports/sample.html
```

## Nội dung báo cáo

| Section | Nguồn dữ liệu |
|---------|--------------|
| Tổng quan | `msisdn_device`, CSV input (nếu có) |
| Swap events | `sim_swap_history`, `device_swap_history` |
| Throughput | Ước lượng từ `audit_log` event_time span (không hard-code) |
| Sessions | `radius_session_state` row count |

Throughput producer/consumer đầy đủ phải lấy từ Prometheus (`localhost:9090`) hoặc
từng target nội bộ: IP `:9200`, ingestion `:9201`, Device `:9202`, SIM `:9203`.
Khi service có nhiều replica, Prometheus DNS discovery scrape từng container;
không tồn tại một service tổng hợp tên `pipeline`.

## Output

```
reports/
└── quality_report_20250601_143022.html
```

File HTML self-contained (Chart.js CDN), mở trực tiếp bằng browser.
