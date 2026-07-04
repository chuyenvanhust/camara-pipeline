# reporting/

Sinh Data Quality Report dạng HTML sau mỗi lần chạy pipeline.
Báo cáo tỷ lệ phát hiện và xử lý theo từng loại vấn đề dữ liệu.

## Files

| File | Vai trò |
|------|---------|
| `quality_report.py` | Query 5 log tables → tính tỷ lệ → render HTML qua Jinja2 |
| `metrics_collector.py` | Thu thập Spark metrics (throughput, lag) và Kafka consumer lag |
| `templates/report.html.jinja2` | HTML template với biểu đồ Chart.js, không cần build step |

## Chạy

```bash
python reporting/quality_report.py --output reports/quality_report.html
# Hoặc:
make report
```

## Nội dung báo cáo (6 section)

| Section | Nguồn dữ liệu | Metrics |
|---------|--------------|---------|
| Tổng quan | pipeline run log | Tổng records, thời gian, throughput rec/s |
| Invalid IMEI | `invalid_log` WHERE error_code LIKE 'ERR_IMEI%' | Rate, phân tách Luhn fail vs TAC unknown |
| Duplicate | `duplicate_log` | Rate, phân bổ theo giờ |
| Conflict | `conflict_log` | Rate tổng, phân tách loại A/B/C |
| Late Arrival | `invalid_log` + `radius_sessions.late_arrival` | Rate, histogram độ trễ |
| Missing Field | `invalid_log` WHERE error_code='ERR_MISSING_FIELD' | Rate, field nào thiếu nhiều nhất |

## Output mẫu

```
reports/
└── quality_report_20250601_143022.html   ← timestamp trong tên file
```

File HTML self-contained (Chart.js CDN), mở được trực tiếp bằng browser.
