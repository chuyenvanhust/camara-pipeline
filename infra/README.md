# infra/

Cấu hình hạ tầng cho các service chạy trong Docker Compose.

## Files

```
infra/
├── kafka/
│   └── kafka_config.properties       # Topic config: retention, segment size
├── prometheus/
│   └── prometheus.yml                # Scrape targets: FastAPI, Spark, PostgreSQL
└── grafana/
    └── dashboards/
        └── pipeline_dashboard.json   # Dashboard: throughput, Kafka lag, API latency p95
```

## Services và ports

| Service | Image | Port | Dashboard |
|---------|-------|------|-----------|
| Kafka | confluentinc/cp-kafka:7.5 | 9092 | — |
| Zookeeper | confluentinc/cp-zookeeper:7.5 | 2181 | — |
| PostgreSQL | postgres:15 | 5432 | — |
| FastAPI | python:3.11 (custom) | 8000 | http://localhost:8000/docs |
| Spark | bitnami/spark:3.5 | 4040 | http://localhost:4040 |
| Prometheus | prom/prometheus | 9090 | http://localhost:9090 |
| Grafana | grafana/grafana | 3000 | http://localhost:3000 (admin/admin) |
| GSMA TAC Mock | python:3.11 (custom) | 8100 | http://localhost:8100/docs |
| HLR/HSS Mock | python:3.11 (custom) | 8200 | http://localhost:8200/docs |
| ITU E.164 Mock | python:3.11 (custom) | 8300 | http://localhost:8300/docs |

## Kafka topics

| Topic | Partitions | Retention | Mô tả |
|-------|-----------|-----------|-------|
| `radius.raw` | 3 | 24h | Record thô từ simulator |
| `radius.valid` | 3 | 12h | Đã qua validation |
| `radius.dedup` | 3 | 12h | Đã loại duplicate |
| `radius.clean` | 3 | 12h | Đã resolve conflict |
| `radius.invalid` | 3 | 48h | Records lỗi (giữ lâu hơn để debug) |
