# `infra/` — hạ tầng quan sát và cấu hình dịch vụ

Thư mục này chỉ chứa cấu hình được mount vào các container trong
`docker-compose.yml`. Không có Spark, PostgreSQL exporter hoặc mock service trong
stack chính hiện tại.

## Nội dung

```text
infra/
├── kafka/kafka_config.properties
├── prometheus/prometheus.yml
├── prometheus/alert.rules.yml
├── grafana/dashboards/pipeline_dashboard.json
├── grafana/provisioning/datasources/datasource.yml
└── redis/sentinel.conf
```

`redis/sentinel.conf` được dùng khi thêm `docker-compose.prod.yml`; stack base chỉ
chạy một Redis instance có AOF và `noeviction`. Production override chỉ thêm HA,
host networking và secret enforcement; tài nguyên vẫn lấy từ hardware profile.

## Services thực tế trong stack chính

| Service Compose | Số instance | Cổng host | Vai trò |
|---|---:|---:|---|
| `zookeeper` | 1 | không expose | Metadata Kafka 7.5 |
| `kafka-1..3` | 3 | `127.0.0.1:29092..29094` | Kafka cluster; client nội bộ dùng `:9092` |
| `postgres` | 1 | không expose | State, history, audit và transactional outbox |
| `redis` | 1 | không expose | Read path; DB 0/1/2 cho IP/Device/SIM |
| `migrate` | job | không expose | Chạy schema migration trước application |
| `fastapi` | 1 | `8000` | CAMARA API |
| `pipeline-ip-msisdn` | theo profile | metrics nội bộ `9200` | Group `cg-ip-msisdn` |
| `pipeline-device-swap` | theo profile | metrics nội bộ `9202` | Group `cg-device-swap` |
| `pipeline-sim-swap` | theo profile | metrics nội bộ `9203` | Group `cg-sim-swap` |
| `radius-ingestion` | 1 mặc định | UDP `1813` | Passive RADIUS mirror receiver |
| `notification-dispatcher` | 1 | không expose | Gửi outbox webhook ngoài critical path |
| `prometheus` | 1 | `127.0.0.1:9090` | Scrape API, ingestion và mọi pipeline replica |
| `grafana` | 1 | `127.0.0.1:3000` | Dashboard từ Prometheus |

## Kafka topics hiện hành

| Topic | Partition | Replication | Vai trò |
|---|---:|---:|---|
| `radius.accounting.raw` | 9/12/24/48 theo profile | 1 ở profile benchmark 8 GiB; 3 ở profile lớn | Một raw stream keyed theo MSISDN, được ba consumer group đọc độc lập |
| `radius.accounting.raw.dlq` | tạo theo cấu hình producer | theo broker/topic policy | Record không thể normalize/process, phục vụ điều tra và replay |
| `__consumer_offsets` | Kafka nội bộ | theo `KAFKA_REPLICATION_FACTOR` | Offset riêng của ba consumer group |

Không tồn tại chuỗi topic `radius.valid` → `radius.dedup` → `radius.clean` trong
implementation hiện tại. Validation, fencing và xử lý nghiệp vụ xảy ra trong ba
consumer module.

## Prometheus discovery

Prometheus dùng DNS service discovery cho ba pipeline service vì mỗi hardware
profile scale ra nhiều container. Ingestion và FastAPI dùng static target. File
hiện tại không scrape trực tiếp Kafka/PostgreSQL/Redis; số liệu của các hệ thống
này trong log pipeline là client-side latency, pool wait, lag và error counters.

Topology và đường dữ liệu đầy đủ: [Pipeline Architecture](../docs/PIPELINE_ARCHITECTURE.md).
