#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo ">>> Building and starting the supervised stack..."
set -a; [ -f .env ] && . .env; set +a

# Bootstrap theo tầng để Docker Compose không tạo broker từ dependency graph
# stale (biểu hiện: "kafka-1 is missing dependency zookeeper"). Điều này cũng
# làm lỗi health của hạ tầng xuất hiện ngay tại đúng tầng gây lỗi.
echo ">>> Starting state stores and ZooKeeper..."
docker compose up -d zookeeper postgres redis
docker compose up -d --wait zookeeper postgres redis

echo ">>> Starting Kafka brokers..."
docker compose up -d kafka-1 kafka-2 kafka-3

# Không dùng `compose up --wait` ở đây: Docker trả lỗi ngay khi một broker mang
# trạng thái unhealthy tạm thời trong lúc ZooKeeper thu hồi session cũ, dù
# restart policy có thể phục hồi broker vài giây sau. Poll toàn cụm trong 120s.
KAFKA_READY=false
for _ in $(seq 1 60); do
  KAFKA_ALL_HEALTHY=true
  for service in kafka-1 kafka-2 kafka-3; do
    cid="$(docker compose ps -q "$service")"
    if [ -z "$cid" ]; then
      KAFKA_ALL_HEALTHY=false
      continue
    fi
    state="$(docker inspect --format '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || true)"
    [ "$state" = "running healthy" ] || KAFKA_ALL_HEALTHY=false
  done
  if [ "$KAFKA_ALL_HEALTHY" = true ]; then
    KAFKA_READY=true
    break
  fi
  sleep 2
done

if [ "$KAFKA_READY" != true ]; then
  docker compose ps -a kafka-1 kafka-2 kafka-3
  docker compose logs --tail 100 kafka-1 kafka-2 kafka-3
  echo "[ERROR] Kafka cluster did not become healthy within 120 seconds." >&2
  exit 1
fi
echo "[OK] All Kafka brokers are healthy"

echo ">>> Applying database migrations..."
docker compose up --build migrate

echo ">>> Starting application services..."
docker compose up -d --build \
  --scale "pipeline-ip-msisdn=${PIPELINE_IP_REPLICAS:-1}" \
  --scale "pipeline-device-swap=${PIPELINE_DEVICE_REPLICAS:-1}" \
  --scale "pipeline-sim-swap=${PIPELINE_SIM_REPLICAS:-1}" \
  --scale "radius-ingestion=${RADIUS_INGESTION_REPLICAS:-1}"
echo ">>> Waiting for API and pipeline health checks..."
for _ in $(seq 1 60); do
  API_HEALTH="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' fastapi-app 2>/dev/null || true)"
  PIPELINE_SERVICES="pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap"
  PIPELINE_ALL_HEALTHY=true
  for svc in $PIPELINE_SERVICES; do
    sids="$(docker compose ps -q "$svc")"
    if [ -z "$sids" ]; then
      PIPELINE_ALL_HEALTHY=false
      break
    fi
    for cid in $sids; do
      st="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || true)"
      [ "$st" = healthy ] || PIPELINE_ALL_HEALTHY=false
    done
  done
  if [ "$API_HEALTH" = healthy ] && [ "$PIPELINE_ALL_HEALTHY" = true ]; then
    docker compose ps
    echo "[OK] Stack is ready. UDP RADIUS listener: localhost:1813"
    exit 0
  fi
  sleep 2
done
docker compose ps
docker compose logs --tail 80 migrate fastapi pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap radius-ingestion
echo "[ERROR] Stack did not become healthy within 120 seconds." >&2
exit 1
