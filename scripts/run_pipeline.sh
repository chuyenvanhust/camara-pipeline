#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo ">>> Building and starting the supervised stack..."
set -a; [ -f .env ] && . .env; set +a
docker compose up -d --build \
  --scale "pipeline=${PIPELINE_REPLICAS:-1}" \
  --scale "radius-ingestion=${RADIUS_INGESTION_REPLICAS:-1}"
echo ">>> Waiting for API and pipeline health checks..."
for _ in $(seq 1 60); do
  API_HEALTH="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' fastapi-app 2>/dev/null || true)"
  # F-PARALLEL: pipeline không còn container_name cố định (để scale được) ->
  # kiểm tra health của TẤT CẢ replica hiện có, không phải 1 tên container cứng.
  PIPELINE_IDS="$(docker compose ps -q pipeline)"
  PIPELINE_ALL_HEALTHY=true
  if [ -z "$PIPELINE_IDS" ]; then
    PIPELINE_ALL_HEALTHY=false
  else
    for cid in $PIPELINE_IDS; do
      st="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || true)"
      [ "$st" = healthy ] || PIPELINE_ALL_HEALTHY=false
    done
  fi
  if [ "$API_HEALTH" = healthy ] && [ "$PIPELINE_ALL_HEALTHY" = true ]; then
    docker compose ps
    echo "[OK] Stack is ready. UDP RADIUS listener: localhost:1813"
    exit 0
  fi
  sleep 2
done
docker compose ps
docker compose logs --tail 80 migrate fastapi pipeline radius-ingestion
echo "[ERROR] Stack did not become healthy within 120 seconds." >&2
exit 1