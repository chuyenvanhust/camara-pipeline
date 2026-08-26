#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo ">>> Building and starting the supervised stack..."
docker compose up -d --build
echo ">>> Waiting for API and pipeline health checks..."
for _ in $(seq 1 60); do
  API_HEALTH="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' fastapi-app 2>/dev/null || true)"
  PIPELINE_HEALTH="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' camara-pipeline 2>/dev/null || true)"
  if [ "$API_HEALTH" = healthy ] && [ "$PIPELINE_HEALTH" = healthy ]; then
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
