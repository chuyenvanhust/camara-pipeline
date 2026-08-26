#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

case "${1:-}" in
  up)
    # F-PARALLEL: đọc PIPELINE_REPLICAS/RADIUS_INGESTION_REPLICAS từ .env để scale
    # thật số container — Docker Compose không tự đọc biến này, phải qua --scale.
    docker compose up -d --build \
      --scale "pipeline=${PIPELINE_REPLICAS:-1}" \
      --scale "radius-ingestion=${RADIUS_INGESTION_REPLICAS:-1}"
    ;;
  down) docker compose down ;;
  status) docker compose ps ;;
  logs) docker compose logs -f "${2:-pipeline}" ;;
  ingest-csv) bash scripts/run_ingest_csv.sh "${2:?CSV path is required}" ;;
  simulate-radius) bash scripts/simulate_radius_device.sh "${2:?CSV path is required}" "${3:-50}" "${4:-}" ;;
  reset-db) bash scripts/reset_db.sh ;;
  *) echo "Usage: scripts/run.sh {up|down|status|logs [service]|ingest-csv FILE|simulate-radius FILE [rate] [--loop]|reset-db}"; exit 2 ;;
esac