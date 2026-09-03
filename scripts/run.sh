#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

case "${1:-}" in
  up)
    # Dùng chung một quy trình bootstrap/health-check, tránh hai script lệch nhau.
    exec bash scripts/run_pipeline.sh "${2:-${CAMARA_ENV_PROFILE:-}}"
    ;;
  down) docker compose down ;;
  status) docker compose ps ;;
  logs) docker compose logs -f "${2:-pipeline-ip-msisdn}" ;;
  ingest-csv) bash scripts/run_ingest_csv.sh "${2:?CSV path is required}" "${3:-${CAMARA_ENV_PROFILE:-}}" ;;
  simulate-radius) bash scripts/simulate_radius_device.sh "${2:?CSV path is required}" "${3:-50}" "${4:-}" ;;
  reset-db) bash scripts/reset_db.sh ;;
  *) echo "Usage: scripts/run.sh {up [profile.env]|down|status|logs [service]|ingest-csv FILE [profile.env]|simulate-radius FILE [rate] [--loop]|reset-db}"; exit 2 ;;
esac
