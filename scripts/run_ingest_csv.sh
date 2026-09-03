#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
INPUT_FILE="${1:?Usage: scripts/run_ingest_csv.sh FILE.csv [profile.env]}"
PROFILE_ENV="${2:-${CAMARA_ENV_PROFILE:-}}"
COMPOSE_ENV_ARGS=(--env-file .env)
if [ -n "$PROFILE_ENV" ]; then
  if [ ! -f "$PROFILE_ENV" ]; then
    echo "[ERROR] Hardware profile not found: $PROFILE_ENV" >&2
    exit 2
  fi
  COMPOSE_ENV_ARGS+=(--env-file "$PROFILE_ENV")
fi
if [ ! -f "$INPUT_FILE" ]; then echo "CSV file not found: $INPUT_FILE" >&2; exit 1; fi
ABS_INPUT="$(cd "$(dirname "$INPUT_FILE")" && pwd)/$(basename "$INPUT_FILE")"
docker compose "${COMPOSE_ENV_ARGS[@]}" run --rm --no-deps -v "$ABS_INPUT:/input.csv:ro" radius-ingestion \
  python -m pipeline.ingestion.producer --file /input.csv
