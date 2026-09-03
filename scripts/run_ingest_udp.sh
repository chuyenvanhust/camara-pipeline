#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
PROFILE_ENV="${1:-${CAMARA_ENV_PROFILE:-}}"
COMPOSE_ENV_ARGS=(--env-file .env)
if [ -n "$PROFILE_ENV" ]; then
  if [ ! -f "$PROFILE_ENV" ]; then
    echo "[ERROR] Hardware profile not found: $PROFILE_ENV" >&2
    exit 2
  fi
  COMPOSE_ENV_ARGS+=(--env-file "$PROFILE_ENV")
fi
docker compose "${COMPOSE_ENV_ARGS[@]}" up -d --build radius-ingestion
docker compose "${COMPOSE_ENV_ARGS[@]}" ps radius-ingestion
echo "RADIUS UDP listener is supervised by Docker. Profile: ${PROFILE_ENV:-.env (default)}"
echo "Logs: docker compose ${COMPOSE_ENV_ARGS[*]} logs -f radius-ingestion"
