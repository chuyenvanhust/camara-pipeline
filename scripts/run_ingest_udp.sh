#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
docker compose up -d --build radius-ingestion
docker compose ps radius-ingestion
echo "RADIUS UDP listener is supervised by Docker. Logs: docker compose logs -f radius-ingestion"
