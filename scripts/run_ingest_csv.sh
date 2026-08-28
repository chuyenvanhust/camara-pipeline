#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
INPUT_FILE="${1:?Usage: scripts/run_ingest_csv.sh FILE.csv}"
if [ ! -f "$INPUT_FILE" ]; then echo "CSV file not found: $INPUT_FILE" >&2; exit 1; fi
ABS_INPUT="$(cd "$(dirname "$INPUT_FILE")" && pwd)/$(basename "$INPUT_FILE")"
docker compose run --rm --no-deps -v "$ABS_INPUT:/input.csv:ro" radius-ingestion \
  python -m pipeline.ingestion.producer --file /input.csv
