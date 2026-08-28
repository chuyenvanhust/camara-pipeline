#!/usr/bin/env bash
# ==============================================================================
# PostgreSQL Restore Script for CAMARA Data Pipeline Disaster Recovery
# ==============================================================================
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <path_to_backup_file.sql.gz>"
    exit 1
fi

BACKUP_FILE="$1"
CONTAINER_NAME="${CONTAINER_NAME:-camara-postgres}"
DB_NAME="${POSTGRES_DB:-camara_db}"
DB_USER="${POSTGRES_USER:-postgres}"

if [ ! -f "${BACKUP_FILE}" ]; then
    echo "[ERROR] Backup file '${BACKUP_FILE}' does not exist!"
    exit 1
fi

echo "[WARNING] Re-creating database '${DB_NAME}' from '${BACKUP_FILE}'."
echo "[WARNING] All active pipeline consumers, dispatcher, and API containers will be stopped during restore!"
read -p "Are you sure you want to proceed? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "[INFO] Restore operation cancelled."
    exit 0
fi

echo "[INFO] Step 1: Stopping active pipeline, dispatcher, and API services to prevent DB locks..."
docker compose stop pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap notification-dispatcher fastapi radius-ingestion 2>/dev/null || true

echo "[INFO] Step 2: Restoring PostgreSQL database from ${BACKUP_FILE}..."
gunzip -c "${BACKUP_FILE}" | docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d postgres

echo "[INFO] Step 3: Restarting application services..."
docker compose start pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap notification-dispatcher fastapi radius-ingestion 2>/dev/null || true

echo "[INFO] Database restore operation completed successfully!"
