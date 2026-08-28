#!/usr/bin/env bash
# ==============================================================================
# Automated PostgreSQL Backup Script for CAMARA Data Pipeline
# ==============================================================================
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/var/backups/camara_postgres}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
CONTAINER_NAME="${CONTAINER_NAME:-camara-postgres}"
DB_NAME="${POSTGRES_DB:-camara_db}"
DB_USER="${POSTGRES_USER:-postgres}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"

mkdir -p "${BACKUP_DIR}"

BACKUP_FILE="${BACKUP_DIR}/camara_db_${TIMESTAMP}.sql.gz"

echo "[INFO] Starting PostgreSQL backup for database '${DB_NAME}' from container '${CONTAINER_NAME}'..."

docker exec "${CONTAINER_NAME}" pg_dump -U "${DB_USER}" -d "${DB_NAME}" --clean --if-exists --create | gzip -9 > "${BACKUP_FILE}"

echo "[INFO] Backup completed successfully: ${BACKUP_FILE}"
echo "[INFO] Backup file size: $(du -h "${BACKUP_FILE}" | cut -f1)"

echo "[INFO] Cleaning up backups older than ${RETENTION_DAYS} days..."
find "${BACKUP_DIR}" -type f -name "camara_db_*.sql.gz" -mtime +"${RETENTION_DAYS}" -exec rm -f {} \;
echo "[INFO] Backup procedure finished."
