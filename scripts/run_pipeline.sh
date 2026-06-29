#!/usr/bin/env bash
# scripts/run_pipeline.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

set -a; [ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"; set +a

INPUT_FILE="${1:-data/radius_log.csv}"

echo "=================================================="
echo ">>> Đang khởi động RADIUS Pipeline (Exec Mode)..."
echo ">>> Input Data: $INPUT_FILE"
echo "=================================================="

# 1. Khởi động toàn bộ stack
docker compose up -d

# 2. Đợi FastAPI sẵn sàng
echo ">>> Đợi FastAPI sẵn sàng..."
until curl -sf http://localhost:8000/health > /dev/null 2>&1; do
    echo "    ... FastAPI chưa ready, thử lại sau 3s"
    sleep 3
done
echo "[OK] FastAPI sẵn sàng"

# 3. Đợi Kafka sẵn sàng
echo ">>> Đợi Kafka sẵn sàng..."
until docker exec camara-kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo "    ... Kafka chưa ready, thử lại sau 3s"
    sleep 3
done
echo "[OK] Kafka sẵn sàng"

# 4. Đợi PostgreSQL sẵn sàng
echo ">>> Đợi PostgreSQL sẵn sàng..."
until docker exec camara-postgres pg_isready -U postgres -d camara_db > /dev/null 2>&1; do
    echo "    ... PostgreSQL chưa ready, thử lại sau 3s"
    sleep 3
done
echo "[OK] PostgreSQL sẵn sàng"

# 5. Đợi Spark Cluster sẵn sàng
echo ">>> Đợi Spark Cluster sẵn sàng..."
until nc -z localhost 7077 > /dev/null 2>&1; do
    echo "    ... Đợi Spark Master (port 7077)..."
    sleep 3
done

# 6. Kiểm tra Worker đã đăng ký thành công chưa
echo ">>> Kiểm tra trạng thái Worker..."
# Kiểm tra log của Master xem có dòng báo nhận Worker mới không
until docker logs camara-spark-master 2>&1 | grep -q "Registering worker"; do
    echo "    ... Đợi Spark Worker đăng ký với Master (đang kiểm tra log)..."
    sleep 3
done
echo "[OK] Spark Cluster đã sẵn sàng với Worker đang hoạt động"

# 7. Khởi động mock services (S2 validation cần HTTP tới GSMA/HLR/ITU)
echo ">>> Khởi động mock services..."
docker compose -f mock_services/docker-compose.mock.yml up -d 2>/dev/null || true
sleep 5

# 8. Khởi động pipeline (truyền .env + unbuffered stdout)
echo ">>> Khởi động pipeline trong spark-master container..."
docker exec \
    --env-file "$ROOT_DIR/.env" \
    -e PYTHONUNBUFFERED=1 \
    -e HOME=/opt/spark/work-dir \
    -e SPARK_IVY_DIR=/tmp/ivy2 \
    -w /workspace \
    camara-spark-master \
    python3 -u -m pipeline.run_pipeline --input "$INPUT_FILE"

EXIT_CODE=$?
# 9. Kết thúc (Đã tách rời gen_report)
if [ $EXIT_CODE -eq 0 ]; then
    echo ">>> Pipeline hoàn thành thành công."
else
    echo "[!] Pipeline dừng bất thường (exit code: $EXIT_CODE)."
    exit $EXIT_CODE
fi