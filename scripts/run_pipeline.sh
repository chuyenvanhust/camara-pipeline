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

echo ">>> Khởi tạo cấu trúc Database (Migrations)..."
# Lấy danh sách các file .sql, sắp xếp theo thứ tự (001, 002...) để chạy đúng quy trình
# Lệnh 'ls' kết hợp 'sort' đảm bảo 001_init luôn chạy trước 003_partitions
for sql_file in $(ls storage/migrations/*.sql | sort); do
    filename=$(basename "$sql_file")
    echo "    ... Đang thực thi: $filename"
    
    # Thực thi nội dung file SQL vào container
    # -i: truyền nội dung từ file local vào stdin của psql trong container
    if docker exec -i camara-postgres psql -U postgres -d camara_db < "$sql_file" > /dev/null 2>&1; then
        echo "        [OK] $filename hoàn tất"
    else
        echo "        [!] Lỗi khi thực thi $filename (có thể bảng đã tồn tại, bỏ qua...)"
    fi
done
echo "[OK] Toàn bộ cấu trúc Database đã sẵn sàng"

# 4b. Đợi Redis sẵn sàng (Global State Store cho Conflict C/D)
echo ">>> Đợi Redis sẵn sàng..."
until docker exec camara-redis redis-cli ping 2>/dev/null | grep -q PONG; do
    echo "    ... Redis chưa ready, thử lại sau 3s"
    sleep 3
done
echo "[OK] Redis sẵn sàng"


# 5. Khởi động pipeline
echo ">>> Khởi động pipeline..."
docker exec \
    --env-file "$ROOT_DIR/.env" \
    -e PYTHONUNBUFFERED=1 \
    -w /workspace \
    camara-pipeline \
    python3 -u -m pipeline.run_pipeline --input "$INPUT_FILE"


EXIT_CODE=$?
# 9. Kết thúc (Đã tách rời gen_report)
if [ $EXIT_CODE -eq 0 ]; then
    echo ">>> Pipeline hoàn thành thành công."
else
    echo "[!] Pipeline dừng bất thường (exit code: $EXIT_CODE)."
    exit $EXIT_CODE
fi