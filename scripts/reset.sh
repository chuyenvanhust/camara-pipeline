#!/usr/bin/env bash
# scripts/reset.sh
# Reset toàn bộ state của pipeline: Kafka topic (xóa + tạo lại đúng 4 partitions),
# Postgres (truncate các bảng dữ liệu), Redis (flush toàn bộ).
# Chạy trước khi bash scripts/run_pipeline.sh nếu nghi ngờ dữ liệu bị double-ingest,
# đứng bậy do process cũ chưa được dọn sạch, hoặc muốn có baseline sạch để đo lại benchmark.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

set -a; [ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"; set +a

KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW:-radius.accounting.raw}"
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-4}"

echo "=================================================="
echo ">>> CẢNH BÁO: Sắp reset toàn bộ state pipeline"
echo "    - Kafka topic : $KAFKA_TOPIC_RAW (xóa + tạo lại $KAFKA_PARTITIONS partitions)"
echo "    - Postgres    : truncate sim_swap_history, device_swap_history,"
echo "                    msisdn_sim, msisdn_device, audit_log"
echo "    - Redis       : FLUSHALL"
echo "=================================================="
read -p "Bạn có chắc chắn muốn tiếp tục? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo ">>> Đã hủy."
    exit 0
fi

# 1. Kill process pipeline cũ (nếu container còn sống và còn instance đang chạy dở)
# Image không có pkill/ps (procps chưa cài) nên duyệt trực tiếp /proc bằng shell builtin
echo ">>> Dừng process pipeline cũ (nếu có)..."
docker exec camara-pipeline sh -c '
    for p in /proc/[0-9]*; do
        if grep -q "pipeline.run_pipeline" "$p/cmdline" 2>/dev/null; then
            kill -9 "${p#/proc/}" 2>/dev/null && echo "    ... đã kill PID ${p#/proc/}"
        fi
    done
' || true
sleep 1
echo "[OK] Đã dừng process cũ"

# 2. Xóa và tạo lại Kafka topic đúng số partitions, tránh phụ thuộc auto-create.topics.enable
echo ">>> Reset Kafka topic: $KAFKA_TOPIC_RAW..."
if docker exec camara-kafka kafka-topics --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep -qx "$KAFKA_TOPIC_RAW"; then
    docker exec camara-kafka kafka-topics --bootstrap-server localhost:9092 \
        --delete --topic "$KAFKA_TOPIC_RAW"
    echo "[OK] Đã gửi lệnh xóa topic"
else
    echo "    ... topic chưa tồn tại, bỏ qua bước xóa"
fi

# Việc xóa vật lý segment file trên Kafka bị trễ theo log.segment.delete.delay.ms
# (mặc định 60s) nên topic có thể vẫn còn tồn tại trong --list rất lâu sau lệnh --delete.
# Thay vì đợi rồi tạo 1 lần, retry --create liên tục và coi lỗi "already exists"
# là dấu hiệu "còn đang xóa dở" để thử lại, tối đa 90s.
echo ">>> Đợi topic bị xóa hẳn và tạo lại (có thể mất tới ~60-90s)..."
CREATED=0
for i in $(seq 1 45); do
    if docker exec camara-kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic "$KAFKA_TOPIC_RAW" --partitions "$KAFKA_PARTITIONS" \
        --replication-factor 1 2>/tmp/reset_kafka_create.log; then
        CREATED=1
        break
    fi
    if ! grep -q "already exists" /tmp/reset_kafka_create.log; then
        echo "    ... lỗi không mong đợi khi tạo topic:"
        cat /tmp/reset_kafka_create.log
        exit 1
    fi
    echo "    ... topic cũ chưa xóa xong, thử lại (${i}/45)"
    sleep 2
done

if [ "$CREATED" -ne 1 ]; then
    echo "    [!] Không tạo được topic sau 90s. Kiểm tra: docker compose logs kafka --tail 50"
    exit 1
fi
echo "[OK] Kafka topic đã sẵn sàng với $KAFKA_PARTITIONS partitions"

# 3. Truncate Postgres
echo ">>> Reset Postgres..."
docker exec camara-postgres psql -U postgres -d camara_db -c \
    "TRUNCATE sim_swap_history, device_swap_history, msisdn_sim, msisdn_device, audit_log RESTART IDENTITY CASCADE;"
echo "[OK] Postgres đã được truncate"

# 4. Flush Redis
echo ">>> Reset Redis..."
docker exec camara-redis redis-cli FLUSHALL > /dev/null
echo "[OK] Redis đã được flush"

echo "=================================================="
echo ">>> Reset hoàn tất. Chạy: bash scripts/run_pipeline.sh"
echo "=================================================="