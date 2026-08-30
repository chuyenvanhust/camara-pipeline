#!/usr/bin/env bash
# scripts/reset.sh
# Reset toàn bộ state của pipeline: Kafka topic (xóa + tạo lại đúng số partitions),
# Postgres (truncate các bảng dữ liệu), Redis (flush toàn bộ).
# Chạy trước khi bash scripts/run_pipeline.sh nếu nghi ngờ dữ liệu bị double-ingest,
# đứng bậy do process cũ chưa được dọn sạch, hoặc muốn có baseline sạch để đo lại benchmark.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
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
dc() { docker compose "${COMPOSE_ENV_ARGS[@]}" "$@"; }

# Profile được source sau .env để các giá trị partition/resource override có
# cùng thứ tự ưu tiên với scripts/run_pipeline.sh và Docker Compose.
set -a; [ -f .env ] && source .env; set +a
set -a; [ -n "$PROFILE_ENV" ] && source "$PROFILE_ENV"; set +a

# F-PARALLEL: đồng bộ đúng tên biến với docker-compose.yml/.env.example
KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW:-radius.accounting.raw}"
KAFKA_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-12}"
KAFKA_REPL="${KAFKA_REPLICATION_FACTOR:-3}"
KAFKA_CONSUMER_GROUPS="${KAFKA_CONSUMER_GROUPS:-cg-ip-msisdn cg-device-swap cg-sim-swap}"
POSTGRES_USER="${POSTGRES_LOCAL_USER:-postgres}"
POSTGRES_DB="${POSTGRES_LOCAL_DB:-camara_db}"
REDIS_PASSWORD="${REDIS_PASSWORD:-camara_redis_dev_secret}"

echo "=================================================="
echo ">>> CẢNH BÁO: Sắp reset toàn bộ state pipeline"
echo "    - Profile     : ${PROFILE_ENV:-.env (default)}"
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

RESET_TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/camara-reset.XXXXXX")"
trap 'rm -rf -- "$RESET_TMP_DIR"' EXIT

# 1. Dừng toàn bộ producer/consumer trước khi xóa topic. Nếu để ingestion hoặc
# pipeline chạy, Kafka auto-create/client ensure-topic có thể tạo lại topic ngay
# giữa lúc script đang chờ xóa và gây vòng lặp "Topic already exists".
echo ">>> Dừng producer/consumer để khóa luồng ghi Kafka..."
dc stop pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap radius-ingestion notification-dispatcher >/dev/null 2>&1 || true
echo "[OK] Producer/consumer đã dừng"

if ! docker exec camara-kafka kafka-broker-api-versions \
    --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "[ERROR] Kafka broker chưa sẵn sàng. Chạy bash scripts/run_pipeline.sh trước." >&2
    exit 1
fi

# Offset cũ có thể lớn hơn log mới sau khi tạo lại topic, khiến consumer bỏ qua
# dữ liệu hoặc phát sinh OffsetOutOfRange. Xóa group khi mọi consumer đã dừng.
echo ">>> Xóa consumer-group offsets cũ..."
for group in $KAFKA_CONSUMER_GROUPS; do
    if docker exec camara-kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 --delete --group "$group" \
        >"$RESET_TMP_DIR/kafka-group.log" 2>&1; then
        echo "    [OK] $group"
    elif grep -Eqi "does not exist|Group.*not found" "$RESET_TMP_DIR/kafka-group.log"; then
        echo "    [SKIP] $group chưa tồn tại"
    else
        echo "[ERROR] Không xóa được consumer group $group:" >&2
        cat "$RESET_TMP_DIR/kafka-group.log" >&2
        exit 1
    fi
done

# 2. Xóa và tạo lại Kafka topic đúng số partitions, tránh phụ thuộc auto-create.topics.enable
echo ">>> Reset Kafka topic: $KAFKA_TOPIC_RAW..."
if docker exec camara-kafka kafka-topics --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep -Fqx "$KAFKA_TOPIC_RAW"; then
    if ! docker exec camara-kafka kafka-topics --bootstrap-server localhost:9092 \
        --delete --topic "$KAFKA_TOPIC_RAW" >"$RESET_TMP_DIR/kafka-delete.log" 2>&1; then
        echo "[ERROR] Không gửi được lệnh xóa topic:" >&2
        cat "$RESET_TMP_DIR/kafka-delete.log" >&2
        exit 1
    fi
    echo "[OK] Đã gửi lệnh xóa topic"
else
    echo "    ... topic chưa tồn tại, bỏ qua bước xóa"
fi

# Chờ metadata không còn topic rồi mới tạo đúng một lần. Không spam --create:
# lỗi "already exists" khi delete bất đồng bộ là trạng thái chờ, không phải lỗi tạo.
echo ">>> Đợi Kafka xác nhận topic đã bị xóa..."
DELETED=0
for i in $(seq 1 60); do
    if ! docker exec camara-kafka kafka-topics --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep -Fqx "$KAFKA_TOPIC_RAW"; then
        DELETED=1
        break
    fi
    if (( i % 5 == 0 )); then
        echo "    ... vẫn đang xóa (${i}/60)"
    fi
    sleep 2
done

if [ "$DELETED" -ne 1 ]; then
    echo "[ERROR] Topic chưa bị xóa sau 120s. Kiểm tra: docker compose logs --tail 100 kafka-1 kafka-2 kafka-3" >&2
    exit 1
fi

echo ">>> Tạo topic mới: partitions=$KAFKA_PARTITIONS, replication=$KAFKA_REPL..."
if ! docker exec camara-kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic "$KAFKA_TOPIC_RAW" --partitions "$KAFKA_PARTITIONS" \
    --replication-factor "$KAFKA_REPL" >"$RESET_TMP_DIR/kafka-create.log" 2>&1; then
    echo "[ERROR] Không tạo được topic:" >&2
    cat "$RESET_TMP_DIR/kafka-create.log" >&2
    exit 1
fi
echo "[OK] Kafka topic đã sẵn sàng với $KAFKA_PARTITIONS partitions"

# 3. Truncate Postgres
echo ">>> Reset Postgres..."
docker exec camara-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "TRUNCATE sim_swap_history, device_swap_history, msisdn_sim, msisdn_device, audit_log RESTART IDENTITY CASCADE;"
echo "[OK] Postgres đã được truncate"

# 4. Flush Redis
echo ">>> Reset Redis..."
REDIS_FLUSH_RESULT="$(
    docker exec -e REDISCLI_AUTH="$REDIS_PASSWORD" camara-redis \
        redis-cli --no-auth-warning --raw FLUSHALL
)"
if [ "$REDIS_FLUSH_RESULT" != "OK" ]; then
    echo "[ERROR] Redis FLUSHALL thất bại: $REDIS_FLUSH_RESULT" >&2
    exit 1
fi
REDIS_DBSIZE="$(
    docker exec -e REDISCLI_AUTH="$REDIS_PASSWORD" camara-redis \
        redis-cli --no-auth-warning --raw DBSIZE
)"
if [ "$REDIS_DBSIZE" != "0" ]; then
    echo "[ERROR] Redis vẫn còn $REDIS_DBSIZE key sau FLUSHALL" >&2
    exit 1
fi
echo "[OK] Redis đã được flush và xác nhận dbsize=0"

echo "=================================================="
echo ">>> Reset hoàn tất. Producer/consumer đang dừng."
if [ -n "$PROFILE_ENV" ]; then
    echo ">>> Khởi động lại bằng: bash scripts/run_pipeline.sh $PROFILE_ENV"
else
    echo ">>> Khởi động lại bằng: bash scripts/run_pipeline.sh"
fi
echo "=================================================="
