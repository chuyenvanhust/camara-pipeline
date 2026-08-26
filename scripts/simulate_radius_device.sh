#!/usr/bin/env bash
# scripts/simulate_radius_device.sh
#
# CHẾ ĐỘ 2 (phần PHÁT) — đọc CSV, đóng gói thành gói tin RADIUS
# Accounting-Request nhị phân thật (pipeline/ingestion/radius_udp_sender.py),
# bắn UDP tới host:port để giả lập 1 thiết bị NAS/GGSN thật đang gửi
# accounting request qua mạng.
#
# Chạy SAU KHI scripts/run_ingest_udp.sh đã lên và đang lắng nghe ở phía kia,
# nếu không gói tin sẽ bị rơi (UDP không có handshake/retry).
#
# Chạy từ HOST (không cần vào container) vì UDP/1813 đã được publish ra host
# qua docker-compose.yml (`ports: - "1813:1813/udp"`).
#
# Usage:
#   scripts/simulate_radius_device.sh data/radius_log.csv
#   scripts/simulate_radius_device.sh data/radius_log.csv 50 --loop   # 50 pkt/s, lặp vô hạn

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

set -a; [ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"; set +a

INPUT_FILE="${1:?Thiếu đường dẫn CSV. Usage: scripts/simulate_radius_device.sh <file.csv> [rate] [--loop]}"
RATE="${2:-50}"
LOOP_FLAG="${3:-}"

HOST="${RADIUS_TARGET_HOST:-127.0.0.1}"
PORT="${RADIUS_TARGET_PORT:-1813}"

if [ ! -f "$INPUT_FILE" ]; then
    echo "[!] Không tìm thấy file: $INPUT_FILE"
    exit 1
fi

echo ">>> [Giả lập thiết bị RADIUS] Gửi '$INPUT_FILE' -> UDP $HOST:$PORT, rate=${RATE}pkt/s"

EXTRA_ARGS=()
if [ "$LOOP_FLAG" == "--loop" ]; then
    EXTRA_ARGS+=(--loop)
fi

python3 -m pipeline.ingestion.radius_udp_sender \
    --csv "$INPUT_FILE" \
    --host "$HOST" \
    --port "$PORT" \
    --rate "$RATE" \
    "${EXTRA_ARGS[@]}"