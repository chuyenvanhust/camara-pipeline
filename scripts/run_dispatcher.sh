#!/usr/bin/env bash
# scripts/run_dispatcher.sh
#
# Quản lý vòng đời của notification-dispatcher (outbox pattern, F-03) —
# process HOÀN TOÀN ĐỘC LẬP với pipeline chính (xem pipeline/README.md).
#
# Container `camara-notification-dispatcher` đã được định nghĩa sẵn trong
# docker-compose.yml và tự khởi động khi `docker compose up -d`. Script này
# tồn tại vì:
#   1. Container KHÔNG có `restart:` policy — nếu dispatcher crash, nó nằm
#      chết cho tới khi ai đó chủ động khởi động lại.
#   2. Không có cách nhanh để chỉ xem log / restart riêng dispatcher mà
#      không đụng tới toàn bộ stack.
#   3. Cần cách chạy dispatcher ngoài Docker (local Python) để debug nhanh.
#
# Usage:
#   scripts/run_dispatcher.sh start     # đảm bảo Postgres sẵn sàng, (re)start container
#   scripts/run_dispatcher.sh stop      # dừng container
#   scripts/run_dispatcher.sh restart   # restart container (dùng khi dispatcher bị treo)
#   scripts/run_dispatcher.sh status    # kiểm tra container đang chạy hay đã chết
#   scripts/run_dispatcher.sh logs      # follow log
#   scripts/run_dispatcher.sh local     # chạy trực tiếp bằng python (không qua Docker) — debug

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

set -a; [ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"; set +a

CONTAINER="camara-notification-dispatcher"
SERVICE="notification-dispatcher"

wait_for_postgres() {
    echo ">>> Đợi PostgreSQL sẵn sàng..."
    until docker exec camara-postgres pg_isready -U postgres -d camara_db > /dev/null 2>&1; do
        echo "    ... PostgreSQL chưa ready, thử lại sau 3s"
        sleep 3
    done
    echo "[OK] PostgreSQL sẵn sàng"
}

cmd_start() {
    wait_for_postgres
    echo ">>> Khởi động notification-dispatcher (độc lập với pipeline)..."
    docker compose up -d "$SERVICE"
    echo "[OK] $CONTAINER đang chạy. Xem log: scripts/run_dispatcher.sh logs"
}

cmd_stop() {
    echo ">>> Dừng notification-dispatcher..."
    docker compose stop "$SERVICE"
}

cmd_restart() {
    wait_for_postgres
    echo ">>> Restart notification-dispatcher..."
    docker compose restart "$SERVICE"
    echo "[OK] Đã restart $CONTAINER"
}

cmd_status() {
    if docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
        echo "[UP] $CONTAINER đang chạy."
        docker inspect -f '   Started at: {{.State.StartedAt}}' "$CONTAINER"
    else
        echo "[DOWN] $CONTAINER KHÔNG chạy — notification đang bị kẹt ở trạng thái PENDING trong DB."
        echo "       Chạy: scripts/run_dispatcher.sh start"
        exit 1
    fi
}

cmd_logs() {
    docker logs -f --tail 100 "$CONTAINER"
}

cmd_local() {
    # Chạy trực tiếp ngoài Docker — cần .env trỏ DB_HOST=localhost (hoặc host
    # có thể resolve camara-postgres) và Python deps đã cài (requirements.txt).
    echo ">>> Chạy dispatcher trực tiếp bằng Python (KHÔNG qua Docker)..."
    echo ">>> Nhấn Ctrl+C để dừng."
    python3 -m pipeline.dispatcher.notification_dispatcher
}

case "${1:-}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_restart ;;
    status)  cmd_status ;;
    logs)    cmd_logs ;;
    local)   cmd_local ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|local}"
        exit 1
        ;;
esac