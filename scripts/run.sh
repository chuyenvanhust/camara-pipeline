#scripts\run.sh
#!/bin/bash

# Thiết lập đường dẫn cơ sở (Vị trí thư mục gốc)
BASE_DIR=$(dirname "$0")/..
cd "$BASE_DIR" || exit

# Hàm hỗ trợ chạy script với kiểm tra lỗi
run_script() {
    bash "$1" "${@:2}" || { echo "[Lỗi] Bước $1 thất bại!"; exit 1; }
}

usage() {
    echo "================================================"
    echo " RADIUS Pipeline Orchestrator (Optimized)"
    echo "================================================"
    echo "Commands: up, sim, pipeline, report, load-test, reset-db, all"
    echo "================================================"
}

case "$1" in
    up)
        echo ">>> Dọn dẹp triệt để các container cũ..."
        # Dừng và xóa tất cả container đang chạy có thể gây xung đột
        docker compose down -v 2>/dev/null
       
        docker ps -aq | xargs -r docker rm -f # Xóa mọi container còn sót lại
        
        echo ">>> Khởi động Stack..."
        docker compose up -d
        
        
       
        ;;
    sim)
        run_script scripts/run_simulator.sh
        ;;
    pipeline)
        run_script scripts/run_pipeline.sh data/radius_log.csv
        ;;
    report)
        run_script scripts/generate_report.sh
        ;;
    load-test)
        run_script scripts/run_load_test.sh
        ;;
    reset-db)
        run_script scripts/reset_db.sh
        ;;
    all)
        echo ">>> Chạy toàn bộ quy trình..."
        bash "$0" up && bash "$0" sim && bash "$0" pipeline && bash "$0" report && bash "$0" load-test
        ;;
    *)
        usage
        ;;
esac