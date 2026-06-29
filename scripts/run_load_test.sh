#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Load biến môi trường
if [ -f ".env" ]; then
    set -a; source .env; set +a
fi

BASE_URL=${BASE_URL:-"http://host.docker.internal:8000"}
API_KEY="${API_KEY:-dev-secret}"

mkdir -p reports

# Hàm chạy k6 bằng Docker
run_test() {
    local script="$1"
    local report="$2"
    
    echo ">>> Đang chạy: $script"

    # Dùng lệnh này để chạy k6, nó sẽ tự kết thúc khi xong stages
    docker run --rm \
        -v "$PWD:/workspace" \
        -w /workspace \
        grafana/k6 run \
        --no-usage-report \
        "$script" \
        --env BASE_URL="$BASE_URL" \
        --env API_KEY="$API_KEY" \
        --out json="/workspace/$report"
}

# Thực thi các test
run_test "load_tests/sim_swap.js" "reports/sim_swap_load.json"
echo "xong 1"
run_test "load_tests/device_swap.js" "reports/device_swap_load.json"
echo "xong 2"
run_test "load_tests/number_verification.js" "reports/number_verification_load.json"
echo "xong 3"
run_test "load_tests/all_endpoints.js" "reports/all_endpoints_load.json"
echo "xong 4"
echo ">>> All load tests completed. Reports in ./reports"