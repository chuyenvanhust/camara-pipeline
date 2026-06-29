#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Load biến môi trường từ .env
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
fi

# Giá trị mặc định nếu chưa khai báo trong .env
BASE_URL="${BASE_URL:-http://localhost:8000}"
API_KEY="${API_KEY:-dev-secret}"

# Kiểm tra k6
if ! command -v k6 >/dev/null 2>&1; then
    echo "[ERROR] k6 chưa được cài đặt."
    exit 1
fi

mkdir -p reports

run_test() {
    local script="$1"
    local report="$2"
    local summary="${report%.json}_load_test.json"

    echo
    echo "========================================"
    echo "Running : $script"
    echo "Report  : $report"
    echo "Summary : $summary"
    echo "========================================"

    k6 run \
        "$script" \
        --env BASE_URL="$BASE_URL" \
        --env API_KEY="$API_KEY" \
        --summary-export="$summary" \
        --out json="$report"
}

run_test "load_tests/sim_swap.js" "reports/sim_swap_load.json"
run_test "load_tests/device_swap.js" "reports/device_swap_load.json"
run_test "load_tests/number_verification.js" "reports/number_verification_load.json"
run_test "load_tests/all_endpoints.js" "reports/all_endpoints_load.json"

echo
echo "========================================"
echo "All load tests completed."
echo "Reports are available in ./reports"
echo "========================================"