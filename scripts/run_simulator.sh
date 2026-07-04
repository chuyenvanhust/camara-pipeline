#!/bin/bash
#scripts\run_simulator.sh
echo ">>> Đang chạy Simulator..."

# Đảm bảo thư mục data tồn tại
mkdir -p "$(pwd)/data"


docker run --rm \
  -v "$(pwd):/workspace" \
  -w /workspace \
  -e PYTHONPATH=/workspace \
  python:3.11-slim \
  bash -c "pip install --quiet requests pydantic altair pandas asyncpg && python3 -m simulator.simulator --records 2000000 --subscribers 100000 --seed 42 --output data/radius_log.csv"

# Kiểm tra file
if [ -f "data/radius_log.csv" ]; then
    echo ">>> Thành công! File đã được tạo tại: $(pwd)/data/radius_log.csv"
else
    echo "[Lỗi] Simulator không tạo được file CSV."
    exit 1
fi