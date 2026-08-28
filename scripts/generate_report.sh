#!/bin/bash

# Nạp biến môi trường
set -a; [ -f .env ] && . .env; set +a

# Tạo thư mục báo cáo nếu chưa có
mkdir -p reports

# Tạo timestamp để đặt tên file theo chuẩn README
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REPORT_NAME="reports/quality_report_${TIMESTAMP}.html"
REPORT_NAME_IN_CONTAINER="/workspace/${REPORT_NAME}"

echo ">>> Đang sinh báo cáo chất lượng dữ liệu tại: $REPORT_NAME"

# Chạy trong container (tránh lỗi thiếu psycopg2/thư viện trên host)
# F-PARALLEL: `docker compose exec` định vị theo SERVICE, không cần tên container
# cố định — vẫn đúng dù pipeline đang scale >1 (tự chọn 1 replica bất kỳ).
docker compose exec -T pipeline-ip-msisdn \
    python3 /workspace/reporting/quality_report.py \
    --output "$REPORT_NAME_IN_CONTAINER"

# Copy từ container ra host nếu script không tự ghi vào mount
if ! [ -f "$REPORT_NAME" ]; then
    CONTAINER_ID="$(docker compose ps -q pipeline-ip-msisdn | head -1)"
    docker cp "${CONTAINER_ID}:${REPORT_NAME_IN_CONTAINER}" "$REPORT_NAME" 2>/dev/null
fi

echo ">>> Báo cáo đã hoàn tất tại: $REPORT_NAME"

    # Tự động phát hiện trình duyệt mở file
if command -v start >/dev/null 2>&1; then
        # Dành cho Windows (Git Bash/CMD)
    start "$REPORT_NAME"
elif command -v open >/dev/null 2>&1; then
        # Dành cho macOS
    open "$REPORT_NAME"
elif command -v xdg-open >/dev/null 2>&1; then
        # Dành cho Linux
    xdg-open "$REPORT_NAME"
else
    echo ">>> Không tìm thấy lệnh mở trình duyệt tự động. Vui lòng mở thủ công file: $REPORT_NAME"
fi
