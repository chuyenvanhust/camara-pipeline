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
docker exec camara-spark-master \
    python3 /workspace/reporting/quality_report.py \
    --output "$REPORT_NAME_IN_CONTAINER"

# Copy từ container ra host nếu script không tự ghi vào mount
if ! [ -f "$REPORT_NAME" ]; then
    docker cp "camara-spark-master:${REPORT_NAME_IN_CONTAINER}" "$REPORT_NAME" 2>/dev/null
fi

# Kiểm tra xem báo cáo đã được sinh ra chưa
if [ -f "$REPORT_NAME" ]; then
    echo ">>> Báo cáo đã hoàn tất."

    # Mở trình duyệt
    if [[ "$OSTYPE" == "msys" ]]; then
        start "$REPORT_NAME"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        open "$REPORT_NAME"
    else
        xdg-open "$REPORT_NAME"
    fi
else
    echo ">>> Lỗi: Không thể tạo file báo cáo."
    exit 1
fi