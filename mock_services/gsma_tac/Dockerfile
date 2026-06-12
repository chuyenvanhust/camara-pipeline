# ==========================================
# STAGE 1: Build dependencies sạch
# ==========================================
FROM python:3.12-alpine

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Giả định file requirements nằm ở shared hoặc gốc module, copy vào để install
COPY mock_services/gsma_tac/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# ==========================================
# STAGE 2: Runtime image siêu nhẹ
# ==========================================
FROM python:3.12-alpine AS runtime

WORKDIR /workspace

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workspace

# Khôi phục các gói thư viện từ Stage 1
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Copy module tiện ích dùng chung (Shared) và mã nguồn gsma_tac
COPY mock_services/shared/ /workspace/mock_services/shared/
COPY mock_services/gsma_tac/ /workspace/mock_services/gsma_tac/

# Stage S2 Validation cần dữ liệu 2000 dòng tĩnh có sẵn khi boot up
# Nếu chưa có sẵn, script startup trong app.py sẽ log ra cảnh báo và chờ seed
EXPOSE 8100

CMD ["uvicorn", "mock_services.gsma_tac.app:app", "--host", "0.0.0.0", "--port", "8100"]