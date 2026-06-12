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

COPY mock_services/itu_e164/requirements.txt .
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

# Copy module tiện ích dùng chung (Shared) và mã nguồn itu_e164
COPY mock_services/shared/ /workspace/mock_services/shared/
COPY mock_services/itu_e164/ /workspace/mock_services/itu_e164/

EXPOSE 8300

CMD ["uvicorn", "mock_services.itu_e164.app:app", "--host", "0.0.0.0", "--port", "8300"]