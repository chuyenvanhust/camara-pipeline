FROM python:3.11-slim

WORKDIR /workspace

# Security: Create non-root user
RUN groupadd -g 10001 appuser && \
    useradd -u 10001 -g appuser -s /bin/sh -m appuser

# Cài đặt công cụ hệ thống
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Copy file requirements
COPY requirements.txt .

# Cài đặt trực tiếp vào đường dẫn của python hiện tại
RUN /usr/local/bin/python -m pip install --upgrade pip && \
    /usr/local/bin/python -m pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chown -R appuser:appuser /workspace

ENV PYTHONPATH=/workspace
ENV FORWARDED_ALLOW_IPS="127.0.0.1"

EXPOSE 8000

USER appuser

# Security: Avoid wildcard --forwarded-allow-ips "*" by default
CMD ["/usr/local/bin/python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--proxy-headers", "--forwarded-allow-ips", "127.0.0.1"]
