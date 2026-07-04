FROM python:3.11-slim

WORKDIR /workspace

# Cài đặt công cụ hệ thống
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Copy file requirements
COPY requirements.txt .

# Cài đặt trực tiếp vào đường dẫn của python hiện tại
RUN /usr/local/bin/python -m pip install --upgrade pip && \
    /usr/local/bin/python -m pip install --no-cache-dir -r requirements.txt

# Kiểm tra xem uvicorn đã thực sự cài chưa ngay tại bước build
RUN /usr/local/bin/python -m pip list | grep -i uvicorn

COPY . .

ENV PYTHONPATH=/workspace

EXPOSE 8000

# Chạy bằng đường dẫn tuyệt đối
CMD ["/usr/local/bin/python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]