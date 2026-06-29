FROM python:3.11-slim

WORKDIR /workspace

# Cài đặt build tools + curl để healthcheck hoạt động
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements từ thư mục gốc
COPY requirements.txt .

# Cài đặt các thư viện trong requirements.txt (KHÔNG bao gồm pyspark)
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào /workspace
COPY . .

# Khai báo PYTHONPATH để Python nhận ra package pipeline
ENV PYTHONPATH=/workspace

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
