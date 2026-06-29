#mock_services\gsma_tac\Dockerfile
FROM python:3.12-slim
WORKDIR /workspace
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workspace

# Cài đặt công cụ build cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy file requirements 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY mock_services/shared/ /workspace/mock_services/shared/
COPY mock_services/gsma_tac/ /workspace/mock_services/gsma_tac/

CMD ["python", "-m", "uvicorn", "mock_services.gsma_tac.app:app", "--host", "0.0.0.0", "--port", "8100"]