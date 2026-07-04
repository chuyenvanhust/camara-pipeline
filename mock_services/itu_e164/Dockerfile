#mock_services\itu_e164\Dockerfile
FROM python:3.12-slim
WORKDIR /workspace
ENV PYTHONPATH=/workspace

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mock_services/shared/ /workspace/mock_services/shared/
COPY mock_services/itu_e164/ /workspace/mock_services/itu_e164/

CMD ["python", "-m", "uvicorn", "mock_services.itu_e164.app:app", "--host", "0.0.0.0", "--port", "8300"]