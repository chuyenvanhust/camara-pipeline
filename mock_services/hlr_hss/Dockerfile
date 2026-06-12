FROM python:3.12-alpine
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY mock_services/hlr_hss/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.12-alpine AS runtime
WORKDIR /workspace
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workspace

COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

COPY mock_services/shared/ /workspace/mock_services/shared/
COPY mock_services/hlr_hss/ /workspace/mock_services/hlr_hss/

EXPOSE 8200
CMD ["uvicorn", "mock_services.hlr_hss.app:app", "--host", "0.0.0.0", "--port", "8200"]