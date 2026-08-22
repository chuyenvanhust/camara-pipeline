FROM python:3.11-slim

WORKDIR /workspace

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt /workspace/
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /workspace/

ENV PYTHONPATH=/workspace

CMD ["python", "-m", "pipeline.run_pipeline"]
