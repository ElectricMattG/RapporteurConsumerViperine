# Multi-stage build for Kafka consumers
FROM python:3.12-slim as builder

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Final stage
FROM python:3.12-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies from builder
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY src/ ./src/
COPY setup.py .
COPY pyproject.toml .
COPY version .

# Make sure scripts are in PATH
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/app

# Create cache directories
RUN mkdir -p /app/cache/web_scrapes /app/cache/session_backups /app/cache/archive /app/cache/indices

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import sys; sys.exit(0)"

# Default consumer to run (override with CMD in deployment)
CMD ["python", "-u", "src/consumers/run_kafka_web_scraper.py"]
