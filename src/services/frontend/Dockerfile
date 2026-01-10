# Dockerfile for Frontend Service
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY src/services/frontend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy frontend files
COPY src/services/frontend/ ./src/services/frontend/

# Set Python path
ENV PYTHONPATH=/app

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD curl -f http://localhost:5000/api/health || exit 1

# Run the service (SSE works with Flask using data event heartbeats)
CMD ["python", "-u", "src/services/frontend/app.py"]
