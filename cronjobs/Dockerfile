FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy CronJob script
COPY time_series_indexer.py .

# Environment
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "time_series_indexer.py"]
