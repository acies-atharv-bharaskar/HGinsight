# S3 to PostgreSQL Pipeline Dockerfile
FROM python:3.9-slim

# Set work directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (for better caching)
COPY setup.py /app/
# Create an empty README.md if it doesn't exist
RUN touch /app/README.md

# Install dependencies
RUN pip install --no-cache-dir .

# Create a non-root user
RUN adduser --disabled-password --gecos "" appuser

# Copy source code
COPY src/ /app/src/
COPY scripts/ /app/scripts/

# Set ownership for application files
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Create directory for logs
RUN mkdir -p /app/logs

# Set default config folder
ENV CONFIG_DIR=/app/config

# Default command
ENTRYPOINT ["python", "scripts/run_pipeline.py"]

# Default arguments (can be overridden)
CMD ["--log-file", "/app/logs/pipeline.log"]