# Phase 1: Basic Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install ffmpeg for ffprobe and dumb-init
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg dumb-init && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml ./
COPY requirements.txt ./
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src /app/src

# Create a non-root user and switch to it (good practice)
RUN useradd --create-home appuser
USER appuser

# Expose Prometheus metrics port
ARG PROM_PORT=8000
ENV PROM_PORT=$PROM_PORT
EXPOSE ${PROM_PORT}

# Set the entrypoint and command
# Ensure the user has permissions to write to any necessary directories if not running as root
# USER appuser # Example if a non-root user is setup

ENTRYPOINT ["dumb-init", "--", "python", "-m", "src.main"]
# CMD is not strictly needed if ENTRYPOINT is a list like above and main.py handles all logic 