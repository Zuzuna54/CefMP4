# Phase 1: Basic Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install ffmpeg for ffprobe and dumb-init
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg dumb-init && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml ./
# In later phases, we'll copy requirements.txt after generating it
COPY src /app/src

# Create a non-root user and switch to it (good practice)
RUN useradd --create-home appuser
USER appuser

# Set up entrypoint and default command
ENTRYPOINT ["dumb-init", "--"]
CMD ["python", "-m", "src.main"] # Assuming a main.py will exist 