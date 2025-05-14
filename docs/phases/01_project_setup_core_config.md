# Phase 1: Project Setup & Core Configuration

**Objective:** Establish the foundational structure of the project, including directory layout, initial configuration, basic Docker setup, and version control.

**Builds upon:** N/A (This is the initial phase)

**Steps & Design Choices:**

1.  **Initialize Git Repository:**

    - **Action:** Run `git init` in the `CefMP4` workspace root.
    - **Reasoning:** Essential for version control from the very beginning.

2.  **Create Core Directory Structure:**

    - **Action:**
      - Create `src/` for all application source code.
      - Create `tests/` for unit and integration tests.
      - Create `docs/` for documentation (this `phases` subdirectory will live here).
      - Create `.cursor/rules/` (already done in the previous step by the assistant).
    - **Reasoning:** Standard project layout for Python applications, separating concerns.

3.  **Create `pyproject.toml`:**

    - **Action:** Create a `pyproject.toml` file in the root.

      ```toml
      [project]
      name = "cefmp4_stream_processor"
      version = "0.1.0"
      description = "Monitors MP4 video streams, processes them in chunks, and uploads to S3."
      requires-python = ">=3.12"
      dependencies = [
          "pydantic-settings",
          "python-dotenv",
          # Other core dependencies will be added in later phases
      ]

      [tool.ruff]
      line-length = 88 # Example, can be adjusted
      select = ["E", "F", "W", "I", "UP", "C4", "B"] # Common useful rules
      ignore = []

      [tool.ruff.format]
      quote-style = "double"

      [tool.mypy]
      python_version = "3.12"
      warn_return_any = true
      warn_unused_configs = true
      ignore_missing_imports = true # Initially, can be stricter later
      # Add paths if needed, e.g., mypy_path = "src"
      ```

    - **Reasoning:** Standard for managing Python project metadata, dependencies, and tool configurations (like `ruff` and `mypy`). `pydantic-settings` is chosen for robust configuration management as per `requiremnts.md`. `python-dotenv` helps load environment variables from a `.env` file during development.

4.  **Create `example.env`:**

    - **Action:** Create `example.env` in the root.

      ```env
      # Core Application Settings
      WATCH_DIR="/input-videos" # Directory to monitor for .mp4 files
      CHUNK_SIZE_MB="10"        # Chunk size in Megabytes for processing
      STREAM_TIMEOUT_SECONDS="30" # Timeout in seconds to consider a stream complete/idle

      # S3 Configuration (Details will be used in later phases)
      S3_ENDPOINT_URL="http://minio:9000"
      S3_ACCESS_KEY_ID="minioadmin"
      S3_SECRET_ACCESS_KEY="minioadmin"
      S3_BUCKET_NAME="video-streams"
      # S3_REGION_NAME="us-east-1" # Optional, depending on S3 provider

      # Redis Configuration (Details will be used in later phases)
      REDIS_URL="redis://redis:6379/0"

      # Observability (Details will be used in later phases)
      LOG_LEVEL="INFO" # DEBUG, INFO, WARNING, ERROR
      PROM_PORT="8000" # Port for Prometheus metrics endpoint

      # FFprobe (Details will be used in later phases)
      # FFPROBE_PATH="/usr/bin/ffprobe" # Optional: explicit path to ffprobe
      ```

    - **Reasoning:** Provides a template for required environment variables. Facilitates local development and makes configuration clear.

5.  **Implement Initial Configuration (`src/config.py`):**

    - **Action:** Create `src/config.py`.

      ```python
      from pydantic_settings import BaseSettings, SettingsConfigDict
      from pydantic import Field

      class Settings(BaseSettings):
          model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

          watch_dir: str = "/input-videos"
          chunk_size_mb: int = Field(10, gt=0, description="Chunk size in MB")
          stream_timeout_seconds: int = Field(30, gt=0, description="Stream idle timeout in seconds")

          s3_endpoint_url: str = "http://minio:9000"
          s3_access_key_id: str = "minioadmin"
          s3_secret_access_key: str = "minioadmin"
          s3_bucket_name: str = "video-streams"
          s3_region_name: str | None = None

          redis_url: str = "redis://redis:6379/0"

          log_level: str = "INFO"
          prom_port: int = 8000

          ffprobe_path: str | None = None # Default to ffprobe in PATH

          @property
          def chunk_size_bytes(self) -> int:
              return self.chunk_size_mb * 1024 * 1024

      settings = Settings()
      ```

    - **Reasoning:** Uses `pydantic-settings` to load and validate configurations from environment variables (and `.env` file). Provides typed access to settings. The `chunk_size_bytes` property is a convenient derived value.

6.  **Create Basic `Dockerfile`:**

    - **Action:** Create `Dockerfile` in the root.

      ```dockerfile
      # Phase 1: Basic Dockerfile
      FROM python:3.12-slim

      WORKDIR /app

      # Install dumb-init (recommended for signal handling)
      RUN apt-get update && apt-get install -y dumb-init && rm -rf /var/lib/apt/lists/*

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
      ```

    - **Reasoning:** Sets up a basic Python environment. `dumb-init` is included as per `requiremnts.md` for better signal handling. A non-root user is good security practice. The CMD assumes a `src.main` module, which will be created next.

7.  **Create Initial `docker-compose.yml`:**

    - **Action:** Create `docker-compose.yml` in the root.

      ```yaml
      version: "3.9"

      services:
        app:
          build: .
          volumes:
            - ./input-videos:/input-videos # Mount input directory
            - ./src:/app/src # Mount src for development hot-reloading (optional for dev)
          env_file:
            - example.env # Load environment variables
          ports:
            - "${PROM_PORT:-8000}:${PROM_PORT:-8000}" # Expose Prometheus port
          depends_on:
            - minio
            - redis
          # command: ["python", "-m", "src.main"] # Can override Dockerfile CMD if needed

        minio:
          image: minio/minio:latest
          ports:
            - "9000:9000" # S3 API
            - "9001:9001" # MinIO Console
          volumes:
            - minio-data:/data
          environment:
            MINIO_ROOT_USER: ${S3_ACCESS_KEY_ID:-minioadmin}
            MINIO_ROOT_PASSWORD: ${S3_SECRET_ACCESS_KEY:-minioadmin}
          command: server /data --console-address ":9001"

        redis:
          image: redis:7-alpine
          volumes:
            - redis-data:/data
          # ports: # Uncomment to expose Redis for direct debugging
          #   - "6379:6379"

      volumes:
        minio-data:
        redis-data:
      ```

    - **Reasoning:** Defines the application service (`app`), MinIO (`minio`), and Redis (`redis`) as required. Mounts volumes for persistent data and the input video directory. Loads configuration from `example.env`.

8.  **Create Basic Application Entry Point (`src/main.py`):**

    - **Action:** Create `src/main.py`.

      ```python
      import asyncio
      import logging
      from src.config import settings

      # Basic logging setup for now, will be replaced by structlog in a later phase
      logging.basicConfig(level=settings.log_level.upper(),
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      logger = logging.getLogger(__name__)

      async def main():
          logger.info("Application starting...")
          logger.info(f"Watching directory: {settings.watch_dir}")
          logger.info(f"Chunk size: {settings.chunk_size_mb} MB")
          logger.info(f"Stream timeout: {settings.stream_timeout_seconds} seconds")
          # Main application loop will be built here in subsequent phases
          try:
              while True:
                  # Placeholder for future watcher logic
                  await asyncio.sleep(1)
          except KeyboardInterrupt:
              logger.info("Application shutting down...")
          finally:
              logger.info("Application stopped.")

      if __name__ == "__main__":
          try:
              asyncio.run(main())
          except KeyboardInterrupt:
              logger.info("Application interrupted by user.")
      ```

    - **Reasoning:** Provides a minimal runnable `asyncio` application. Loads settings and logs basic information. This will be the main orchestrator.

9.  **Create `src/__init__.py` and `tests/__init__.py`:**

    - **Action:** Create empty `__init__.py` files in `src/` and `tests/`.
    - **Reasoning:** Makes these directories Python packages.

10. **Create Basic `README.md`:**

    - **Action:** Create `README.md` in the root.

      ````markdown
      # CefMP4 Stream Processor

      Monitors a specified directory for MP4 video stream files, processes them in chunks, and uploads to S3-compatible object storage with checkpoint management using Redis.

      ## Prerequisites

      - Docker
      - Docker Compose

      ## Setup & Running

      1.  Clone the repository.
      2.  Ensure you have an `.env` file (you can copy `example.env` to `.env` and modify if needed).
          ```bash
          cp example.env .env
          ```
      3.  Create the directory for input videos if it doesn't exist:
          ```bash
          mkdir -p input-videos
          ```
      4.  Build and run the services:
          ```bash
          docker compose up --build
          ```

      The application will start monitoring the directory specified by `WATCH_DIR` in your `.env` file (default: `./input-videos`).

      ## Configuration

      Configuration is managed via environment variables. See `example.env` for available options.

      ## Project Structure

      - `src/`: Application source code.
      - `tests/`: Tests.
      - `docs/`: Project documentation.
      - `Dockerfile`: Defines the application's Docker image.
      - `docker-compose.yml`: Orchestrates the application and its dependencies (MinIO, Redis).
      - `pyproject.toml`: Project metadata and dependencies.
      - `.cursor/rules/`: Cursor AI rules for development.

      (More details will be added as the project progresses)
      ````

    - **Reasoning:** Provides essential information for anyone new to the project, including setup instructions.

11. **Add `.gitignore`:**

    - **Action:** Create a `.gitignore` file.

      ```
      # Python
      __pycache__/
      *.py[cod]
      *$py.class
      *.so
      .Python
      build/
      develop-eggs/
      dist/
      downloads/
      eggs/
      .eggs/
      lib/
      lib60/
      parts/
      sdist/
      var/
      wheels/
      share/python-wheels/
      *.egg-info/
      .installed.cfg
      *.egg
      MANIFEST

      # Environments
      .env
      .venv
      env/
      venv/
      ENV/
      env.bak/
      venv.bak/

      # IDE / Editor
      .vscode/
      .idea/
      *.suo
      *.ntvs*
      *.njsproj
      *.sln
      *.sw?

      # Docker
      docker-compose.override.yml

      # Cursor
      .cursor/*
      !.cursor/rules/ # Keep rules in VCS

      # Other
      *.log
      local_settings.py
      ```

    - **Reasoning:** Prevents common unnecessary files from being committed to version control. Crucially, it ignores `.cursor/*` but un-ignores `.cursor/rules/` so that the AI rules are version-controlled.

**Expected Outcome:**

- A runnable (though minimal) Docker Compose setup.
- Core configuration loaded and accessible.
- Project structure and initial files in place.
- Version control initiated.
- Basic README for project overview and setup.

**Next Phase:** Phase 2 will focus on implementing the file watching mechanism using `watchfiles`.
