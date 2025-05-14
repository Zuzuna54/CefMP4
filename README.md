# CefMP4 Stream Processor

CefMP4 Stream Processor monitors a specified directory for MP4 video files. When a new video file is detected or an existing one is modified, the application processes it in chunks, uploads these chunks to an S3-compatible object storage service, and manages the state of these uploads using Redis. This allows for resumable uploads and robust handling of large video files.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
- [Setup](#setup)
  - [1. Clone Repository](#1-clone-repository)
  - [2. Docker Setup (Recommended for Services)](#2-docker-setup-recommended-for-services)
  - [3. Local Development Setup (Python Virtual Environment)](#3-local-development-setup-python-virtual-environment)
- [Running the Application](#running-the-application)
  - [Using Docker (Application Container)](#using-docker-application-container)
  - [Locally (Python Virtual Environment)](#locally-python-virtual-environment)
- [How It Works](#how-it-works)
  - [Phase 1: Core Setup & Configuration](#phase-1-core-setup--configuration)
  - [Phase 2: File Watching Mechanism](#phase-2-file-watching-mechanism)
  - [Phase 3: Stream State & S3 Initialization](#phase-3-stream-state--s3-initialization)
  - [Phase 4: Chunk Processing & S3 Upload](#phase-4-chunk-processing--s3-upload)
  - [Phase 5: Stream Finalization & S3 Completion](#phase-5-stream-finalization--s3-completion)
- [Testing](#testing)
- [Development Notes](#development-notes)

## Prerequisites

- **Python**: Version 3.11+ (for local development)
- **Docker & Docker Compose**: For running MinIO (S3-compatible storage) and Redis services, and optionally the application itself.
- **Git**: For cloning the repository.

## Project Structure

```
CefMP4/
├── .cursor/            # Cursor AI configuration
├── .venv/              # Python virtual environment (if created locally)
├── docs/               # Project documentation, including phase designs
│   └── phases/
├── local_watch_dir/    # Default directory monitored for video files
├── minio_data/         # Data persistence for MinIO (if run via Docker Compose)
├── src/                # Application source code
│   ├── __init__.py
│   ├── config.py       # Configuration loading (Pydantic settings)
│   ├── events.py       # Event definitions for file watching
│   ├── main.py         # Main application entry point
│   ├── redis_client.py # Redis interactions
│   ├── s3_client.py    # S3 interactions
│   ├── stream_processor.py # Core logic for processing video streams
│   └── watcher.py      # File system watcher
├── tests/              # Unit and integration tests
├── .env                # Local environment variables (gitignored)
├── .gitignore
├── docker-compose.yml  # Docker Compose configuration for services (MinIO, Redis)
├── Dockerfile          # Dockerfile for the application
├── example.env         # Example environment file
├── pyproject.toml      # Project metadata and dependencies (Poetry)
├── README.md           # This file
└── requirements.txt    # Python dependencies
```

## Configuration

Configuration is managed via environment variables, loaded from an `.env` file using `pydantic-settings`.

1.  Copy the example environment file:
    ```bash
    cp example.env .env
    ```
2.  Modify `.env` with your desired settings.

### Key Environment Variables

(Refer to `src/config.py` and `example.env` for a full list and default values)

- `WATCH_DIR`: Directory the application monitors for video files (e.g., `./local_watch_dir`).
- `PYTHON_LOG_LEVEL`: Logging level (e.g., `INFO`, `DEBUG`).
- `S3_ENDPOINT_URL`: URL for the S3-compatible service (e.g., `http://localhost:9000` for local MinIO).
- `S3_ACCESS_KEY_ID`: S3 access key.
- `S3_SECRET_ACCESS_KEY`: S3 secret key.
- `S3_BUCKET_NAME`: S3 bucket to upload files to.
- `REDIS_HOST`: Redis host (e.g., `localhost`).
- `REDIS_PORT`: Redis port (e.g., `6379`).
- `STREAM_CHUNK_SIZE_MB`: Size of chunks (in MB) for S3 multipart uploads.
- `STREAM_TIMEOUT_SECONDS`: Idle time in seconds after which a stream is considered stale and checked for finalization.
- `WATCH_FILTER_GLOBS`: Glob patterns for files to watch (e.g., `["*.mp4", "*.mov"]`).
- `WATCH_FILE_MIN_AGE_SECONDS_FOR_IDLE`: Minimum age of a file with no activity before it's considered idle by the watcher.

## Setup

### 1. Clone Repository

```bash
git clone <repository_url>
cd CefMP4
```

### 2. Docker Setup (Recommended for Services)

This setup uses Docker Compose to run MinIO (S3) and Redis services.

1.  Ensure Docker and Docker Compose are installed.
2.  Start the services:
    ```bash
    docker compose up -d minio redis
    ```
    This will start MinIO and Redis in detached mode.
    - MinIO will be accessible at `http://localhost:9000` (console at `http://localhost:9001`). Use credentials from your `.env` (default `minioadmin`/`minioadmin`).
    - Redis will be accessible at `localhost:6379`.
3.  You may need to manually create the S3 bucket specified in `S3_BUCKET_NAME` via the MinIO console if the application doesn't create it automatically (though it should attempt to).

### 3. Local Development Setup (Python Virtual Environment)

For running the application directly on your host machine (connecting to Dockerized MinIO/Redis or other instances).

1.  Ensure Python 3.11+ is installed.
2.  Create and activate a virtual environment:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # On macOS/Linux
    # .venv\\Scripts\\activate   # On Windows
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4.  Ensure the `WATCH_DIR` directory exists (or create it):
    ```bash
    mkdir -p local_watch_dir # Or the path you set in .env
    ```

## Running the Application

### Using Docker (Application Container)

If you want to run the application itself in a Docker container (recommended for consistency with deployed environments):

1.  Ensure MinIO and Redis services are running (see Docker Setup).
2.  Update your `.env` file:
    - Ensure `S3_ENDPOINT_URL` is accessible from within the Docker network (e.g., `http://minio:9000` if `minio` is the service name in `docker-compose.yml`).
    - Ensure `REDIS_HOST` is accessible (e.g., `redis` if `redis` is the service name).
3.  Build and run the application container along with the services:
    ```bash
    docker compose up --build app
    ```
    Or, if services are already running:
    ```bash
    docker compose up --build -d app # To run in detached mode
    docker compose logs -f app # To follow logs
    ```

### Locally (Python Virtual Environment)

1.  Ensure MinIO and Redis services are running (e.g., via Docker Compose as described above, or other instances).
2.  Ensure your `.env` file is configured correctly for these services (e.g., `S3_ENDPOINT_URL=http://localhost:9000`, `REDIS_HOST=localhost`).
3.  Activate your virtual environment:
    ```bash
    source .venv/bin/activate
    ```
4.  Run the application:
    `bash
python src/main.py
`
    The application will start monitoring the `WATCH_DIR`. To trigger processing, copy an MP4 file into this directory.

## How It Works

The application processes video files in several stages, evolving through development phases:

### Phase 1: Core Setup & Configuration

- **Goal**: Establish the project structure, configuration management, and initial clients for S3 and Redis.
- **Key Components**:
  - `src/config.py`: Uses `pydantic-settings` to load configuration from an `.env` file.
  - `src/s3_client.py`: Basic `aioboto3` client for S3 interactions.
  - `src/redis_client.py`: Basic `redis-py` (async) client for Redis interactions.
  - `docker-compose.yml`: Sets up MinIO and Redis services.
  - `src/main.py`: Initial application entry point.

### Phase 2: File Watching Mechanism

- **Goal**: Implement a system to monitor the `WATCH_DIR` for new or modified video files.
- **Key Components**:
  - `src/watcher.py`: Uses the `watchdog` library to monitor file system events (`CREATE`, `MODIFY`, `DELETE`).
  - `src/events.py`: Defines `StreamEvent` and `WatcherChangeType` to represent file system changes.
  - `src/main.py`: Integrates the watcher to receive and log events. Events like `CREATE`, `WRITE`, `DELETE`, and `IDLE` are handled.

### Phase 3: Stream State & S3 Initialization

- **Goal**: Manage the state of each video stream in Redis and initialize multipart uploads on S3 when a new stream is detected.
- **Key Components**:
  - `src/redis_client.py`:
    - `init_stream_metadata()`: Stores initial metadata about a stream (file path, S3 upload ID, status, etc.) in Redis upon a `CREATE` event.
  - `src/s3_client.py`:
    - `create_s3_multipart_upload()`: Initiates a multipart upload on S3 and returns an `UploadId`.
  - `src/main.py`: On a `CREATE` event, calls `init_stream_metadata` and `create_s3_multipart_upload`. The concept of a `StreamProcessor` is introduced to manage active uploads, stored in an `active_processors` dictionary.

### Phase 4: Chunk Processing & S3 Upload

- **Goal**: Process video files in chunks and upload them to S3.
- **Key Components**:
  - `src/stream_processor.py`:
    - `StreamProcessor` class: Manages the lifecycle of a single video stream.
    - `process_file_write()`: Triggered by `WRITE` events. Reads a chunk of the video file from its current offset, uploads it as a part of the S3 multipart upload (`upload_s3_part`), and updates Redis with part information, total bytes sent, and last activity timestamp.
  - `src/redis_client.py`: Functions to update stream part information, total bytes sent, and last activity.
  - `src/main.py`: For `WRITE` events, delegates to the corresponding `StreamProcessor`'s `process_file_write()` method.

### Phase 5: Stream Finalization & S3 Completion

- **Goal**: Detect when a stream is complete (idle and fully uploaded) and finalize the S3 multipart upload.
- **Key Components**:
  - `src/main.py`:
    - `periodic_stale_stream_check()`: A background task that periodically checks Redis for "active" streams.
    - If a stream's `last_activity_at_utc` exceeds `STREAM_TIMEOUT_SECONDS`:
      - It compares `total_bytes_sent` from Redis with the current disk file size.
      - If they match, the stream is considered fully processed and ready for finalization.
      - If `total_bytes_sent` is less than disk size (file might still be growing or processing lagged), it re-triggers processing for that stream.
      - If `total_bytes_sent` is greater than disk size (file truncated), it attempts to abort the S3 upload and marks the stream as failed.
      - If the file is missing, it aborts the S3 upload and marks as failed.
    - `run_finalization_and_cleanup()`: Helper to manage finalization and cleanup of processors.
  - `src/stream_processor.py`:
    - `finalize_stream()`: Completes the S3 multipart upload using `complete_s3_multipart_upload`, updates the stream status in Redis to "completed" or "failed_finalization", and cleans up Redis keys associated with the stream parts.
  - `src/s3_client.py`:
    - `complete_s3_multipart_upload()`: Finalizes the S3 upload.
    - `abort_s3_multipart_upload()`: Aborts an ongoing S3 upload.
  - `src/redis_client.py`: Functions to get all active stream IDs, move streams to pending completion, and update final statuses.

## Testing

Unit and integration tests are located in the `tests/` directory and can be run using `pytest`.

1.  Ensure you have the development dependencies installed (including `pytest`). If not, from your active virtual environment:
    ```bash
    pip install -r requirements.txt
    # Or specifically for testing if there are test-specific deps in the future:
    # pip install pytest pytest-asyncio aiohttp # etc.
    ```
2.  Ensure MinIO and Redis are running and accessible as per your `.env` configuration for tests (usually `localhost`).
3.  Run tests:
    ```bash
    pytest
    ```
    Or for more verbose output:
    ```bash
    pytest -vv
    ```

## Development Notes

- The application uses `asyncio` for concurrent operations.
- Logging is configured in `src/main.py`. Consider enhancing with structured logging (e.g., `structlog`) for better observability in later phases.
- Error handling and retry mechanisms for S3/Redis operations are progressively added throughout the phases.
- State management is crucial: Redis is the source of truth for stream progress and S3 upload details.
- Idempotency for operations is important, especially for recovery scenarios (covered in later phases).
