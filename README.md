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
  - [Phase 6: Metadata Generation (ffprobe & JSON)](#phase-6-metadata-generation-ffprobe--json)
  - [Phase 7: Checkpoint Recovery & Resume Logic](#phase-7-checkpoint-recovery--resume-logic)
  - [Phase 8: Robust Error Handling & Graceful Shutdown](#phase-8-robust-error-handling--graceful-shutdown)
  - [Phase 9: Observability (Logging & Metrics)](#phase-9-observability-logging--metrics)
  - [Phase 10: Finalizing Dockerization, CI, Documentation & Testing](#phase-10-finalizing-dockerization-ci-documentation--testing)
- [Testing](#testing)
- [Development Notes](#development-notes)
- [Observability](#observability)
  - [Logging](#logging)
  - [Metrics](#metrics)
- [Documentation & Diagrams](#documentation--diagrams)
  - [Phase Documentation](#phase-documentation)
  - [System Diagrams](#system-diagrams)
- [Troubleshooting](#troubleshooting)

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

- `APP_ENV`: Application environment (`development` or `production`). Default: `development` (local), `production` (Docker).
- `LOG_LEVEL`: Logging level (e.g., `INFO`, `DEBUG`, `WARNING`). Default: `INFO`.
- `PROM_PORT`: Port for the Prometheus metrics endpoint. Default: `8000`.
- `MAX_CONCURRENT_STREAMS`: Maximum number of streams to process concurrently. Default: `5`.

- `WATCH_DIR`: Directory the application monitors for video files (e.g., `./local_watch_dir`).
- `CHUNK_SIZE_BYTES`: Size of chunks in bytes for S3 multipart uploads (e.g., `10485760` for 10MB).
- `STREAM_TIMEOUT_SECONDS`: Idle time in seconds after which a stream is checked for finalization. Default: `30`.

- `S3_ENDPOINT_URL`: URL for the S3-compatible service (e.g., `http://localhost:9000` for local MinIO).
- `S3_ACCESS_KEY_ID`: S3 access key.
- `S3_SECRET_ACCESS_KEY`: S3 secret key.
- `S3_BUCKET_NAME`: S3 bucket to upload files to.
- `S3_REGION_NAME`: S3 region (optional for MinIO).

- `REDIS_URL`: Connection URL for Redis (e.g., `redis://localhost:6379/0`).

- `FFPROBE_PATH`: Optional: Full path to the `ffprobe` executable if not in system PATH.

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
- **Documentation**: [Full Phase 1 Documentation](docs/phases/01_project_setup_core_config.md)
- **Key Components**:
  - `src/config.py`: Uses `pydantic-settings` to load configuration from an `.env` file.
  - `src/s3_client.py`: Basic `aioboto3` client for S3 interactions.
  - `src/redis_client.py`: Basic `redis-py` (async) client for Redis interactions.
  - `docker-compose.yml`: Sets up MinIO and Redis services.
  - `src/main.py`: Initial application entry point.
- **Diagrams**: [Architecture Overview](docs/diagrams/architecture.mmd)

### Phase 2: File Watching Mechanism

- **Goal**: Implement a system to monitor the `WATCH_DIR` for new or modified video files.
- **Documentation**: [Full Phase 2 Documentation](docs/phases/02_file_watching_mechanism.md)
- **Key Components**:
  - `src/watcher.py`: Uses the `watchdog` library to monitor file system events (`CREATE`, `MODIFY`, `DELETE`).
  - `src/events.py`: Defines `StreamEvent` and `WatcherChangeType` to represent file system changes.
  - `src/main.py`: Integrates the watcher to receive and log events. Events like `CREATE`, `WRITE`, `DELETE`, and `IDLE` are handled.
- **Diagrams**: [File Watcher Flow](docs/diagrams/file_watcher_flow.mmd)

### Phase 3: Stream State & S3 Initialization

- **Goal**: Manage the state of each video stream in Redis and initialize multipart uploads on S3 when a new stream is detected.
- **Documentation**: [Full Phase 3 Documentation](docs/phases/03_stream_state_s3_init.md)
- **Key Components**:
  - `src/redis_client.py`:
    - `init_stream_metadata()`: Stores initial metadata about a stream (file path, S3 upload ID, status, etc.) in Redis upon a `CREATE` event.
  - `src/s3_client.py`:
    - `create_s3_multipart_upload()`: Initiates a multipart upload on S3 and returns an `UploadId`.
  - `src/main.py`: On a `CREATE` event, calls `init_stream_metadata` and `create_s3_multipart_upload`. The concept of a `StreamProcessor` is introduced to manage active uploads, stored in an `active_processors` dictionary.
- **Diagrams**: [Stream Initialization Sequence](docs/diagrams/stream_initialization_sequence.mmd), [Stream Processing Flow](docs/diagrams/stream_processing_flow.mmd)

### Phase 4: Chunk Processing & S3 Upload

- **Goal**: Process video files in chunks and upload them to S3.
- **Documentation**: [Full Phase 4 Documentation](docs/phases/04_chunk_processing_s3_upload.md)
- **Key Components**:
  - `src/stream_processor.py`:
    - `StreamProcessor` class: Manages the lifecycle of a single video stream.
    - `process_file_write()`: Triggered by `WRITE` events. Reads a chunk of the video file from its current offset, uploads it as a part of the S3 multipart upload (`upload_s3_part`), and updates Redis with part information, total bytes sent, and last activity timestamp.
  - `src/redis_client.py`: Functions to update stream part information, total bytes sent, and last activity.
  - `src/main.py`: For `WRITE` events, delegates to the corresponding `StreamProcessor`'s `process_file_write()` method.
- **Diagrams**: [Chunk Processing Sequence](docs/diagrams/chunk_processing_sequence.mmd)

### Phase 5: Stream Finalization & S3 Completion

- **Goal**: Detect when a stream is complete (idle and fully uploaded) and finalize the S3 multipart upload.
- **Documentation**: [Full Phase 5 Documentation](docs/phases/05_stream_finalization_s3_complete.md)
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
- **Diagrams**: [Stream Finalization Sequence](docs/diagrams/stream_finalization_sequence.mmd)

### Phase 6: Metadata Generation (ffprobe & JSON)

- **Goal**: After successful S3 multipart upload, use `ffprobe` to get video duration, gather all metadata, create a JSON metadata file, and upload it to S3.
- **Documentation**: [Full Phase 6 Documentation](docs/phases/06_metadata_generation_ffprobe_json.md)
- **Key Components**:
  - `Dockerfile`: Modified to install `ffmpeg` (which provides `ffprobe`).
  - `src/utils/ffprobe_utils.py`:
    - `get_video_duration()`: Utility to run `ffprobe` asynchronously and extract video duration in seconds.
  - `src/metadata_generator.py`:
    - `generate_metadata_json()`: Gathers information from Redis (stream metadata, part details including `UploadedAtUTC`) and `ffprobe` (duration). Constructs a comprehensive JSON object for the stream, including `stream_id`, file paths, S3 details, total size, duration, timestamps, and detailed chunk information.
  - `src/redis_client.py`:
    - `get_stream_parts()`: Provides detailed part information, including `UploadedAtUTC`, necessary for the metadata JSON.
  - `src/s3_client.py`:
    - `upload_json_to_s3()`: Uploads the generated JSON metadata file to the S3 bucket, typically alongside the processed video stream.
  - `src/stream_processor.py`:
    - `finalize_stream()`: Integrates metadata generation (calling `generate_metadata_json`) and S3 upload (calling `upload_json_to_s3`) after a successful S3 multipart upload completion. Updates stream status in Redis to reflect metadata status (e.g., "completed_with_meta").
  - `src/config.py`: The `ffprobe_path` setting allows specifying a custom `ffprobe` binary location if needed.
- **Diagrams**: [Metadata Generation Sequence](docs/diagrams/metadata_generation_sequence.mmd)

### Phase 7: Checkpoint Recovery & Resume Logic

- **Goal**: Enable the application to recover and resume processing of incomplete streams (those in "active" or "pending_completion" states in Redis) upon startup.
- **Documentation**: [Full Phase 7 Documentation](docs/phases/07_checkpoint_recovery_resume.md)
- **Key Components**:
  - `src/redis_client.py`:
    - `get_active_stream_ids()`, `get_pending_completion_stream_ids()`: Retrieve lists of stream IDs that were being processed or awaiting finalization before shutdown.
    - `get_stream_meta()`, `get_stream_parts()`, `get_stream_next_part()`, `get_stream_bytes_sent()`: Provide all necessary checkpoint data (original path, S3 upload details, part information, next part number, total bytes sent) for a given stream ID.
    - `add_stream_to_failed_set()`: Manages streams that cannot be resumed (e.g., if the original file is missing on startup).
  - `src/stream_processor.py`:
    - `_initialize_from_checkpoint()`: Fully implemented to re-hydrate the processor's state (file offset, next part number, S3 details, list of already uploaded parts) by fetching data from Redis using the stream ID. It robustly handles cases like missing original files by marking the stream as failed.
  - `src/main.py`:
    - `resume_stream_processing()`: A function called during application startup for each potentially resumable stream ID. It fetches metadata, instantiates a `StreamProcessor`, calls `_initialize_from_checkpoint()` on it, and then, based on the recovered stream status (e.g., "active", "pending_completion"), schedules the appropriate follow-up action (e.g., `processor.process_file_write()` to check for more data, or `processor.finalize_stream()` to attempt completion).
    - Startup Sequence: The main application startup logic first attempts to resume any interrupted streams before initiating new file watching or other periodic tasks.
  - Idempotency: Operations are designed to be idempotent where possible, or state checks are performed to prevent issues if an operation is re-tried during resume (e.g., not re-finalizing an already completed stream).
- **Diagrams**: [Checkpoint Recovery Sequence](docs/diagrams/checkpoint_recovery_sequence.mmd)

### Phase 8: Robust Error Handling & Graceful Shutdown

- **Goal**: Enhance application resilience with custom exceptions, robust retry logic for transient errors (especially network I/O), and ensure graceful shutdown on signals like SIGINT/SIGTERM.
- **Documentation**: [Full Phase 8 Documentation](docs/phases/08_error_handling_shutdown.md)
- **Key Components**:
  - `src/exceptions.py`: Defines custom application-specific exceptions (e.g., `StreamInitializationError`, `ChunkProcessingError`, `S3OperationError`, `RedisOperationError`).
  - `src/utils/retry.py`: Implements `async_retry_transient` decorator using `tenacity` for retrying operations prone to transient failures.
  - `src/main.py`: Signal handlers for `SIGINT` and `SIGTERM` set a `shutdown_signal_event`. The main loop and task creation helpers respect this event to stop new work and cancel ongoing tasks. A semaphore (`stream_processing_semaphore` based on `MAX_CONCURRENT_STREAMS`) is used to limit concurrent stream processing.
  - `src/s3_client.py`, `src/redis_client.py`: Key functions decorated with `async_retry_transient` to handle transient network issues.
  - `src/stream_processor.py`: Enhanced error handling to catch specific exceptions, use `add_stream_to_failed_set`, and coordinate with the shutdown signal.
- **Diagrams**: [Error Handling & Graceful Shutdown](docs/diagrams/error_handling_graceful_shutdown.mmd)

### Phase 9: Observability (Logging & Metrics)

- **Goal**: Implement comprehensive structured logging using `structlog` for improved diagnostics and set up Prometheus metrics for monitoring application performance and health.
- **Documentation**: [Full Phase 9 Documentation](docs/phases/09_observability_logging_metrics.md)
- **Key Components**:
  - `src/logging_config.py`: Configures `structlog` for structured JSON logging in production and console rendering in development. Manages log levels and mutes noisy loggers.
  - `structlog` Integration: Replaced standard `logging` with `structlog` across all relevant modules. `StreamProcessor` uses bound loggers with `stream_id` for contextual logging.
  - `src/metrics.py`: Defines Prometheus metrics (Counters, Gauges, Histograms) for key application events like chunks/bytes uploaded, stream duration, processing time, active streams, and error counts.
  - `src/main.py`: Initializes the Prometheus metrics server (`start_metrics_server`) on `PROM_PORT` (e.g., 8000) exposing a `/metrics` endpoint. Manages `ACTIVE_STREAMS_GAUGE`.
  - `src/stream_processor.py` & other modules: Instrumented to increment/observe relevant Prometheus metrics during operations (e.g., uploads, finalization, errors).
  - `Dockerfile`, `docker-compose.yml`: Updated to expose `PROM_PORT`.
- **Diagrams**: [Observability Setup](docs/diagrams/observability_setup.mmd)

### Phase 10: Finalizing Dockerization, CI, Documentation & Testing

- **Goal**: Polish the application, establish a CI pipeline, complete documentation, and ensure the system is robust.
- **Documentation**: [Full Phase 10 Documentation](docs/phases/10_finalizing_docker_ci_docs_testing.md)
- **Key Components (Focus on non-testing for this update)**:
  - `Dockerfile`: Installs `ffmpeg` (for `ffprobe`), sets up a non-root user (`appuser`) for running the application. `ENTRYPOINT` is set to directly run the application via `python -m src.main`.
  - `docker-compose.yml`: Refined for local development and testing, including healthchecks for `app`, `minio`, and `redis` services. Uses environment variables from `.env` extensively.
  - `README.md`: Comprehensively updated with detailed setup, configuration, operational instructions, and troubleshooting tips.
  - `.github/workflows/ci.yml`: Basic CI pipeline established using GitHub Actions for linting (`ruff check`), formatting checks (`ruff format --check`), and type checking (`mypy`).
  - `example.env`: Finalized to include all relevant configurable parameters with clear comments and defaults.
- **Diagrams**: [CI/CD Pipeline](docs/diagrams/ci_cd_pipeline.mmd), [Deployment Overview](docs/diagrams/deployment_overview.mmd), [Class Interactions](docs/diagrams/class_interactions.mmd), [Design Decisions](docs/diagrams/design_decisions.mmd)

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

## Observability

### Logging

The application uses `structlog` for structured logging.

- In `development` mode (`APP_ENV=development`), logs are pretty-printed to the console.
- In `production` mode (`APP_ENV=production`), logs are formatted as JSON, suitable for ingestion into log management systems.

Key log attributes include `timestamp`, `level`, `logger_name`, `event` (the log message), and any bound context variables like `stream_id` and `file_path`.

The log level can be configured via the `LOG_LEVEL` environment variable (e.g., `INFO`, `DEBUG`, `WARNING`). Default is `INFO`.

### Metrics

The application exports Prometheus metrics on port `${PROM_PORT}` (default `8000`) at the `/metrics` endpoint (e.g., `http://localhost:8000/metrics`).

Key exposed metrics include:

- `video_chunks_uploaded_total{stream_id}`: Total number of video chunks successfully uploaded per stream.
- `video_bytes_uploaded_total{stream_id}`: Total number of bytes successfully uploaded per stream.
- `video_stream_duration_seconds{stream_id}`: Histogram of processed video stream durations.
- `video_processing_time_seconds{stream_id}`: Histogram of time taken to process video streams.
- `video_failed_operations_total{stream_id, operation_type}`: Counter for failed operations (e.g., s3_upload, redis_update).
- `active_streams_gauge`: Current number of actively processing streams.
- `streams_completed_total`: Total number of streams successfully processed.
- `streams_failed_total`: Total number of streams that ended in a failed state.

**Sample Prometheus Scrape Configuration:**

```yaml
# prometheus.yml example snippet
scrape_configs:
  - job_name: "cefmp4-processor"
    static_configs:
      - targets: ["localhost:8000"] # Adjust target to your app's host and PROM_PORT
```

## Documentation & Diagrams

The project includes comprehensive documentation and visual diagrams to help understand the system architecture and workflows.

### Phase Documentation

Detailed documentation for each development phase is available in the `docs/phases/` directory:

1. [Core Setup & Configuration](docs/phases/01_project_setup_core_config.md)
2. [File Watching Mechanism](docs/phases/02_file_watching_mechanism.md)
3. [Stream State & S3 Initialization](docs/phases/03_stream_state_s3_init.md)
4. [Chunk Processing & S3 Upload](docs/phases/04_chunk_processing_s3_upload.md)
5. [Stream Finalization & S3 Completion](docs/phases/05_stream_finalization_s3_complete.md)
6. [Metadata Generation (ffprobe & JSON)](docs/phases/06_metadata_generation_ffprobe_json.md)
7. [Checkpoint Recovery & Resume Logic](docs/phases/07_checkpoint_recovery_resume.md)
8. [Robust Error Handling & Graceful Shutdown](docs/phases/08_error_handling_shutdown.md)
9. [Observability (Logging & Metrics)](docs/phases/09_observability_logging_metrics.md)
10. [Finalizing Dockerization, CI, Documentation & Testing](docs/phases/10_finalizing_docker_ci_docs_testing.md)

### System Diagrams

The project includes Mermaid diagrams in the `docs/diagrams/` directory to visualize various aspects of the system:

| Diagram                                                                                  | Description                                                                               |
| ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| [Architecture Overview](docs/diagrams/architecture.mmd)                                  | High-level architecture of the application showing core components and their interactions |
| [Stream Processing Flow](docs/diagrams/stream_processing_flow.mmd)                       | Main processing flow from application start to stream completion                          |
| [File Watcher Flow](docs/diagrams/file_watcher_flow.mmd)                                 | How the file watching mechanism detects and processes file system events                  |
| [Stream Initialization Sequence](docs/diagrams/stream_initialization_sequence.mmd)       | Sequence of operations when a new stream is created                                       |
| [Chunk Processing Sequence](docs/diagrams/chunk_processing_sequence.mmd)                 | How file chunks are read and uploaded to S3                                               |
| [Stream Finalization Sequence](docs/diagrams/stream_finalization_sequence.mmd)           | Steps taken to finalize a stream once all chunks are processed                            |
| [Metadata Generation Sequence](docs/diagrams/metadata_generation_sequence.mmd)           | How video metadata is generated and uploaded                                              |
| [Checkpoint Recovery Sequence](docs/diagrams/checkpoint_recovery_sequence.mmd)           | Process for recovering and resuming streams after application restart                     |
| [Error Handling & Graceful Shutdown](docs/diagrams/error_handling_graceful_shutdown.mmd) | How errors are handled and how the application shuts down gracefully                      |
| [Observability Setup](docs/diagrams/observability_setup.mmd)                             | Logging and metrics configuration and flow                                                |
| [CI/CD Pipeline](docs/diagrams/ci_cd_pipeline.mmd)                                       | Continuous integration and deployment workflow                                            |
| [Deployment Overview](docs/diagrams/deployment_overview.mmd)                             | Application deployment architecture                                                       |
| [Class Interactions](docs/diagrams/class_interactions.mmd)                               | Key classes and their relationships                                                       |
| [Design Decisions](docs/diagrams/design_decisions.mmd)                                   | Mind map of major design decisions and trade-offs                                         |

These diagrams are in Mermaid format and can be rendered using various Markdown viewers that support Mermaid, GitHub's built-in Mermaid renderer, or online tools like the [Mermaid Live Editor](https://mermaid.live/).

## Troubleshooting

- **`active_streams_gauge` shows negative value:** This indicates an imbalance in metric increments/decrements. This was a known issue and should be resolved. If it persists, review `ACTIVE_STREAMS_GAUGE.inc()` and `.dec()` calls in `src/main.py`.
- **Cannot connect to MinIO/Redis:**
  - Ensure MinIO and Redis containers are running: `docker ps`.
  - Check `S3_ENDPOINT_URL` and `REDIS_URL` in your `.env` file. If running the app outside Docker but services in Docker, these should point to `localhost`. If running the app inside Docker, they should point to the service names (e.g., `http://minio:9000`, `redis://redis:6379/0`).
  - Verify MinIO credentials (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`) match those used to start the MinIO service.
- **Permission errors writing to `local_watch_dir` (when app is in Docker):** Ensure the directory on the host machine that is volume-mounted into `/app/local_watch_dir` has appropriate write permissions for the `appuser` (UID 1001) inside the container. For local testing, a simple `chmod -R 777 local_watch_dir` on the host can work, but adjust permissions as needed for your environment.
- **`ffprobe` not found:** If the application logs errors about `ffprobe` not being found, and you are not running in Docker, ensure `ffmpeg` (which includes `ffprobe`) is installed and in your system's PATH, or set the `FFPROBE_PATH` environment variable to its full path.
- **Metrics endpoint not available:** Check application logs to ensure the Prometheus metrics server started correctly on `PROM_PORT`.
