# Phase 9: Observability (Logging & Metrics)

**Objective:** Implement comprehensive structured logging using `structlog` for better diagnostics and set up Prometheus metrics for monitoring application performance and health.

**Builds upon:** All previous phases, particularly Phase 8 (Error Handling) as robust error details feed into better logging.

**Steps & Design Choices:**

1.  **Integrate `structlog` for Structured JSON Logging:**

    - **Action:**

      - Add `structlog` to `pyproject.toml` dependencies.
      - Create `src/logging_config.py` to configure `structlog`.

        ```python
        # src/logging_config.py
        import logging
        import sys
        import structlog
        from src.config import settings # Assuming settings.log_level

        def setup_logging():
            """Configures structlog for structured JSON logging."""
            log_level_str = settings.log_level.upper()
            log_level = getattr(logging, log_level_str, logging.INFO)

            shared_processors = [
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.StackInfoRenderer(), # For stack traces
                structlog.dev.set_exc_info, # For exception details
                structlog.processors.format_exc_info,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
            ]

            if settings.app_env == "development": # Or some other indicator for dev
                processors = shared_processors + [
                    structlog.dev.ConsoleRenderer(), # Pretty printing for dev
                ]
            else:
                processors = shared_processors + [
                    structlog.processors.dict_tracebacks, # For JSON tracebacks
                    structlog.processors.JSONRenderer(), # Render to JSON
                ]

            structlog.configure(
                processors=processors,
                logger_factory=structlog.stdlib.LoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )

            # Configure standard logging to use structlog
            root_logger = logging.getLogger()
            handler = logging.StreamHandler(sys.stdout) # Or sys.stderr
            # No formatter needed here for handler if structlog handles it all
            root_logger.addHandler(handler)
            root_logger.setLevel(log_level)

            # Mute noisy loggers if necessary
            logging.getLogger("watchfiles").setLevel(logging.INFO)
            logging.getLogger("asyncio").setLevel(logging.INFO)
            logging.getLogger("botocore").setLevel(logging.WARNING) # Recommended to reduce noise
            logging.getLogger("aiobotocore").setLevel(logging.WARNING) # Recommended to reduce noise
            # logging.getLogger("s3transfer").setLevel(logging.WARNING)


            logger = structlog.get_logger("logging_config")
            logger.info("Structlog configured", log_level=log_level_str, app_env=settings.app_env)

        # Call this early in your application startup (e.g., in main.py)
        ```

      - In `src/main.py`, call `setup_logging()` at the beginning.
      - Replace all `logging.getLogger(__name__)` instances with `structlog.get_logger(__name__)`.
      - Update `src/config.py` to include `LOG_LEVEL: str = "INFO"` and `APP_ENV: str = "development"` (defaulting to development, override to "production" via environment variable in deployed environments).
      - **Contextual Logging:** To add `stream_id` or other contextual data to logs within specific tasks (e.g., inside `StreamProcessor` methods), use `structlog.contextvars`:

        ```python
        # Example within a StreamProcessor method or task processing a specific stream
        # import structlog
        # from structlog.contextvars import bind_contextvars, clear_contextvars

        # async def some_processing_method(self, ...):
        #     bind_contextvars(stream_id=self.stream_id, file_path=str(self.file_path))
        #     logger = structlog.get_logger(self.__class__.__name__)
        #     logger.info("Processing started for stream")
        #     # ... do work ...
        #     if error_occurred:
        #        logger.error("An error occurred", details=...)
        #     # clear_contextvars() # Optional: clear if context is no longer valid or task is ending
        #     # Structlog automatically manages contextvars with asyncio tasks, so explicit clear might not always be needed
        #     # if bound within the task scope.
        ```

    - **Reasoning:** `structlog` enables rich, structured logging in formats like JSON. This is invaluable for log management systems (e.g., ELK stack, Splunk, Datadog) allowing for easier searching, filtering, and analysis of log data, especially in distributed environments. Context variables (`structlog.contextvars`) can be used to add contextual information (like `stream_id`) to all log messages within a specific task.

2.  **Implement Prometheus Metrics Exporter:**

    - **Action:**

      - Add `prometheus_client` to `pyproject.toml` dependencies.
      - Create `src/metrics.py` to define and initialize metrics.

        ```python
        # src/metrics.py
        from prometheus_client import Counter, Gauge, Histogram, start_http_server
        import logging # Use standard logging or structlog for this internal setup log
        from src.config import settings

        logger = logging.getLogger(__name__) # Or structlog.get_logger() after setup

        # --- Define Metrics ---
        VIDEO_CHUNKS_UPLOADED_TOTAL = Counter(
            "video_chunks_uploaded_total",
            "Total number of video chunks successfully uploaded.",
            ["stream_id"] # Add stream_id label
        )

        VIDEO_BYTES_UPLOADED_TOTAL = Counter(
            "video_bytes_uploaded_total",
            "Total number of bytes successfully uploaded for video streams.",
            ["stream_id"]
        )

        # Consider Histogram for duration to get buckets
        VIDEO_STREAM_DURATION_SECONDS = Histogram(
            "video_stream_duration_seconds",
            "Duration of processed video streams in seconds. Obtained from ffprobe (Phase 6) during metadata generation.",
            ["stream_id"],
            buckets=(5, 10, 30, 60, 120, 300, 600, 1800, 3600) # Example buckets (seconds)
        )

        VIDEO_PROCESSING_TIME_SECONDS = Histogram(
            "video_processing_time_seconds",
            "Time taken to process a video stream from initial detection (e.g., StreamProcessor creation) to successful metadata upload, in seconds.",
            ["stream_id"],
            buckets=(10, 30, 60, 120, 300, 600, 1800, 3600, 7200)
        )

        VIDEO_FAILED_OPERATIONS_TOTAL = Counter(
            "video_failed_operations_total",
            "Total number of failed operations related to video processing.",
            ["stream_id", "operation_type"] # e.g., s3_upload, redis_update, ffprobe, metadata_gen
        )

        ACTIVE_STREAMS_GAUGE = Gauge(
            "active_streams_gauge",
            "Current number of actively processing video streams."
        )

        STREAMS_COMPLETED_TOTAL = Counter(
            "streams_completed_total",
            "Total number of streams successfully processed and completed."
        )

        STREAMS_FAILED_TOTAL = Counter(
            "streams_failed_total",
            "Total number of streams that ended in a failed state."
        )
        # --- End Define Metrics ---

        def start_metrics_server():
            """Starts the Prometheus metrics HTTP server."""
            try:
                port = settings.prom_port
                start_http_server(port)
                logger.info(f"Prometheus metrics server started on port {port}")
            except Exception as e:
                logger.error(f"Could not start Prometheus metrics server: {e}", exc_info=True)

        # Call start_metrics_server() in your main application startup (e.g., in main.py)
        # Ensure it's called in a way that doesn't block the main asyncio loop if main is async.
        # Usually, start_http_server runs in its own thread.
        ```

      - In `src/main.py`, import `start_metrics_server` from `src.metrics` and call it during application startup. Since `start_http_server` is blocking, if `main.py` is `async`, run it in a separate thread using `loop.run_in_executor`.
        ```python
        # In src/main.py (async main example)
        # import asyncio
        # from src.metrics import start_metrics_server
        # from functools import partial
        #
        # async def main():
        #     # ... other setup ...
        #     loop = asyncio.get_running_loop()
        #     await loop.run_in_executor(None, partial(start_metrics_server)) # Runs in default thread pool
        #     # ... rest of async main ...
        ```
      - Add `PROM_PORT: int = 8000` to `src/config.py`.

    - **Reasoning:** Prometheus is a widely adopted open-source monitoring and alerting toolkit. Exporting metrics allows for tracking application behavior, performance bottlenecks, error rates, and resource usage over time. This data can be visualized with tools like Grafana and used to set up alerts.

3.  **Update Application Code to Emit Metrics:**

    - **Action:** Go through the codebase and increment/set metrics at relevant points.
      - `src/stream_processor.py`:
        - In `__init__` or when a stream becomes active: `ACTIVE_STREAMS_GAUGE.inc()`.
        - When a stream truly finishes (success or permanent fail): `ACTIVE_STREAMS_GAUGE.dec()`.
        - `_upload_chunk_to_s3` (on success): `VIDEO_CHUNKS_UPLOADED_TOTAL.labels(stream_id=self.stream_id).inc()`, `VIDEO_BYTES_UPLOADED_TOTAL.labels(stream_id=self.stream_id).add(chunk_size)`.
        - `finalize_stream` (on metadata success):
          - `VIDEO_STREAM_DURATION_SECONDS.labels(stream_id=self.stream_id).observe(duration_from_ffprobe_result)` (value from Phase 6).
          - `STREAMS_COMPLETED_TOTAL.inc()`.
          - Calculate processing time: `end_time = datetime.now(timezone.utc)`, `start_time = datetime.fromisoformat(self.redis_meta.get('started_at_utc'))`, `processing_duration = (end_time - start_time).total_seconds()`. Then `VIDEO_PROCESSING_TIME_SECONDS.labels(stream_id=self.stream_id).observe(processing_duration)`.
        - Error paths (e.g., S3 failure, ffprobe failure): `VIDEO_FAILED_OPERATIONS_TOTAL.labels(stream_id=self.stream_id, operation_type="s3_upload_part").inc()`. When marking a stream as finally failed: `STREAMS_FAILED_TOTAL.inc()`.
      - `src/redis_client.py` / `src/s3_client.py`: Can also increment `VIDEO_FAILED_OPERATIONS_TOTAL` for specific operation failures if not caught and labeled more specifically by `StreamProcessor`.
    - **Reasoning:** This step connects the application's actual operations to the defined metrics, making them reflect real-time activity.

4.  **Update Docker Configuration for Metrics:**

    - **Action:**
      - In `Dockerfile`: `EXPOSE ${PROM_PORT}` (ensure `ARG PROM_PORT` and `ENV PROM_PORT=$PROM_PORT` are set if using build args).
      - In `docker-compose.yml` for the `app` service:
        ```yaml
        # services:
        #   app:
        #     ports:
        #       - "${PROM_PORT:-8000}:${PROM_PORT:-8000}" # Expose PROM_PORT
        #     environment:
        #       - PROM_PORT=${PROM_PORT:-8000}
        ```
    - **Reasoning:** Makes the `/metrics` endpoint accessible from outside the container, allowing Prometheus to scrape it.

5.  **Documentation (README & `example.env` Updates):**

    - **Action:**
      - Update `README.md` to explain:
        - The logging setup (`LOG_LEVEL`, structured JSON).
        - How to access the `/metrics` endpoint (e.g., `http://localhost:8000/metrics`).
        - A brief description of key exposed metrics.
      - Add `PROM_PORT=8000` and `LOG_LEVEL=INFO` to `example.env`.
      - (Optional) Provide a sample `prometheus.yml` scrape configuration snippet in the README for users who want to set up Prometheus to scrape the application.
        ```yaml
        # prometheus.yml example snippet
        # scrape_configs:
        #   - job_name: 'cefmp4-processor'
        #     static_configs:
        #       - targets: ['<host_running_docker_or_app_ip>:${PROM_PORT}'] # e.g., localhost:8000 if running locally
        ```
    - **Reasoning:** Guides users on how to utilize and configure the new observability features.

6.  **Unit/Integration Tests:**
    - **Action:**
      - Test `src/logging_config.py`: Verify that `structlog` is configured and logs are produced in the expected format (e.g., JSON for production, console for dev). This might involve capturing log output.
      - Test `src/metrics.py`:
        - Verify metrics are registered with the `prometheus_client` default registry.
        - In integration tests or specific unit tests for `StreamProcessor`, check that metric values are incremented as expected after certain operations. (e.g., after a chunk upload, `VIDEO_CHUNKS_UPLOADED_TOTAL` should increase).
        - Test that the `/metrics` endpoint is started and serves data (can be an integration test that curls the endpoint).
    - **Reasoning:** Ensures the observability mechanisms are correctly implemented and functional.

**Expected Outcome:**

- All application logs are structured (JSON by default in production) and include contextual information, timestamps, log levels, and logger names.
- Key application events and states are instrumented with Prometheus metrics (counters, gauges, histograms).
- A `/metrics` HTTP endpoint is available on `PROM_PORT` for Prometheus to scrape.
- The Docker setup exposes `PROM_PORT`.
- README is updated with information on logging and metrics.

**Next Phase:** Phase 10: Finalizing Dockerization, CI, Documentation & Testing. This will involve polishing the application, setting up a CI pipeline, completing documentation, and performing thorough end-to-end testing.
