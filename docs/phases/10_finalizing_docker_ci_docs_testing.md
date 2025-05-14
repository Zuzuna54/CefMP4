# Phase 10: Finalizing Dockerization, CI, Documentation & Testing

**Objective:** Polish the application, establish a robust CI pipeline, complete all documentation, and perform comprehensive end-to-end testing to ensure the system is ready for use.

**Builds upon:** All previous phases (1-9).

**Steps & Design Choices:**

1.  **Finalize Dockerization (`Dockerfile` & `docker-compose.yml`):**

    - **Action (`Dockerfile`):**
      - Review and optimize `Dockerfile` for production.
      - Implement multi-stage builds: Use a build stage with full build dependencies (compilers, dev headers if any Python packages need them) and a final lighter-weight stage copying only necessary artifacts (Python runtime, application code, `ffprobe`). This reduces the final image size.
        - For `ffprobe`: The `ffmpeg` package (which includes `ffprobe`) should be installed in the final stage. If a specific version is needed or to minimize bloat, a build stage could compile `ffmpeg` from source and then only `ffprobe` (and its direct library dependencies if statically linked) could be copied, but installing the package from the distro's repositories (e.g., `apt-get install -y ffmpeg` on Debian-based images) is often simpler if the resulting image size is acceptable.
      - Ensure a non-root user is used to run the application in the final stage for security (`USER appuser`).
      - Verify `dumb-init` is correctly set as the entrypoint to handle signals properly.
      - Ensure all necessary environment variables (e.g., `PYTHONUNBUFFERED=1`, `PYTHONIOENCODING=UTF-8`) are set.
      - Parameterize versions (e.g., Python version) using `ARG` where appropriate.
      - Clean up any unnecessary layers or commands.
    - **Action (`docker-compose.yml`):**
      - Review and finalize `docker-compose.yml` for local development and testing.
      - Ensure all services (app, MinIO, Redis) are correctly configured with persistent volumes where needed (e.g., for MinIO data, Redis data if persistence is desired for local dev, though for this app Redis is more of a cache/checkpoint store).
      - Use `.env` file for all configurable parameters (ports, credentials, etc.) and reference them in `docker-compose.yml` (e.g., `${MINIO_ROOT_USER}`).
      - Add healthchecks for MinIO and Redis services to ensure they are ready before the application starts, or that the application handles their temporary unavailability gracefully.
      - Ensure resource limits (CPU, memory) can be easily configured if needed (though often managed by orchestrators in production).
      - Consider adding a healthcheck for the `app` service itself, utilizing the Prometheus client's built-in health endpoints:
        ```yaml
        # services:
        #   app:
        #     healthcheck:
        #       test: ["CMD", "curl", "-f", "http://localhost:${PROM_PORT:-8000}/-/healthy"]
        #       interval: 30s
        #       timeout: 10s
        #       retries: 3
        #       start_period: 30s # Give app time to initialize, including metrics server
        ```
    - **Reasoning:** A production-ready Docker setup is crucial for security, efficiency, and reproducibility. Multi-stage builds minimize attack surface and image size. Non-root users enhance security. A well-configured `docker-compose.yml` simplifies local development and testing environments.

2.  **Create Comprehensive `README.md`:**

    - **Action:** Expand and finalize the main `README.md` file. It should include:
      - Project overview and purpose.
      - Prerequisites (Docker, Docker Compose, Python if contributing).
      - **Configuration:**
        - Detailed explanation of all environment variables in `example.env` (and their defaults if any).
        - How to create and use `.env` from `example.env`.
      - **Setup & Running:**
        - Step-by-step instructions to build and run the application using `docker-compose up --build`.
        - How to simulate file events (e.g., copying an MP4 into `WATCH_DIR`).
        - Expected output (S3 objects, metadata JSON, logs).
      - **Accessing Services:**
        - MinIO Console URL and credentials.
        - Redis (e.g., `redis-cli` access if needed for debugging).
        - Prometheus `/metrics` endpoint.
      - **Development:**
        - Instructions for setting up a local development environment (if different from just Docker, e.g., virtualenv, installing dependencies with Poetry/pip).
        - How to run linters, type checkers, and tests.
      - **Troubleshooting:** Common issues and solutions.
      - **Project Structure:** Brief overview of key directories and files.
      - **Observability:** Notes on logging (JSON format) and metrics (Prometheus).
      - (Optional) High-level architecture diagram.
    - **Reasoning:** The `README` is the primary entry point for users and developers. It must be clear, concise, and provide all necessary information to get started and understand the application.

3.  **Set Up CI Pipeline (`.github/workflows/ci.yml`):**

    - **Action:** Create a GitHub Actions workflow (or equivalent for other CI providers).
      - Define triggers (e.g., on push to `main`/`develop`, on pull requests).
      - Set up jobs for:
        - **Linting:** Use `ruff check .` (or `flake8`, `pylint`).
        - **Formatting Check:** Use `ruff format --check .` (or `black --check .`, `isort --check-only .`).
        - **Type Checking:** Use `mypy src tests`.
        - **Unit Tests:**
          - Set up Python.
          - Install dependencies. If using Poetry (implied by `pyproject.toml` modifications in earlier phases), use `poetry install --with dev`. If managing `requirements.txt` files manually or via `poetry export`, ensure `requirements.txt` and `requirements-dev.txt` are up-to-date and used for installation (e.g., `pip install -r requirements.txt -r requirements-dev.txt`). For consistency, it's recommended to choose one primary method.
          - Run unit tests using `pytest` (or `unittest`).
          - Optionally, generate and upload code coverage reports (e.g., using `pytest-cov` and Codecov/Coveralls).
        - **(Optional but Recommended) Integration Test:**
          - This job would need to run `docker-compose up -d minio redis` to have dependencies ready.
          - A small, legally appropriate sample MP4 file (e.g., `test_data/sample.mp4`) should be added to the repository for use by this test.
          - Then run a script (e.g., `tests/run_integration_test.sh` or a Python script) that:
            1.  Builds and starts the application container (`docker-compose up -d --build app`).
            2.  Waits for the app to be healthy (e.g., check logs or its healthcheck status if implemented in `docker-compose.yml`).
            3.  Copies the `sample.mp4` file into the `WATCH_DIR` (e.g., `docker cp test_data/sample.mp4 <container_id>:/app/watch_dir/`).
            4.  Polls MinIO (or checks Redis/logs) to verify the MP4 is uploaded and its `.metadata.json` file is created within a timeout.
            5.  Cleans up (copies file out of watch dir or deletes from S3, `docker-compose down -v`).
          - This ensures the core end-to-end flow works.
      - Ensure the CI pipeline runs on a suitable environment (e.g., `ubuntu-latest`).
      - Cache dependencies to speed up builds.
    - **Reasoning:** A CI pipeline automates testing and quality checks, ensuring code changes don't break existing functionality and maintain code standards. This leads to higher quality software and faster development cycles.

4.  **Complete All Unit Tests and Aim for High Coverage:**

    - **Action:**
      - Review all modules and ensure comprehensive unit tests are written for all critical logic.
      - Focus on `StreamProcessor`, `s3_client`, `redis_client`, `ffprobe_utils`, `metadata_generator`, `watcher`, and `main` (especially startup/shutdown and resume logic).
      - Use mocking extensively (e.g., `unittest.mock.AsyncMock` for async functions, mock S3/Redis clients, `ffprobe` subprocess calls).
      - Aim for high test coverage (e.g., >80-90%). Use `pytest-cov` to measure this.
      - Ensure tests cover success paths, failure paths, edge cases, and idempotency where relevant.
    - **Reasoning:** Thorough unit tests provide confidence in the correctness of individual components and make refactoring safer.

5.  **Thorough Manual End-to-End Testing:**

    - **Action:** Perform manual tests covering various scenarios:
      - **Happy Path:** Process a small, medium, and large MP4 file successfully.
      - **Interruption & Resume:**
        - Start processing, stop the application (Ctrl+C), restart it. Verify processing resumes and completes.
        - Simulate S3/Redis being temporarily unavailable during processing (if possible to mock this locally without too much effort, or just conceptually verify the retry/resume logic should handle it).
      - **Error Conditions:**
        - File disappears from `WATCH_DIR` mid-processing. Verify the application handles this gracefully (e.g., `StreamProcessor` logs a warning/error and marks the stream as failed in Redis using `add_stream_to_failed_set` as per Phase 4/7/8 refinements. This should be tested).
        - Invalid file (not an MP4, or corrupted) – observe ffprobe errors and graceful failure.
        - S3 credentials incorrect (app should fail to start or log errors clearly).
        - Redis unavailable on startup (app should fail to start or retry connecting).
      - **Concurrent Processing:** Drop multiple files into `WATCH_DIR` simultaneously. Verify the `stream_processing_semaphore` (from Phase 8) correctly limits concurrent processing according to `MAX_CONCURRENT_STREAMS` (ensure this is a configurable setting from `src/config.py` via environment variable).
      - **Configuration Changes:** Test with different `CHUNK_SIZE`, `STREAM_TIMEOUT` values.
      - Verify logs (structured JSON) and metrics (`/metrics` endpoint) output during these tests.
      - Check S3 bucket for correct file structure and metadata content.
      - Check Redis keys during and after processing (ensure cleanup on success/failure).
    - **Reasoning:** Manual E2E testing validates the entire system working together as expected and can uncover issues missed by automated tests, especially related to operational aspects and complex interactions.

6.  **Finalize `example.env` and `requiremnts.md`:**
    - **Action:**
      - Ensure `example.env` contains all configurable parameters with sensible defaults and comments.
      - Review `requiremnts.md` (the initial project requirements document) and cross-check that all specified requirements have been met by the implemented phases. Add notes or clarifications if some requirements were adjusted or interpreted differently during development, along with reasoning.
    - **Reasoning:** Ensures configuration is clear and all initial goals are verifiably addressed.

**Expected Outcome:**

- A production-optimized `Dockerfile` and a developer-friendly `docker-compose.yml`.
- A comprehensive `README.md` that allows anyone to understand, set up, and run the application.
- An automated CI pipeline on GitHub Actions (or similar) that runs linters, type checkers, and tests (unit and integration).
- High unit test coverage across the codebase.
- Successful completion of thorough manual end-to-end testing, validating all core features and resilience.
- All initial requirements from `requiremnts.md` are met and documented.
- The application is considered complete and ready for its intended use or further operational deployment.
