# Phase 3: Initial Stream State Management & S3 Multipart Upload Initiation

**Objective:** Upon detecting a new video file (`CREATE` event), generate a unique stream ID, initialize its state in Redis, and initiate an S3 multipart upload.

**Builds upon:** Phase 1 (Config, Docker), Phase 2 (Watcher, StreamEvent, `main.py` event handling)

**Steps & Design Choices:**

1.  **Add Dependencies:**

    - **Action:** Add `redis` (with async support) and `aioboto3` (for S3) to `pyproject.toml`.
      ```toml
      # [project.dependencies]
      # ...
      # "redis[hiredis] >= 4.0.0", # hiredis is optional but recommended for performance
      # "aioboto3",
      # "uuid" # Standard library, uuid is used but not listed as a project dependency.
      ```
    - **Reasoning:** `redis-py` provides async capabilities needed for an `asyncio` application. `aioboto3` is the async version of `boto3` for S3 interactions, as per `requiremnts.md` guidelines. The `uuid` module from the standard library is used for generating unique stream IDs.

2.  **Implement Redis Client Wrapper (`src/redis_client.py`):**

    - **Action:** Create `src/redis_client.py`.

      ```python
      import logging
      import redis.asyncio as redis
      from src.config import settings

      logger = logging.getLogger(__name__) # To be replaced by structlog

      # Global Redis connection pool
      _redis_pool: redis.Redis | None = None

      async def get_redis_connection() -> redis.Redis:
          global _redis_pool
          if _redis_pool is None:
              try:
                  logger.info(f"Initializing Redis connection pool for URL: {settings.redis_url}")
                  _redis_pool = await redis.from_url(settings.redis_url, decode_responses=True)
                  # Test connection
                  await _redis_pool.ping()
                  logger.info("Redis connection successful.")
              except Exception as e:
                  logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
                  _redis_pool = None # Ensure it's None if connection failed
                  raise
          return _redis_pool

      async def close_redis_connection():
          global _redis_pool
          if _redis_pool:
              logger.info("Closing Redis connection pool.")
              await _redis_pool.close()
              _redis_pool = None

      # --- Stream State Functions ---
      async def init_stream_metadata(stream_id: str, file_path: str, s3_upload_id: str, s3_bucket: str, s3_key_prefix: str):
          r = await get_redis_connection()
          # Using ISO format for timestamps
          import datetime
          now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

          stream_meta_key = f"stream:{stream_id}:meta"
          async with r.pipeline(transaction=True) as pipe:
              pipe.hset(stream_meta_key, mapping={
                  "original_path": file_path,
                  "s3_upload_id": s3_upload_id,
                  "s3_bucket": s3_bucket,
                  "s3_key_prefix": s3_key_prefix,
                  "status": "processing", # Initial status
                  "started_at_utc": now_iso,
                  "last_activity_at_utc": now_iso,
                  "total_bytes_expected": -1, # Unknown initially
              })
              # Add to active streams set
              pipe.sadd("streams:active", stream_id)
              await pipe.execute()
          logger.info(f"Initialized metadata for stream {stream_id} (file: {file_path}) in Redis.")

      async def set_stream_status(stream_id: str, status: str):
          r = await get_redis_connection()
          now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
          await r.hset(f"stream:{stream_id}:meta", mapping={
              "status": status,
              "last_activity_at_utc": now_iso
          })
          logger.info(f"Set status for stream {stream_id} to {status}")

      async def add_stream_to_active_set(stream_id: str):
          r = await get_redis_connection()
          await r.sadd("streams:active", stream_id)

      async def remove_stream_from_active_set(stream_id: str):
          r = await get_redis_connection()
          await r.srem("streams:active", stream_id)

      # More Redis functions will be added in subsequent phases (e.g., for parts, next_part)
      ```

    - **Reasoning:**
      - Provides a centralized way to get an async Redis connection (pool).
      - Includes functions for initializing stream metadata in a Redis hash and managing the `streams:active` set, following the schema in `redis_checkpointing.mdc`.
      - Connection pooling is good practice for managing connections efficiently.
      - `decode_responses=True` is convenient for working with strings directly.

3.  **Implement S3 Client Wrapper & Multipart Initiation (`src/s3_client.py`):**

    - **Action:** Create `src/s3_client.py`.

      ```python
      import logging
      import aioboto3
      from botocore.exceptions import ClientError
      from src.config import settings

      logger = logging.getLogger(__name__) # To be replaced by structlog

      # Global S3 session and client (consider if session needs to be per-request/task or global)
      _s3_session: aioboto3.Session | None = None

      def get_s3_session() -> aioboto3.Session:
          global _s3_session
          if _s3_session is None:
              _s3_session = aioboto3.Session()
          return _s3_session

      async def create_s3_multipart_upload(bucket_name: str, object_key: str) -> str | None:
          session = get_s3_session()
          async with session.client("s3",
                                  endpoint_url=settings.s3_endpoint_url,
                                  aws_access_key_id=settings.s3_access_key_id,
                                  aws_secret_access_key=settings.s3_secret_access_key,
                                  region_name=settings.s3_region_name) as s3:
              try:
                  response = await s3.create_multipart_upload(
                      Bucket=bucket_name,
                      Key=object_key
                      # TODO: Consider adding Metadata like original filename, content_type if known
                      # ContentType='video/mp4' # If known
                  )
                  upload_id = response.get('UploadId')
                  logger.info(f"Initiated S3 multipart upload for {bucket_name}/{object_key}. Upload ID: {upload_id}")
                  return upload_id
              except ClientError as e:
                  logger.error(f"Error creating S3 multipart upload for {bucket_name}/{object_key}: {e}", exc_info=True)
                  return None
              except Exception as e:
                  logger.error(f"Unexpected error during S3 multipart upload initiation for {bucket_name}/{object_key}: {e}", exc_info=True)
                  return None

      async def close_s3_resources():
          # aioboto3 clients are typically managed with context managers (async with)
          # Explicitly closing a global session isn't standard if clients are short-lived.
          # If a long-lived client were used, it would be closed here.
          logger.info("S3 resources are managed by context managers; no explicit global close needed for session.")
          pass

      # Other S3 functions (upload_part, complete_multipart_upload, etc.) will be added in later phases.
      ```

    - **Reasoning:**
      - Provides a way to get an `aioboto3` S3 client.
      - `create_s3_multipart_upload` function encapsulates the logic to initiate an S3 multipart upload as per S3 best practices and `s3_handling.mdc`.
      - Uses configured S3 endpoint, credentials, and bucket name.
      - Error handling for S3 operations is included.

4.  **Update `src/main.py` to Handle `CREATE` Events:**

    - **Action:** Modify `src/main.py` to use the Redis and S3 clients.

      ```python
      import asyncio
      import logging
      from pathlib import Path
      import uuid # For generating stream_id

      from src.config import settings
      from src.watcher import video_file_watcher
      from src.events import StreamEvent, WatcherChangeType # Updated import
      from src.redis_client import init_stream_metadata, close_redis_connection # New imports
      from src.s3_client import create_s3_multipart_upload, close_s3_resources # New imports

      logging.basicConfig(level=settings.log_level.upper(),
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      logger = logging.getLogger(__name__)

      # Dictionary to keep track of active stream processing tasks
      # Key: stream_id (str), Value: asyncio.Task
      active_stream_tasks: dict[str, asyncio.Task] = {}

      async def process_new_stream(event: StreamEvent):
          stream_id = str(uuid.uuid4())
          file_path_str = str(event.file_path)
          logger.info(f"Processing new stream {stream_id} for file: {file_path_str}")

          # Define S3 object key (e.g., using filename or a UUID structure)
          # For simplicity, using stream_id and original filename to ensure uniqueness
          s3_object_key_prefix = f"streams/{stream_id}/{event.file_path.name}"

          try:
              # 1. Initiate S3 multipart upload
              s3_upload_id = await create_s3_multipart_upload(
                  bucket_name=settings.s3_bucket_name,
                  object_key=s3_object_key_prefix # This will be the base key for parts
              )

              if not s3_upload_id:
                  logger.error(f"Failed to initiate S3 multipart upload for stream {stream_id}. Aborting processing for this stream.")
                  return

              # 2. Initialize stream metadata in Redis
              await init_stream_metadata(
                  stream_id=stream_id,
                  file_path=file_path_str,
                  s3_upload_id=s3_upload_id,
                  s3_bucket=settings.s3_bucket_name,
                  s3_key_prefix=s3_object_key_prefix
              )

              logger.info(f"Successfully initialized stream {stream_id} for {file_path_str}. S3 Upload ID: {s3_upload_id}")
              # In the next phase, we will start the chunk processing loop here for this stream_id.

          except Exception as e:
              logger.error(f"Error processing new stream {stream_id} for {file_path_str}: {e}", exc_info=True)
              # TODO: Implement cleanup logic (e.g., abort S3 multipart upload, mark stream as failed in Redis)

      async def handle_stream_event(event: StreamEvent):
          logger.info(f"Received event: Type={event.change_type.name}, Path={event.file_path}")

          if event.change_type == WatcherChangeType.CREATE:
              # Launch as a separate task to keep the event handler non-blocking.
              # Full task management (tracking, cleanup, cancellation) will be addressed in later phases.
              asyncio.create_task(process_new_stream(event), name=f"process_stream_{event.file_path.name}")

          elif event.change_type == WatcherChangeType.WRITE:
              # Find the stream_id associated with event.file_path (will be implemented in Phase 4)
              logger.info(f"WRITE event for {event.file_path}. Chunk processing will be handled in Phase 4.")
              # This event will trigger reading chunks and uploading them for an existing stream_id

          elif event.change_type == WatcherChangeType.IDLE:
              logger.info(f"IDLE event for {event.file_path}. Stream finalization will be handled in Phase 5.")
              # This event will trigger finalization of an existing stream_id

          elif event.change_type == WatcherChangeType.DELETE:
              logger.info(f"DELETE event for {event.file_path}. Optional cleanup will be handled later.")
              # Optional: cleanup logic if a file is deleted mid-processing

      async def main():
          logger.info("Application starting...")
          # ... (existing setup code for watch_dir) ...
          watch_dir = Path(settings.watch_dir)
          if not watch_dir.exists():
              logger.warning(f"Watch directory {watch_dir} does not exist. Creating it.")
              watch_dir.mkdir(parents=True, exist_ok=True)
          elif not watch_dir.is_dir():
              logger.error(f"Watch path {watch_dir} exists but is not a directory. Exiting.")
              return

          stop_event = asyncio.Event()
          try:
              # Ensure Redis connection is available before starting watcher
              await get_redis_connection()

              async for event in video_file_watcher(watch_dir, settings.stream_timeout_seconds, stop_event):
                  await handle_stream_event(event)
          except KeyboardInterrupt:
              logger.info("Keyboard interrupt received...")
          except Exception as e:
              logger.error(f"Unhandled exception in main loop: {e}", exc_info=True)
          finally:
              logger.info("Shutting down... Signaling watcher to stop.")
              stop_event.set()
              # active_tasks = [task for task in active_stream_tasks.values() if not task.done()]
              # if active_tasks:
              #     logger.info(f"Waiting for {len(active_tasks)} active stream tasks to complete...")
              #     await asyncio.gather(*active_tasks, return_exceptions=True)
              await asyncio.sleep(0.5) # Give watcher a moment to stop
              await close_redis_connection()
              await close_s3_resources()
              logger.info("Application stopped.")

      # ... (if __name__ == "__main__") ...
      ```

    - **Reasoning:**
      - On `CREATE` events, `process_new_stream` is called.
      - `uuid.uuid4()` generates a unique ID for the stream.
      - Calls `create_s3_multipart_upload` and then `init_stream_metadata`.
      - Error handling is included for these critical initialization steps.
      - Placeholder for `active_stream_tasks` to manage concurrent stream processing (full implementation in later phases).
      - Ensures Redis connection is attempted before starting and closed during shutdown.

5.  **Initial Unit Tests (`tests/test_stream_init.py`, `tests/test_redis_client.py`, `tests/test_s3_client.py`):**

    - **Action:** Create basic test files. Use `pytest-asyncio` and mocking libraries like `unittest.mock` or `pytest-mock`.
      - `tests/test_redis_client.py`: Test `init_stream_metadata` (mock Redis connection, verify commands).
      - `tests/test_s3_client.py`: Test `create_s3_multipart_upload` (mock `aioboto3` client, verify parameters, mock response).
      - `tests/test_main_event_handling.py` (or extend `test_watcher.py`): Test that a `CREATE` event in `main.py` correctly calls the `process_new_stream` pathway (mocking out `create_s3_multipart_upload` and `init_stream_metadata`).
    - **Reasoning:** Ensures the core components for stream initialization work as expected in isolation.
    - **Example Snippet for `tests/test_s3_client.py`:**

      ```python
      import pytest
      from unittest.mock import AsyncMock, patch
      from src.s3_client import create_s3_multipart_upload
      from src.config import settings

      @pytest.mark.asyncio
      async def test_create_s3_multipart_upload_success():
          mock_s3_client = AsyncMock()
          mock_s3_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}

          # Patch the context manager part of aioboto3 session.client
          async def async_context_manager(*args, **kwargs):
              return mock_s3_client
          mock_s3_client.__aenter__ = AsyncMock(return_value=mock_s3_client)
          mock_s3_client.__aexit__ = AsyncMock(return_value=None)

          with patch('src.s3_client.get_s3_session') as mock_get_session:
              mock_session_instance = AsyncMock()
              mock_session_instance.client.return_value = mock_s3_client # context manager object
              mock_get_session.return_value = mock_session_instance

              upload_id = await create_s3_multipart_upload("test-bucket", "test-key/video.mp4")

              assert upload_id == "test-upload-id"
              mock_session_instance.client.assert_called_once_with(
                  "s3",
                  endpoint_url=settings.s3_endpoint_url,
                  aws_access_key_id=settings.s3_access_key_id,
                  aws_secret_access_key=settings.s3_secret_access_key,
                  region_name=settings.s3_region_name
              )
              mock_s3_client.create_multipart_upload.assert_called_once_with(
                  Bucket="test-bucket",
                  Key="test-key/video.mp4"
              )
      ```

**Docker & Dependencies Update:**

- The `Dockerfile` will need `redis`, `aioboto3` (and its dependencies like `botocore`, `aiohttp`). This will be handled when `requirements.txt` generation or `pip install .[all]` is implemented.

**Expected Outcome:**

- When a new `.mp4` file is detected:
  - A unique `stream_id` is generated.
  - An S3 multipart upload is initiated for the file.
  - Initial stream metadata (including `stream_id`, file path, S3 `upload_id`) is stored in Redis.
  - The `stream_id` is added to the `streams:active` set in Redis.
- Basic unit tests for Redis and S3 client interactions are in place.

**Next Phase:** Phase 4 will implement the core chunk processing loop: reading file chunks and uploading them to S3 as parts of the previously initiated multipart upload.
