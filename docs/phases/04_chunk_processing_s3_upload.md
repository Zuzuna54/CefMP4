# Phase 4: Chunk Processing and S3 Upload (Core Loop)

**Objective:** Implement the logic to read incoming video files in chunks, upload these chunks as parts to the S3 multipart upload initiated in Phase 3, and update checkpoint information in Redis.

**Builds upon:** Phase 1 (Config), Phase 2 (Watcher, Events), Phase 3 (Stream Init, S3 Multipart Init, Redis Client)

**Steps & Design Choices:**

1.  **Define Stream Processor (`src/stream_processor.py`):**

    - **Action:** Create `src/stream_processor.py` to encapsulate the logic for processing an individual stream.

      ```python
      import asyncio
      import logging
      import os
      from pathlib import Path
      import datetime # For timestamping parts

      from src.config import settings
      # Redis client functions for getting/setting parts, next_part, bytes_sent
      # S3 client functions for uploading parts
      # These will be imported once created/extended in their respective client files.

      logger = logging.getLogger(__name__) # To be replaced by structlog

      class StreamProcessor:
          def __init__(self, stream_id: str, file_path: Path, s3_upload_id: str, s3_bucket: str, s3_key_prefix: str):
              self.stream_id = stream_id
              self.file_path = file_path
              self.s3_upload_id = s3_upload_id
              self.s3_bucket = s3_bucket
              self.s3_key_prefix = s3_key_prefix # Base key for the stream, parts will be relative or have part number in key

              self.current_file_offset = 0 # Bytes already read and processed
              self.next_part_number = 1    # S3 part numbers are 1-indexed
              self.uploaded_parts_info = [] # List of {"PartNumber": int, "ETag": str, "Size": int}
              self.is_processing = False
              self.lock = asyncio.Lock() # To prevent concurrent processing of the same file path if events overlap

          async def _initialize_from_checkpoint(self):
              # This method will be fully implemented in Phase 7 (Resume Logic) to load state from Redis.
              # For Phase 4 (new streams), it ensures that if any relevant keys were hypothetically in Redis
              # (e.g. from a partially failed previous attempt not yet cleaned up), they might be picked up.
              # However, for a brand new stream, redis calls will return None, and the processor
              # will use its default initial values (next_part_number=1, current_file_offset=0).
              from src.redis_client import get_stream_next_part, get_stream_bytes_sent, get_stream_parts

              next_part_redis = await get_stream_next_part(self.stream_id)
              if next_part_redis is not None:
                  self.next_part_number = next_part_redis

              bytes_sent_redis = await get_stream_bytes_sent(self.stream_id)
              if bytes_sent_redis is not None:
                  # How current_file_offset relates to bytes_sent needs careful consideration.
                  # If bytes_sent is purely S3 payload, offset might be different if headers/etc are involved.
                  # For now, assume they are closely related or equal for uploaded data.
                  self.current_file_offset = bytes_sent_redis

              parts_redis = await get_stream_parts(self.stream_id)
              if parts_redis:
                  # Assuming parts_redis is a list of dicts like {"PartNumber": ..., "ETag": ...}
                  # This structure depends on how get_stream_parts is implemented
                  self.uploaded_parts_info = parts_redis

              logger.info(f"[{self.stream_id}] Initialized processor state: next_part={self.next_part_number}, offset={self.current_file_offset}, known_parts={len(self.uploaded_parts_info)}")


          async def process_file_write(self):
              """Handles a WRITE event from the watcher, reads new data, chunks, and uploads."""
              async with self.lock:
                  if not self.file_path.exists():
                      logger.warning(f"[{self.stream_id}] File {self.file_path} no longer exists. Stopping processing for this stream.")
                      # TODO: Mark stream as failed/aborted in Redis
                      return

                  try:
                      file_size = self.file_path.stat().st_size
                      if file_size <= self.current_file_offset:
                          logger.debug(f"[{self.stream_id}] No new data to process for {self.file_path} (size: {file_size}, offset: {self.current_file_offset}).")
                          return

                      logger.info(f"[{self.stream_id}] Processing WRITE for {self.file_path}. Current size: {file_size}, offset: {self.current_file_offset}")

                      # Import S3 upload_part and Redis update functions here to avoid circular dependency at module level
                      from src.s3_client import upload_s3_part
                      from src.redis_client import set_stream_next_part, incr_stream_bytes_sent, add_stream_part_info, update_stream_last_activity

                      with open(self.file_path, "rb") as f:
                          f.seek(self.current_file_offset)
                          while True:
                              chunk = f.read(settings.chunk_size_bytes)
                              if not chunk:
                                  break # End of current file content

                              chunk_len = len(chunk)
                              logger.debug(f"[{self.stream_id}] Read chunk of size {chunk_len}, part number {self.next_part_number}")

                              part_etag = await upload_s3_part(
                                  bucket_name=self.s3_bucket,
                                  object_key=self.s3_key_prefix, # The main object key for the multipart upload
                                  upload_id=self.s3_upload_id,
                                  part_number=self.next_part_number,
                                  data=chunk
                              )

                              if part_etag:
                                  part_info = {"PartNumber": self.next_part_number, "ETag": part_etag, "Size": chunk_len}
                                  self.uploaded_parts_info.append(part_info)

                                  # Checkpoint to Redis
                                  await add_stream_part_info(self.stream_id, self.next_part_number, part_etag, chunk_len)
                                  await incr_stream_bytes_sent(self.stream_id, chunk_len)
                                  await set_stream_next_part(self.stream_id, self.next_part_number + 1)
                                  await update_stream_last_activity(self.stream_id)

                                  self.current_file_offset += chunk_len
                                  self.next_part_number += 1
                                  logger.info(f"[{self.stream_id}] Uploaded part {self.next_part_number-1}, ETag: {part_etag}, new offset: {self.current_file_offset}")
                              else:
                                  logger.error(f"[{self.stream_id}] Failed to upload part {self.next_part_number}. Halting processing for this stream. Manual intervention may be required.")
                                  # TODO: More robust error handling - e.g., retries, mark stream as failed
                                  return # Stop processing this stream on part upload failure

                      logger.info(f"[{self.stream_id}] Finished processing available data for {self.file_path} up to offset {self.current_file_offset}")

                  except FileNotFoundError:
                      logger.warning(f"[{self.stream_id}] File {self.file_path} disappeared during processing.")
                      # TODO: Mark stream as failed/aborted
                  except Exception as e:
                      logger.error(f"[{self.stream_id}] Error processing file write for {self.file_path}: {e}", exc_info=True)
                      # TODO: Mark stream as failed

          async def finalize_stream(self):
               # This method will be called on IDLE event in Phase 5
              logger.info(f"[{self.stream_id}] Stream finalization requested for {self.file_path}.")
              # Logic for CompleteMultipartUpload and metadata generation will go here.
              pass
      ```

    - **Reasoning:**
      - A `StreamProcessor` class manages the state and processing logic for a single file stream.
      - `current_file_offset` tracks how much of the file has been read and processed to handle continuous writes.
      - `next_part_number` keeps track of S3 part numbers (1-indexed).
      - `process_file_write` contains the core loop: open file, seek to offset, read in `chunk_size_bytes`, upload, update Redis.
      - A lock (`asyncio.Lock`) is added to prevent race conditions if multiple `WRITE` events for the same file arrive very quickly, ensuring sequential processing of a single file stream by this instance.
      - `_initialize_from_checkpoint` is a placeholder for resume logic (Phase 7).

2.  **Extend Redis Client (`src/redis_client.py`) for Part Tracking:**

    - **Action:** Add functions to `src/redis_client.py` for managing part numbers, ETags, and bytes sent.

      ```python
      # In src/redis_client.py
      # ... (existing functions) ...

      async def set_stream_next_part(stream_id: str, part_number: int):
          r = await get_redis_connection()
          await r.hset(f"stream:{stream_id}:meta", "next_part_to_upload", part_number)

      async def get_stream_next_part(stream_id: str) -> int | None:
          r = await get_redis_connection()
          val = await r.hget(f"stream:{stream_id}:meta", "next_part_to_upload")
          return int(val) if val else None # Default to 1 if not set, handled by processor logic

      async def incr_stream_bytes_sent(stream_id: str, chunk_size: int):
          r = await get_redis_connection()
          # Store as part of the main stream metadata hash
          new_bytes_sent = await r.hincrby(f"stream:{stream_id}:meta", "total_bytes_sent", chunk_size)
          return new_bytes_sent

      async def get_stream_bytes_sent(stream_id: str) -> int | None:
          r = await get_redis_connection()
          val = await r.hget(f"stream:{stream_id}:meta", "total_bytes_sent")
          return int(val) if val else None

      async def add_stream_part_info(stream_id: str, part_number: int, etag: str, size_bytes: int):
          r = await get_redis_connection()
          # Storing parts in a Redis Hash: parts:{stream_id} -> {part_no: "etag:size_bytes:uploaded_at_iso"}
          # Simpler alternative: Sorted Set if only ETag and order matter, or if JSON encoding is acceptable.
          # The `requiremnts.md` mentions: "Redis hash parts:{stream} stores {part_no:etag}"
          # For metadata.json, more info is needed. We'll store ETag for now, and consider structure for metadata later.
          parts_key = f"stream:{stream_id}:parts"
          uploaded_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
          part_data = f"{etag}:{size_bytes}:{uploaded_at}" # Combine for now, can be JSON if complex
          await r.hset(parts_key, str(part_number), part_data)

      async def get_stream_parts(stream_id: str) -> list[dict]:
          r = await get_redis_connection()
          parts_key = f"stream:{stream_id}:parts"
          raw_parts = await r.hgetall(parts_key)
          # Convert to format expected by S3 CompleteMultipartUpload: List of {'PartNumber': int, 'ETag': str}
          # And also for our internal StreamProcessor state.
          # The stored format is "etag:size_bytes:uploaded_at_iso"
          parsed_parts = []
          for pn_str, data_str in raw_parts.items():
              try:
                  etag, size_str, _ = data_str.split(':', 2)
                  parsed_parts.append({"PartNumber": int(pn_str), "ETag": etag, "Size": int(size_str)})
              except ValueError:
                  logger.warning(f"Could not parse part data '{data_str}' for stream {stream_id}, part {pn_str}")
          # Sort by PartNumber for S3 and consistency
          parsed_parts.sort(key=lambda p: p["PartNumber"])
          return parsed_parts

      async def update_stream_last_activity(stream_id: str):
          r = await get_redis_connection()
          now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
          await r.hset(f"stream:{stream_id}:meta", "last_activity_at_utc", now_iso)
      ```

    - **Reasoning:** Implements Redis interactions needed for checkpointing during chunk uploads, as per `redis_checkpointing.mdc`. `next_part_to_upload` and `total_bytes_sent` are stored in the main stream meta hash. Part ETags are stored in a separate hash `stream:{stream_id}:parts` keyed by part number.

3.  **Extend S3 Client (`src/s3_client.py`) for Part Upload:**

    - **Action:** Add `upload_s3_part` to `src/s3_client.py`.

      ```python
      # In src/s3_client.py
      # ... (existing functions) ...

      async def upload_s3_part(bucket_name: str, object_key: str, upload_id: str, part_number: int, data: bytes) -> str | None:
          session = get_s3_session()
          async with session.client("s3",
                                  endpoint_url=settings.s3_endpoint_url,
                                  aws_access_key_id=settings.s3_access_key_id,
                                  aws_secret_access_key=settings.s3_secret_access_key,
                                  region_name=settings.s3_region_name) as s3:
              try:
                  response = await s3.upload_part(
                      Bucket=bucket_name,
                      Key=object_key,
                      PartNumber=part_number,
                      UploadId=upload_id,
                      Body=data
                  )
                  etag = response.get('ETag')
                  # ETag from S3 often has quotes around it, which need to be included for CompleteMultipartUpload
                  # logger.debug(f"Uploaded part {part_number} for {object_key}, ETag: {etag}")
                  return etag
              except ClientError as e:
                  logger.error(f"Error uploading S3 part {part_number} for {object_key} (Upload ID: {upload_id}): {e}", exc_info=True)
                  return None
              except Exception as e:
                  logger.error(f"Unexpected error during S3 part upload for {object_key}, part {part_number}: {e}", exc_info=True)
                  return None
      ```

    - **Reasoning:** Implements the S3 `upload_part` call. Returns the ETag on success, which is crucial for the `CompleteMultipartUpload` call later.

4.  **Integrate `StreamProcessor` into `src/main.py`:**

    - **Action:** Modify `src/main.py` to create and manage `StreamProcessor` instances.

      ```python
      # In src/main.py
      import asyncio
      import logging
      from pathlib import Path
      import uuid

      from src.config import settings
      from src.watcher import video_file_watcher
      from src.events import StreamEvent, WatcherChangeType
      from src.redis_client import init_stream_metadata, close_redis_connection, get_stream_meta # Added get_stream_meta
      from src.s3_client import create_s3_multipart_upload, close_s3_resources
      from src.stream_processor import StreamProcessor # New import

      logging.basicConfig(level=settings.log_level.upper(),
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      logger = logging.getLogger(__name__)

      # Dictionary to keep track of active stream processors
      # Key: file_path (Path object), Value: StreamProcessor instance
      active_processors: dict[Path, StreamProcessor] = {}

      async def manage_new_stream_creation(event: StreamEvent):
          """Handles the creation and registration of a new stream processor."""
          file_path = event.file_path
          if file_path in active_processors:
              logger.warning(f"[{event.file_path.name}] Received CREATE event for already tracked file. Ignoring or re-validating.")
              # Optionally, could re-verify S3 upload ID or re-init processor if state is inconsistent.
              # For now, assume first CREATE wins.
              # However, if it's truly a new file replacing an old one with the same name, this needs robust handling.
              # This might indicate a need to associate stream_id more directly in active_processors if files can be replaced.
              # For this phase, we keep it simple: one processor per file_path.
              return

          stream_id = str(uuid.uuid4())
          logger.info(f"[{file_path.name}] New stream detected (CREATE). Stream ID: {stream_id}")

          s3_object_key_prefix = f"streams/{stream_id}/{file_path.name}"

          try:
              s3_upload_id = await create_s3_multipart_upload(
                  bucket_name=settings.s3_bucket_name,
                  object_key=s3_object_key_prefix
              )
              if not s3_upload_id:
                  logger.error(f"[{stream_id}] Failed to initiate S3 multipart upload for {file_path.name}. Aborting.")
                  return

              await init_stream_metadata(
                  stream_id=stream_id,
                  file_path=str(file_path),
                  s3_upload_id=s3_upload_id,
                  s3_bucket=settings.s3_bucket_name,
                  s3_key_prefix=s3_object_key_prefix
              )

              processor = StreamProcessor(
                  stream_id=stream_id,
                  file_path=file_path,
                  s3_upload_id=s3_upload_id,
                  s3_bucket=settings.s3_bucket_name,
                  s3_key_prefix=s3_object_key_prefix
              )
              # Call _initialize_from_checkpoint in case there's any remnant state (though unlikely for new stream).
              # This is more for consistency with the resume path later.
              await processor._initialize_from_checkpoint()

              active_processors[file_path] = processor
              logger.info(f"[{stream_id}] StreamProcessor created and registered for {file_path.name}.")

              # Initial data might exist, so trigger a process_file_write immediately after creation.
              # This ensures if a file is created with data already, it gets processed.
              asyncio.create_task(processor.process_file_write(), name=f"initial_write_{stream_id[:8]}")

          except Exception as e:
              logger.error(f"[{stream_id}] Error during new stream setup for {file_path.name}: {e}", exc_info=True)
              # Cleanup of S3/Redis for failed init would be in Phase 8.

      async def handle_stream_event(event: StreamEvent):
          logger.debug(f"Received event: Type={event.change_type.name}, Path={event.file_path}")

          if event.change_type == WatcherChangeType.CREATE:
              asyncio.create_task(manage_new_stream_creation(event), name=f"manage_create_{event.file_path.name}")

          elif event.change_type == WatcherChangeType.WRITE:
              processor = active_processors.get(event.file_path)
              if processor:
                  # Launch as a task to avoid blocking the event loop if many WRITE events come in quickly
                  # The lock within StreamProcessor will handle concurrency for the same file.
                  asyncio.create_task(processor.process_file_write(), name=f"process_write_{processor.stream_id[:8]}")
              else:
                  logger.warning(f"[{event.file_path.name}] WRITE event for untracked file. Was CREATE missed or file appeared suddenly? Re-evaluating...")
                  # This scenario (WRITE without prior CREATE) can happen if the watcher starts after a file exists
                  # or if a file is copied in and first event detected is modify.
                  # We can treat this as a CREATE event.
                  # The watcher logic in Phase 2 already tries to handle this by emitting CREATE then WRITE.
                  # If it still occurs, we might need to trigger a CREATE flow here too.
                  # For now, log a warning. A more robust solution would involve a re-check or attempting to init.
                  # One approach: if processor is None, try to trigger manage_new_stream_creation.
                  # However, need to be careful about race conditions if watcher also sends CREATE.
                  # The watcher in Phase 2 emits CREATE even on first modify if not seen, so this path should be rare.

          elif event.change_type == WatcherChangeType.DELETE:
              logger.info(f"[{event.file_path.name}] DELETE event received.")
              processor = active_processors.pop(event.file_path, None)
              if processor:
                  logger.info(f"[{processor.stream_id}] Removed processor for deleted file {event.file_path.name}.")
                  # Further cleanup (abort S3, Redis state) will be in later phases (e.g., Phase 5 or 8).
                  # For now, just removing from active tracking.
                  # processor.cancel_processing() # A method to stop ongoing tasks for this processor might be needed.
              else:
                  logger.warning(f"[{event.file_path.name}] DELETE event for untracked file.")

          # IDLE events are handled by a separate stale stream checker (Phase 5)

      async def main():
          logger.info("Application starting...")
          # ... (Ensure Redis/S3 clients are available if needed at global scope, or init them)
          # ... (Ensure `setup_logging()` is called if it's part of Phase 9 being integrated gradually)

          watch_dir_str = settings.watch_dir
          stream_timeout = settings.stream_timeout_seconds

          logger.info(f"Watching directory: {watch_dir_str}")
          logger.info(f"Stream timeout for IDLE detection: {stream_timeout} seconds (used by stale checker - Phase 5)")

          watch_dir = Path(watch_dir_str)
          if not watch_dir.exists():
              logger.warning(f"Watch directory {watch_dir} does not exist. Creating it.")
              watch_dir.mkdir(parents=True, exist_ok=True)
          elif not watch_dir.is_dir():
              logger.error(f"Watch path {watch_dir} exists but is not a directory. Exiting.")
              return

          stop_event = asyncio.Event()
          try:
              # Initialize connections here if they are managed globally and need setup
              # await get_redis_connection() # Example: ensure pool is warm

              async for event in video_file_watcher(watch_dir, stream_timeout, stop_event):
                  await handle_stream_event(event) # This is now non-blocking for CREATE/WRITE

          except KeyboardInterrupt:
              logger.info("Keyboard interrupt received. Signaling services to stop...")
          except Exception as e:
              logger.critical(f"Unhandled exception in main loop: {e}", exc_info=True)
          finally:
              logger.info("Shutting down services...")
              stop_event.set()
              # Graceful shutdown of active_processors and connections (Phase 8)
              # For now, just close connections if they were opened.
              active_processor_tasks = []
              for processor in active_processors.values():
                  # If processors have tasks associated with them directly that need cancellation:
                  # e.g. if process_file_write itself was a long-running task managed by the processor object
                  pass # More complex shutdown in Phase 8

              # Wait for any tasks spawned by handle_stream_event if not managed elsewhere
              # This is a simplified shutdown; Phase 8 will handle this robustly.
              all_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
              if all_tasks:
                  logger.info(f"Cancelling {len(all_tasks)} outstanding tasks...")
                  for task in all_tasks:
                      task.cancel()
                  await asyncio.gather(*all_tasks, return_exceptions=True)
                  logger.info("Outstanding tasks cancelled.")

              await close_redis_connection()
              await close_s3_resources() # Assumes these functions exist from Phase 3
              logger.info("Application stopped.")

      if __name__ == "__main__":
          # main_loop = asyncio.get_event_loop()
          try:
              asyncio.run(main())
          except KeyboardInterrupt:
              logger.info("Application shutdown initiated by user (Ctrl+C).")
          # finally:
              # main_loop.close() # Not typically needed with asyncio.run()
      ```

    - **Reasoning:**
      - Introduces `active_processors: dict[Path, StreamProcessor]` to track ongoing stream processing instances, keyed by file path.
      - `manage_new_stream_creation` function (called on `CREATE` event) handles S3 init, Redis init, and `StreamProcessor` instantiation. It then stores the processor and triggers an initial `process_file_write` as a new task.
      - `handle_stream_event` for `WRITE` events now looks up the `StreamProcessor` from `active_processors` and calls its `process_file_write` method as a new task.
      - Basic handling for `DELETE` events is added (removing the processor from tracking).
      - The main loop calls `handle_stream_event` which is now non-blocking for CREATE and WRITE events, allowing the watcher to remain responsive.
      - Shutdown logic in `main` is updated to include basic task cancellation and closing Redis/S3 connections, though full graceful shutdown will be detailed in Phase 8.

5.  **Unit Tests (`tests/test_stream_processor.py`, `tests/test_main_integration.py`):**
    - **Action:**
      - Create `tests/test_stream_processor.py` to test `StreamProcessor` methods (mock S3 and Redis calls).
        - Test `process_file_write` with various scenarios (new file, file with existing offset, file smaller than chunk, file larger than chunk, multiple chunks, simulated S3/Redis failures).
      - Extend `tests/test_watcher.py` or create `tests/test_main_integration.py` to test the integration of watcher, event handling, and `StreamProcessor` instantiation.
        - Simulate file creation and writes, then verify that `StreamProcessor` methods are called and that S3/Redis client functions are invoked with correct parameters (using mocks).
    - **Reasoning:** Ensures the core processing logic within `StreamProcessor` is correct and that its integration into the main event loop works as expected.

**Expected Outcome:**

- When a `.mp4` file is created or written to in the `WATCH_DIR`:
  - For `CREATE` events: A `StreamProcessor` instance is created, S3 multipart upload is initiated, and initial stream state is stored in Redis.
  - For `WRITE` events: The corresponding `StreamProcessor` reads new data from the file in chunks, uploads each chunk as an S3 part, and updates `next_part_number`, `total_bytes_sent`, and part ETags in Redis.
- The application can handle multiple concurrent streams (up to system/configured limits, though explicit concurrency control is later).
- Checkpointing in Redis is functional for individual chunk uploads.

**Next Phase:** Phase 5 will implement stream finalization: detecting `IDLE` streams (this detection logic will be refined), completing the S3 multipart upload, and preparing for metadata generation.
