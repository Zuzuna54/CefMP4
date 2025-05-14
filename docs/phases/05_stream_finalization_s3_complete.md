# Phase 5: Stream Finalization & Metadata Generation (Part 1 - S3 Completion)

**Objective:** Implement the logic to finalize an S3 multipart upload when a stream is considered complete (IDLE). This phase also refines IDLE detection.

**Builds upon:** Phase 1 (Config), Phase 2 (Watcher, Events), Phase 3 (S3 Init, Redis), Phase 4 (StreamProcessor, Chunk Uploads)

**Steps & Design Choices:**

1.  **Refine IDLE Stream Detection (Periodic Stale Scan Task):**

    - **Action:** Introduce a separate `asyncio` task in `src/main.py` that periodically scans active streams in Redis and checks their `last_activity_at_utc` against `settings.stream_timeout_seconds`.
    - If a stream is stale, this task will generate an internal `StreamEvent` with `change_type=WatcherChangeType.IDLE` or directly trigger the finalization process for that stream.
    - Modify `src/redis_client.py` to include a function like `get_active_streams_with_last_activity()`.

    - **`src/redis_client.py` addition:**

      ```python
      # In src/redis_client.py
      # ... (existing functions) ...
      async def get_active_stream_ids() -> list[str]:
          r = await get_redis_connection()
          return list(await r.smembers("streams:active")) # Ensure result is list of strings

      async def get_stream_meta(stream_id: str) -> dict | None:
          r = await get_redis_connection()
          return await r.hgetall(f"stream:{stream_id}:meta")

      async def move_stream_to_pending_completion(stream_id: str):
          r = await get_redis_connection()
          async with r.pipeline(transaction=True) as pipe:
              pipe.srem("streams:active", stream_id)
              pipe.sadd("streams:pending_completion", stream_id)
              # Update status in meta hash
              now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
              pipe.hset(f"stream:{stream_id}:meta", mapping={
                  "status": "pending_completion",
                  "last_activity_at_utc": now_iso # Update last activity to now for this transition
              })
              await pipe.execute()
          logger.info(f"Moved stream {stream_id} from active to pending_completion.")
      ```

    - **`src/main.py` additions/modifications:**

      ```python
      # In src/main.py
      # ... (imports) ...
      from src.redis_client import get_active_stream_ids, get_stream_meta, move_stream_to_pending_completion, update_stream_last_activity
      # ...

      async def periodic_stale_stream_check(stop_event: asyncio.Event):
          """Periodically checks for streams that have become idle."""
          while not stop_event.is_set():
              try:
                  logger.debug("Running periodic stale stream check...")
                  active_stream_ids = await get_active_stream_ids()
                  now = datetime.datetime.now(datetime.timezone.utc)

                  for stream_id in active_stream_ids:
                      meta = await get_stream_meta(stream_id)
                      if meta and meta.get("last_activity_at_utc"):
                          try:
                              last_activity_str = meta["last_activity_at_utc"]
                              last_activity_dt = datetime.datetime.fromisoformat(last_activity_str)

                              # Ensure last_activity_dt is offset-aware for comparison with now
                              if last_activity_dt.tzinfo is None:
                                  last_activity_dt = last_activity_dt.replace(tzinfo=datetime.timezone.utc)

                              if (now - last_activity_dt).total_seconds() > settings.stream_timeout_seconds:
                                  logger.info(f"Stream {stream_id} detected as STALE (idle). Last activity: {last_activity_str}")
                                  # Create an IDLE event or directly trigger finalization for this processor
                                  file_path_str = meta.get("original_path")
                                  if file_path_str:
                                      processor_path_key = Path(file_path_str) # Convert to Path for lookup
                                      processor = active_processors.get(processor_path_key)
                                      if processor and processor.stream_id == stream_id: # Ensure correct processor
                                          await move_stream_to_pending_completion(stream_id)
                                          # Schedule finalization. The task itself will handle removing the processor from active_processors.
                                          asyncio.create_task(self.run_finalization_and_cleanup(processor, processor_path_key), name=f"idle_finalize_{stream_id[:8]}")
                                      else:
                                          logger.warning(f"Stale stream {stream_id} detected, but no matching active processor found for path {file_path_str} or mismatched ID. Processor in dict: {processor.stream_id if processor else 'None'}")
                                          # If no active processor, but stream is in redis, it might be an orphaned stream.
                                          # Consider moving directly to a cleanup/failed state in Redis if no processor can handle it.
                                  else:
                                      logger.warning(f"Stale stream {stream_id} detected, but no original_path found in metadata.")
                          except ValueError as ve:
                              logger.error(f"Error parsing last_activity_at_utc for stream {stream_id}: {meta.get('last_activity_at_utc')} - {ve}")
                          except Exception as e_inner:
                              logger.error(f"Error processing stale check for stream {stream_id}: {e_inner}", exc_info=True)
                      elif meta:
                          logger.warning(f"Stream {stream_id} is active but has no 'last_activity_at_utc' in meta.")
                          # Potentially update its last activity to now to give it a chance to be processed
                          await update_stream_last_activity(stream_id)

                  # Sleep for a configurable interval, e.g., stream_timeout_seconds / 3 or a fixed value
                  await asyncio.sleep(settings.stream_timeout_seconds / 2)
              except asyncio.CancelledError:
                  logger.info("Stale stream checker task cancelled.")
                  break
              except Exception as e:
                  logger.error(f"Error in periodic_stale_stream_check: {e}", exc_info=True)
                  # Sleep for a bit longer on error to avoid tight error loops
                  await asyncio.sleep(settings.stream_timeout_seconds)

      async def run_finalization_and_cleanup(processor: StreamProcessor, processor_key: Path):
          """Helper to run finalization and then remove processor from active_processors."""
          try:
              await processor.finalize_stream()
          except Exception as e_finalize:
              logger.error(f"[{processor.stream_id}] Error during finalization task for {processor.file_path}: {e_finalize}", exc_info=True)
              # Stream status should already be set to a failed state by finalize_stream itself
          finally:
              # Remove the processor from active tracking after finalization attempt
              if processor_key in active_processors and active_processors[processor_key].stream_id == processor.stream_id:
                  del active_processors[processor_key]
                  logger.info(f"[{processor.stream_id}] Processor for {processor.file_path} removed from active tracking after finalization attempt.")
              else:
                  logger.warning(f"[{processor.stream_id}] Processor for {processor.file_path} not found in active_processors at key {processor_key} for cleanup, or stream_id mismatch.")

      # In main() function:
      # async def main():
      #    ...
      #    stale_check_stop_event = asyncio.Event()
      #    stale_check_task = asyncio.create_task(periodic_stale_stream_check(stale_check_stop_event))
      #    ...
      #    try:
      #        ...
      #    finally:
      #        logger.info("Shutting down... Signaling stale checker and watcher to stop.")
      #        stale_check_stop_event.set()
      #        stop_event.set() # For watcher
      #        await asyncio.gather(stale_check_task, return_exceptions=True) # Wait for stale checker to finish
      #        # ... rest of cleanup ...
      ```

    - **Reasoning:** This aligns with the "Periodic stale‑scan task" mentioned in `requiremnts.md`. It decouples IDLE detection from the `watchfiles` events, which primarily report direct file system changes. The watcher updates `last_activity_at_utc` on `WRITE` events (via `StreamProcessor`), and this task polls that timestamp. Streams identified as idle are moved to a `streams:pending_completion` set in Redis and their `StreamProcessor.finalize_stream()` method is invoked.

2.  **Extend S3 Client (`src/s3_client.py`) for Completing and Aborting Uploads:**

    - **Action:** Add `complete_s3_multipart_upload` and `abort_s3_multipart_upload` to `src/s3_client.py`.

      ```python
      # In src/s3_client.py
      # ... (existing functions) ...

      async def complete_s3_multipart_upload(bucket_name: str, object_key: str, upload_id: str, parts: list[dict]) -> bool:
          """ Completes a multipart upload. parts should be a list of {'PartNumber': int, 'ETag': str}. """
          session = get_s3_session()
          async with session.client("s3", endpoint_url=settings.s3_endpoint_url,
                                  aws_access_key_id=settings.s3_access_key_id,
                                  aws_secret_access_key=settings.s3_secret_access_key,
                                  region_name=settings.s3_region_name) as s3:
              if not parts:
                  logger.warning(f"No parts provided for completing multipart upload {upload_id} for {object_key}. Aborting instead.")
                  # S3 complete_multipart_upload requires at least one part.
                  # If there are no parts, it means no data was ever uploaded successfully.
                  # We should abort it to clean up the UploadId on S3.
                  await abort_s3_multipart_upload(bucket_name, object_key, upload_id)
                  return False # Indicate completion failed / was aborted
              try:
                  logger.info(f"Completing S3 multipart upload for {object_key}, Upload ID: {upload_id} with {len(parts)} parts.")
                  await s3.complete_multipart_upload(
                      Bucket=bucket_name,
                      Key=object_key,
                      UploadId=upload_id,
                      MultipartUpload={'Parts': parts}
                  )
                  logger.info(f"Successfully completed S3 multipart upload for {object_key}, Upload ID: {upload_id}.")
                  return True
              except ClientError as e:
                  logger.error(f"Error completing S3 multipart upload {upload_id} for {object_key}: {e}", exc_info=True)
                  return False

      async def abort_s3_multipart_upload(bucket_name: str, object_key: str, upload_id: str) -> bool:
          session = get_s3_session()
          async with session.client("s3", endpoint_url=settings.s3_endpoint_url,
                                  aws_access_key_id=settings.s3_access_key_id,
                                  aws_secret_access_key=settings.s3_secret_access_key,
                                  region_name=settings.s3_region_name) as s3:
              try:
                  logger.info(f"Aborting S3 multipart upload for {object_key}, Upload ID: {upload_id}.")
                  await s3.abort_multipart_upload(
                      Bucket=bucket_name,
                      Key=object_key,
                      UploadId=upload_id
                  )
                  logger.info(f"Successfully aborted S3 multipart upload for {object_key}, Upload ID: {upload_id}.")
                  return True
              except ClientError as e:
                  logger.error(f"Error aborting S3 multipart upload {upload_id} for {object_key}: {e}", exc_info=True)
                  return False
      ```

    - **Reasoning:** Provides necessary S3 operations for finalizing or cancelling an upload, crucial for resource management and cost.

3.  **Implement `finalize_stream` in `StreamProcessor` (`src/stream_processor.py`):**

    - **Action:** Fill in the `finalize_stream` method.

      ```python
      # In src/stream_processor.py
      # class StreamProcessor:
      #    ...
      async def finalize_stream(self):
          """Handles stream finalization: complete S3 multipart upload."""
          logger.info(f"[{self.stream_id}] Finalizing stream for {self.file_path}.")
          async with self.lock: # Ensure no other operations like process_file_write happen concurrently
              if self.is_processing: # Should ideally not happen if IDLE logic is correct
                  logger.warning(f"[{self.stream_id}] Finalization called while still processing. This might indicate an issue.")
                  # return # Or wait? For now, proceed cautiously.

              from src.s3_client import complete_s3_multipart_upload, abort_s3_multipart_upload
              from src.redis_client import get_stream_parts, set_stream_status, remove_stream_from_pending_completion, add_stream_to_completed_set # (add_stream_to_completed_set to be created)

              try:
                  # Retrieve all uploaded parts from Redis (or use self.uploaded_parts_info if it's kept perfectly in sync)
                  # For robustness, fetch from Redis as the source of truth for parts before completing.
                  # self.uploaded_parts_info primarily for internal tracking during an active session.
                  s3_parts_for_completion = await get_stream_parts(self.stream_id) # Ensures parts are {"PartNumber": N, "ETag": "xxx"}

                  # Filter out parts that might not have ETag, though get_stream_parts should handle this
                  s3_parts_for_completion = [p for p in s3_parts_for_completion if p.get("ETag")]

                  if not s3_parts_for_completion:
                      logger.warning(f"[{self.stream_id}] No parts found in Redis for {self.file_path} to complete S3 upload. Aborting upload.")
                      await abort_s3_multipart_upload(self.s3_bucket, self.s3_key_prefix, self.s3_upload_id)
                      await set_stream_status(self.stream_id, "aborted_no_parts")
                      # Move from pending_completion to a failed/aborted set might be good here.
                      await remove_stream_from_pending_completion(self.stream_id)
                      return

                  success = await complete_s3_multipart_upload(
                      bucket_name=self.s3_bucket,
                      object_key=self.s3_key_prefix,
                      upload_id=self.s3_upload_id,
                      parts=s3_parts_for_completion # Ensure this list is {'PartNumber': ..., 'ETag': ...}
                  )

                  if success:
                      logger.info(f"[{self.stream_id}] S3 multipart upload completed for {self.file_path}.")
                      await set_stream_status(self.stream_id, "s3_completed") # New status
                      # Next steps in Phase 6: metadata generation and upload, then mark as fully 'completed'
                  else:
                      logger.error(f"[{self.stream_id}] Failed to complete S3 multipart upload for {self.file_path}. Stream marked as failed_s3_complete.")
                      await set_stream_status(self.stream_id, "failed_s3_complete")
                      # Consider moving to a failed set in Redis

                  # Regardless of S3 success/failure, remove from pending_completion if it was moved there by stale checker
                  await remove_stream_from_pending_completion(self.stream_id)
                  # If successful, it might move to a `streams:pending_metadata` set or similar in Phase 6.

              except Exception as e:
                  logger.error(f"[{self.stream_id}] Error during stream finalization for {self.file_path}: {e}", exc_info=True)
                  await set_stream_status(self.stream_id, "failed_finalization")
                  await remove_stream_from_pending_completion(self.stream_id) # Ensure cleanup from this set
      ```

    - **Reasoning:** Orchestrates the S3 multipart completion. Retrieves part information from Redis (as the source of truth) and calls the S3 client function. Updates stream status in Redis based on outcome.

4.  **Update Redis Client (`src/redis_client.py`) for Finalization State:**

    - **Action:** Add sets like `streams:pending_completion`, `streams:completed`, `streams:failed` and functions to manage them.

      ```python
      # In src/redis_client.py
      async def remove_stream_from_pending_completion(stream_id: str):
          r = await get_redis_connection()
          await r.srem("streams:pending_completion", stream_id)
          logger.debug(f"Removed stream {stream_id} from streams:pending_completion set.")

      async def add_stream_to_completed_set(stream_id: str):
          r = await get_redis_connection()
          await r.sadd("streams:completed", stream_id)
          logger.info(f"Added stream {stream_id} to streams:completed set.")

      # (Similar functions for failed sets can be added as needed)
      ```

    - **Reasoning:** Manages the lifecycle of streams through different Redis sets, aiding in tracking and potential recovery/auditing.

5.  **Modify `src/main.py` Event Handling for IDLE:**

    - **Action:** The `periodic_stale_stream_check` already calls `processor.finalize_stream()`. Ensure the main event loop in `main.py` correctly uses the `active_processors` dictionary.
    - The previous `handle_stream_event`'s IDLE part was a placeholder; the stale checker now drives this more directly by invoking `finalize_stream` on the correct processor instance.

6.  **Unit Tests:**
    - **Action:**
      - Test the `periodic_stale_stream_check` in `src/main.py` (mock Redis calls, verify it correctly identifies stale streams and triggers finalization calls).
      - Test `StreamProcessor.finalize_stream` (mock S3 complete/abort, Redis get_parts/set_status, verify logic for success and failure cases, including no parts scenario).
      - Test new S3 client functions (`complete_s3_multipart_upload`, `abort_s3_multipart_upload`) with mocked `aioboto3` calls.
      - Test new Redis client functions.
    - **Reasoning:** Ensures the IDLE detection and S3 completion logic are robust.

**Expected Outcome:**

- A periodic task reliably identifies streams that have been inactive for longer than `STREAM_TIMEOUT_SECONDS`.
- For IDLE streams:
  - All uploaded part ETags are retrieved from Redis.
  - `CompleteMultipartUpload` is called on S3.
  - Stream status in Redis is updated to `s3_completed` on success, or an error status on failure.
  - The stream is moved from `streams:active` (or `streams:pending_completion`) to an appropriate terminal state set in Redis (e.g., `streams:pending_metadata` or `streams:failed`).
- If no parts were uploaded, the S3 multipart upload is aborted.

**Next Phase:** Phase 6 will cover Metadata Generation (Part 2 - `ffprobe` & JSON), where the actual `.metadata.json` file is created and uploaded to S3 after successful S3 completion.
