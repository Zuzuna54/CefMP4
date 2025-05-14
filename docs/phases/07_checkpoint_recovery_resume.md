# Phase 7: Checkpoint Recovery & Resume Logic

**Objective:** Implement logic for the application to recover and resume processing of incomplete streams upon startup, based on checkpoint data stored in Redis.

**Builds upon:** Phase 1 (Config), Phase 3 (Redis Client, S3 Client), Phase 4 (StreamProcessor), Phase 5 (Stream Finalization), Phase 6 (Metadata Generation)

**Steps & Design Choices:**

1.  **Refine Redis Client for Resume Data:**

    - **Action:** Ensure `src/redis_client.py` has comprehensive functions to retrieve all necessary data for resuming a stream. Many of these (like `get_stream_meta`, `get_stream_parts`, `get_stream_next_part`, `get_stream_bytes_sent`) were added in earlier phases but are critical here.
    - Specifically, `get_stream_meta(stream_id)` should return all fields stored in `init_stream_metadata` and subsequent status updates.
    - `get_stream_parts(stream_id)` should return the list of parts suitable for `StreamProcessor.uploaded_parts_info` and for S3 `CompleteMultipartUpload`.
    - Add functions to get streams from `streams:pending_completion` (if not already present).

    - **`src/redis_client.py` (potential additions/verifications):**

      ```python
      # In src/redis_client.py
      # ... (ensure existing functions are robust for resume)

      async def get_pending_completion_stream_ids() -> list[str]:
          r = await get_redis_connection()
          return list(await r.smembers("streams:pending_completion"))

      # Ensure get_stream_meta returns a comprehensive dict including s3_upload_id, s3_key_prefix, etc.
      # Ensure get_stream_parts returns list of dicts: {"PartNumber": int, "ETag": str, "Size": int}
      # Ensure get_stream_next_part returns the correct next part number to upload.
      # Ensure get_stream_bytes_sent returns the total confirmed bytes for offset calculation.
      ```

    - **Reasoning:** Reliable data retrieval from Redis is the cornerstone of the resume functionality.

    - **Add `add_stream_to_failed_set` to `src/redis_client.py`:**
      ```python
      # In src/redis_client.py
      async def add_stream_to_failed_set(stream_id: str, reason: str = "unknown"):
          """Marks a stream as failed and moves it to the failed set."""
          r = await get_redis_connection()
          async with r.pipeline(transaction=True) as pipe:
              pipe.sadd("streams:failed", stream_id)
              pipe.srem("streams:active", stream_id)
              pipe.srem("streams:pending_completion", stream_id)
              # Update status in meta hash
              now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
              pipe.hset(f"stream:{stream_id}:meta", mapping={
                  "status": f"failed_{reason}",
                  "last_activity_at_utc": now_iso,
                  "failure_reason": reason
              })
              # Optionally, consider if parts or other specific stream data should be removed or kept for diagnosis.
              # For now, we keep them but move the stream out of active processing queues.
              await pipe.execute()
          logger.info(f"Moved stream {stream_id} to streams:failed set. Reason: {reason}")
      ```

2.  **Implement Resume Logic in `StreamProcessor._initialize_from_checkpoint`:**

    - **Action:** Fully implement the `_initialize_from_checkpoint` method in `src/stream_processor.py`.

      ```python
      # In src/stream_processor.py
      # class StreamProcessor:
      #    ...
      async def _initialize_from_checkpoint(self):
          """Loads the processor's state from Redis based on its stream_id."""
          logger.info(f"[{self.stream_id}] Attempting to initialize from checkpoint.")
          from src.redis_client import get_stream_meta, get_stream_next_part, get_stream_bytes_sent, get_stream_parts

          meta = await get_stream_meta(self.stream_id)
          if not meta:
              logger.warning(f"[{self.stream_id}] No metadata found in Redis. Cannot initialize from checkpoint. Assuming new stream (problematic if called for resume).")
              # This situation should ideally not occur if resume is triggered for known stream_ids.
              return

          # Update processor attributes from Redis meta if they were passed initially
          # or if this processor instance is being created solely from a stream_id found on resume.
          self.file_path = Path(meta.get("original_path", str(self.file_path))) # Ensure Path object
          self.s3_upload_id = meta.get("s3_upload_id", self.s3_upload_id)
          self.s3_bucket = meta.get("s3_bucket", self.s3_bucket)
          self.s3_key_prefix = meta.get("s3_key_prefix", self.s3_key_prefix)
          # current_status = meta.get("status") # Can be used to decide next action

          next_part_redis = await get_stream_next_part(self.stream_id) # This should get from stream:{id}:meta now
          if next_part_redis is not None:
              self.next_part_number = int(next_part_redis)
          else:
              # If not set, implies either no parts uploaded or an issue. Defaulting to 1.
              # However, existing parts list should be the better guide if next_part isn't explicitly stored.
              self.next_part_number = 1

          bytes_sent_redis = await get_stream_bytes_sent(self.stream_id) # This should get from stream:{id}:meta
          if bytes_sent_redis is not None:
              self.current_file_offset = int(bytes_sent_redis)
          else:
              self.current_file_offset = 0

          parts_redis = await get_stream_parts(self.stream_id)
          if parts_redis:
              self.uploaded_parts_info = parts_redis
               # If next_part_number was not found, derive from parts_redis
              if next_part_redis is None and self.uploaded_parts_info:
                  self.next_part_number = max(p["PartNumber"] for p in self.uploaded_parts_info) + 1
              # If current_file_offset was not found, derive from parts_redis (sum of sizes)
              if bytes_sent_redis is None and self.uploaded_parts_info:
                  self.current_file_offset = sum(p["Size"] for p in self.uploaded_parts_info)
          else:
              self.uploaded_parts_info = []
              if next_part_redis is None: self.next_part_number = 1
              if bytes_sent_redis is None: self.current_file_offset = 0

          logger.info(f"[{self.stream_id}] Initialized from checkpoint: File='{self.file_path}', next_part={self.next_part_number}, offset={self.current_file_offset}, known_parts={len(self.uploaded_parts_info)}, S3 UploadID='{self.s3_upload_id}')

          if not self.file_path.exists():
              logger.warning(f"[{self.stream_id}] Original file {self.file_path} not found during checkpoint initialization. Stream cannot be processed further.")
              # TODO: Mark stream as failed/unrecoverable in Redis.
              from src.redis_client import add_stream_to_failed_set # Removed set_stream_status, remove_stream_from_active_set
              # No need to call set_stream_status or remove_stream_from_active_set separately,
              # add_stream_to_failed_set handles these state transitions.
              await add_stream_to_failed_set(self.stream_id, reason="file_missing_on_resume")
              raise FileNotFoundError(f"Original file {self.file_path} missing for stream {self.stream_id}")
      ```

    - **Reasoning:** This method now correctly populates the `StreamProcessor` instance with state fetched from Redis, enabling it to continue from where it left off. It also handles the case where the original file might be missing on resume.

3.  **Implement Application Startup Resume Logic in `src/main.py`:**

    - **Action:** Modify the `main()` function to scan Redis for active/pending streams on startup and create/resume `StreamProcessor` tasks for them.

      ```python
      # In src/main.py
      # ... (imports)
      from src.redis_client import get_active_stream_ids, get_pending_completion_stream_ids, get_stream_meta # ... and others
      from src.stream_processor import StreamProcessor

      async def resume_stream_processing(stream_id: str):
          logger.info(f"Attempting to resume processing for stream_id: {stream_id}")
          meta = await get_stream_meta(stream_id)
          if not meta:
              logger.error(f"No metadata found for stream_id: {stream_id} during resume. Skipping.")
              # Potentially move to a failed/unknown set if this happens
              return

          file_path_str = meta.get("original_path")
          s3_upload_id = meta.get("s3_upload_id")
          s3_bucket = meta.get("s3_bucket")
          s3_key_prefix = meta.get("s3_key_prefix")
          status = meta.get("status")

          if not all([file_path_str, s3_upload_id, s3_bucket, s3_key_prefix]):
              logger.error(f"Incomplete metadata for stream_id: {stream_id} during resume. Skipping. Meta: {meta}")
              # Mark as failed in Redis
              return

          file_path = Path(file_path_str)
          processor = StreamProcessor(stream_id, file_path, s3_upload_id, s3_bucket, s3_key_prefix)
          try:
              await processor._initialize_from_checkpoint()
          except FileNotFoundError:
              logger.error(f"Failed to initialize processor for stream {stream_id} due to missing file: {file_path}. Stream will not be resumed.")
              # Further actions like marking as permanently failed in Redis would be handled by _initialize_from_checkpoint
              return
          except Exception as e_init:
              logger.error(f"Failed to initialize processor for stream {stream_id} from checkpoint: {e_init}", exc_info=True)
              return

          active_processors[file_path] = processor # Use Path object as key

          if status == "pending_completion" or status == "s3_completed": # s3_completed if metadata failed previously
              logger.info(f"[{stream_id}] Resuming: stream is pending completion or S3 completed. Triggering finalize_stream.")
              asyncio.create_task(processor.finalize_stream(), name=f"resume_finalize_{stream_id[:8]}")
          elif status == "processing" or status is None: # None implies it was active but status not set, or new fields
              logger.info(f"[{stream_id}] Resuming: stream is in processing state. Triggering process_file_write to check for new data.")
              asyncio.create_task(processor.process_file_write(), name=f"resume_write_{stream_id[:8]}")
          else:
              logger.info(f"[{stream_id}] Resuming: stream has status '{status}'. No immediate action taken by resume_stream_processing, expecting main loop or stale checker.")

      # In main() function, before starting watcher and stale checker:
      # async def main():
      #    logger.info("Application starting...")
      #    await get_redis_connection() # Ensure Redis is connected

      #    logger.info("--- Attempting to resume interrupted streams ---")
      #    active_ids_to_resume = await get_active_stream_ids()
      #    pending_ids_to_resume = await get_pending_completion_stream_ids()
      #    all_ids_to_resume = set(active_ids_to_resume + pending_ids_to_resume)
      #
      #    if all_ids_to_resume:
      #        logger.info(f"Found {len(all_ids_to_resume)} streams to potentially resume: {all_ids_to_resume}")
      #        resume_tasks = [resume_stream_processing(sid) for sid in all_ids_to_resume]
      #        await asyncio.gather(*resume_tasks, return_exceptions=True) # Process all resumes
      #    else:
      #        logger.info("No interrupted streams found to resume.")
      #    logger.info("--- Resume attempt finished ---")

      #    # ... then start watcher, stale_check_task etc. ...
      ```

    - **Reasoning:** On startup, the application queries Redis for streams in `streams:active` or `streams:pending_completion`. For each, it instantiates a `StreamProcessor`, calls `_initialize_from_checkpoint`, and then decides the next action (e.g., `process_file_write` or `finalize_stream`) based on the recovered status.

4.  **Idempotency Considerations:**

    - **Action:** Review S3 and Redis operations to ensure they are idempotent or handle re-execution gracefully.
      - S3 `upload_part`: Uploading the same part number with the same data is generally okay, but if data differs, S3 keeps the latest. The ETag will change. This is mostly handled by seeking in the file.
      - S3 `complete_multipart_upload`: Calling this again after success might error or be ignored. The logic should check stream status before re-triggering.
      - Redis `INCRBY`, `HSET`: These are generally idempotent in effect if setting the same value. `SADD` is idempotent.
    - **Design:** The `StreamProcessor` and resume logic should be designed so that if a step is re-tried (e.g., uploading a part), it doesn't corrupt state. Checking current state in Redis before acting is key. For example, if a stream is already `completed`, don't try to re-finalize.
    - **Reasoning:** Ensures that restarting the application or re-processing a stream due to transient errors doesn't lead to data corruption or incorrect state.

5.  **Cleanup of Old/Failed Streams (Conceptual):**

    - **Action:** While `remove_stream_keys` in Phase 6 cleans up _successfully_ completed streams, consider a strategy for streams that are permanently failed.
      - Move them to a `streams:failed` set in Redis.
      - Potentially have a separate cleanup/archival process for entries in `streams:failed` after some retention period or manual review.
    - **Reasoning:** Prevents Redis from filling up with state for streams that will never be processed.
    - **Note:** Full implementation of a janitor process for failed streams is out of scope for this phase but important for long-term stability.

6.  **Unit Tests:**
    - **Action:**
      - Test `StreamProcessor._initialize_from_checkpoint`: Mock Redis calls, provide various Redis states (e.g., no parts, some parts, missing meta fields), and verify processor state is correctly initialized. Test `FileNotFoundError` scenario.
      - Test `resume_stream_processing` in `main.py`: Mock Redis `get_active_stream_ids`, etc., and `get_stream_meta`. Verify that `StreamProcessor` is correctly instantiated, initialized, and the correct follow-up method (`process_file_write` or `finalize_stream`) is called based on resumed status.
      - Test idempotency aspects where feasible (e.g., re-initializing a processor multiple times from the same Redis state).
    - **Reasoning:** Ensures the resume logic is robust and correctly handles various states of interrupted streams.

**Expected Outcome:**

- On application startup:
  - The application scans Redis for streams in `streams:active` and `streams:pending_completion`.
  - For each such stream, a `StreamProcessor` instance is created and its state (file offset, next part number, uploaded parts) is re-hydrated from Redis.
  - If the original file is missing, the stream is marked appropriately and not processed.
  - Processing continues from the last checkpoint: either by reading new file chunks or by proceeding to finalization.
- The system demonstrates resilience to restarts, continuing uploads without losing significant progress.

**Next Phase:** Phase 8 will focus on Robust Error Handling & Graceful Shutdown, making the application more resilient to operational issues and ensuring clean exits.
