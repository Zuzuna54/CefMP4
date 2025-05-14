# Phase 6: Metadata Generation (Part 2 - `ffprobe` & JSON)

**Objective:** After successful S3 multipart upload completion, use `ffprobe` to get video stream duration, gather all required metadata, create a JSON metadata file, and upload it to S3.

**Builds upon:** Phase 1 (Config), Phase 3 (S3 Client, Redis Client), Phase 4 (StreamProcessor), Phase 5 (S3 Completion, Stream Finalization Logic)

**Steps & Design Choices:**

1.  **Install `ffprobe` in Docker Image:**

    - **Action:** Modify the `Dockerfile` to include `ffmpeg` (which provides `ffprobe`).
      ```dockerfile
      # In Dockerfile, after FROM python:3.12-slim and WORKDIR /app
      # Before RUN useradd ...
      RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg dumb-init && apt-get clean && rm -rf /var/lib/apt/lists/*
      # Note: dumb-init was moved here from a separate RUN to combine layers.
      ```
    - **Reasoning:** `ffprobe` is required to extract video duration. Installing it ensures it's available in the container's PATH. `settings.ffprobe_path` can still be used to override if a specific binary location is needed outside the standard PATH.

2.  **Implement `ffprobe` Utility (`src/utils/ffprobe_utils.py`):**

    - **Action:** Create a new utility module for `ffprobe` interaction.
      Create `src/utils/__init__.py` (empty).
      Create `src/utils/ffprobe_utils.py`:

      ```python
      import asyncio
      import json
      import logging
      from src.config import settings

      logger = logging.getLogger(__name__) # To be replaced by structlog

      async def get_video_duration(file_path: str) -> float | None:
          """Runs ffprobe to get the video duration in seconds."""
          ffprobe_cmd = settings.ffprobe_path if settings.ffprobe_path else "ffprobe"
          args = [
              ffprobe_cmd,
              "-v", "error",
              "-show_entries", "format=duration",
              "-of", "default=noprint_wrappers=1:nokey=1",
              file_path
          ]
          logger.debug(f"Running ffprobe for duration: {' '.join(args)}")
          try:
              process = await asyncio.create_subprocess_exec(
                  *args,
                  stdout=asyncio.subprocess.PIPE,
                  stderr=asyncio.subprocess.PIPE
              )
              stdout, stderr = await process.communicate()

              if process.returncode == 0 and stdout:
                  duration_str = stdout.decode().strip()
                  logger.debug(f"ffprobe stdout for duration: {duration_str}")
                  return float(duration_str)
              else:
                  stderr_str = stderr.decode().strip()
                  logger.error(f"ffprobe error for {file_path} (return code {process.returncode}): {stderr_str}")
                  return None
          except FileNotFoundError:
              logger.error(f"ffprobe command not found (path: '{ffprobe_cmd}'). Ensure ffmpeg is installed and ffprobe is in PATH or FFPROBE_PATH is set correctly.")
              return None
          except Exception as e:
              logger.error(f"Error running ffprobe for {file_path}: {e}", exc_info=True)
              return None
      ```

    - **Reasoning:** Encapsulates the logic for executing the `ffprobe` command asynchronously and parsing its output to get the video duration. This keeps the `StreamProcessor` cleaner.

3.  **Implement Metadata JSON Generation (`src/metadata_generator.py`):**

    - **Action:** Create `src/metadata_generator.py`.
      Create `src/__init__.py` if it does not exist.

      ```python
      import datetime
      import json
      import logging
      from pathlib import Path
      from src.utils.ffprobe_utils import get_video_duration
      from src.redis_client import get_stream_meta, get_stream_parts # Assuming get_stream_parts returns part details
      from src.config import settings

      logger = logging.getLogger(__name__) # To be replaced by structlog

      async def generate_metadata_json(stream_id: str, original_file_path: Path) -> dict | None:
          logger.info(f"[{stream_id}] Generating metadata JSON for {original_file_path.name}.")

          stream_meta_redis = await get_stream_meta(stream_id)
          if not stream_meta_redis:
              logger.error(f"[{stream_id}] Could not retrieve stream metadata from Redis to generate JSON.")
              return None

          # Get all parts info (ETag, Size, UploadedAtUTC from Redis)
          # The get_stream_parts in Phase 4 stored "etag:size_bytes:uploaded_at_iso".
          # It MUST be updated to parse and return UploadedAtUTC.
          # (See updated get_stream_parts snippet later in this document or ensure Phase 4 is amended)
          stream_parts_info_redis = await get_stream_parts(stream_id) # This must return richer part info

          # Extract duration using ffprobe from the original local file
          # This assumes the original file is still accessible locally when metadata is generated.
          # If not, ffprobe might need to run earlier or on a downloaded copy.
          # For this phase, assume original_file_path is accessible.
          duration_seconds = await get_video_duration(str(original_file_path))
          if duration_seconds is None:
              logger.warning(f"[{stream_id}] Could not determine video duration for {original_file_path.name}. Duration will be null in metadata.")

          total_size_bytes = sum(part.get("Size", 0) for part in stream_parts_info_redis)

          # Construct metadata object according to s3_handling.mdc rule
          metadata = {
              "stream_id": stream_id,
              "original_file_path": str(original_file_path),
              "s3_bucket": stream_meta_redis.get("s3_bucket"),
              "s3_key_prefix": stream_meta_redis.get("s3_key_prefix"),
              "total_size_bytes": total_size_bytes,
              "duration_seconds": duration_seconds,
              "processed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "stream_started_at_utc": stream_meta_redis.get("started_at_utc"),
              "stream_completed_at_utc": stream_meta_redis.get("last_activity_at_utc"), # Or a more specific completion time if set
              "chunks": [
                  {
                      "part_number": part.get("PartNumber"),
                      "size_bytes": part.get("Size"),
                      "etag": part.get("ETag"),
                      "uploaded_at_utc": part.get("UploadedAtUTC") # Relies on get_stream_parts providing this
                  } for part in sorted(stream_parts_info_redis, key=lambda x: x.get("PartNumber", 0))
              ]
          }
          logger.info(f"[{stream_id}] Metadata JSON generated for {original_file_path.name}.")
          return metadata
      ```

    - **Reasoning:** This module is responsible for gathering all necessary information (from Redis, `ffprobe`) and constructing the JSON object as defined in the `s3_handling.mdc` Cursor rule and `requiremnts.md`.
    - **Crucial Dependency Update for `redis_client.get_stream_parts` (from Phase 4):**
      The `get_stream_parts` function in `src/redis_client.py`, originally defined in Phase 4, needs to be updated to parse the `uploaded_at_iso` timestamp from the stored string `etag:size_bytes:uploaded_at_iso` and include it in the returned part information.
      **Updated `get_stream_parts` in `src/redis_client.py`:**
      ```python
      # In src/redis_client.py
      # ... (other functions) ...
      async def get_stream_parts(stream_id: str) -> list[dict]:
          r = await get_redis_connection()
          parts_key = f"stream:{stream_id}:parts"
          raw_parts = await r.hgetall(parts_key)
          parsed_parts = []
          for pn_str, data_str in raw_parts.items():
              try:
                  # Ensure data_str has at least two colons for splitting
                  if data_str.count(':') >= 2:
                      etag, size_str, uploaded_at_iso = data_str.split(':', 2)
                      parsed_parts.append({
                          "PartNumber": int(pn_str),
                          "ETag": etag,
                          "Size": int(size_str),
                          "UploadedAtUTC": uploaded_at_iso # Add the timestamp
                      })
                  else: # Fallback for old format or corrupted data
                      etag, size_str = data_str.split(':', 1)
                      parsed_parts.append({
                          "PartNumber": int(pn_str),
                          "ETag": etag,
                          "Size": int(size_str),
                          "UploadedAtUTC": None # Indicate missing timestamp
                      })
                      logger.warning(f"Part data for stream {stream_id}, part {pn_str} has unexpected format: '{data_str}'. Missing UploadedAtUTC.")
              except ValueError as e:
                  logger.warning(f"Could not parse part data '{data_str}' for stream {stream_id}, part {pn_str}: {e}")
          # Sort by PartNumber for S3 and consistency
          parsed_parts.sort(key=lambda p: p["PartNumber"])
          return parsed_parts
      ```

4.  **Extend S3 Client (`src/s3_client.py`) to Upload JSON Data:**

    - **Action:** Add `upload_json_to_s3` and `remove_stream_keys` to `src/s3_client.py`.

      ```python
      # In src/s3_client.py
      import json # Add this import

      # ... (existing functions) ...

      async def upload_json_to_s3(bucket_name: str, object_key: str, data: dict) -> bool:
          session = get_s3_session()
          async with session.client("s3", endpoint_url=settings.s3_endpoint_url,
                                  aws_access_key_id=settings.s3_access_key_id,
                                  aws_secret_access_key=settings.s3_secret_access_key,
                                  region_name=settings.s3_region_name) as s3:
              try:
                  json_bytes = json.dumps(data, indent=2).encode('utf-8')
                  await s3.put_object(
                      Bucket=bucket_name,
                      Key=object_key,
                      Body=json_bytes,
                      ContentType='application/json'
                  )
                  logger.info(f"Successfully uploaded JSON metadata to s3://{bucket_name}/{object_key}")
                  return True
              except ClientError as e:
                  logger.error(f"Error uploading JSON to S3 for {object_key}: {e}", exc_info=True)
                  return False
              except Exception as e:
                  logger.error(f"Unexpected error during JSON S3 upload for {object_key}: {e}", exc_info=True)
                  return False

      async def remove_stream_keys(stream_id: str):
          """Removes all keys associated with a stream_id from Redis after successful processing or unrecoverable failure."""
          r = await get_redis_connection()
          keys_to_delete = []
          keys_to_delete.append(f"stream:{stream_id}:meta")
          keys_to_delete.append(f"stream:{stream_id}:parts")
          # Add any other keys that might be associated with the stream_id pattern

          # Remove from general sets as well
          # This should ideally happen when transitioning states, but as a final cleanup:
          await r.srem("streams:active", stream_id)
          await r.srem("streams:pending_completion", stream_id)
          # No need to remove from streams:completed as this is the success state, unless it means "to be archived"

          if keys_to_delete:
              deleted_count = await r.delete(*keys_to_delete)
              logger.info(f"Cleaned up {deleted_count} Redis keys for successfully completed stream {stream_id}.")
          else:
              logger.info(f"No specific stream keys found to delete for {stream_id}, sets handled.")
      ```

    - **Reasoning:** Generic function to upload any Python dictionary as a JSON file to S3.

5.  **Integrate Metadata Generation into `StreamProcessor.finalize_stream` (`src/stream_processor.py`):**

    - **Action:** Modify `finalize_stream` to call metadata generation and upload after successful S3 multipart completion.

      ```python
      # In src/stream_processor.py
      # class StreamProcessor:
      #    ...
      async def finalize_stream(self):
          """Handles stream finalization: complete S3 multipart upload and then generate/upload metadata JSON."""
          logger.info(f"[{self.stream_id}] Finalizing stream for {self.file_path}.")
          async with self.lock:
              # ... (existing S3 completion logic from Phase 5) ...
              from src.s3_client import complete_s3_multipart_upload, abort_s3_multipart_upload, upload_json_to_s3 # Added upload_json_to_s3
              from src.redis_client import get_stream_parts, set_stream_status, remove_stream_from_pending_completion, add_stream_to_completed_set, remove_stream_keys # Added remove_stream_keys
              from src.metadata_generator import generate_metadata_json # New import

              try:
                  s3_parts_for_completion = await get_stream_parts(self.stream_id)
                  s3_parts_for_completion = [p for p in s3_parts_for_completion if p.get("ETag")]

                  if not s3_parts_for_completion:
                      logger.warning(f"[{self.stream_id}] No parts for {self.file_path}. Aborting upload.")
                      await abort_s3_multipart_upload(self.s3_bucket, self.s3_key_prefix, self.s3_upload_id)
                      await set_stream_status(self.stream_id, "aborted_no_parts")
                      await remove_stream_from_pending_completion(self.stream_id)
                      # TODO: Consider moving to a failed set explicitly for cleanup
                      # await remove_stream_keys(self.stream_id) # Or a more specific cleanup for aborted streams
                      return

                  s3_upload_success = await complete_s3_multipart_upload(
                      bucket_name=self.s3_bucket,
                      object_key=self.s3_key_prefix,
                      upload_id=self.s3_upload_id,
                      parts=s3_parts_for_completion
                  )

                  if s3_upload_success:
                      logger.info(f"[{self.stream_id}] S3 multipart upload completed for {self.file_path}.")
                      await set_stream_status(self.stream_id, "s3_completed")

                      # --- Metadata Generation and Upload ---
                      metadata_obj = await generate_metadata_json(self.stream_id, self.file_path)
                      if metadata_obj:
                          metadata_s3_key = f"{self.s3_key_prefix}.metadata.json"
                          metadata_upload_success = await upload_json_to_s3(
                              bucket_name=self.s3_bucket,
                              object_key=metadata_s3_key,
                              data=metadata_obj
                          )
                          if metadata_upload_success:
                              logger.info(f"[{self.stream_id}] Metadata JSON uploaded to S3: {metadata_s3_key}")
                              await set_stream_status(self.stream_id, "completed") # Final success status
                              await add_stream_to_completed_set(self.stream_id) # Move to final completed set
                              # Clean up all Redis keys for this stream after successful completion
                              await remove_stream_keys(self.stream_id)
                          else:
                              logger.error(f"[{self.stream_id}] Failed to upload metadata JSON for {self.file_path}.")
                              await set_stream_status(self.stream_id, "failed_metadata_upload")
                      else:
                          logger.error(f"[{self.stream_id}] Failed to generate metadata JSON for {self.file_path}.")
                          await set_stream_status(self.stream_id, "failed_metadata_generation")
                      # --- End Metadata ---
                  else:
                      logger.error(f"[{self.stream_id}] Failed S3 complete for {self.file_path}.")
                      await set_stream_status(self.stream_id, "failed_s3_complete")

                  # Ensure stream is removed from pending_completion regardless of metadata outcome if S3 part was handled
                  await remove_stream_from_pending_completion(self.stream_id)
                  # If not fully completed and cleaned, it might go to a failed set for later inspection.

              except Exception as e:
                  logger.error(f"[{self.stream_id}] Error during stream finalization (Phase 6 extensions) for {self.file_path}: {e}", exc_info=True)
                  await set_stream_status(self.stream_id, "failed_finalization_meta")
                  await remove_stream_from_pending_completion(self.stream_id)
      ```

    - **Reasoning:** Integrates the metadata steps into the finalization flow. If S3 completion is successful, it proceeds to generate and upload metadata. Stream status is updated accordingly. Final cleanup of Redis keys for the stream happens here after full completion.

6.  **Update Redis Client (`src/redis_client.py`) for Full Cleanup:**

    - **Action:** Add `remove_stream_keys` to perform a full cleanup of all Redis keys associated with a stream ID.

      ```python
      # In src/redis_client.py
      async def remove_stream_keys(stream_id: str):
          """Removes all keys associated with a stream_id from Redis after successful processing or unrecoverable failure."""
          r = await get_redis_connection()
          keys_to_delete = []
          keys_to_delete.append(f"stream:{stream_id}:meta")
          keys_to_delete.append(f"stream:{stream_id}:parts")
          # Add any other keys that might be associated with the stream_id pattern

          # Remove from general sets as well
          # This should ideally happen when transitioning states, but as a final cleanup:
          await r.srem("streams:active", stream_id)
          await r.srem("streams:pending_completion", stream_id)
          # No need to remove from streams:completed as this is the success state, unless it means "to be archived"

          if keys_to_delete:
              deleted_count = await r.delete(*keys_to_delete)
              logger.info(f"Cleaned up {deleted_count} Redis keys for successfully completed stream {stream_id}.")
          else:
              logger.info(f"No specific stream keys found to delete for {stream_id}, sets handled.")
      ```

    - **Reasoning:** After a stream is fully processed and metadata uploaded, its checkpoint data in Redis is no longer needed and should be cleaned up.

7.  **Unit Tests:**
    - **Action:**
      - Test `get_video_duration` (mock `asyncio.create_subprocess_exec`, test success, failure, ffprobe not found).
      - Test `generate_metadata_json` (mock `get_video_duration` and Redis calls, verify JSON structure).
      - Test `upload_json_to_s3` (mock `aioboto3` client, verify parameters and body).
      - Extend tests for `StreamProcessor.finalize_stream` to cover metadata generation and upload success/failure paths.
      - Test `remove_stream_keys` in `redis_client.py`.
    - **Reasoning:** Ensures metadata generation, ffprobe interaction, and S3 upload are correct.

**Docker & Dependencies Update:**

- `ffmpeg` is now installed in the Docker image.
- No new Python dependencies for this phase beyond what `ffprobe` (system) and existing libraries provide.

**Expected Outcome:**

- After S3 multipart upload is completed for an IDLE stream:
  - `ffprobe` is executed to get the video duration from the original local file.
  - A metadata JSON file, conforming to the specified schema, is generated.
  - This JSON file is uploaded to S3 with a `.metadata.json` suffix.
  - The stream status in Redis is updated to `completed`.
  - All associated Redis keys for the stream are cleaned up.
- The Docker image contains `ffprobe`.

**Next Phase:** Phase 7 will focus on Checkpoint Recovery & Resume Logic, allowing the application to continue processing interrupted uploads on restart.
