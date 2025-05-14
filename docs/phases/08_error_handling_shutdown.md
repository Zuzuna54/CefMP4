# Phase 8: Robust Error Handling & Graceful Shutdown

**Objective:** Enhance the application with robust error handling mechanisms, including custom exceptions, retry logic for external calls, and a well-coordinated graceful shutdown process.

**Builds upon:** All previous phases, particularly Phase 7 (Resume Logic) and Phase 5 (Stale Scan Task).

**Steps & Design Choices:**

1.  **Define Custom Exceptions (`src/exceptions.py`):**

    - **Action:** Create `src/exceptions.py`.

      ```python
      class CefMp4ProcessorError(Exception):
          """Base exception for this application."""
          pass

      class StreamInitializationError(CefMp4ProcessorError):
          """Error during stream initialization (e.g., S3 multipart init, Redis init)."""
          pass

      class ChunkProcessingError(CefMp4ProcessorError):
          """Error during the chunk reading or S3 upload part phase."""
          pass

      class StreamFinalizationError(CefMp4ProcessorError):
          """Error during stream finalization (e.g., S3 complete, metadata generation/upload)."""
          pass

      class FFprobeError(CefMp4ProcessorError):
          """Error related to ffprobe execution or parsing."""
          pass

      class S3OperationError(CefMp4ProcessorError):
          """General error during an S3 operation not covered by others."""
          def __init__(self, operation: str, message: str):
              self.operation = operation
              super().__init__(f"S3 operation '{operation}' failed: {message}")

      class RedisOperationError(CefMp4ProcessorError):
          """General error during a Redis operation."""
          def __init__(self, operation: str, message: str):
              self.operation = operation
              super().__init__(f"Redis operation '{operation}' failed: {message}")

      # Add more specific exceptions as needed
      ```

    - **Reasoning:** Custom exceptions provide more context than generic ones, allowing for more targeted error handling and logging.

2.  **Implement Retry Logic (Decorator/Utility):**

    - **Action:** Create a retry decorator or utility function (e.g., in `src/utils/retry.py`) for operations prone to transient failures (S3, Redis calls). This can use libraries like `tenacity` or be a custom implementation.
      Create `src/utils/retry.py`:

      ```python
      import asyncio
      import logging
      from functools import wraps
      from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

      logger = logging.getLogger(__name__) # To be replaced by structlog

      # Define common transient exceptions for S3 and Redis if not using aioboto3/redis-py built-in retries extensively
      # For aioboto3, botocore.exceptions.ClientError can be broad. Might need to inspect error codes for retryable ones.
      # For redis-py, redis.exceptions.ConnectionError, redis.exceptions.TimeoutError are common.

      # Example: A general retry decorator for async functions
      def async_retry_transient(attempts=3, initial_wait=1, max_wait=10, transient_exceptions=(Exception,)):
          def decorator(func):
              @wraps(func)
              @retry(
                  stop=stop_after_attempt(attempts),
                  wait=wait_exponential(multiplier=1, min=initial_wait, max=max_wait),
                  retry=retry_if_exception_type(transient_exceptions),
                  reraise=True # Reraise the exception if all attempts fail
              )
              async def wrapper(*args, **kwargs):
                  try:
                      return await func(*args, **kwargs)
                  except Exception as e:
                      logger.warning(f"Retryable operation {func.__name__} failed (attempt {wrapper.retry.statistics.get('attempt_number',0)}/{attempts}): {e}")
                      raise # Important to re-raise for tenacity to catch and retry
              return wrapper
          return decorator
      ```

    - **Action (Apply):** Apply this retry logic to critical S3 and Redis client functions in `src/s3_client.py` and `src/redis_client.py` where appropriate.
      Examples for application:
      - `redis_client.get_redis_connection` (specifically the `_redis_pool.ping()` part during initial connection).
      - `s3_client.create_s3_multipart_upload`
      - `s3_client.upload_s3_part`
      - `s3_client.complete_s3_multipart_upload`
      - `s3_client.abort_s3_multipart_upload`
      - `redis_client` functions performing individual HSET/SADD if not part of a pipeline already robust to Redis being busy (e.g., `init_stream_metadata`, `set_stream_status`, `add_stream_part_info`).
      - Note: `aioboto3` already has some built-in retry mechanisms configured via `botocore`. This custom retry would be for application-level retries or if more control is needed (e.g. different retry strategy for specific operations, or retrying on custom exceptions).
      - `redis-py` async client also has some retry on connection establishment.
    - **Reasoning:** Improves resilience to temporary network issues or service hiccups.

3.  **Refine Error Handling in `StreamProcessor` and Clients:**

    - **Action:** Throughout `src/stream_processor.py`, `src/s3_client.py`, `src/redis_client.py`, catch specific exceptions (both custom and from libraries like `botocore.exceptions.ClientError`, `redis.exceptions.RedisError`) and handle them by:

      - Logging detailed error information (Phase 9 will improve this with `structlog`).
      - Raising custom application exceptions where appropriate.
      - Updating stream status in Redis to reflect failure (e.g., `failed_s3_upload`, `failed_redis_update`).
      - Moving `stream_id` to a `streams:failed` set in Redis.

    - **Example in `src/s3_client.py` `upload_s3_part`:**
      ```python
      # from src.exceptions import S3OperationError
      # from botocore.exceptions import ClientError # Already there
      # ... inside upload_s3_part ...
      # except ClientError as e:
      #     err_msg = f"S3 ClientError uploading part {part_number} for {object_key}: {e}"
      #     logger.error(err_msg, exc_info=True)
      #     raise S3OperationError(operation="upload_part", message=str(e))
      # except Exception as e: # Catch-all for unexpected
      #     err_msg = f"Unexpected error during S3 part upload for {object_key}, part {part_number}: {e}"
      #     logger.error(err_msg, exc_info=True)
      #     raise S3OperationError(operation="upload_part", message=str(e))
      ```
    - **Note on `add_stream_to_failed_set`:** This function should have been defined in `src/redis_client.py` as part of Phase 7 (Checkpoint Recovery & Resume Logic) when handling unrecoverable states like a missing file on resume. Phase 8 will utilize this function extensively when non-transient errors occur during stream processing or finalization.
    - **Reasoning:** Makes error states explicit and recorded, aiding debugging and potential manual intervention or automated cleanup.

4.  **Implement Graceful Shutdown Coordination (`src/main.py`):**

    - **Action:** Enhance `main()` to handle `SIGINT` and `SIGTERM` for graceful shutdown.

      - Use `asyncio.Event`s to signal shutdown to all long-running tasks (watcher, stale stream checker, active stream processors).
      - Ensure `StreamProcessor` tasks, when cancelled or signaled, attempt to finish critical work (like a current chunk upload if possible) or at least update Redis state before exiting.
      - Use `asyncio.gather` with `return_exceptions=True` to wait for tasks to complete during shutdown.

    - **`src/main.py` modifications:**

      ```python
      # In src/main.py
      import signal
      # ...
      shutdown_signal_event = asyncio.Event() # Global event for all tasks to monitor

      def signal_handler(sig, frame):
          logger.info(f"Signal {sig} received. Initiating graceful shutdown...")
          shutdown_signal_event.set()

      # In main() function:
      # async def main():
      #    loop = asyncio.get_running_loop()
      #    loop.add_signal_handler(signal.SIGINT, signal_handler, signal.SIGINT, None)
      #    loop.add_signal_handler(signal.SIGTERM, signal_handler, signal.SIGTERM, None)
      #
      #    # Pass shutdown_signal_event to video_file_watcher, periodic_stale_stream_check
      #    # The awatch already takes a stop_event, which can be shutdown_signal_event.
      #    # periodic_stale_stream_check loop needs to check `while not shutdown_signal_event.is_set():`
      #
      #    # When creating tasks for StreamProcessors, they also need a way to know about shutdown_signal_event
      #    # or be explicitly cancelled and handle CancelledError to do cleanup.
      #    # For example, StreamProcessor methods might check this event before long operations.
      #
      #    try:
      #        # ... main execution ...
      #        # Replace primary while True loop with: `while not shutdown_signal_event.is_set(): await asyncio.sleep(0.1)` if main has its own loop,
      #        # or rely on tasks checking the event.
      #        # If main primarily orchestrates starting tasks and waiting, it might wait for the shutdown event here.
      #        await shutdown_signal_event.wait() # Main thread waits here until signal
      #    except asyncio.CancelledError:
      #        logger.info("Main task cancelled.") # Should not happen if signal_handler sets event
      #    finally:
      #        logger.info("Graceful shutdown sequence started in main finally block...")
      #        # stale_check_stop_event.set() # This was the old way, now use shutdown_signal_event
      #        # stop_event.set() # For watcher, also use shutdown_signal_event if awatch is modified or re-wrapped
      #
      #        # Ensure all created tasks are collected and awaited
      #        tasks_to_await = []
      #        if 'stale_check_task' in locals() and stale_check_task:
      #            tasks_to_await.append(stale_check_task)
      #        # Watcher task from `async for` loop needs to be handled; `awatch` stop_event will cause it to exit
      #
      #        # For StreamProcessor tasks in active_processors:
      #        for processor_task in active_stream_tasks.values(): # Assuming active_stream_tasks holds the asyncio.Task objects
      #            if not processor_task.done():
      #                processor_task.cancel() # Signal cancellation
      #                tasks_to_await.append(processor_task)
      #
      #        if tasks_to_await:
      #            logger.info(f"Waiting for {len(tasks_to_await)} background tasks to complete shutdown...")
      #            await asyncio.gather(*tasks_to_await, return_exceptions=True)
      #
      #        await close_redis_connection()
      #        await close_s3_resources()
      #        logger.info("Application stopped gracefully.")
      ```

      **Refine `StreamProcessor` task management:**

      - It is recommended to transition from `active_processors: dict[Path, StreamProcessor]` (used in Phase 4/5, keyed by file path) to `active_stream_tasks: dict[str, asyncio.Task]`.
      - The key for `active_stream_tasks` should ideally be the `stream_id` (a string UUID).
      - This dictionary will store the main asyncio.Task responsible for a given stream's lifecycle (e.g., the task returned by `manage_new_stream_creation` or `resume_stream_processing`).
      - When tasks are created in `manage_new_stream_creation` (Phase 4, for new streams) and `resume_stream_processing` (Phase 7, for resumed streams), they should be added to this `active_stream_tasks` dictionary, keyed by their `stream_id`.
      - The `periodic_stale_stream_check` (Phase 5) currently looks up processors from `active_processors` using file path. If `active_processors` is removed in favor of `active_stream_tasks`, the stale checker would need to either:
        a) Iterate `active_stream_tasks.values()`, get the processor from the task (if possible, or task has a reference), then proceed.
        b) More simply, the stale checker identifies a stale `stream_id` from Redis. If `StreamProcessor.finalize_stream()` is the designated method, it would need a way to get the `StreamProcessor` instance. One way is for `StreamProcessor` instances themselves to be stored in a dict keyed by `stream_id` if they need to be accessed directly by `stream_id` for operations like `finalize_stream` when invoked by the stale checker. Alternatively, the task in `active_stream_tasks` could be specifically for the `processor.process_file_write()` loop, and `finalize_stream` is called on the processor instance obtained differently.
      - For simplicity and consistency with earlier phases where `active_processors` (keyed by `Path`) was used to get the `StreamProcessor` instance to call methods like `finalize_stream`, we can keep `active_processors: dict[Path, StreamProcessor]` for direct processor access based on file path, and `active_stream_tasks: dict[str, asyncio.Task]` for managing the lifecycle of the main processing coroutine (e.g. what `manage_new_stream_creation` or `resume_stream_processing` spawns).
      - During shutdown, iterate `active_stream_tasks.values()` to cancel and await these primary tasks.

      **In `StreamProcessor` methods (e.g., `process_file_write`, `finalize_stream`):** Handle `asyncio.CancelledError` to perform cleanup.

      ```python
      # In StreamProcessor.process_file_write
      # from src.redis_client import set_stream_status # Assuming a generic status update function
      # try:
      #    # ... main loop ...
      #    if shutdown_signal_event.is_set(): # Hypothetical check if shutdown is signaled
      #        logger.info(f"[{self.stream_id}] Shutdown signaled during processing. Attempting to save state.")
      #        # Perform quick save if possible
      #        await set_stream_status(self.stream_id, "interrupted_shutdown") # New status
      #        return # Exit loop
      # except asyncio.CancelledError:
      #    logger.info(f"[{self.stream_id}] Processing task cancelled. Attempting to save state.")
      #    await set_stream_status(self.stream_id, "interrupted_cancelled") # New status
      #    # Perform any other essential cleanup for this stream part.
      #    raise # Important to re-raise CancelledError
      ```

      - **Note on Redis status:** New statuses like "interrupted_shutdown" or "interrupted_cancelled" should be considered. The existing `set_stream_status(stream_id, status)` can be used. This implies the main processing loops within `StreamProcessor` might also need access to the global `shutdown_signal_event` if they are to react proactively before cancellation.

    - **Reasoning:** Ensures data integrity by allowing ongoing operations to complete or save state before the application exits. `dumb-init` in Docker helps with proper signal propagation.

5.  **Task Management and Concurrency Control (Refinement):**

    - **Action:** Introduce a semaphore or a bounded `asyncio.Queue` if needed to limit the number of concurrently active `StreamProcessor` tasks, preventing resource exhaustion if many files arrive simultaneously.

      ```python
      # In src/main.py
      # MAX_CONCURRENT_STREAMS = int(os.getenv("MAX_CONCURRENT_STREAMS", "10"))
      # stream_processing_semaphore = asyncio.Semaphore(MAX_CONCURRENT_STREAMS)

      # When creating a task for a new stream in handle_stream_event or resume_stream_processing:
      # async def process_stream_with_semaphore(processor_coro):
      #    async with stream_processing_semaphore:
      #        await processor_coro
      #
      # # Example usage:
      # coro = processor.process_file_write() # or process_new_stream(event)
      # task = asyncio.create_task(process_stream_with_semaphore(coro))
      # active_stream_tasks[stream_id] = task
      ```

    - **Reasoning:** Provides stability under high load.

6.  **Unit Tests:**
    - **Action:**
      - Test custom exceptions are raised correctly.
      - Test retry logic (mock decorated functions to throw transient errors and verify retries).
      - Test graceful shutdown: Simulate signals, verify tasks are cancelled/awaited, and resources are closed. This might involve more complex `asyncio` testing.
      - Test error handling paths in `StreamProcessor` (e.g., S3 upload fails, Redis update fails, stream status is set to failed).
    - **Reasoning:** Verifies the enhanced resilience and shutdown procedures.

**Expected Outcome:**

- The application uses custom exceptions for better error tracking.
- Transient errors in S3/Redis calls are retried automatically.
- Permanent errors lead to streams being marked as `failed` in Redis.
- The application shuts down gracefully on `SIGINT`/`SIGTERM`, allowing active tasks to attempt completion or save state.
- Concurrency of stream processing can be limited to prevent resource exhaustion.

**Next Phase:** Phase 9 will focus on Observability, implementing structured logging with `structlog` and Prometheus metrics.
