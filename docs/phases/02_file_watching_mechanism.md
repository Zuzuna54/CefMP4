# Phase 2: File Watching Mechanism

**Objective:** Implement the core file watching logic to detect new and modified `.mp4` files in the specified directory, and to identify when a stream becomes idle.

**Builds upon:** Phase 1 (Project Setup, `src/config.py`, `src/main.py`)

**Steps & Design Choices:**

1.  **Add Dependencies:**

    - **Action:** Add `watchfiles` to `pyproject.toml` under `[project.dependencies]`.
      ```toml
      # [project]
      # dependencies = [
      #    ...
      #    "watchfiles",
      # ]
      ```
    - **Reasoning:** `watchfiles` is specified in `requiremnts.md` for its robust async file watching capabilities.
    - **Note:** After updating `pyproject.toml`, you would typically run `pip install -e .` or similar to update your local environment, and the Docker image will pick this up on the next build via `requirements.txt` (which we'll generate later or manage through `pip install .`). For now, we note the dependency.

2.  **Define `StreamEvent` Data Structure (`src/events.py`):**

    - **Action:** Create `src/events.py`.

      ```python
      from enum import Enum, auto
      from pathlib import Path
      from pydantic import BaseModel

      class WatcherChangeType(Enum):
          CREATE = auto()  # File created or first seen
          WRITE = auto()   # File modified
          DELETE = auto()  # File deleted (optional, for future robustness)
          IDLE = auto()    # File has not been modified for stream_timeout_seconds

      class StreamEvent(BaseModel):
          change_type: WatcherChangeType
          file_path: Path
          # stream_id: str | None = None # stream_id will be generated by the processor

      ```

    - **Reasoning:** Defines a clear and typed structure for events emitted by the watcher. `WatcherChangeType` includes `CREATE`, `WRITE`, and `IDLE` as per requirements. `DELETE` is included for future-proofing. `pydantic.BaseModel` ensures type validation.
    - **Note for Phase 2:** While `IDLE` is part of `WatcherChangeType` for overall system event definition, the `video_file_watcher` implemented in this phase will _not_ yet generate `IDLE` events. IDLE detection based on `stream_timeout_seconds` will be handled by a separate mechanism (likely a periodic stale scan task) in a later phase (e.g., Phase 5), which will then emit the `IDLE` event.

3.  **Implement File Watcher Logic (`src/watcher.py`):**

    - **Action:** Create `src/watcher.py`.

      ```python
      import asyncio
      import logging
      from pathlib import Path
      from typing import AsyncGenerator

      from watchfiles import awatch, Change

      from src.config import settings
      from src.events import StreamEvent, WatcherChangeType

      logger = logging.getLogger(__name__) # Will be replaced by structlog

      # Keep track of files and their last modification times and active status
      # Key: file_path (str), Value: last_event_is_write (bool)
      # This helps to only emit CREATE once, then subsequent changes are WRITEs until IDLE
      # And to only emit IDLE if the last actual filesystem event was a write (not an add then idle)
      _active_files_last_event_write: dict[str, bool] = {}

      async def video_file_watcher(
          watch_dir: Path,
          stream_timeout_seconds: float,
          stop_event: asyncio.Event
      ) -> AsyncGenerator[StreamEvent, None]:
          logger.info(f"Starting watcher on {watch_dir} for .mp4 files. Timeout: {stream_timeout_seconds}s")

          async for changes in awatch(watch_dir, stop_event=stop_event, watch_filter=lambda change, filename: filename.endswith('.mp4')):
              for change_type_raw, file_path_str in changes:
                  file_path = Path(file_path_str)
                  logger.debug(f"Raw change detected: {change_type_raw.name} for {file_path}")

                  if change_type_raw == Change.added:
                      if file_path_str not in _active_files_last_event_write:
                          logger.info(f"New file detected (CREATE): {file_path}")
                          _active_files_last_event_write[file_path_str] = False # Initial event is add, not write
                          yield StreamEvent(change_type=WatcherChangeType.CREATE, file_path=file_path)
                      # else: file re-appeared after deletion, or a duplicate add event, treat as modified if already known.
                      # For simplicity, we only trigger CREATE once. Subsequent adds could be treated as modifies if needed.

                  elif change_type_raw == Change.modified:
                      # If it was previously just added, the first modification also can signify it's truly being written to.
                      # If it was already known and modified, it's a clear WRITE event.
                      if file_path_str not in _active_files_last_event_write:
                           # File appeared and was immediately modified (e.g. SCP)
                          logger.info(f"New file detected via modify (CREATE): {file_path}")
                          _active_files_last_event_write[file_path_str] = True
                          yield StreamEvent(change_type=WatcherChangeType.CREATE, file_path=file_path)
                          # Then immediately yield a WRITE as well, as it was modified
                          yield StreamEvent(change_type=WatcherChangeType.WRITE, file_path=file_path)
                      else:
                          logger.info(f"File modified (WRITE): {file_path}")
                          _active_files_last_event_write[file_path_str] = True
                          yield StreamEvent(change_type=WatcherChangeType.WRITE, file_path=file_path)

                  elif change_type_raw == Change.deleted:
                      logger.info(f"File deleted (DELETE): {file_path}")
                      if file_path_str in _active_files_last_event_write:
                          del _active_files_last_event_write[file_path_str]
                      yield StreamEvent(change_type=WatcherChangeType.DELETE, file_path=file_path)
                      # Further processing for DELETE (e.g. cleanup) would be handled by the main loop

              # Check for IDLE streams - This is a simplified periodic check within the watcher loop.
              # A more robust solution for IDLE detection might involve a separate task that tracks
              # last_activity_at for each file, especially if watchfiles events are sparse.
              # However, `requiremnts.md` mentions: "Watcher re‑fires on modified; reads deltas until idle timeout"
              # and "Stream‑completed detection by mtime‑Δ > timeout", implying the watcher or a closely related mechanism
              # is aware of the timeout. `watchfiles` `debounce` and `timeout` primarily relate to event batching.
              # The provided `awaitWriteFinish` in the JS example implies an external timeout mechanism for writes.
              # For now, we'll rely on a simplified idle check loop here, or integrate it more tightly with the consumer.
              # The 'Gap Analysis' in `requiremnts.md` mentions: "Periodic stale‑scan task; STREAM_TIMEOUT env".
              # This confirms a separate task. The `video_file_watcher` in this phase will therefore focus on
              # emitting `CREATE`, `WRITE`, and `DELETE` events. The `stream_timeout_seconds` parameter is accepted
              # but will be primarily utilized by the future stale-scan task for IDLE detection.

      ```

    - **Reasoning:**
      - Uses `awatch` from `watchfiles` for asynchronous monitoring.
      - Filters for `.mp4` files directly in `awatch` using `watch_filter`.
      - Translates `watchfiles.Change` (ADD, MODIFIED, DELETED) into our domain-specific `WatcherChangeType`.
      - The `_active_files_last_event_write` dictionary helps differentiate a true new file (`CREATE`) from subsequent modifications (`WRITE`).
      - **IDLE Detection Strategy for Phase 2:** As noted, this `video_file_watcher` implementation focuses on reacting to direct filesystem events (`add`, `modify`, `delete`) from `watchfiles`. It translates these into `CREATE`, `WRITE`, and `DELETE` `StreamEvent`s. The generation of `IDLE` events, which requires tracking inactivity against `settings.stream_timeout_seconds`, is deferred to a dedicated "periodic stale-scan task" to be implemented in a later phase (likely Phase 5, aligning with stream finalization logic). This separation keeps the watcher focused on direct file changes.

4.  **Integrate Watcher into `src/main.py`:**

    - **Action:** Modify `src/main.py` to use the watcher.

      ```python
      import asyncio
      import logging
      from pathlib import Path

      from src.config import settings
      from src.watcher import video_file_watcher # New import
      from src.events import StreamEvent # New import

      logging.basicConfig(level=settings.log_level.upper(),
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      logger = logging.getLogger(__name__)

      async def handle_stream_event(event: StreamEvent):
          logger.info(f"Received event: Type={event.change_type.name}, Path={event.file_path}")
          # In future phases, this will trigger processing pipelines based on event type.
          # For CREATE: Initiate stream processing, create S3 multipart upload, etc. (Phase 3)
          # For WRITE: Read chunks, upload to S3. (Phase 4)
          # For IDLE (generated by a separate stale-scan task in a later phase): Finalize stream, complete S3 upload, generate metadata. (Phase 5 onwards)
          # For DELETE: (Optional) clean up resources.
          await asyncio.sleep(0.1) # Simulate work

      async def main():
          logger.info("Application starting...")
          logger.info(f"Watching directory: {settings.watch_dir}")
          logger.info(f"Stream timeout for IDLE detection: {settings.stream_timeout_seconds} seconds (will be used by a separate task later)")

          watch_dir = Path(settings.watch_dir)
          if not watch_dir.exists():
              logger.warning(f"Watch directory {watch_dir} does not exist. Creating it.")
              watch_dir.mkdir(parents=True, exist_ok=True)
          elif not watch_dir.is_dir():
              logger.error(f"Watch path {watch_dir} exists but is not a directory. Exiting.")
              return

          stop_event = asyncio.Event()
          try:
              async for event in video_file_watcher(watch_dir, settings.stream_timeout_seconds, stop_event):
                  await handle_stream_event(event)
          except KeyboardInterrupt:
              logger.info("Keyboard interrupt received. Signaling watcher to stop...")
          finally:
              logger.info("Shutting down watcher...")
              stop_event.set()
              # Allow watcher to process stop_event, then cleanup
              await asyncio.sleep(1) # Give watcher a moment to stop gracefully
              logger.info("Application stopped.")

      if __name__ == "__main__":
          try:
              asyncio.run(main())
          except KeyboardInterrupt:
              # Main() already handles KeyboardInterrupt for graceful shutdown
              pass # Or logger.info("Application exited via interrupt.")
      ```

    - **Reasoning:**
      - Imports and calls the `video_file_watcher`.
      - Creates the watch directory if it doesn't exist.
      - Iterates over the events and logs them (actual processing will be in later phases).
      - Includes an `asyncio.Event` (`stop_event`) for graceful shutdown of the watcher, which `awatch` supports.
      - The `handle_stream_event` is a placeholder for future logic.

5.  **Initial Unit Tests for Watcher (`tests/test_watcher.py`):**

    - **Action:** Create `tests/test_watcher.py` (basic structure, more comprehensive tests later).

      ```python
      import asyncio
      from pathlib import Path
      import pytest
      import tempfile
      from unittest.mock import patch # If needed for more complex scenarios

      from src.watcher import video_file_watcher
      from src.events import WatcherChangeType, StreamEvent

      @pytest.mark.asyncio
      async def test_video_file_watcher_create_write_events():
          with tempfile.TemporaryDirectory() as tmpdir_name:
              watch_dir = Path(tmpdir_name)
              test_file = watch_dir / "test_video.mp4"
              stop_event = asyncio.Event()

              events_received = []

              async def consume_events():
                  async for event in video_file_watcher(watch_dir, stream_timeout_seconds=1, stop_event=stop_event):
                      events_received.append(event)
                      if len(events_received) >= 2: # Expect CREATE then WRITE
                          stop_event.set() # Stop after getting expected events

              # Create the file and then write to it shortly after
              async def generate_file_events():
                  await asyncio.sleep(0.1) # Ensure watcher is running
                  test_file.touch() # CREATE event
                  await asyncio.sleep(0.1) # Brief pause
                  with open(test_file, "a") as f:
                      f.write("some data") # WRITE event
                  await asyncio.sleep(0.1)
                  if not stop_event.is_set(): # If not stopped by consumer, stop it to end test
                      stop_event.set()

              watcher_task = asyncio.create_task(consume_events())
              generator_task = asyncio.create_task(generate_file_events())

              await asyncio.wait([watcher_task, generator_task], timeout=5)

              assert len(events_received) >= 2
              assert events_received[0].change_type == WatcherChangeType.CREATE
              assert events_received[0].file_path == test_file
              assert events_received[1].change_type == WatcherChangeType.WRITE
              assert events_received[1].file_path == test_file

              # Cleanup tasks if they are still running (e.g., due to timeout)
              if not watcher_task.done():
                  watcher_task.cancel()
              if not generator_task.done():
                  generator_task.cancel()
      ```

    - **Reasoning:** Establishes the testing structure. This initial test checks for basic CREATE and WRITE event generation. `tempfile` is used to create a temporary directory for watching. More sophisticated tests for IDLE, DELETE, and edge cases will be added as those features are fully implemented.

**Docker & Dependencies Update:**

- The `Dockerfile` from Phase 1 will need `watchfiles` installed. This is usually handled by installing from `requirements.txt`. For now, we'll assume `pip install watchfiles` would be part of the build process. A dedicated step to manage `requirements.txt` from `pyproject.toml` will be handled in a later phase (e.g., Phase 10, or when CI is set up).

**Expected Outcome:**

- The application can monitor the `WATCH_DIR` for `.mp4` files.
- `CREATE` and `WRITE` events are logged when files are added or modified.
- The watcher can be gracefully shut down.
- Basic unit tests for the watcher are in place.
- The groundwork for IDLE event detection by a separate component is laid.

**Next Phase:** Phase 3 will focus on Initial Stream State Management using Redis and initiating S3 multipart uploads upon `CREATE` events.
