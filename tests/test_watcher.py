import asyncio
from pathlib import Path
import pytest
import tempfile
# from unittest.mock import patch # If needed for more complex scenarios, not used in this basic test

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
            nonlocal events_received # Ensure we are modifying the outer list
            try:
                async for event in video_file_watcher(watch_dir, stream_timeout_seconds=1, stop_event=stop_event):
                    events_received.append(event)
                    if len(events_received) >= 2: # Expect CREATE then WRITE
                        # Small delay to ensure test doesn't exit before producer stops if it's fast
                        await asyncio.sleep(0.01)
                        if not stop_event.is_set():
                           stop_event.set() # Stop after getting expected events
            except asyncio.CancelledError:
                # print("Consumer cancelled")
                pass

        # Create the file and then write to it shortly after
        async def generate_file_events():
            try:
                await asyncio.sleep(0.1) # Ensure watcher is running
                test_file.touch() # CREATE event
                await asyncio.sleep(0.1) # Brief pause
                with open(test_file, "a") as f:
                    f.write("some data") # WRITE event
                await asyncio.sleep(0.1) # Give consumer a chance to process
                if not stop_event.is_set(): # If not stopped by consumer, stop it to end test
                    # print("Generator stopping event")
                    stop_event.set()
            except asyncio.CancelledError:
                # print("Generator cancelled")
                pass

        watcher_task = asyncio.create_task(consume_events())
        generator_task = asyncio.create_task(generate_file_events())

        try:
            await asyncio.wait_for(asyncio.gather(watcher_task, generator_task, return_exceptions=True), timeout=5.0)
        except asyncio.TimeoutError:
            # print("Test timed out")
            pass # Allow assertions to run
        finally:
            # Ensure tasks are cancelled if not done (e.g., due to timeout or other issues)
            if not stop_event.is_set():
                stop_event.set() # Make sure event is set for tasks to stop cleanly
            if not watcher_task.done():
                watcher_task.cancel()
            if not generator_task.done():
                generator_task.cancel()
            # Await cancellations to complete cleanly
            await asyncio.gather(watcher_task, generator_task, return_exceptions=True)

        assert len(events_received) >= 2, f"Expected at least 2 events, got {len(events_received)}"
        assert events_received[0].change_type == WatcherChangeType.CREATE, f"First event was {events_received[0].change_type}"
        assert events_received[0].file_path == test_file, f"First event path was {events_received[0].file_path}"
        assert events_received[1].change_type == WatcherChangeType.WRITE, f"Second event was {events_received[1].change_type}"
        assert events_received[1].file_path == test_file, f"Second event path was {events_received[1].file_path}" 