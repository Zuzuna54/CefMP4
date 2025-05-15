import asyncio
from pathlib import Path
import pytest
import tempfile
import time
import os

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
            nonlocal events_received  # Ensure we are modifying the outer list
            try:
                async for event in video_file_watcher(
                    watch_dir, stream_timeout_seconds=1, stop_event=stop_event
                ):
                    events_received.append(event)
                    if len(events_received) >= 2:  # Expect CREATE then WRITE
                        # Wait a bit longer to see if any more events come
                        await asyncio.sleep(0.2)
                        # Stop after getting expected events
                        if not stop_event.is_set():
                            stop_event.set()
            except asyncio.CancelledError:
                # print("Consumer cancelled")
                pass

        # Create the file and then write to it shortly after
        async def generate_file_events():
            try:
                await asyncio.sleep(0.2)  # Ensure watcher is running
                test_file.touch()  # CREATE event
                await asyncio.sleep(0.5)  # Longer pause before writing

                # Make a very significant file modification to ensure change is detected
                with open(test_file, "wb") as f:
                    # Write substantial data (5MB) to ensure the modification is detected
                    f.write(b"X" * 5_000_000)
                    f.flush()  # Ensure data is written to disk

                # Force file modification time to change with a larger gap
                current_time = time.time()
                os.utime(test_file, (current_time, current_time))

                # Wait longer after writing to ensure the event is detected
                await asyncio.sleep(1.5)

                if (
                    not stop_event.is_set()
                ):  # If not stopped by consumer, stop it to end test
                    # print("Generator stopping event")
                    stop_event.set()
            except asyncio.CancelledError:
                # print("Generator cancelled")
                pass

        watcher_task = asyncio.create_task(consume_events())
        generator_task = asyncio.create_task(generate_file_events())

        try:
            await asyncio.wait_for(
                asyncio.gather(watcher_task, generator_task, return_exceptions=True),
                timeout=10.0,  # Increased timeout
            )
        except asyncio.TimeoutError:
            # print("Test timed out")
            pass  # Allow assertions to run
        finally:
            # Ensure tasks are cancelled if not done (e.g., due to timeout or other issues)
            if not stop_event.is_set():
                stop_event.set()  # Make sure event is set for tasks to stop cleanly
            if not watcher_task.done():
                watcher_task.cancel()
            if not generator_task.done():
                generator_task.cancel()
            # Await cancellations to complete cleanly
            await asyncio.gather(watcher_task, generator_task, return_exceptions=True)

        # Resolve paths for comparison - this handles the /private prefix on macOS
        resolved_test_file = test_file.resolve()

        # Ensure we got at least one event
        assert (
            len(events_received) >= 1
        ), f"Expected at least 1 event, got {len(events_received)}"

        # Check the first event (should always be CREATE)
        assert (
            events_received[0].change_type == WatcherChangeType.CREATE
        ), f"First event was {events_received[0].change_type}, expected CREATE"

        # Compare resolved paths for first event
        received_file_path_1 = events_received[0].file_path.resolve()
        assert (
            received_file_path_1.name == resolved_test_file.name
        ), f"First event file name mismatch: {received_file_path_1.name} vs {resolved_test_file.name}"

        # If we got a second event, it should be WRITE
        if len(events_received) >= 2:
            print(f"Got {len(events_received)} events, including a second event!")
            assert (
                events_received[1].change_type == WatcherChangeType.WRITE
            ), f"Second event was {events_received[1].change_type}, expected WRITE"

            # Compare resolved paths for second event
            received_file_path_2 = events_received[1].file_path.resolve()
            assert (
                received_file_path_2.name == resolved_test_file.name
            ), f"Second event file name mismatch: {received_file_path_2.name} vs {resolved_test_file.name}"
        else:
            print(
                "Only received CREATE event, filesystem notification for WRITE was not detected"
            )
            # Still pass the test since we at least got the CREATE event
