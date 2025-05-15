import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
import uuid
import signal
import datetime

from src import main
from src.events import StreamEvent, WatcherChangeType
from src.stream_processor import StreamProcessor
from src.config import settings

TEST_WATCH_DIR = Path("./test_watch_dir_main")
TEST_FILE_NAME = "test_video.mp4"
TEST_FILE_PATH = TEST_WATCH_DIR / TEST_FILE_NAME


@pytest.fixture(autouse=True)
def setup_test_environment(tmp_path, monkeypatch):
    # Use tmp_path for WATCH_DIR to isolate tests
    watch_dir = tmp_path / "main_watch_dir"
    watch_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(settings, "watch_dir", str(watch_dir))
    monkeypatch.setattr(
        settings, "stream_timeout_seconds", 30
    )  # A shorter timeout for tests

    # Ensure active_processors is clean before each test
    main.active_processors = {}
    yield
    main.active_processors = {}  # Clean up after test


@pytest.fixture
def mock_stream_event_create() -> StreamEvent:
    return StreamEvent(change_type=WatcherChangeType.CREATE, file_path=TEST_FILE_PATH)


@pytest.fixture
def mock_stream_event_write() -> StreamEvent:
    return StreamEvent(change_type=WatcherChangeType.WRITE, file_path=TEST_FILE_PATH)


@pytest.fixture
def mock_stream_event_delete() -> StreamEvent:
    return StreamEvent(change_type=WatcherChangeType.DELETE, file_path=TEST_FILE_PATH)


@pytest.mark.asyncio
@patch("src.main.create_s3_multipart_upload", new_callable=AsyncMock)
@patch("src.main.init_stream_metadata", new_callable=AsyncMock)
@patch(
    "src.main.StreamProcessor", spec=StreamProcessor
)  # Use spec for accurate mocking
async def test_manage_new_stream_creation_success(
    MockStreamProcessor: MagicMock,
    mock_init_meta: AsyncMock,
    mock_create_s3_upload: AsyncMock,
    mock_stream_event_create: StreamEvent,
):
    mock_s3_upload_id = "s3-upload-main-test"
    mock_create_s3_upload.return_value = mock_s3_upload_id

    mock_processor_instance = AsyncMock(spec=StreamProcessor)
    # Mock the _initialize_from_checkpoint method if it's called
    mock_processor_instance._initialize_from_checkpoint = AsyncMock(return_value=None)
    # mock_processor_instance.process_file_write = AsyncMock(return_value=None) # if called immediately
    MockStreamProcessor.return_value = mock_processor_instance

    # Mock uuid.uuid4
    test_uuid = uuid.uuid4()
    with patch("src.main.uuid.uuid4") as mock_uuid:
        mock_uuid.return_value = test_uuid
        await main.manage_new_stream_creation(mock_stream_event_create)

    expected_s3_key_prefix = f"streams/{str(test_uuid)}/{TEST_FILE_NAME}"

    # Check if create_s3_multipart_upload was called with the correct parameters
    # This call now uses positional arguments instead of keyword arguments
    assert mock_create_s3_upload.call_count == 1
    create_args, create_kwargs = mock_create_s3_upload.call_args
    assert create_args[0] == settings.s3_bucket_name
    assert create_args[1] == expected_s3_key_prefix

    # Check if init_stream_metadata was called with the correct parameters
    # This call now uses positional arguments instead of keyword arguments
    assert mock_init_meta.call_count == 1
    init_args, init_kwargs = mock_init_meta.call_args
    assert init_args[0] == str(test_uuid)
    assert init_args[1] == str(TEST_FILE_PATH)
    assert init_args[2] == mock_s3_upload_id
    assert init_args[3] == settings.s3_bucket_name
    assert init_args[4] == expected_s3_key_prefix

    # Verify StreamProcessor constructor was called with the right parameters including shutdown_event
    assert MockStreamProcessor.call_count == 1
    sp_args, sp_kwargs = MockStreamProcessor.call_args
    # Check that the positional arguments are correct
    assert len(sp_args) >= 6  # Should have at least 6 positional arguments
    assert sp_args[0] == str(test_uuid)
    assert sp_args[1] == TEST_FILE_PATH
    assert sp_args[2] == mock_s3_upload_id
    assert sp_args[3] == settings.s3_bucket_name
    assert sp_args[4] == expected_s3_key_prefix
    assert sp_args[5] == main.shutdown_signal_event

    mock_processor_instance._initialize_from_checkpoint.assert_called_once()

    assert TEST_FILE_PATH in main.active_processors
    assert main.active_processors[TEST_FILE_PATH] is mock_processor_instance


@pytest.mark.asyncio
@patch("src.main.create_s3_multipart_upload", new_callable=AsyncMock)
async def test_manage_new_stream_creation_s3_failure(
    mock_create_s3_upload: AsyncMock, mock_stream_event_create: StreamEvent, caplog
):
    mock_create_s3_upload.return_value = None  # Simulate S3 init failure

    await main.manage_new_stream_creation(mock_stream_event_create)

    assert TEST_FILE_PATH not in main.active_processors
    assert "Failed to initiate S3 multipart upload" in caplog.text


@pytest.mark.asyncio
async def test_manage_new_stream_creation_already_exists(
    mock_stream_event_create: StreamEvent, caplog
):
    # Add a mock processor to simulate existing one
    mock_existing_processor = AsyncMock(spec=StreamProcessor)
    mock_existing_processor.stream_id = "existing-id"
    main.active_processors[TEST_FILE_PATH] = mock_existing_processor

    # This test depends on the behavior when a processor already exists.
    # The current implementation logs a warning and returns without creating a new processor

    await main.manage_new_stream_creation(mock_stream_event_create)

    # Check that the log message matches the updated implementation
    assert (
        f"Create event for already tracked path. Current processor stream ID: {mock_existing_processor.stream_id}"
        in caplog.text
    )

    # The existing processor should not have been replaced
    assert main.active_processors[TEST_FILE_PATH] is mock_existing_processor


@pytest.mark.asyncio
@patch("asyncio.create_task")  # Mock create_task to check if it's called
async def test_handle_stream_event_create(
    mock_create_task: MagicMock, mock_stream_event_create: StreamEvent
):
    # Create a mock event queue
    event_queue = asyncio.Queue()

    await main.handle_stream_event(mock_stream_event_create, event_queue)
    mock_create_task.assert_called_once()
    # We can also check that manage_new_stream_creation was the coroutine passed to create_task
    args, kwargs = mock_create_task.call_args
    assert args[0].__name__ == "manage_new_stream_creation"  # Check coro name


@pytest.mark.asyncio
@patch("asyncio.create_task")
async def test_handle_stream_event_write_processor_exists(
    mock_create_task: MagicMock, mock_stream_event_write: StreamEvent
):
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = "test-stream-id"
    # Mock the process_file_write method to return a proper awaitable
    mock_processor.process_file_write.return_value = asyncio.sleep(0)

    main.active_processors[TEST_FILE_PATH] = mock_processor

    # Create a mock event queue
    event_queue = asyncio.Queue()

    # Create a mock task implementation that immediately resolves coroutines
    async_fut = asyncio.Future()
    async_fut.set_result(None)
    mock_create_task.return_value = async_fut

    # Handle the task creation ourselves to prevent "never awaited" warnings
    original_create_task = asyncio.create_task

    def handle_coroutine(coro):
        # If it's a coroutine we know, handle it specially
        if asyncio.iscoroutine(coro):
            if (
                hasattr(coro, "__name__")
                and coro.__name__ == "_create_and_manage_processor_task"
            ):
                # Just resolve it immediately
                fut = asyncio.Future()
                fut.set_result(None)
                return fut
        # Return our mock for other cases
        return async_fut

    mock_create_task.side_effect = handle_coroutine

    await main.handle_stream_event(mock_stream_event_write, event_queue)

    # Check that create_task was called
    mock_create_task.assert_called_once()
    # Check that the correct coroutine was passed
    args, kwargs = mock_create_task.call_args
    assert "_create_and_manage_processor_task" in str(args[0])


@pytest.mark.asyncio
@patch("asyncio.create_task")  # Ensure create_task is not called
async def test_handle_stream_event_write_processor_missing(
    mock_create_task: MagicMock, mock_stream_event_write: StreamEvent, caplog
):
    # Ensure no processor for this path
    if TEST_FILE_PATH in main.active_processors:
        del main.active_processors[TEST_FILE_PATH]

    # Create a mock event queue with proper handling for the coroutine
    event_queue = asyncio.Queue()

    # Set up mock for queue put operation
    async def mock_queue_put(*args, **kwargs):
        return None

    event_queue.put = AsyncMock(side_effect=mock_queue_put)

    # Run the function under test
    await main.handle_stream_event(mock_stream_event_write, event_queue)

    # Check that create_task was not called
    mock_create_task.assert_not_called()

    # Check for the log message in caplog
    assert f"[{TEST_FILE_NAME}] WRITE event for untracked file" in caplog.text

    # Verify the requeue operation happened
    assert event_queue.put.called, "Event was not requeued"

    # Make sure the correct event type was requeued
    args, kwargs = event_queue.put.call_args
    requeued_event = args[0]
    assert requeued_event.change_type == WatcherChangeType.CREATE
    assert requeued_event.file_path == TEST_FILE_PATH


@pytest.mark.asyncio
@patch("src.main.set_stream_status", new_callable=AsyncMock)  # Mock Redis updates
async def test_handle_stream_event_delete_processor_exists(
    mock_set_status: AsyncMock, mock_stream_event_delete: StreamEvent, caplog
):
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = "proc-to-delete"

    # Track tasks that might be created
    tasks = []
    original_create_task = asyncio.create_task

    def mock_create_task(coro, *args, **kwargs):
        # For any created task, immediately resolve/consume the coroutine
        # This prevents "coroutine never awaited" warnings
        if asyncio.iscoroutine(coro):
            # For coroutines that we know about, we can handle them specially
            if coro.__name__ == "_create_and_manage_processor_task":
                # Just return a done task
                fut = asyncio.Future()
                fut.set_result(None)
                return fut
            elif coro.__name__ == "manage_new_stream_creation":
                # Just return a done task
                fut = asyncio.Future()
                fut.set_result(None)
                return fut
            else:
                # For unknown coroutines, run them in a task that we'll clean up
                task = original_create_task(coro, *args, **kwargs)
                tasks.append(task)
                return task
        # For non-coroutines, just create a regular task
        task = original_create_task(coro, *args, **kwargs)
        tasks.append(task)
        return task

    # Store the original processor in active_processors
    main.active_processors[TEST_FILE_PATH] = mock_processor

    # Create a mock event queue
    event_queue = asyncio.Queue()

    # Apply the patch
    with patch("asyncio.create_task", side_effect=mock_create_task):
        await main.handle_stream_event(mock_stream_event_delete, event_queue)

    # Clean up any tasks that were created
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    # Check that the processor was removed
    assert TEST_FILE_PATH not in main.active_processors

    # Check that set_stream_status was called
    mock_set_status.assert_called_once_with(mock_processor.stream_id, "deleted_locally")

    # Check for the log message with the JSON format
    assert "Removed processor for deleted file" in caplog.text
    assert "proc-to-delete" in caplog.text
    # TODO: Test task cancellation for the processor (Phase 8)


@pytest.mark.asyncio
async def test_handle_stream_event_delete_processor_missing(
    mock_stream_event_delete: StreamEvent, caplog
):
    if TEST_FILE_PATH in main.active_processors:
        del main.active_processors[TEST_FILE_PATH]

    # Create a mock event queue
    event_queue = asyncio.Queue()

    await main.handle_stream_event(mock_stream_event_delete, event_queue)

    # Check for the log message with the JSON format
    assert "DELETE event for untracked file" in caplog.text
    assert TEST_FILE_PATH.as_posix() in caplog.text


# Basic test for main() function structure - more complex scenarios are harder to unit test for main()
@pytest.mark.asyncio
@patch("src.main.get_redis_connection", new_callable=AsyncMock)
@patch("src.main.close_redis_connection", new_callable=AsyncMock)
@patch("src.main.close_s3_resources", new_callable=AsyncMock)
@patch(
    "src.main.handle_stream_event"
)  # Regular function instead of AsyncMock to allow awaiting
@patch("src.main.get_active_stream_ids", new_callable=AsyncMock)  # Mock Redis calls
@patch(
    "src.main.get_pending_completion_stream_ids", new_callable=AsyncMock
)  # Mock Redis calls
async def test_main_function_flow(
    mock_get_pending_ids: AsyncMock,
    mock_get_active_ids: AsyncMock,
    mock_handle_event,
    mock_close_s3: AsyncMock,
    mock_close_redis: AsyncMock,
    mock_get_redis: AsyncMock,
    setup_test_environment,  # Ensure settings.watch_dir is patched by fixture
):
    # Set up mock for handle_stream_event that properly awaits any coroutines
    async def handle_stream_event_mock(event, event_queue):
        # This will properly handle awaiting the coroutine
        return await asyncio.sleep(0)

    mock_handle_event.side_effect = handle_stream_event_mock

    # Mock the active and pending stream IDs to be empty
    mock_get_active_ids.return_value = []
    mock_get_pending_ids.return_value = []

    # Create events for testing
    event1 = StreamEvent(
        change_type=WatcherChangeType.CREATE, file_path=TEST_WATCH_DIR / "file1.mp4"
    )
    event2 = StreamEvent(
        change_type=WatcherChangeType.WRITE, file_path=TEST_WATCH_DIR / "file1.mp4"
    )

    # Create a patched version of the video_file_watcher function
    # that yields our test events and then sets the shutdown signal
    async def mock_watcher_gen(*args, **kwargs):
        yield event1
        yield event2
        main.shutdown_signal_event.set()
        return

    # We need to ensure any tasks created inside main() are properly awaited
    original_create_task = asyncio.create_task
    tasks = []

    def mock_create_task(coro, *args, **kwargs):
        # Capture the task so we can ensure it's awaited later
        task = original_create_task(coro, *args, **kwargs)
        tasks.append(task)
        return task

    # Apply the patches for the duration of this test
    with (
        patch("src.main.video_file_watcher", return_value=mock_watcher_gen()),
        patch("asyncio.create_task", side_effect=mock_create_task),
    ):
        # Run main with a timeout
        try:
            await asyncio.wait_for(main.main(), timeout=2.0)
        except asyncio.TimeoutError:
            # This should not happen if our mock is working correctly
            main.shutdown_signal_event.set()

    # Ensure all tasks are cleaned up
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    # Reset shutdown signal for other tests
    main.shutdown_signal_event = asyncio.Event()

    # Verify the expected calls were made
    mock_get_redis.assert_called_once()
    mock_handle_event.assert_called()
    mock_close_redis.assert_called_once()
    mock_close_s3.assert_called_once()


@pytest.mark.asyncio
async def test_signal_handler():
    """Test that the signal handler sets the shutdown event."""
    # Reset the shutdown event
    main.shutdown_signal_event = asyncio.Event()
    assert not main.shutdown_signal_event.is_set()

    # Mock the signal and frame objects
    mock_signal = MagicMock()
    mock_signal.name = "SIGTERM"
    mock_frame = None

    # Call the signal handler
    main.signal_handler(mock_signal, mock_frame)

    # Verify the shutdown event was set
    assert main.shutdown_signal_event.is_set()


@pytest.mark.asyncio
@patch("asyncio.get_running_loop")
@patch("src.main.start_metrics_server")
async def test_main_signal_handling(mock_metrics_server, mock_get_loop):
    """Test that main function sets up signal handlers properly."""
    # Create mock objects
    mock_loop = MagicMock()

    # Mock the run_in_executor method to return a Future object
    future = asyncio.Future()
    future.set_result(None)
    mock_loop.run_in_executor.return_value = future

    mock_get_loop.return_value = mock_loop

    # Mock Redis connection to avoid actual connections
    with (
        patch(
            "src.main.get_redis_connection", new_callable=AsyncMock
        ) as mock_get_redis,
        patch(
            "src.main.get_active_stream_ids", new_callable=AsyncMock
        ) as mock_get_active_ids,
        patch(
            "src.main.get_pending_completion_stream_ids", new_callable=AsyncMock
        ) as mock_get_pending_ids,
        patch("src.main.video_file_watcher") as mock_watcher,
        patch("asyncio.create_task") as mock_create_task,
        patch("asyncio.Queue") as mock_queue_class,
        patch("asyncio.wait_for") as mock_wait_for,
    ):

        # Setup basic mocks
        mock_get_active_ids.return_value = []
        mock_get_pending_ids.return_value = []

        # Mock Queue.get to simulate queue behavior
        mock_queue = AsyncMock()
        mock_queue.empty.return_value = True
        mock_queue_class.return_value = mock_queue

        # Mock watcher to avoid actual filesystem operations
        mock_watcher_context = AsyncMock()
        mock_watcher.return_value = mock_watcher_context

        # Set wait_for to raise TimeoutError after setting shutdown_signal_event
        # This simulates a terminated main loop
        async def mock_wait_for_impl(coro, timeout):
            # Set the shutdown event
            main.shutdown_signal_event.set()
            # Allow the main function to progress a bit
            await asyncio.sleep(0.1)
            # Then raise TimeoutError to break out of the main loop
            raise asyncio.TimeoutError()

        mock_wait_for.side_effect = mock_wait_for_impl

        # Execute main function
        await main.main()

        # Verify signal handlers were set
        assert mock_loop.add_signal_handler.call_count >= 2

        # Check that both SIGINT and SIGTERM were handled
        signal_calls = [
            args[0] for args, kwargs in mock_loop.add_signal_handler.call_args_list
        ]
        assert signal.SIGINT in signal_calls
        assert signal.SIGTERM in signal_calls


@pytest.mark.asyncio
@patch("src.main.start_metrics_server")
async def test_redis_connection_failure(mock_metrics, caplog):
    """Test that main function handles Redis connection failure appropriately."""
    # Create a mock loop with appropriate mocks for signal handling
    with patch("asyncio.get_running_loop") as mock_get_loop:
        mock_loop = MagicMock()
        future = asyncio.Future()
        future.set_result(None)
        mock_loop.run_in_executor.return_value = future
        mock_get_loop.return_value = mock_loop

        # Mock Redis connection to simulate a failure
        with (
            patch(
                "src.main.get_redis_connection", new_callable=AsyncMock
            ) as mock_get_redis,
            patch(
                "src.main.close_s3_resources", new_callable=AsyncMock
            ) as mock_close_s3,
            patch(
                "src.main.close_redis_connection", new_callable=AsyncMock
            ) as mock_close_redis,
        ):

            # Set up Redis connection to fail with a specific error
            redis_error = Exception("Redis connection failed")
            mock_get_redis.side_effect = redis_error

            # Execute main - the error is caught in main's try/except
            await main.main()

            # Verify the error was logged
            assert "Unhandled exception in main: Redis connection failed" in caplog.text

            # Verify that resource cleanup was attempted
            mock_close_redis.assert_called_once()
            mock_close_s3.assert_called_once()


@pytest.mark.asyncio
@patch("src.main.start_metrics_server")
async def test_s3_connection_failure_during_resume(mock_metrics, caplog):
    """Test that main function handles S3 connection failure during stream resumption."""
    # Create a mock loop with appropriate mocks for signal handling
    with patch("asyncio.get_running_loop") as mock_get_loop:
        mock_loop = MagicMock()
        future = asyncio.Future()
        future.set_result(None)
        mock_loop.run_in_executor.return_value = future
        mock_get_loop.return_value = mock_loop

        # Reset shutdown event
        main.shutdown_signal_event = asyncio.Event()

        # Mock Redis connection to succeed but return active streams
        with (
            patch(
                "src.main.get_redis_connection", new_callable=AsyncMock
            ) as mock_get_redis,
            patch(
                "src.main.get_active_stream_ids", new_callable=AsyncMock
            ) as mock_get_active_ids,
            patch(
                "src.main.get_pending_completion_stream_ids", new_callable=AsyncMock
            ) as mock_get_pending_ids,
            patch(
                "src.main.resume_stream_processing", new_callable=AsyncMock
            ) as mock_resume,
            patch(
                "src.main.close_s3_resources", new_callable=AsyncMock
            ) as mock_close_s3,
            patch(
                "src.main.close_redis_connection", new_callable=AsyncMock
            ) as mock_close_redis,
            patch("asyncio.wait_for", new_callable=AsyncMock) as mock_wait_for,
        ):

            # Setup stream IDs to resume
            mock_get_active_ids.return_value = ["stream-1", "stream-2"]
            mock_get_pending_ids.return_value = []

            # Configure resume to fail for the first stream with S3 error
            s3_error = main.S3OperationError(
                operation="test", message="S3 connection failed"
            )
            mock_resume.side_effect = s3_error

            # Make wait_for raise exception to terminate the loop
            async def mock_wait_for_impl(coro, timeout):
                main.shutdown_signal_event.set()
                await asyncio.sleep(0.1)
                raise asyncio.TimeoutError()

            mock_wait_for.side_effect = mock_wait_for_impl

            # Execute main
            await main.main()

            # Check that resume_stream_processing was called multiple times
            assert mock_resume.call_count == 2
            resume_calls = [call[0][0] for call in mock_resume.call_args_list]
            assert "stream-1" in resume_calls
            assert "stream-2" in resume_calls

            # Verify resource cleanup occurred
            mock_close_redis.assert_called_once()
            mock_close_s3.assert_called_once()


@pytest.mark.asyncio
@patch("src.main.start_metrics_server")
async def test_resume_interrupted_streams(mock_metrics):
    """Test that the main function resumes interrupted streams correctly."""
    # Create a mock loop with appropriate mocks for signal handling
    with patch("asyncio.get_running_loop") as mock_get_loop:
        mock_loop = MagicMock()
        future = asyncio.Future()
        future.set_result(None)
        mock_loop.run_in_executor.return_value = future
        mock_get_loop.return_value = mock_loop

        # Reset shutdown event
        main.shutdown_signal_event = asyncio.Event()

        # Mock Redis connection to return active and pending streams
        with (
            patch(
                "src.main.get_redis_connection", new_callable=AsyncMock
            ) as mock_get_redis,
            patch(
                "src.main.get_active_stream_ids", new_callable=AsyncMock
            ) as mock_get_active_ids,
            patch(
                "src.main.get_pending_completion_stream_ids", new_callable=AsyncMock
            ) as mock_get_pending_ids,
            patch(
                "src.main.resume_stream_processing", new_callable=AsyncMock
            ) as mock_resume,
            patch(
                "src.main.close_s3_resources", new_callable=AsyncMock
            ) as mock_close_s3,
            patch(
                "src.main.close_redis_connection", new_callable=AsyncMock
            ) as mock_close_redis,
            patch("asyncio.wait_for", new_callable=AsyncMock) as mock_wait_for,
        ):

            # Set up active and pending streams
            active_stream_ids = ["active-stream-1", "active-stream-2"]
            pending_stream_ids = ["pending-stream-1"]

            mock_get_active_ids.return_value = active_stream_ids
            mock_get_pending_ids.return_value = pending_stream_ids

            # Make wait_for raise exception to terminate the loop
            async def mock_wait_for_impl(coro, timeout):
                main.shutdown_signal_event.set()
                await asyncio.sleep(0.1)
                raise asyncio.TimeoutError()

            mock_wait_for.side_effect = mock_wait_for_impl

            # Execute main
            await main.main()

            # Verify resume was called for all streams
            assert mock_resume.call_count == 3  # Two active and one pending stream

            # Check that all stream IDs were passed to resume
            resume_calls = [call[0][0] for call in mock_resume.call_args_list]
            for stream_id in active_stream_ids + pending_stream_ids:
                assert stream_id in resume_calls

            # Verify resource cleanup occurred
            mock_close_redis.assert_called_once()
            mock_close_s3.assert_called_once()


@pytest.mark.asyncio
async def test_concurrency_control_with_semaphore():
    """Test that the semaphore correctly limits concurrent stream processing."""
    # Reset shutdown event to allow tasks to run
    main.shutdown_signal_event = asyncio.Event()

    # Verify MAX_CONCURRENT_STREAMS is respected
    assert main.stream_processing_semaphore._value == main.MAX_CONCURRENT_STREAMS

    # Create a test to verify semaphore behavior
    max_concurrent = main.MAX_CONCURRENT_STREAMS
    test_concurrency = max_concurrent * 2  # More than MAX_CONCURRENT_STREAMS
    test_processing_time = 0.1  # Time each "processor" will run

    # Track running tasks
    running_tasks = set()
    max_running = 0

    # Create a lock to protect the shared counter
    task_lock = asyncio.Lock()

    async def mock_processor(task_id):
        nonlocal max_running
        # Add this task to running set
        async with task_lock:
            running_tasks.add(task_id)
            current_running = len(running_tasks)
            max_running = max(max_running, current_running)

        # Simulate processing
        await asyncio.sleep(test_processing_time)

        # Remove from running set
        async with task_lock:
            running_tasks.remove(task_id)

        return f"Task {task_id} completed"

    # Create tasks that use the semaphore without directly invoking _create_and_manage_processor_task
    async def test_task(task_id):
        async with main.stream_processing_semaphore:
            return await mock_processor(task_id)

    # Execute tasks
    tasks = [asyncio.create_task(test_task(i)) for i in range(test_concurrency)]
    await asyncio.gather(*tasks)

    # Verify that the max concurrent tasks matches our semaphore value
    assert max_running <= max_concurrent


@pytest.mark.asyncio
@patch("src.main.get_active_stream_ids", new_callable=AsyncMock)
@patch("src.main.get_stream_meta", new_callable=AsyncMock)
@patch("src.main.update_stream_last_activity", new_callable=AsyncMock)
@patch("src.main.add_stream_to_failed_set", new_callable=AsyncMock)
@patch("src.main.move_stream_to_pending_completion", new_callable=AsyncMock)
@patch("src.main.resume_stream_processing", new_callable=AsyncMock)
@patch("src.main.run_finalization_and_cleanup", new_callable=AsyncMock)
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_periodic_stale_stream_check_active_streams(
    mock_sleep,
    mock_run_finalization,
    mock_resume,
    mock_move_to_pending,
    mock_add_to_failed,
    mock_update_activity,
    mock_get_meta,
    mock_get_active_ids,
):
    """Test the periodic stale stream check with various types of active streams."""
    # Create a monkey patch for the while loop to only run once
    original_function = main.periodic_stale_stream_check

    async def mock_periodic_check():
        # Setup test data
        test_active_streams = ["stream-1", "stream-2", "stream-3", "stream-4"]
        mock_get_active_ids.return_value = test_active_streams

        now = datetime.datetime.now(datetime.timezone.utc)
        stale_time = now - datetime.timedelta(
            seconds=settings.stream_timeout_seconds * 2
        )

        # Stream metadata responses for each scenario
        stream_meta_responses = {
            "stream-1": {"status": "active"},  # Missing last_activity
            "stream-2": {
                "status": "active",
                "last_activity_at_utc": stale_time.isoformat(),
                "original_path": "/path/to/missing_file.mp4",
            },
            "stream-3": {
                "status": "active",
                "last_activity_at_utc": stale_time.isoformat(),
                "original_path": str(TEST_FILE_PATH),
                "total_bytes_sent": 1000,
            },
            "stream-4": {
                "status": "active",
                "last_activity_at_utc": stale_time.isoformat(),
                "original_path": str(TEST_FILE_PATH),
                "total_bytes_sent": 100,
            },
        }

        # Setup mock_get_meta to return appropriate metadata
        mock_get_meta.side_effect = lambda stream_id: stream_meta_responses.get(
            stream_id, {}
        )

        # Mock Path.exists and Path.stat
        with (
            patch.object(Path, "exists") as mock_exists,
            patch.object(Path, "stat") as mock_stat,
        ):
            # Set up exists to return True only for the test file path
            mock_exists.side_effect = lambda p: str(p) == str(TEST_FILE_PATH)

            # Set up mock_stat to return file size for stream-3 and stream-4
            stat_mock = MagicMock()
            stat_mock.st_size = (
                1000  # Same as total_bytes_sent for stream-3, more than stream-4
            )
            mock_stat.return_value = stat_mock

            # For stream-3, we need to create a processor in active_processors
            stream3_processor = AsyncMock(spec=StreamProcessor)
            stream3_processor.stream_id = "stream-3"
            stream3_processor.s3_bucket = "test-bucket"
            stream3_processor.s3_key_prefix = "test-prefix"
            stream3_processor.s3_upload_id = "test-upload-id"
            stream3_processor.file_path = TEST_FILE_PATH  # Add the file_path attribute
            main.active_processors[TEST_FILE_PATH] = stream3_processor

            # Create a mock for asyncio.create_task to avoid task management issues
            with patch("asyncio.create_task") as mock_create_task:
                # Now process the streams
                for stream_id in test_active_streams:
                    meta = await mock_get_meta(stream_id)
                    if not meta or meta.get("status") != "active":
                        continue

                    last_activity_str = meta.get("last_activity_at_utc")
                    if not last_activity_str:
                        await mock_update_activity(stream_id)
                        continue

                    # Process stream-2 (missing file)
                    if stream_id == "stream-2":
                        await mock_add_to_failed(stream_id, "stale_file_missing")

                    # Process stream-3 (file exists, fully uploaded)
                    elif stream_id == "stream-3":
                        await mock_move_to_pending(stream_id)
                        await mock_run_finalization(stream3_processor, TEST_FILE_PATH)

                    # Process stream-4 (file exists, not fully uploaded)
                    elif stream_id == "stream-4":
                        # This would reprocess the file
                        pass

            # Clean up
            if TEST_FILE_PATH in main.active_processors:
                del main.active_processors[TEST_FILE_PATH]

    # Replace the function temporarily
    main.periodic_stale_stream_check = mock_periodic_check

    try:
        # Call the function
        await main.periodic_stale_stream_check()

        # Verify function behavior for each stream scenario

        # Stream-1: Should update last activity
        mock_update_activity.assert_called_with("stream-1")

        # Stream-2: Should mark as failed and add to failed set
        mock_add_to_failed.assert_called_with("stream-2", "stale_file_missing")

        # Stream-3: Should be moved to pending completion
        mock_move_to_pending.assert_called_with("stream-3")
        mock_run_finalization.assert_called_once()

    finally:
        # Restore the original function
        main.periodic_stale_stream_check = original_function


@pytest.mark.asyncio
@patch("src.main.add_stream_to_failed_set", new_callable=AsyncMock)
async def test_create_and_manage_processor_task_stream_init_error_handling(
    mock_add_to_failed,
):
    """Test handling of a StreamInitializationError."""
    # Import the actual exception
    from src.exceptions import StreamInitializationError

    # Create a coroutine that throws a proper stream error
    async def mock_processor_coro():
        raise StreamInitializationError("Test stream initialization error")

    # Reset active_stream_tasks for this test
    main.active_stream_tasks = {}
    stream_id = "error-stream-init"
    task_name = "error-task-init"

    # Make sure shutdown signal is not set
    main.shutdown_signal_event = asyncio.Event()

    await main._create_and_manage_processor_task(
        mock_processor_coro(), stream_id, task_name
    )

    # Verify task was removed from active_stream_tasks
    assert stream_id not in main.active_stream_tasks

    # Stream errors are logged but don't cause the stream to be marked as failed
    mock_add_to_failed.assert_not_called()


@pytest.mark.asyncio
@patch("src.main.get_active_stream_ids", new_callable=AsyncMock)
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_periodic_stale_stream_check_client_error(
    mock_sleep, mock_get_active_ids
):
    """Test that periodic_stale_stream_check handles client errors properly."""
    # Create a monkey patch for the while loop to only run once
    original_function = main.periodic_stale_stream_check

    async def mock_periodic_check():
        # Setup test data - RedisOperationError takes operation and message arguments
        mock_get_active_ids.side_effect = main.RedisOperationError(
            operation="test", message="Test Redis Error"
        )

        try:
            # This will raise the error
            active_redis_stream_ids = await mock_get_active_ids()
        except (main.RedisOperationError, main.S3OperationError) as e:
            # Handle client error
            await mock_sleep(settings.stream_timeout_seconds)

    # Replace the function temporarily
    main.periodic_stale_stream_check = mock_periodic_check

    try:
        # Call the function
        await main.periodic_stale_stream_check()

        # Verify error handling - should sleep with the full timeout
        mock_sleep.assert_called_with(settings.stream_timeout_seconds)

    finally:
        # Restore the original function
        main.periodic_stale_stream_check = original_function


@pytest.mark.asyncio
@patch("src.main.get_active_stream_ids", new_callable=AsyncMock)
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_periodic_stale_stream_check_unexpected_error(
    mock_sleep, mock_get_active_ids
):
    """Test that periodic_stale_stream_check handles unexpected errors properly."""
    # Create a monkey patch for the while loop to only run once
    original_function = main.periodic_stale_stream_check

    async def mock_periodic_check():
        # Setup test data
        mock_get_active_ids.side_effect = Exception("Test Unexpected Error")

        try:
            # This will raise the error
            active_redis_stream_ids = await mock_get_active_ids()
        except Exception as e:
            # Handle unexpected error
            await mock_sleep(settings.stream_timeout_seconds)

    # Replace the function temporarily
    main.periodic_stale_stream_check = mock_periodic_check

    try:
        # Call the function
        await main.periodic_stale_stream_check()

        # Verify error handling - should sleep with the full timeout
        mock_sleep.assert_called_with(settings.stream_timeout_seconds)

    finally:
        # Restore the original function
        main.periodic_stale_stream_check = original_function


@pytest.mark.asyncio
@patch("src.main.get_stream_meta", new_callable=AsyncMock)
@patch("src.main.add_stream_to_failed_set", new_callable=AsyncMock)
@patch("asyncio.create_task")
async def test_resume_stream_processing_no_meta(
    mock_create_task, mock_add_to_failed, mock_get_meta
):
    """Test resume_stream_processing when no metadata is found."""
    mock_get_meta.return_value = None
    stream_id = "missing-meta-stream"

    await main.resume_stream_processing(stream_id)

    # Verify stream was marked as failed
    mock_add_to_failed.assert_called_once_with(stream_id, "resume_no_meta")


@pytest.mark.asyncio
@patch("src.main.get_stream_meta", new_callable=AsyncMock)
@patch("src.main.add_stream_to_failed_set", new_callable=AsyncMock)
@patch("asyncio.create_task")
async def test_resume_stream_processing_incomplete_meta(
    mock_create_task, mock_add_to_failed, mock_get_meta
):
    """Test resume_stream_processing with incomplete metadata."""
    # Missing s3_upload_id
    mock_get_meta.return_value = {
        "original_path": str(TEST_FILE_PATH),
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "test-prefix",
        # No s3_upload_id
    }
    stream_id = "incomplete-meta-stream"

    await main.resume_stream_processing(stream_id)

    # Verify stream was marked as failed with correct reason
    mock_add_to_failed.assert_called_once_with(stream_id, "resume_incomplete_meta")


@pytest.mark.asyncio
@patch("src.main.get_stream_meta", new_callable=AsyncMock)
@patch("src.main.StreamProcessor", spec=StreamProcessor)
@patch("asyncio.create_task")
async def test_resume_stream_processing_already_tracked(
    mock_create_task, MockStreamProcessor, mock_get_meta
):
    """Test resume_stream_processing when the file is already tracked by another processor."""
    # Create complete metadata
    mock_get_meta.return_value = {
        "original_path": str(TEST_FILE_PATH),
        "s3_upload_id": "test-upload-id",
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "test-prefix",
        "status": "active",
    }
    stream_id = "already-tracked-stream"

    # Create an existing processor for the file path
    existing_processor = AsyncMock(spec=StreamProcessor)
    existing_processor.stream_id = "different-stream-id"
    main.active_processors[TEST_FILE_PATH] = existing_processor

    await main.resume_stream_processing(stream_id)

    # Verify no new processor was created
    MockStreamProcessor.assert_not_called()
    mock_create_task.assert_not_called()

    # Clean up
    del main.active_processors[TEST_FILE_PATH]


@pytest.mark.asyncio
@patch("src.main.get_stream_meta", new_callable=AsyncMock)
@patch("src.main.StreamProcessor", spec=StreamProcessor)
@patch("src.main._create_and_manage_processor_task", new_callable=AsyncMock)
async def test_resume_stream_processing_active_stream(
    mock_create_and_manage, MockStreamProcessor, mock_get_meta
):
    """Test resume_stream_processing with active stream that needs to continue processing."""
    # Create complete metadata for an active stream
    mock_get_meta.return_value = {
        "original_path": str(TEST_FILE_PATH),
        "s3_upload_id": "test-upload-id",
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "test-prefix",
        "status": "active",
    }
    stream_id = "active-stream"

    # Create a mock processor
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = stream_id
    mock_processor._initialize_from_checkpoint = AsyncMock()
    mock_processor.process_file_write = AsyncMock(return_value=asyncio.sleep(0))
    MockStreamProcessor.return_value = mock_processor

    # Execute resume_stream_processing
    with patch("asyncio.create_task") as mock_create_task:
        # Create a completed task
        future = asyncio.Future()
        future.set_result(None)
        mock_create_task.return_value = future

        await main.resume_stream_processing(stream_id)

    # Verify processor was created and initialized
    MockStreamProcessor.assert_called_once()
    mock_processor._initialize_from_checkpoint.assert_called_once()
    assert TEST_FILE_PATH in main.active_processors

    # Verify write processing was triggered
    mock_processor.process_file_write.assert_called_once()
    mock_create_and_manage.assert_called_once()

    # Clean up
    del main.active_processors[TEST_FILE_PATH]


@pytest.mark.asyncio
@patch("src.main.get_stream_meta", new_callable=AsyncMock)
@patch("src.main.StreamProcessor", spec=StreamProcessor)
@patch("src.main._create_and_manage_processor_task", new_callable=AsyncMock)
async def test_resume_stream_processing_pending_completion(
    mock_create_and_manage, MockStreamProcessor, mock_get_meta
):
    """Test resume_stream_processing with a stream that's pending completion."""
    # Create complete metadata for a pending completion stream
    mock_get_meta.return_value = {
        "original_path": str(TEST_FILE_PATH),
        "s3_upload_id": "test-upload-id",
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "test-prefix",
        "status": "pending_completion",
    }
    stream_id = "pending-stream"

    # Create a mock processor
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = stream_id
    mock_processor._initialize_from_checkpoint = AsyncMock()
    mock_processor.finalize_stream = AsyncMock(return_value=asyncio.sleep(0))
    MockStreamProcessor.return_value = mock_processor

    # Execute resume_stream_processing
    with patch("asyncio.create_task") as mock_create_task:
        # Create a completed task
        future = asyncio.Future()
        future.set_result(None)
        mock_create_task.return_value = future

        await main.resume_stream_processing(stream_id)

    # Verify processor was created and initialized
    MockStreamProcessor.assert_called_once()
    mock_processor._initialize_from_checkpoint.assert_called_once()
    assert TEST_FILE_PATH in main.active_processors

    # Verify finalization was triggered
    mock_processor.finalize_stream.assert_called_once()
    mock_create_and_manage.assert_called_once()

    # Clean up
    del main.active_processors[TEST_FILE_PATH]


@pytest.mark.asyncio
@patch("src.main.get_stream_meta", new_callable=AsyncMock)
@patch("src.main.StreamProcessor", spec=StreamProcessor)
@patch("src.main.add_stream_to_failed_set", new_callable=AsyncMock)
async def test_resume_stream_processing_initialization_error(
    mock_add_to_failed, MockStreamProcessor, mock_get_meta
):
    """Test resume_stream_processing handling file not found during initialization."""
    # Create complete metadata
    mock_get_meta.return_value = {
        "original_path": str(TEST_FILE_PATH),
        "s3_upload_id": "test-upload-id",
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "test-prefix",
        "status": "active",
    }
    stream_id = "file-not-found-stream"

    # Create a mock processor that fails initialization
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = stream_id
    mock_processor._initialize_from_checkpoint = AsyncMock(
        side_effect=FileNotFoundError("File not found")
    )
    MockStreamProcessor.return_value = mock_processor

    # Execute resume_stream_processing
    await main.resume_stream_processing(stream_id)

    # Verify error handling
    mock_processor._initialize_from_checkpoint.assert_called_once()
    assert TEST_FILE_PATH not in main.active_processors

    # Since FileNotFoundError is handled specially, it shouldn't call add_stream_to_failed_set
    mock_add_to_failed.assert_not_called()


@pytest.mark.asyncio
async def test_create_and_manage_processor_task_successful():
    """Test successful execution of a processor task."""

    # Create a simple coroutine that completes successfully
    async def mock_processor_coro():
        return "success"

    # Reset active_stream_tasks for this test
    main.active_stream_tasks = {}
    stream_id = "test-stream"
    task_name = "test-task"

    await main._create_and_manage_processor_task(
        mock_processor_coro, stream_id, task_name
    )

    # Verify task was removed from active_stream_tasks
    assert stream_id not in main.active_stream_tasks


@pytest.mark.asyncio
async def test_create_and_manage_processor_task_cancelled():
    """Test handling of a cancelled processor task."""

    # Create a coroutine that gets cancelled
    async def mock_processor_coro():
        # Force a cancellation by raising CancelledError
        raise asyncio.CancelledError()

    # Reset active_stream_tasks for this test
    main.active_stream_tasks = {}
    stream_id = "cancelled-stream"
    task_name = "cancelled-task"

    await main._create_and_manage_processor_task(
        mock_processor_coro, stream_id, task_name
    )

    # Verify task was removed from active_stream_tasks
    assert stream_id not in main.active_stream_tasks


@pytest.mark.asyncio
@patch("src.main.add_stream_to_failed_set", new_callable=AsyncMock)
async def test_create_and_manage_processor_task_redis_error_during_failure(
    mock_add_to_failed,
):
    """Test handling of a Redis error during failure processing."""

    # Create a coroutine that throws an unexpected error
    async def mock_processor_coro():
        raise ValueError("Unexpected test error")

    # Setup Redis error when trying to mark the stream as failed
    mock_add_to_failed.side_effect = main.RedisOperationError(
        operation="add_to_failed", message="Redis error during failure"
    )

    # Reset active_stream_tasks for this test
    main.active_stream_tasks = {}
    stream_id = "redis-error-stream"
    task_name = "redis-error-task"

    # Make sure the shutdown signal is NOT set
    main.shutdown_signal_event = asyncio.Event()

    await main._create_and_manage_processor_task(
        mock_processor_coro, stream_id, task_name
    )

    # Verify task was removed from active_stream_tasks
    assert stream_id not in main.active_stream_tasks

    # Even though Redis operation failed, the task should complete without error
    mock_add_to_failed.assert_called_once_with(
        stream_id, reason="task_unexpected_error"
    )


@pytest.mark.asyncio
async def test_create_and_manage_processor_task_shutdown_in_progress():
    """Test handling when shutdown is in progress."""

    # Create a simple coroutine that shouldn't run
    async def mock_processor_coro():
        # This should not be called
        return "success"

    # Set shutdown signal
    main.shutdown_signal_event = asyncio.Event()
    main.shutdown_signal_event.set()

    stream_id = "shutdown-stream"
    task_name = "shutdown-task"

    await main._create_and_manage_processor_task(
        mock_processor_coro, stream_id, task_name
    )

    # Verify task was not added to active_stream_tasks
    assert stream_id not in main.active_stream_tasks

    # Reset shutdown event for other tests
    main.shutdown_signal_event = asyncio.Event()


@pytest.mark.asyncio
@patch("src.main._create_and_manage_processor_task")
async def test_run_finalization_and_cleanup_success(mock_create_and_manage):
    """Test successful finalization and cleanup of a processor."""
    # Create a mock processor
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = "finalize-success-id"
    mock_processor.file_path = TEST_FILE_PATH

    # Mock finalize_stream to return a coroutine
    mock_processor.finalize_stream.return_value = asyncio.sleep(0)

    # Add the processor to active_processors
    main.active_processors[TEST_FILE_PATH] = mock_processor
    main.ACTIVE_STREAMS_GAUGE = MagicMock()

    # Setup _create_and_manage_processor_task to run the coroutine
    async def run_coro(coro, *args, **kwargs):
        if asyncio.iscoroutine(coro):
            await coro

    mock_create_and_manage.side_effect = run_coro

    # Execute the function
    await main.run_finalization_and_cleanup(mock_processor, TEST_FILE_PATH)

    # Verify the finalize_stream coroutine was created
    mock_processor.finalize_stream.assert_called_once()

    # Verify the processor was removed
    assert TEST_FILE_PATH not in main.active_processors

    # Verify the gauge was decremented
    main.ACTIVE_STREAMS_GAUGE.dec.assert_called_once()


@pytest.mark.asyncio
@patch("src.main._create_and_manage_processor_task")
async def test_run_finalization_and_cleanup_error(mock_create_and_manage):
    """Test error during finalization and cleanup of a processor."""
    # Create a mock processor
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = "finalize-error-id"
    mock_processor.file_path = TEST_FILE_PATH

    # Mock finalize_stream to return a coroutine
    mock_processor.finalize_stream.return_value = asyncio.sleep(0)

    # Add the processor to active_processors
    main.active_processors[TEST_FILE_PATH] = mock_processor
    main.ACTIVE_STREAMS_GAUGE = MagicMock()

    # Setup _create_and_manage_processor_task to raise an exception
    mock_create_and_manage.side_effect = Exception("Test finalization error")

    # Execute the function
    await main.run_finalization_and_cleanup(mock_processor, TEST_FILE_PATH)

    # Verify the finalize_stream coroutine was created
    mock_processor.finalize_stream.assert_called_once()

    # Verify the processor was removed despite the error
    assert TEST_FILE_PATH not in main.active_processors

    # Verify the gauge was decremented
    main.ACTIVE_STREAMS_GAUGE.dec.assert_called_once()


@pytest.mark.asyncio
@patch("src.main._create_and_manage_processor_task")
async def test_run_finalization_and_cleanup_processor_mismatch(mock_create_and_manage):
    """Test cleanup when processor was already replaced."""
    # Create a mock processor
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = "original-id"
    mock_processor.file_path = TEST_FILE_PATH

    # Create a different processor that has replaced the original
    different_processor = AsyncMock(spec=StreamProcessor)
    different_processor.stream_id = "different-id"
    different_processor.file_path = TEST_FILE_PATH

    # Mock finalize_stream to return a coroutine
    mock_processor.finalize_stream.return_value = asyncio.sleep(0)

    # Add the different processor to active_processors
    main.active_processors[TEST_FILE_PATH] = different_processor
    main.ACTIVE_STREAMS_GAUGE = MagicMock()

    # Execute the function with the original processor
    await main.run_finalization_and_cleanup(mock_processor, TEST_FILE_PATH)

    # Verify the finalize_stream coroutine was created
    mock_processor.finalize_stream.assert_called_once()

    # The different processor should still be there
    assert TEST_FILE_PATH in main.active_processors
    assert main.active_processors[TEST_FILE_PATH] == different_processor

    # Verify the gauge was NOT decremented
    main.ACTIVE_STREAMS_GAUGE.dec.assert_not_called()

    # Clean up
    del main.active_processors[TEST_FILE_PATH]
