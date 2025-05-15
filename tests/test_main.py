import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
import uuid

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
    assert sp_kwargs.get("stream_id") == str(test_uuid)
    assert sp_kwargs.get("file_path") == TEST_FILE_PATH
    assert sp_kwargs.get("s3_upload_id") == mock_s3_upload_id
    assert sp_kwargs.get("s3_bucket") == settings.s3_bucket_name
    assert sp_kwargs.get("s3_key_prefix") == expected_s3_key_prefix
    assert sp_kwargs.get("shutdown_event") == main.shutdown_signal_event

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
    main.active_processors[TEST_FILE_PATH] = mock_processor

    # Create a mock event queue
    event_queue = asyncio.Queue()

    await main.handle_stream_event(mock_stream_event_write, event_queue)

    mock_create_task.assert_called_once()
    # Check that processor.process_file_write was scheduled
    args, kwargs = mock_create_task.call_args
    # args[0] is the coroutine, its __self__ should be the processor if it's a bound method
    assert args[0].__self__ is mock_processor
    assert args[0].__name__ == "process_file_write"


@pytest.mark.asyncio
@patch("asyncio.create_task")  # Ensure create_task is not called
async def test_handle_stream_event_write_processor_missing(
    mock_create_task: MagicMock, mock_stream_event_write: StreamEvent, caplog
):
    # Ensure no processor for this path
    if TEST_FILE_PATH in main.active_processors:
        del main.active_processors[TEST_FILE_PATH]

    # Create a mock event queue
    event_queue = asyncio.Queue()

    await main.handle_stream_event(mock_stream_event_write, event_queue)

    mock_create_task.assert_not_called()
    assert f"[{TEST_FILE_NAME}] WRITE event for untracked file." in caplog.text


@pytest.mark.asyncio
async def test_handle_stream_event_delete_processor_exists(
    mock_stream_event_delete: StreamEvent, caplog
):
    mock_processor = AsyncMock(spec=StreamProcessor)
    mock_processor.stream_id = "proc-to-delete"
    main.active_processors[TEST_FILE_PATH] = mock_processor

    # Create a mock event queue
    event_queue = asyncio.Queue()

    await main.handle_stream_event(mock_stream_event_delete, event_queue)

    assert TEST_FILE_PATH not in main.active_processors
    assert (
        f"[{mock_processor.stream_id}] Removed processor for deleted file {TEST_FILE_NAME}."
        in caplog.text
    )
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

    assert f"[{TEST_FILE_NAME}] DELETE event for untracked file." in caplog.text


# Basic test for main() function structure - more complex scenarios are harder to unit test for main()
@pytest.mark.asyncio
@patch(
    "src.main.video_file_watcher", new_callable=AsyncMock
)  # Mock the watcher generator
@patch("src.main.get_redis_connection", new_callable=AsyncMock)
@patch("src.main.close_redis_connection", new_callable=AsyncMock)
@patch("src.main.close_s3_resources", new_callable=AsyncMock)
@patch("src.main.handle_stream_event", new_callable=AsyncMock)  # Mock event handler
async def test_main_function_flow(
    mock_handle_event: AsyncMock,
    mock_close_s3: AsyncMock,
    mock_close_redis: AsyncMock,
    mock_get_redis: AsyncMock,
    mock_watcher: AsyncMock,
    setup_test_environment,  # Ensure settings.watch_dir is patched by fixture
):
    # Simulate watcher yielding a few events then stopping (e.g., by raising StopAsyncIteration)
    event1 = StreamEvent(
        change_type=WatcherChangeType.CREATE, file_path=TEST_WATCH_DIR / "file1.mp4"
    )
    event2 = StreamEvent(
        change_type=WatcherChangeType.WRITE, file_path=TEST_WATCH_DIR / "file1.mp4"
    )

    # Make the mock_watcher an async generator that yields these events
    async def mock_watcher_gen(*args, **kwargs):
        yield event1
        yield event2
        # No StopAsyncIteration needed, exiting generator is enough

    mock_watcher.side_effect = mock_watcher_gen

    # Run main, but use asyncio.wait_for to prevent it from running indefinitely if there's an issue.
    # This is a simplified run; main() has its own KeyboardInterrupt handling.
    try:
        await asyncio.wait_for(main.main(), timeout=1.0)
    except asyncio.TimeoutError:
        # This might happen if the stop_event logic isn't hit correctly or watcher doesn't end.
        # For this test, we expect it to finish if watcher ends.
        pass
        # If main() truly ran to completion via watcher ending, no TimeoutError.
        # If watcher mock doesn't correctly stop the loop, it will timeout.

    mock_get_redis.assert_called_once()
    assert mock_watcher.call_count == 1  # Watcher was iterated
    # We can't easily check handle_stream_event calls with exact arguments
    # since main() now puts events in a queue and processes them asynchronously
    assert mock_handle_event.call_count > 0  # It should be called at least once

    # Shutdown calls
    mock_close_redis.assert_called_once()
    mock_close_s3.assert_called_once()

    # Check if watch_dir was created if it didn't exist (covered by main logic)
    # For this test, the fixture ensures it exists.
    # We can check if Path.mkdir was called if we mock Path.
    # with patch.object(Path, 'mkdir') as mock_mkdir:
    #    if not Path(settings.watch_dir).exists():
    #        # Re-run main or part of it if necessary to test this path
    #        pass # This path is tricky to hit if fixture always creates it
