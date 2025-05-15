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
    main.active_processors[TEST_FILE_PATH] = mock_processor

    # Create a mock event queue
    event_queue = asyncio.Queue()

    await main.handle_stream_event(mock_stream_event_write, event_queue)

    # Check that create_task was called
    mock_create_task.assert_called_once()
    # Check that processor.process_file_write was scheduled
    args, kwargs = mock_create_task.call_args
    # args[0] is the coroutine passed to create_task
    assert args[0].__name__ == "_create_and_manage_processor_task"


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
@patch("src.main.handle_stream_event", new_callable=AsyncMock)  # Mock event handler
@patch("src.main.get_active_stream_ids", new_callable=AsyncMock)  # Mock Redis calls
@patch(
    "src.main.get_pending_completion_stream_ids", new_callable=AsyncMock
)  # Mock Redis calls
async def test_main_function_flow(
    mock_get_pending_ids: AsyncMock,
    mock_get_active_ids: AsyncMock,
    mock_handle_event: AsyncMock,
    mock_close_s3: AsyncMock,
    mock_close_redis: AsyncMock,
    mock_get_redis: AsyncMock,
    setup_test_environment,  # Ensure settings.watch_dir is patched by fixture
):
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

    # Apply the patch for the duration of this test
    with patch("src.main.video_file_watcher", return_value=mock_watcher_gen()):
        # Run main with a timeout
        try:
            await asyncio.wait_for(main.main(), timeout=2.0)
        except asyncio.TimeoutError:
            # This should not happen if our mock is working correctly
            pass

    # Reset shutdown signal for other tests
    main.shutdown_signal_event = asyncio.Event()

    # Verify the expected calls were made
    mock_get_redis.assert_called_once()
    mock_handle_event.assert_called()
    mock_close_redis.assert_called_once()
    mock_close_s3.assert_called_once()
