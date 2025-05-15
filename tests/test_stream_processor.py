# Unit tests for StreamProcessor functionality
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio

from src.stream_processor import StreamProcessor
from src.config import settings  # Used by StreamProcessor indirectly

# Mock settings for tests if they influence StreamProcessor directly or via other modules it uses.
# For now, assume default settings are fine or specific tests will patch where needed.

TEST_STREAM_ID = "test-stream-123"
TEST_FILE_NAME = "video.mp4"
TEST_S3_UPLOAD_ID = "s3-upload-id-456"
TEST_S3_BUCKET = "test-bucket"
TEST_S3_KEY_PREFIX = f"streams/{TEST_STREAM_ID}/{TEST_FILE_NAME}"


@pytest.fixture
def mock_file_path(tmp_path: Path) -> Path:
    file = tmp_path / TEST_FILE_NAME
    # file.touch() # Create the file if tests need it to exist
    return file


@pytest.fixture
def mock_shutdown_event():
    return asyncio.Event()


@pytest.fixture
def stream_processor_instance(
    mock_file_path: Path, mock_shutdown_event
) -> StreamProcessor:
    return StreamProcessor(
        stream_id=TEST_STREAM_ID,
        file_path=mock_file_path,
        s3_upload_id=TEST_S3_UPLOAD_ID,
        s3_bucket=TEST_S3_BUCKET,
        s3_key_prefix=TEST_S3_KEY_PREFIX,
        shutdown_event=mock_shutdown_event,
    )


@pytest.mark.asyncio
async def test_stream_processor_initialization(
    stream_processor_instance: StreamProcessor, mock_file_path: Path
):
    assert stream_processor_instance.stream_id == TEST_STREAM_ID
    assert stream_processor_instance.file_path == mock_file_path
    assert stream_processor_instance.s3_upload_id == TEST_S3_UPLOAD_ID
    assert stream_processor_instance.s3_bucket == TEST_S3_BUCKET
    assert stream_processor_instance.s3_key_prefix == TEST_S3_KEY_PREFIX
    assert stream_processor_instance.current_file_offset == 0
    assert stream_processor_instance.next_part_number == 1
    assert stream_processor_instance.uploaded_parts_info == []
    assert not stream_processor_instance.is_processing_write
    assert isinstance(stream_processor_instance.lock, asyncio.Lock)
    assert isinstance(stream_processor_instance.shutdown_event, asyncio.Event)


@pytest.mark.asyncio
@patch("src.stream_processor.get_stream_next_part", new_callable=AsyncMock)
@patch("src.stream_processor.get_stream_bytes_sent", new_callable=AsyncMock)
@patch("src.stream_processor.get_stream_parts", new_callable=AsyncMock)
async def test_initialize_from_checkpoint_new_stream(
    mock_get_parts: AsyncMock,
    mock_get_bytes_sent: AsyncMock,
    mock_get_next_part: AsyncMock,
    stream_processor_instance: StreamProcessor,
):
    mock_get_next_part.return_value = None
    mock_get_bytes_sent.return_value = None
    mock_get_parts.return_value = []

    await stream_processor_instance._initialize_from_checkpoint()

    assert stream_processor_instance.next_part_number == 1
    assert stream_processor_instance.current_file_offset == 0
    assert stream_processor_instance.uploaded_parts_info == []
    mock_get_next_part.assert_called_once_with(TEST_STREAM_ID)
    mock_get_bytes_sent.assert_called_once_with(TEST_STREAM_ID)
    mock_get_parts.assert_called_once_with(TEST_STREAM_ID)


@pytest.mark.asyncio
@patch("src.stream_processor.get_stream_next_part", new_callable=AsyncMock)
@patch("src.stream_processor.get_stream_bytes_sent", new_callable=AsyncMock)
@patch("src.stream_processor.get_stream_parts", new_callable=AsyncMock)
async def test_initialize_from_checkpoint_existing_stream(
    mock_get_parts: AsyncMock,
    mock_get_bytes_sent: AsyncMock,
    mock_get_next_part: AsyncMock,
    stream_processor_instance: StreamProcessor,
):
    expected_next_part = 5
    expected_bytes_sent = 10240
    expected_parts_info = [
        {"PartNumber": i, "ETag": f"etag-{i}", "Size": 2048}
        for i in range(1, expected_next_part)
    ]

    mock_get_next_part.return_value = expected_next_part
    mock_get_bytes_sent.return_value = expected_bytes_sent
    mock_get_parts.return_value = expected_parts_info

    await stream_processor_instance._initialize_from_checkpoint()

    assert stream_processor_instance.next_part_number == expected_next_part
    assert stream_processor_instance.current_file_offset == expected_bytes_sent
    assert stream_processor_instance.uploaded_parts_info == expected_parts_info


# TODO: Add tests for process_file_write:
#   - New file, single chunk, multiple chunks
#   - File with existing offset (covered by checkpoint init and then write)
#   - File smaller than chunk size
#   - Simulated S3 upload failure
#   - Simulated Redis update failure
#   - File disappearance during processing (file_path.exists() returns False)
#   - No new data to process
# TODO: Add tests for finalize_stream (once implemented in Phase 5)


@pytest.mark.asyncio
@patch("src.stream_processor.upload_s3_part", new_callable=AsyncMock)
@patch("src.stream_processor.add_stream_part_info", new_callable=AsyncMock)
@patch("src.stream_processor.incr_stream_bytes_sent", new_callable=AsyncMock)
@patch("src.stream_processor.set_stream_next_part", new_callable=AsyncMock)
@patch("src.stream_processor.update_stream_last_activity", new_callable=AsyncMock)
async def test_process_file_write_new_data_single_chunk(
    mock_update_last_activity: AsyncMock,
    mock_set_next_part: AsyncMock,
    mock_incr_bytes_sent: AsyncMock,
    mock_add_part_info: AsyncMock,
    mock_upload_s3: AsyncMock,
    stream_processor_instance: StreamProcessor,
    mock_file_path: Path,
):
    # Setup file with data
    file_content = b"A" * (settings.chunk_size_bytes // 2)  # Smaller than full chunk
    mock_file_path.write_bytes(file_content)

    mock_upload_s3.return_value = "etag-part1"
    mock_incr_bytes_sent.return_value = len(
        file_content
    )  # Simulate Redis returning new total

    await stream_processor_instance.process_file_write()

    assert stream_processor_instance.current_file_offset == len(file_content)
    assert stream_processor_instance.next_part_number == 2
    assert len(stream_processor_instance.uploaded_parts_info) == 1
    assert stream_processor_instance.uploaded_parts_info[0]["PartNumber"] == 1
    assert stream_processor_instance.uploaded_parts_info[0]["ETag"] == "etag-part1"
    assert stream_processor_instance.uploaded_parts_info[0]["Size"] == len(file_content)

    mock_upload_s3.assert_called_once()
    # Check that data passed to upload_s3_part was correct
    args, kwargs = mock_upload_s3.call_args
    assert kwargs["data"] == file_content

    mock_add_part_info.assert_called_once_with(
        TEST_STREAM_ID, 1, "etag-part1", len(file_content)
    )
    mock_incr_bytes_sent.assert_called_once_with(TEST_STREAM_ID, len(file_content))
    mock_set_next_part.assert_called_once_with(TEST_STREAM_ID, 2)
    mock_update_last_activity.assert_called_once_with(TEST_STREAM_ID)
    assert (
        not stream_processor_instance.is_processing_write
    )  # Should be false after completion


@pytest.mark.asyncio
@patch("src.stream_processor.upload_s3_part", new_callable=AsyncMock)
@patch("src.stream_processor.add_stream_part_info", new_callable=AsyncMock)
@patch("src.stream_processor.incr_stream_bytes_sent", new_callable=AsyncMock)
@patch("src.stream_processor.set_stream_next_part", new_callable=AsyncMock)
@patch("src.stream_processor.update_stream_last_activity", new_callable=AsyncMock)
async def test_process_file_write_multiple_chunks(
    mock_update_last_activity: AsyncMock,
    mock_set_next_part: AsyncMock,
    mock_incr_bytes_sent: AsyncMock,
    mock_add_part_info: AsyncMock,
    mock_upload_s3: AsyncMock,
    stream_processor_instance: StreamProcessor,
    mock_file_path: Path,
):
    # File content: 1.5 chunks
    file_content = b"B" * int(settings.chunk_size_bytes * 1.5)
    mock_file_path.write_bytes(file_content)

    # Simulate S3 returning ETags
    mock_upload_s3.side_effect = ["etag-part1-multi", "etag-part2-multi"]

    # Simulate incr_stream_bytes_sent behavior correctly for multiple calls
    current_offset = 0

    def incr_side_effect(stream_id, chunk_len):
        nonlocal current_offset
        current_offset += chunk_len
        return current_offset

    mock_incr_bytes_sent.side_effect = incr_side_effect

    await stream_processor_instance.process_file_write()

    expected_total_bytes = len(file_content)
    assert stream_processor_instance.current_file_offset == expected_total_bytes
    assert stream_processor_instance.next_part_number == 3  # Two parts uploaded
    assert len(stream_processor_instance.uploaded_parts_info) == 2

    # Part 1
    assert stream_processor_instance.uploaded_parts_info[0]["PartNumber"] == 1
    assert (
        stream_processor_instance.uploaded_parts_info[0]["ETag"] == "etag-part1-multi"
    )
    assert (
        stream_processor_instance.uploaded_parts_info[0]["Size"]
        == settings.chunk_size_bytes
    )
    # Part 2
    assert stream_processor_instance.uploaded_parts_info[1]["PartNumber"] == 2
    assert (
        stream_processor_instance.uploaded_parts_info[1]["ETag"] == "etag-part2-multi"
    )
    assert (
        stream_processor_instance.uploaded_parts_info[1]["Size"]
        == expected_total_bytes - settings.chunk_size_bytes
    )

    assert mock_upload_s3.call_count == 2
    # Check data for first call
    first_call_args, first_call_kwargs = mock_upload_s3.call_args_list[0]
    assert first_call_kwargs["data"] == file_content[: settings.chunk_size_bytes]
    assert first_call_kwargs["part_number"] == 1
    # Check data for second call
    second_call_args, second_call_kwargs = mock_upload_s3.call_args_list[1]
    assert second_call_kwargs["data"] == file_content[settings.chunk_size_bytes :]
    assert second_call_kwargs["part_number"] == 2

    assert mock_add_part_info.call_count == 2
    mock_add_part_info.assert_any_call(
        TEST_STREAM_ID, 1, "etag-part1-multi", settings.chunk_size_bytes
    )
    mock_add_part_info.assert_any_call(
        TEST_STREAM_ID,
        2,
        "etag-part2-multi",
        expected_total_bytes - settings.chunk_size_bytes,
    )

    assert mock_incr_bytes_sent.call_count == 2
    mock_incr_bytes_sent.assert_any_call(TEST_STREAM_ID, settings.chunk_size_bytes)
    mock_incr_bytes_sent.assert_any_call(
        TEST_STREAM_ID, expected_total_bytes - settings.chunk_size_bytes
    )

    assert mock_set_next_part.call_count == 2
    mock_set_next_part.assert_any_call(TEST_STREAM_ID, 2)
    mock_set_next_part.assert_any_call(TEST_STREAM_ID, 3)

    assert (
        mock_update_last_activity.call_count == 2
    )  # Called after each successful part upload
    assert not stream_processor_instance.is_processing_write


@pytest.mark.asyncio
async def test_process_file_write_no_new_data(
    stream_processor_instance: StreamProcessor,
    mock_file_path: Path,
    caplog,  # To capture log messages
):
    # File exists but is empty, or offset matches size
    mock_file_path.touch()  # Ensure file exists
    stream_processor_instance.current_file_offset = 0
    # Ensure stat().st_size is 0 or matches offset
    with patch.object(Path, "stat") as mock_stat:
        mock_stat.return_value.st_size = 0
        await stream_processor_instance.process_file_write()

    assert stream_processor_instance.current_file_offset == 0  # No change
    assert stream_processor_instance.next_part_number == 1  # No change
    assert len(stream_processor_instance.uploaded_parts_info) == 0  # No change
    assert f"[{TEST_STREAM_ID}] No new data for {mock_file_path}" in caplog.text


@pytest.mark.asyncio
@patch("src.stream_processor.upload_s3_part", new_callable=AsyncMock)
async def test_process_file_write_s3_upload_failure(
    mock_upload_s3: AsyncMock,
    stream_processor_instance: StreamProcessor,
    mock_file_path: Path,
    caplog,
):
    file_content = b"C" * 100
    mock_file_path.write_bytes(file_content)

    mock_upload_s3.return_value = None  # Simulate S3 upload failure

    await stream_processor_instance.process_file_write()

    assert (
        stream_processor_instance.current_file_offset == 0
    )  # Should not advance offset on failure
    assert (
        stream_processor_instance.next_part_number == 1
    )  # Should not advance part number
    assert (
        len(stream_processor_instance.uploaded_parts_info) == 0
    )  # No part info should be added
    assert f"[{TEST_STREAM_ID}] Failed to upload part 1. Halting stream." in caplog.text
    assert (
        not stream_processor_instance.is_processing_write
    )  # is_processing_write should be False


@pytest.mark.asyncio
async def test_process_file_write_file_disappears_before_processing(
    stream_processor_instance: StreamProcessor,
    mock_file_path: Path,  # In this case, we ensure it doesn't exist
    caplog,
):
    # Ensure the mock_file_path does NOT exist for this test
    if mock_file_path.exists():
        mock_file_path.unlink()

    await stream_processor_instance.process_file_write()

    assert (
        f"[{TEST_STREAM_ID}] File {mock_file_path} no longer exists. Stopping processing."
        in caplog.text
    )
    # TODO: Check for stream marked as failed in Redis (Phase 8)


@pytest.mark.asyncio
@patch(
    "src.stream_processor.upload_s3_part", new_callable=AsyncMock
)  # Mock S3 to avoid actual calls
async def test_process_file_write_file_disappears_during_processing(
    mock_upload_s3: AsyncMock,  # Mocked, but we'll make the file disappear before it's used
    stream_processor_instance: StreamProcessor,
    mock_file_path: Path,
    caplog,
):
    # File exists initially
    file_content = b"D" * (settings.chunk_size_bytes + 100)  # More than one chunk
    mock_file_path.write_bytes(file_content)

    mock_upload_s3.return_value = "etag-part1-disappear"  # First part uploads fine

    # Simulate file disappearing after first read, before second read attempt or during it
    original_open = open

    def faulty_open(*args, **kwargs):
        if (
            args[0] == stream_processor_instance.file_path
            and stream_processor_instance.next_part_number > 1
        ):
            # On subsequent access, for the second chunk (or if trying to re-read after seek)
            if (
                mock_file_path.exists()
            ):  # if it's for the second chunk, make it disappear
                mock_file_path.unlink()
            raise FileNotFoundError("Simulated disappearance")
        return original_open(*args, **kwargs)

    with patch("builtins.open", faulty_open):
        # In this specific setup, the FileNotFoundError will be caught by the outer try-except
        # within process_file_write.
        # If the first chunk processing works, it will attempt the second.
        # If the file disappears *before* the `with open(...)` statement for the second chunk,
        # that would be a different flow. Here we test disappearance *during* the loop.
        await stream_processor_instance.process_file_write()

    # Depending on exact disappearance point, one part might be uploaded.
    # If disappears *before* any chunk is fully processed *and registered*, then 0.
    # If one chunk is processed, its state is saved, then file disappears, then 1 part is registered.
    # The current StreamProcessor logic logs the error and stops.
    # It processes chunks sequentially within one call to process_file_write.
    # If an error occurs (like FileNotFoundError from a read), it logs and exits.

    # Given the provided StreamProcessor code:
    # 1. It opens the file.
    # 2. Loops to read chunks.
    # 3. If faulty_open raises FileNotFoundError during a `f.read()`, it's caught by the broad Exception.
    # Let's refine the mock to make the *file* disappear, and `f.read()` would then fail or Path.exists()
    # The primary check `if not self.file_path.exists():` is at the start.
    # The `try...except FileNotFoundError:` is for `open()` itself.
    # The `except Exception as e:` catches errors during `f.read()` or S3/Redis calls.

    # For this test, let's assume the first chunk succeeds, then the file disappears.
    # The `process_file_write` method might complete the first chunk, then on the next iteration of its
    # `while True` loop, `f.read()` might raise an error if the file handle becomes invalid, or
    # if `file_size - self.current_file_offset` calculation relies on a fresh `stat` that fails.
    # The current `StreamProcessor` re-stats `file_size` only once at the beginning of `process_file_write`.
    # So, if a file shrinks or disappears, `f.read()` would return empty or fewer bytes.

    # A more direct way to test "disappearance during processing":
    # Mock `f.read()` to raise an error after the first successful read.
    mock_file_path.write_bytes(file_content)  # Recreate file
    stream_processor_instance.current_file_offset = 0  # Reset state
    stream_processor_instance.next_part_number = 1
    stream_processor_instance.uploaded_parts_info = []

    with patch("builtins.open", MagicMock()) as mock_open:
        mock_file = MagicMock()
        mock_file.read.side_effect = [
            file_content[: settings.chunk_size_bytes],  # First chunk
            FileNotFoundError("Simulated read error after first chunk"),
        ]
        mock_file.seek.return_value = None
        mock_open.return_value.__enter__.return_value = (
            mock_file  # for 'with open(...) as f:'
        )

        mock_upload_s3.return_value = "etag-part1-disappear"  # First part uploads fine

        await stream_processor_instance.process_file_write()

    assert (
        stream_processor_instance.current_file_offset == settings.chunk_size_bytes
    )  # First chunk processed
    assert (
        stream_processor_instance.next_part_number == 2
    )  # Incremented after first successful upload
    assert len(stream_processor_instance.uploaded_parts_info) == 1
    assert (
        stream_processor_instance.uploaded_parts_info[0]["ETag"]
        == "etag-part1-disappear"
    )
    assert (
        f"[{TEST_STREAM_ID}] Error processing file write for {mock_file_path}: Simulated read error after first chunk"
        in caplog.text
    )
    assert not stream_processor_instance.is_processing_write


# Reset settings.chunk_size_bytes if it was changed by a test (it wasn't explicitly here but good practice)
# settings.chunk_size_bytes = original_chunk_size (if it was stored)
