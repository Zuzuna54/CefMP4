"""Integration tests for core component interactions."""

import asyncio
import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from src.events import WatcherChangeType, StreamEvent
from src.stream_processor import StreamProcessor
from src.exceptions import S3OperationError, StreamFinalizationError


@pytest.fixture
def mock_stream_processor():
    """Mock StreamProcessor for testing."""
    mock = AsyncMock(spec=StreamProcessor)
    mock.stream_id = "test-stream-id"
    mock.process_file_write = AsyncMock()
    mock.finalize_stream = AsyncMock()
    return mock


@pytest.mark.asyncio
async def test_watcher_events_flow_to_stream_processor():
    """Test that watcher events correctly flow to StreamProcessor methods in main.py"""
    # This test will verify that when a file watcher event occurs,
    # the main.py routes it correctly to a StreamProcessor
    from src.main import handle_stream_event, active_processors

    # Setup a test file path and event
    test_file_path = Path("/test/path/test_video.mp4")
    write_event = StreamEvent(
        change_type=WatcherChangeType.WRITE,
        file_path=test_file_path,
    )

    # Mock active_processors to contain our test path
    mock_processor = AsyncMock()
    mock_processor.process_file_write = AsyncMock()

    # Patch the active_processors dict
    with patch("src.main.active_processors", {test_file_path: mock_processor}):
        # Create a mock event queue
        event_queue = AsyncMock()

        # Call the handler with our WRITE event
        await handle_stream_event(write_event, event_queue)

        # Verify the processor's process_file_write was called
        mock_processor.process_file_write.assert_called_once()


@pytest.mark.asyncio
async def test_stream_processor_finalization_triggers_metadata_generation():
    """Test that StreamProcessor finalization triggers metadata generation."""
    # This test will verify that when finalize_stream is called,
    # it properly generates metadata

    # Create mocks
    mock_s3_client = AsyncMock()
    mock_redis_client = AsyncMock()
    mock_metadata_generator = AsyncMock()

    # Set up return values
    mock_redis_client.get_stream_parts.return_value = [
        {"PartNumber": 1, "ETag": "etag1", "Size": 1000},
        {"PartNumber": 2, "ETag": "etag2", "Size": 2000},
    ]
    mock_s3_client.complete_s3_multipart_upload.return_value = True
    mock_metadata_generator.generate_metadata_json.return_value = {"test": "metadata"}
    mock_s3_client.upload_json_to_s3.return_value = True

    # Create test file
    with tempfile.NamedTemporaryFile(suffix=".mp4") as temp_file:
        file_path = Path(temp_file.name)

        # Create a real StreamProcessor instance
        stream_id = "test-stream-id"
        s3_upload_id = "test-upload-id"
        s3_bucket = "test-bucket"
        s3_key_prefix = "test/prefix"
        shutdown_event = asyncio.Event()

        # Use patching to replace the imported functions in finalize_stream
        # We need to patch the modules that are imported inside the method
        with (
            patch(
                "src.s3_client.complete_s3_multipart_upload",
                mock_s3_client.complete_s3_multipart_upload,
            ),
            patch(
                "src.s3_client.abort_s3_multipart_upload",
                mock_s3_client.abort_s3_multipart_upload,
            ),
            patch("src.s3_client.upload_json_to_s3", mock_s3_client.upload_json_to_s3),
            patch(
                "src.redis_client.get_stream_parts", mock_redis_client.get_stream_parts
            ),
            patch(
                "src.redis_client.set_stream_status",
                mock_redis_client.set_stream_status,
            ),
            patch(
                "src.redis_client.remove_stream_from_pending_completion",
                mock_redis_client.remove_stream_from_pending_completion,
            ),
            patch(
                "src.redis_client.remove_stream_keys",
                mock_redis_client.remove_stream_keys,
            ),
            patch(
                "src.redis_client.add_stream_to_failed_set",
                mock_redis_client.add_stream_to_failed_set,
            ),
            patch(
                "src.metadata_generator.generate_metadata_json",
                mock_metadata_generator.generate_metadata_json,
            ),
        ):

            # Create the processor
            processor = StreamProcessor(
                stream_id=stream_id,
                file_path=file_path,
                s3_upload_id=s3_upload_id,
                s3_bucket=s3_bucket,
                s3_key_prefix=s3_key_prefix,
                shutdown_event=shutdown_event,
            )

            # Call finalize_stream
            await processor.finalize_stream()

            # Verify interactions
            mock_redis_client.get_stream_parts.assert_called_once_with(stream_id)
            mock_s3_client.complete_s3_multipart_upload.assert_called_once()
            mock_redis_client.set_stream_status.assert_any_call(
                stream_id, "s3_completed"
            )
            mock_metadata_generator.generate_metadata_json.assert_called_once_with(
                stream_id, file_path
            )
            mock_s3_client.upload_json_to_s3.assert_called_once()
            mock_redis_client.set_stream_status.assert_any_call(
                stream_id, "completed_with_meta"
            )
            mock_redis_client.remove_stream_keys.assert_called_once_with(stream_id)


@pytest.mark.asyncio
async def test_stream_processor_finalization_handles_s3_error():
    """Test that StreamProcessor finalization properly handles S3 errors."""
    # Create mocks
    mock_s3_client = AsyncMock()
    mock_redis_client = AsyncMock()

    # Set up return values
    mock_redis_client.get_stream_parts.return_value = [
        {"PartNumber": 1, "ETag": "etag1", "Size": 1000},
        {"PartNumber": 2, "ETag": "etag2", "Size": 2000},
    ]

    # Configure S3 client to raise an exception with proper operation name
    # This is important because the code checks e.operation == "complete_multipart_upload"
    s3_error = S3OperationError(
        "complete_multipart_upload", "Failed to complete multipart upload"
    )
    mock_s3_client.complete_s3_multipart_upload.side_effect = s3_error

    # Create test file
    with tempfile.NamedTemporaryFile(suffix=".mp4") as temp_file:
        file_path = Path(temp_file.name)

        # Create a real StreamProcessor instance
        stream_id = "test-stream-id"
        s3_upload_id = "test-upload-id"
        s3_bucket = "test-bucket"
        s3_key_prefix = "test/prefix"
        shutdown_event = asyncio.Event()

        # Use patching to replace the imported functions in finalize_stream
        with (
            patch(
                "src.s3_client.complete_s3_multipart_upload",
                mock_s3_client.complete_s3_multipart_upload,
            ),
            patch(
                "src.s3_client.abort_s3_multipart_upload",
                mock_s3_client.abort_s3_multipart_upload,
            ),
            patch(
                "src.redis_client.get_stream_parts", mock_redis_client.get_stream_parts
            ),
            patch(
                "src.redis_client.set_stream_status",
                mock_redis_client.set_stream_status,
            ),
            patch(
                "src.redis_client.remove_stream_from_pending_completion",
                mock_redis_client.remove_stream_from_pending_completion,
            ),
            patch(
                "src.redis_client.add_stream_to_failed_set",
                mock_redis_client.add_stream_to_failed_set,
            ),
        ):

            # Create the processor
            processor = StreamProcessor(
                stream_id=stream_id,
                file_path=file_path,
                s3_upload_id=s3_upload_id,
                s3_bucket=s3_bucket,
                s3_key_prefix=s3_key_prefix,
                shutdown_event=shutdown_event,
            )

            # Call finalize_stream and expect an exception
            with pytest.raises(StreamFinalizationError):
                await processor.finalize_stream()

            # Verify error handling
            mock_redis_client.get_stream_parts.assert_called_once_with(stream_id)
            mock_s3_client.complete_s3_multipart_upload.assert_called_once()
            mock_redis_client.add_stream_to_failed_set.assert_called_once_with(
                stream_id, reason="finalize_s3_failure"
            )
            mock_s3_client.abort_s3_multipart_upload.assert_called_once_with(
                s3_bucket, s3_key_prefix, s3_upload_id
            )
            mock_redis_client.remove_stream_from_pending_completion.assert_called_once_with(
                stream_id
            )
