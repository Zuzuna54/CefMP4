import asyncio
import datetime
import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock
from freezegun import freeze_time

from src.metadata_generator import generate_metadata_json


@pytest.mark.asyncio
@freeze_time("2023-01-01 12:00:00")
async def test_generate_metadata_json_success():
    """Test successful metadata generation."""
    # Mock data
    stream_id = "test-stream-123"
    original_path = Path("/path/to/video.mp4")

    # Mock Redis responses
    mock_stream_meta = {
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "videos/test",
        "started_at_utc": "2023-01-01T11:50:00+00:00",
        "last_activity_at_utc": "2023-01-01T11:55:00+00:00",
    }

    mock_stream_parts = [
        {
            "PartNumber": 1,
            "Size": 1024,
            "ETag": '"abc123"',
            "UploadedAtUTC": "2023-01-01T11:51:00+00:00",
        },
        {
            "PartNumber": 2,
            "Size": 2048,
            "ETag": '"def456"',
            "UploadedAtUTC": "2023-01-01T11:52:00+00:00",
        },
    ]

    # Apply mocks
    with (
        patch(
            "src.metadata_generator.get_stream_meta",
            AsyncMock(return_value=mock_stream_meta),
        ) as mock_get_meta,
        patch(
            "src.metadata_generator.get_stream_parts",
            AsyncMock(return_value=mock_stream_parts),
        ) as mock_get_parts,
        patch(
            "src.metadata_generator.get_video_duration", AsyncMock(return_value=120.5)
        ) as mock_get_duration,
    ):

        # Call the function
        result = await generate_metadata_json(stream_id, original_path)

    # Verify function calls
    mock_get_meta.assert_called_once_with(stream_id)
    mock_get_parts.assert_called_once_with(stream_id)
    mock_get_duration.assert_called_once_with(str(original_path))

    # Verify result structure
    assert result is not None
    assert result["stream_id"] == stream_id
    assert result["original_file_path"] == str(original_path)
    assert result["s3_bucket"] == "test-bucket"
    assert result["s3_key_prefix"] == "videos/test"
    assert result["total_size_bytes"] == 3072  # 1024 + 2048
    assert result["duration_seconds"] == 120.5
    assert result["processed_at_utc"] == "2023-01-01T12:00:00+00:00"
    assert result["stream_started_at_utc"] == "2023-01-01T11:50:00+00:00"
    assert result["stream_completed_at_utc"] == "2023-01-01T11:55:00+00:00"

    # Verify chunks
    assert len(result["chunks"]) == 2
    assert result["chunks"][0]["part_number"] == 1
    assert result["chunks"][0]["size_bytes"] == 1024
    assert result["chunks"][0]["etag"] == '"abc123"'
    assert result["chunks"][1]["part_number"] == 2
    assert result["chunks"][1]["size_bytes"] == 2048


@pytest.mark.asyncio
async def test_generate_metadata_json_missing_stream_meta():
    """Test metadata generation when stream metadata is missing."""
    stream_id = "missing-stream-123"
    original_path = Path("/path/to/video.mp4")

    # Mock Redis get_stream_meta to return None (missing)
    with patch("src.metadata_generator.get_stream_meta", AsyncMock(return_value=None)):
        result = await generate_metadata_json(stream_id, original_path)

    # Should return None if metadata is missing
    assert result is None


@pytest.mark.asyncio
async def test_generate_metadata_json_empty_parts():
    """Test metadata generation when parts list is empty."""
    stream_id = "empty-parts-stream-123"
    original_path = Path("/path/to/video.mp4")

    # Mock data
    mock_stream_meta = {
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "videos/test",
        "started_at_utc": "2023-01-01T11:50:00+00:00",
        "last_activity_at_utc": "2023-01-01T11:55:00+00:00",
    }

    # Empty parts list
    mock_stream_parts = []

    # Apply mocks
    with (
        patch(
            "src.metadata_generator.get_stream_meta",
            AsyncMock(return_value=mock_stream_meta),
        ),
        patch(
            "src.metadata_generator.get_stream_parts",
            AsyncMock(return_value=mock_stream_parts),
        ),
        patch(
            "src.metadata_generator.get_video_duration", AsyncMock(return_value=120.5)
        ),
    ):

        # Call the function
        result = await generate_metadata_json(stream_id, original_path)

    # Should still generate metadata with empty chunks and zero total size
    assert result is not None
    assert result["total_size_bytes"] == 0
    assert result["chunks"] == []


@pytest.mark.asyncio
async def test_generate_metadata_json_missing_duration():
    """Test metadata generation when video duration cannot be determined."""
    stream_id = "no-duration-stream-123"
    original_path = Path("/path/to/video.mp4")

    # Mock data
    mock_stream_meta = {
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "videos/test",
        "started_at_utc": "2023-01-01T11:50:00+00:00",
        "last_activity_at_utc": "2023-01-01T11:55:00+00:00",
    }

    mock_stream_parts = [
        {
            "PartNumber": 1,
            "Size": 1024,
            "ETag": '"abc123"',
            "UploadedAtUTC": "2023-01-01T11:51:00+00:00",
        }
    ]

    # Apply mocks with get_video_duration returning None
    with (
        patch(
            "src.metadata_generator.get_stream_meta",
            AsyncMock(return_value=mock_stream_meta),
        ),
        patch(
            "src.metadata_generator.get_stream_parts",
            AsyncMock(return_value=mock_stream_parts),
        ),
        patch(
            "src.metadata_generator.get_video_duration", AsyncMock(return_value=None)
        ),
    ):

        # Call the function
        result = await generate_metadata_json(stream_id, original_path)

    # Should generate metadata with None for duration
    assert result is not None
    assert result["duration_seconds"] is None
    assert result["total_size_bytes"] == 1024


@pytest.mark.asyncio
async def test_generate_metadata_json_part_sorting():
    """Test that parts are correctly sorted by part number in the output."""
    stream_id = "unsorted-parts-stream-123"
    original_path = Path("/path/to/video.mp4")

    # Mock data with unsorted parts
    mock_stream_meta = {
        "s3_bucket": "test-bucket",
        "s3_key_prefix": "videos/test",
        "started_at_utc": "2023-01-01T11:50:00+00:00",
        "last_activity_at_utc": "2023-01-01T11:55:00+00:00",
    }

    # Parts in unsorted order
    mock_stream_parts = [
        {
            "PartNumber": 3,
            "Size": 3072,
            "ETag": '"ghi789"',
            "UploadedAtUTC": "2023-01-01T11:53:00+00:00",
        },
        {
            "PartNumber": 1,
            "Size": 1024,
            "ETag": '"abc123"',
            "UploadedAtUTC": "2023-01-01T11:51:00+00:00",
        },
        {
            "PartNumber": 2,
            "Size": 2048,
            "ETag": '"def456"',
            "UploadedAtUTC": "2023-01-01T11:52:00+00:00",
        },
    ]

    # Apply mocks
    with (
        patch(
            "src.metadata_generator.get_stream_meta",
            AsyncMock(return_value=mock_stream_meta),
        ),
        patch(
            "src.metadata_generator.get_stream_parts",
            AsyncMock(return_value=mock_stream_parts),
        ),
        patch(
            "src.metadata_generator.get_video_duration", AsyncMock(return_value=120.5)
        ),
    ):

        # Call the function
        result = await generate_metadata_json(stream_id, original_path)

    # Verify chunks are sorted by part number
    assert result["chunks"][0]["part_number"] == 1
    assert result["chunks"][1]["part_number"] == 2
    assert result["chunks"][2]["part_number"] == 3
