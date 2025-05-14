# Unit tests for Redis client functionality
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import datetime

# Modules to test
from src import redis_client
from src.config import settings  # Potentially for REDIS_URL if not mocked away

# Constants for testing
TEST_STREAM_ID = "test-stream-redis-123"
TEST_FILE_PATH_STR = "/test/video.mp4"
TEST_S3_UPLOAD_ID = "s3-upload-redis-456"
TEST_S3_BUCKET = "test-bucket-redis"
TEST_S3_KEY_PREFIX = f"streams/{TEST_STREAM_ID}/video.mp4"


@pytest.fixture
async def mock_redis_connection():
    # Reset the global pool in redis_client to ensure get_redis_connection tries to init
    if hasattr(redis_client, "_redis_pool"):
        redis_client._redis_pool = None

    mock_conn = AsyncMock()  # This is the object that should have .hset, .sadd etc.
    # If ping is called during get_redis_connection
    mock_conn.ping = AsyncMock(return_value="PONG")

    with patch("src.redis_client.get_redis_connection") as mock_get_conn_function:
        mock_get_conn_function.return_value = mock_conn
        # Ensure that multiple calls to get_redis_connection in one test get the same mock
        # This is implicitly handled if redis_client._redis_pool is set by the first call
        # and subsequent calls return it. Our mock setup bypasses the pool creation mostly.

        # Let's also patch the actual redis.from_url in case get_redis_connection is more complex
        # This ensures the _redis_pool in redis_client.py gets set to our mock_conn
        # if the real get_redis_connection logic runs.
        with patch(
            "redis.asyncio.from_url", new_callable=AsyncMock, return_value=mock_conn
        ) as mock_redis_from_url:
            yield mock_conn  # This is the mock Redis client instance


@pytest.mark.asyncio
async def test_init_stream_metadata(mock_redis_connection: AsyncMock):
    r = mock_redis_connection
    # Pipeline mocking might be needed if the test relies on pipeline execution results
    mock_pipeline = AsyncMock()
    r.pipeline.return_value.__aenter__.return_value = (
        mock_pipeline  # Mock the async context manager for pipeline
    )

    await redis_client.init_stream_metadata(
        stream_id=TEST_STREAM_ID,
        file_path=TEST_FILE_PATH_STR,
        s3_upload_id=TEST_S3_UPLOAD_ID,
        s3_bucket=TEST_S3_BUCKET,
        s3_key_prefix=TEST_S3_KEY_PREFIX,
    )

    expected_meta_key = f"stream:{TEST_STREAM_ID}:meta"

    # Check calls on the pipeline object
    mock_pipeline.hset.assert_any_call(
        expected_meta_key,
        mapping={
            "original_path": TEST_FILE_PATH_STR,
            "s3_upload_id": TEST_S3_UPLOAD_ID,
            "s3_bucket": TEST_S3_BUCKET,
            "s3_key_prefix": TEST_S3_KEY_PREFIX,
            "status": "processing",
            "started_at_utc": mock_pipeline.hset.call_args_list[0][1]["mapping"][
                "started_at_utc"
            ],  # Fragile, better to mock datetime
            "last_activity_at_utc": mock_pipeline.hset.call_args_list[0][1]["mapping"][
                "last_activity_at_utc"
            ],
            "total_bytes_expected": -1,
        },
    )
    mock_pipeline.sadd.assert_called_once_with("streams:active", TEST_STREAM_ID)
    mock_pipeline.execute.assert_called_once()


@pytest.mark.asyncio
async def test_set_and_get_stream_status(mock_redis_connection: AsyncMock):
    r = mock_redis_connection
    new_status = "processing"
    # Mock datetime for consistent timestamp
    fixed_now = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    ).isoformat()
    with patch("src.redis_client.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.isoformat.return_value = fixed_now
        mock_dt.datetime.now.return_value.tzinfo = datetime.timezone.utc

        await redis_client.set_stream_status(TEST_STREAM_ID, new_status)
        r.hset.assert_called_once_with(
            f"stream:{TEST_STREAM_ID}:meta",
            mapping={"status": new_status, "last_activity_at_utc": fixed_now},
        )
    r.hset.reset_mock()
    # get_stream_status is not in the provided redis_client.py, assuming it's for a later phase or was a typo
    # If it were to exist and call r.hget:
    # r.hget.return_value = new_status # hget returns decoded string due to decode_responses=True
    # ret_status = await redis_client.get_stream_status(TEST_STREAM_ID)
    # r.hget.assert_called_once_with(f"stream:{TEST_STREAM_ID}:meta", "status")
    # assert ret_status == new_status


@pytest.mark.asyncio
async def test_get_stream_metadata_all(mock_redis_connection: AsyncMock):
    r = mock_redis_connection
    expected_data = {
        "original_path": TEST_FILE_PATH_STR,
        "s3_upload_id": TEST_S3_UPLOAD_ID,
        "status": "active",
    }
    r.hgetall.return_value = expected_data  # hgetall returns decoded dict

    # get_stream_metadata_all is not in the provided redis_client.py
    # Assuming a hypothetical implementation:
    # meta = await redis_client.get_stream_metadata_all(TEST_STREAM_ID)
    # r.hgetall.assert_called_once_with(f"stream:{TEST_STREAM_ID}:meta")
    # assert meta == expected_data # Direct comparison if no processing
    pass  # Test needs function to exist in redis_client.py


@pytest.mark.asyncio
async def test_add_and_remove_stream_from_active_set(mock_redis_connection: AsyncMock):
    r = mock_redis_connection
    await redis_client.add_stream_to_active_set(TEST_STREAM_ID)
    r.sadd.assert_called_once_with("streams:active", TEST_STREAM_ID)
    r.sadd.reset_mock()

    await redis_client.remove_stream_from_active_set(TEST_STREAM_ID)
    r.srem.assert_called_once_with("streams:active", TEST_STREAM_ID)


@pytest.mark.asyncio
async def test_set_and_get_stream_next_part(mock_redis_connection: AsyncMock):
    r = mock_redis_connection
    part_number = 10
    await redis_client.set_stream_next_part(TEST_STREAM_ID, part_number)
    r.hset.assert_called_once_with(
        f"stream:{TEST_STREAM_ID}:meta", "next_part_to_upload", part_number
    )
    r.hset.reset_mock()

    r.hget.return_value = str(part_number)  # Returns str due to decode_responses=True
    ret_part = await redis_client.get_stream_next_part(TEST_STREAM_ID)
    r.hget.assert_called_once_with(
        f"stream:{TEST_STREAM_ID}:meta", "next_part_to_upload"
    )
    assert ret_part == part_number

    r.hget.return_value = None
    ret_part_none = await redis_client.get_stream_next_part(TEST_STREAM_ID)
    assert ret_part_none is None


@pytest.mark.asyncio
async def test_incr_and_get_stream_bytes_sent(mock_redis_connection: AsyncMock):
    r = mock_redis_connection
    chunk_size = 1024

    r.hincrby.return_value = chunk_size  # First increment
    new_total = await redis_client.incr_stream_bytes_sent(TEST_STREAM_ID, chunk_size)
    r.hincrby.assert_called_once_with(
        f"stream:{TEST_STREAM_ID}:meta", "total_bytes_sent", chunk_size
    )
    assert new_total == chunk_size
    r.hincrby.reset_mock()

    r.hget.return_value = str(chunk_size)
    ret_bytes = await redis_client.get_stream_bytes_sent(TEST_STREAM_ID)
    r.hget.assert_called_once_with(f"stream:{TEST_STREAM_ID}:meta", "total_bytes_sent")
    assert ret_bytes == chunk_size

    r.hget.return_value = None  # Test case: not set, should default to 0
    ret_bytes_none = await redis_client.get_stream_bytes_sent(TEST_STREAM_ID)
    assert ret_bytes_none == 0


@pytest.mark.asyncio
async def test_add_and_get_stream_part_info(mock_redis_connection: AsyncMock):
    r = mock_redis_connection
    part_number = 1
    etag = "etag123"
    size_bytes = 512

    fixed_now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    with patch("src.redis_client.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = fixed_now
        # mock_datetime.timezone.utc = datetime.timezone.utc # Not needed if fixed_now is timezone-aware

        await redis_client.add_stream_part_info(
            TEST_STREAM_ID, part_number, etag, size_bytes
        )

        expected_parts_key = f"stream:{TEST_STREAM_ID}:parts"
        expected_iso_ts = fixed_now.isoformat()
        expected_part_data = f"{etag}:{size_bytes}:{expected_iso_ts}"
        r.hset.assert_called_once_with(
            expected_parts_key, str(part_number), expected_part_data
        )

    r.hset.reset_mock()

    raw_parts_data = {
        "1": f"{etag}:{size_bytes}:{fixed_now.isoformat()}",
        "2": f"etag456:1024:{fixed_now.isoformat()}",
    }
    r.hgetall.return_value = raw_parts_data

    parsed_parts = await redis_client.get_stream_parts(TEST_STREAM_ID)
    r.hgetall.assert_called_once_with(f"stream:{TEST_STREAM_ID}:parts")

    assert len(parsed_parts) == 2
    assert parsed_parts[0]["PartNumber"] == 1
    assert parsed_parts[0]["ETag"] == etag
    assert parsed_parts[0]["Size"] == size_bytes
    assert parsed_parts[0]["UploadedAtUTC"] == fixed_now.isoformat()

    assert parsed_parts[1]["PartNumber"] == 2
    assert parsed_parts[1]["ETag"] == "etag456"
    assert parsed_parts[1]["Size"] == 1024
    assert parsed_parts[1]["UploadedAtUTC"] == fixed_now.isoformat()


@pytest.mark.asyncio
async def test_get_stream_parts_malformed_data(
    mock_redis_connection: AsyncMock, caplog
):
    r = mock_redis_connection
    fixed_now_iso = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    ).isoformat()
    raw_parts_data = {
        "1": f"etag1:100:{fixed_now_iso}",
        "2": "etag2:200",  # Missing timestamp
        "3": "totally_malformed",
    }
    r.hgetall.return_value = raw_parts_data
    parsed_parts = await redis_client.get_stream_parts(TEST_STREAM_ID)

    assert len(parsed_parts) == 2
    assert parsed_parts[0]["PartNumber"] == 1
    assert parsed_parts[0]["UploadedAtUTC"] == fixed_now_iso
    assert parsed_parts[1]["PartNumber"] == 2
    assert parsed_parts[1]["UploadedAtUTC"] is None

    assert (
        f"Part data for stream {TEST_STREAM_ID}, part 2 has unexpected format: 'etag2:200'. Missing UploadedAtUTC."
        in caplog.text
    )
    assert (
        f"Could not parse part data 'totally_malformed' for stream {TEST_STREAM_ID}, part 3"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_update_stream_last_activity(mock_redis_connection: AsyncMock):
    r = mock_redis_connection
    fixed_now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    with patch("src.redis_client.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = fixed_now
        # mock_datetime.timezone.utc = datetime.timezone.utc

        await redis_client.update_stream_last_activity(TEST_STREAM_ID)

        expected_meta_key = f"stream:{TEST_STREAM_ID}:meta"
        expected_iso_ts = fixed_now.isoformat()
        r.hset.assert_called_once_with(
            expected_meta_key, "last_activity_at_utc", expected_iso_ts
        )


@pytest.mark.asyncio
async def test_close_redis_connection(mock_redis_connection: AsyncMock):
    # This test needs to ensure that redis_client._redis_pool is set to a mock
    # that has an .aclose() method (or .close() if not async).
    # The fixture mock_redis_connection should yield the client mock.
    # The actual close_redis_connection closes the global _redis_pool.

    # Setup: Make sure _redis_pool in redis_client is our mock_redis_connection or similar mock
    # The mock_redis_connection fixture itself should handle patching get_redis_connection
    # or redis.from_url such that _redis_pool becomes the mock_conn.

    # If redis_client.get_redis_connection() was called by a previous test and set _redis_pool,
    # it might not be our specific AsyncMock from this test instance of the fixture if not careful.
    # The fixture now resets _redis_pool = None and patches redis.asyncio.from_url

    # Call get_redis_connection to ensure _redis_pool is populated (by the patched from_url)
    await redis_client.get_redis_connection()
    assert (
        redis_client._redis_pool is mock_redis_connection
    )  # Check if global pool is our mock

    await redis_client.close_redis_connection()

    mock_redis_connection.close.assert_called_once()  # The mock_conn itself should be closed.
    # redis.asyncio.Redis uses .close(), not .aclose() for the client instance.
    assert redis_client._redis_pool is None

    await redis_client.close_redis_connection()
    mock_redis_connection.close.assert_called_once()
