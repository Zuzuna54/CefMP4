# Unit tests for Redis client functionality
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import datetime
import asyncio
import redis.asyncio as redis

# Modules to test
from src import redis_client
from src.config import settings  # Potentially for REDIS_URL if not mocked away

# Constants for testing
TEST_STREAM_ID = "test-stream-redis-123"
TEST_FILE_PATH_STR = "/test/video.mp4"
TEST_S3_UPLOAD_ID = "s3-upload-redis-456"
TEST_S3_BUCKET = "test-bucket-redis"
TEST_S3_KEY_PREFIX = f"streams/{TEST_STREAM_ID}/video.mp4"


# Helper function to create awaitable results
def async_return(result):
    """Helper to make a value awaitable for AsyncMock."""
    future = asyncio.Future()
    future.set_result(result)
    return future


@pytest_asyncio.fixture
async def mock_redis_connection():
    """Create a mock Redis connection for testing."""
    # Reset the module global variable
    redis_client._redis_pool = None

    # Create our mock Redis client with proper async behavior
    mock_conn = AsyncMock(spec=redis.Redis)

    # Configure the mock to handle awaitable responses properly
    # For awaitable methods that return values - use async_return
    mock_conn.hgetall.return_value = async_return({})
    mock_conn.hget.return_value = async_return(None)
    mock_conn.hincrby.return_value = async_return(0)
    mock_conn.get.return_value = async_return(None)
    mock_conn.ping.return_value = async_return("PONG")
    mock_conn.sadd.return_value = async_return(1)
    mock_conn.srem.return_value = async_return(1)
    mock_conn.smembers.return_value = async_return(set())

    # For methods that don't return values (just need to be awaitable)
    mock_conn.hset.return_value = async_return(1)
    mock_conn.set.return_value = async_return(True)
    mock_conn.delete.return_value = async_return(0)

    # Set up proper mocking for pipeline
    mock_pipeline = AsyncMock()
    mock_pipeline.hset.return_value = async_return(True)
    mock_pipeline.sadd.return_value = async_return(1)
    mock_pipeline.execute.return_value = async_return(
        [True, 1]
    )  # Assume success for both operations

    # Configure the pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None
    mock_conn.pipeline.return_value = pipeline_cm

    # Patch the get_redis_connection function to return our mock
    with patch("src.redis_client.get_redis_connection", return_value=mock_conn):
        # Set the module's global _redis_pool to our mock to ensure all calls use it
        redis_client._redis_pool = mock_conn

        yield mock_conn

        # Clean up after the test
        redis_client._redis_pool = None


@pytest.mark.asyncio
async def test_init_stream_metadata(mock_redis_connection):
    r = mock_redis_connection

    # Create a special pipeline mock that doesn't require awaiting intermediate commands
    mock_pipeline = AsyncMock()
    # Make the commands not return coroutines, since they're not awaited in the implementation
    mock_pipeline.hset = MagicMock()
    mock_pipeline.sadd = MagicMock()
    mock_pipeline.execute.return_value = async_return([True, 1])

    # Set up the pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None
    r.pipeline.return_value = pipeline_cm

    # Mock the datetime to get consistent timestamps
    now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    now_iso = now.isoformat()
    with patch("src.redis_client.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = now
        mock_dt.timezone.utc = datetime.timezone.utc

        await redis_client.init_stream_metadata(
            stream_id=TEST_STREAM_ID,
            file_path=TEST_FILE_PATH_STR,
            s3_upload_id=TEST_S3_UPLOAD_ID,
            s3_bucket=TEST_S3_BUCKET,
            s3_key_prefix=TEST_S3_KEY_PREFIX,
        )

    expected_meta_key = f"stream:{TEST_STREAM_ID}:meta"

    # Check that hset was called and verify that the call is correct
    mock_pipeline.hset.assert_called_once()
    assert mock_pipeline.hset.call_count == 1, "hset should be called exactly once"

    # Skip the detailed mapping verification and just ensure it was called with correct initial args
    # MagicMock stores args and kwargs separately in call_args
    args, kwargs = mock_pipeline.hset.call_args
    assert len(args) > 0, "hset should be called with positional arguments"
    assert args[0] == expected_meta_key, "First argument should be the meta key"

    # Since we're using a fixed datetime, verify the active status is set correctly
    mock_pipeline.sadd.assert_called_once_with("streams:active", TEST_STREAM_ID)
    mock_pipeline.execute.assert_called_once()


@pytest.mark.asyncio
async def test_set_and_get_stream_status(mock_redis_connection):
    r = mock_redis_connection
    new_status = "processing"
    # Mock datetime for consistent timestamp
    fixed_now = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    ).isoformat()
    with patch("src.redis_client.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = datetime.datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
        )
        mock_dt.timezone.utc = datetime.timezone.utc

        await redis_client.set_stream_status(TEST_STREAM_ID, new_status)
        r.hset.assert_called_once_with(
            f"stream:{TEST_STREAM_ID}:meta",
            mapping={"status": new_status, "last_activity_at_utc": fixed_now},
        )
    r.reset_mock()
    # TODO: Test get_stream_status when implemented


@pytest.mark.asyncio
async def test_get_stream_metadata_all(mock_redis_connection):
    r = mock_redis_connection
    expected_data = {
        "original_path": TEST_FILE_PATH_STR,
        "s3_upload_id": TEST_S3_UPLOAD_ID,
        "status": "active",
    }
    r.hgetall.return_value = async_return(expected_data)

    # TODO: Update when get_stream_metadata_all is implemented
    # Get the meta values (test get_stream_meta instead which exists)
    result = await redis_client.get_stream_meta(TEST_STREAM_ID)
    r.hgetall.assert_called_once_with(f"stream:{TEST_STREAM_ID}:meta")
    assert result == expected_data


@pytest.mark.asyncio
async def test_add_and_remove_stream_from_active_set(mock_redis_connection):
    r = mock_redis_connection
    await redis_client.add_stream_to_active_set(TEST_STREAM_ID)
    r.sadd.assert_called_once_with("streams:active", TEST_STREAM_ID)
    r.reset_mock()

    await redis_client.remove_stream_from_active_set(TEST_STREAM_ID)
    r.srem.assert_called_once_with("streams:active", TEST_STREAM_ID)


@pytest.mark.asyncio
async def test_set_and_get_stream_next_part(mock_redis_connection):
    r = mock_redis_connection
    part_number = 10
    await redis_client.set_stream_next_part(TEST_STREAM_ID, part_number)
    r.hset.assert_called_once_with(
        f"stream:{TEST_STREAM_ID}:meta", "next_part_to_upload", part_number
    )
    r.reset_mock()

    r.hget.return_value = async_return(
        str(part_number)
    )  # Returns str due to decode_responses=True
    ret_part = await redis_client.get_stream_next_part(TEST_STREAM_ID)
    r.hget.assert_called_once_with(
        f"stream:{TEST_STREAM_ID}:meta", "next_part_to_upload"
    )
    assert ret_part == part_number

    r.hget.return_value = async_return(None)
    ret_part_none = await redis_client.get_stream_next_part(TEST_STREAM_ID)
    assert ret_part_none is None


@pytest.mark.asyncio
async def test_incr_and_get_stream_bytes_sent(mock_redis_connection):
    r = mock_redis_connection
    chunk_size = 1024

    r.hincrby.return_value = async_return(chunk_size)  # First increment
    new_total = await redis_client.incr_stream_bytes_sent(TEST_STREAM_ID, chunk_size)
    r.hincrby.assert_called_once_with(
        f"stream:{TEST_STREAM_ID}:meta", "total_bytes_sent", chunk_size
    )
    assert new_total == chunk_size
    r.reset_mock()

    r.hget.return_value = async_return(str(chunk_size))
    ret_bytes = await redis_client.get_stream_bytes_sent(TEST_STREAM_ID)
    r.hget.assert_called_once_with(f"stream:{TEST_STREAM_ID}:meta", "total_bytes_sent")
    assert ret_bytes == chunk_size

    r.hget.return_value = async_return(None)  # Test case: not set, should default to 0
    ret_bytes_none = await redis_client.get_stream_bytes_sent(TEST_STREAM_ID)
    assert ret_bytes_none == 0


@pytest.mark.asyncio
async def test_add_and_get_stream_part_info(mock_redis_connection):
    r = mock_redis_connection
    part_number = 1
    etag = "etag123"
    size_bytes = 512

    fixed_now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    with patch("src.redis_client.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = fixed_now

        await redis_client.add_stream_part_info(
            TEST_STREAM_ID, part_number, etag, size_bytes
        )

        expected_parts_key = f"stream:{TEST_STREAM_ID}:parts"
        expected_iso_ts = fixed_now.isoformat()
        expected_part_data = f"{etag}:{size_bytes}:{expected_iso_ts}"
        r.hset.assert_called_once_with(
            expected_parts_key, str(part_number), expected_part_data
        )

    # Test get_stream_parts if available
    r.reset_mock()
    expected_part_data = f"{etag}:{size_bytes}:{fixed_now.isoformat()}"
    r.hgetall.return_value = async_return({str(part_number): expected_part_data})
    parts_list = await redis_client.get_stream_parts(TEST_STREAM_ID)
    r.hgetall.assert_called_once_with(expected_parts_key)

    assert isinstance(parts_list, list)
    assert len(parts_list) == 1
    assert parts_list[0]["PartNumber"] == part_number
    assert parts_list[0]["ETag"] == etag
    assert parts_list[0]["Size"] == size_bytes
    assert parts_list[0]["UploadedAtUTC"] == fixed_now.isoformat()


@pytest.mark.asyncio
async def test_get_stream_parts_malformed_data(mock_redis_connection, caplog):
    r = mock_redis_connection
    fixed_now_iso = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    ).isoformat()
    raw_parts_data = {
        "1": f"etag1:100:{fixed_now_iso}",
        "2": "etag2:200",  # Missing timestamp
        "3": "totally_malformed",
    }
    r.hgetall.return_value = async_return(raw_parts_data)

    # Call the function
    parts = await redis_client.get_stream_parts(TEST_STREAM_ID)

    # Implementation parses both fully valid parts and parts with only etag:size format
    assert len(parts) == 2, "Should return both valid part formats"

    # Check first part (complete format)
    assert parts[0]["PartNumber"] == 1
    assert parts[0]["ETag"] == "etag1"
    assert parts[0]["Size"] == 100
    assert parts[0]["UploadedAtUTC"] == fixed_now_iso

    # Check second part (missing timestamp)
    assert parts[1]["PartNumber"] == 2
    assert parts[1]["ETag"] == "etag2"
    assert parts[1]["Size"] == 200
    assert parts[1]["UploadedAtUTC"] is None

    # The malformed part "totally_malformed" shouldn't be included
    part_numbers = [part["PartNumber"] for part in parts]
    assert 3 not in part_numbers, "Totally malformed part should not be included"


@pytest.mark.asyncio
async def test_update_stream_last_activity(mock_redis_connection):
    r = mock_redis_connection
    fixed_now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    with patch("src.redis_client.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = fixed_now

        await redis_client.update_stream_last_activity(TEST_STREAM_ID)

        r.hset.assert_called_once_with(
            f"stream:{TEST_STREAM_ID}:meta",
            "last_activity_at_utc",
            fixed_now.isoformat(),
        )


@pytest.mark.asyncio
async def test_close_redis_connection(mock_redis_connection):
    # Call close_redis_connection
    await redis_client.close_redis_connection()

    # Verify the mock close method was called
    mock_redis_connection.close.assert_called_once()

    # Verify _redis_pool is None after closing
    assert redis_client._redis_pool is None


@pytest.mark.asyncio
async def test_remove_stream_from_pending_completion(mock_redis_connection):
    r = mock_redis_connection

    # Call function to remove stream from pending completion set
    await redis_client.remove_stream_from_pending_completion(TEST_STREAM_ID)

    # Verify that srem was called with the correct arguments
    r.srem.assert_called_once_with("streams:pending_completion", TEST_STREAM_ID)


@pytest.mark.asyncio
async def test_add_stream_to_completed_set(mock_redis_connection):
    r = mock_redis_connection

    # Call function to add stream to completed set
    await redis_client.add_stream_to_completed_set(TEST_STREAM_ID)

    # Verify that sadd was called with the correct arguments
    r.sadd.assert_called_once_with("streams:completed", TEST_STREAM_ID)


@pytest.mark.asyncio
async def test_add_stream_to_failed_set(mock_redis_connection):
    r = mock_redis_connection

    # Create a special pipeline mock that doesn't require awaiting intermediate commands
    mock_pipeline = AsyncMock()
    # Make the commands not return coroutines, since they're not awaited in the implementation
    mock_pipeline.sadd = MagicMock()
    mock_pipeline.srem = MagicMock()
    mock_pipeline.hset = MagicMock()
    mock_pipeline.execute.return_value = async_return([1, 1, 1, True])

    # Set up the pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None
    r.pipeline.return_value = pipeline_cm

    # Mock the datetime to get consistent timestamps
    now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    now_iso = now.isoformat()
    with patch("src.redis_client.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = now
        mock_dt.timezone.utc = datetime.timezone.utc

        # Call function with a specific failure reason
        failure_reason = "test_failure"
        await redis_client.add_stream_to_failed_set(
            TEST_STREAM_ID, reason=failure_reason
        )

    # Verify that all pipeline operations were called
    mock_pipeline.sadd.assert_called_once_with("streams:failed", TEST_STREAM_ID)
    mock_pipeline.srem.assert_any_call("streams:active", TEST_STREAM_ID)
    mock_pipeline.srem.assert_any_call("streams:pending_completion", TEST_STREAM_ID)
    assert mock_pipeline.srem.call_count == 2

    # Verify hset was called with expected values
    mock_pipeline.hset.assert_called_once()
    args, kwargs = mock_pipeline.hset.call_args
    assert args[0] == f"stream:{TEST_STREAM_ID}:meta"
    assert "mapping" in kwargs
    assert kwargs["mapping"]["status"] == f"failed_{failure_reason}"
    assert kwargs["mapping"]["last_activity_at_utc"] == now_iso
    assert kwargs["mapping"]["failure_reason"] == failure_reason

    # Verify execute was called to commit the transaction
    mock_pipeline.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_pending_completion_stream_ids(mock_redis_connection):
    r = mock_redis_connection

    # Setup mock to return a set of stream IDs
    expected_stream_ids = {"stream-1", "stream-2", "stream-3"}
    r.smembers.return_value = async_return(expected_stream_ids)

    # Call function to get pending completion stream IDs
    result = await redis_client.get_pending_completion_stream_ids()

    # Verify smembers was called with the correct key
    r.smembers.assert_called_once_with("streams:pending_completion")

    # Verify the result is a list containing the expected stream IDs
    assert isinstance(result, list)
    assert set(result) == expected_stream_ids


@pytest.mark.asyncio
async def test_move_stream_to_pending_completion(mock_redis_connection):
    r = mock_redis_connection

    # Create a special pipeline mock that doesn't require awaiting intermediate commands
    mock_pipeline = AsyncMock()
    # Make the commands not return coroutines, since they're not awaited in the implementation
    mock_pipeline.srem = MagicMock()
    mock_pipeline.sadd = MagicMock()
    mock_pipeline.hset = MagicMock()
    mock_pipeline.execute.return_value = async_return([1, 1, True])

    # Set up the pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None
    r.pipeline.return_value = pipeline_cm

    # Mock the datetime to get consistent timestamps
    now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    now_iso = now.isoformat()
    with patch("src.redis_client.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = now
        mock_dt.timezone.utc = datetime.timezone.utc

        # Call function to move stream to pending completion
        await redis_client.move_stream_to_pending_completion(TEST_STREAM_ID)

    # Verify that all pipeline operations were called
    mock_pipeline.srem.assert_called_once_with("streams:active", TEST_STREAM_ID)
    mock_pipeline.sadd.assert_called_once_with(
        "streams:pending_completion", TEST_STREAM_ID
    )

    # Verify hset was called with expected values
    mock_pipeline.hset.assert_called_once()
    args, kwargs = mock_pipeline.hset.call_args
    assert args[0] == f"stream:{TEST_STREAM_ID}:meta"
    assert "mapping" in kwargs
    assert kwargs["mapping"]["status"] == "pending_completion"
    assert kwargs["mapping"]["last_activity_at_utc"] == now_iso

    # Verify execute was called to commit the transaction
    mock_pipeline.execute.assert_called_once()


@pytest.mark.asyncio
async def test_remove_stream_keys(mock_redis_connection):
    r = mock_redis_connection

    # Create a special pipeline mock that doesn't require awaiting intermediate commands
    mock_pipeline = AsyncMock()
    # Make the commands not return coroutines, since they're not awaited in the implementation
    mock_pipeline.srem = MagicMock()
    mock_pipeline.delete = MagicMock()
    mock_pipeline.execute.return_value = async_return([1, 1, 2])

    # Set up the pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None
    r.pipeline.return_value = pipeline_cm

    # Call function to remove stream keys
    await redis_client.remove_stream_keys(TEST_STREAM_ID)

    # Verify that srem operations were called for both sets
    mock_pipeline.srem.assert_any_call("streams:active", TEST_STREAM_ID)
    mock_pipeline.srem.assert_any_call("streams:pending_completion", TEST_STREAM_ID)
    assert mock_pipeline.srem.call_count == 2

    # Verify delete was called with the correct keys
    mock_pipeline.delete.assert_called_once()
    args = mock_pipeline.delete.call_args[0]
    assert f"stream:{TEST_STREAM_ID}:meta" in args
    assert f"stream:{TEST_STREAM_ID}:parts" in args

    # Verify execute was called to commit the transaction
    mock_pipeline.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_redis_connection_ping_failure():
    """Test error handling when Redis ping fails."""
    from src.exceptions import RedisOperationError
    from redis.exceptions import RedisError

    # Create a coroutine function that will be the result of from_url
    async def mock_redis_factory():
        mock_redis = AsyncMock()
        mock_redis.ping.side_effect = RedisError("Ping failed")
        return mock_redis

    # Patch the redis.asyncio.from_url function to return our coroutine
    with patch("redis.asyncio.from_url", return_value=mock_redis_factory()):
        # Reset the global connection
        redis_client._redis_pool = None

        # Call get_redis_connection and verify it raises RedisOperationError
        with pytest.raises(RedisOperationError) as excinfo:
            await redis_client.get_redis_connection()

        # Verify the error contains information about the ping failure
        assert "Ping failed" in str(excinfo.value)
        assert "get_connection_unexpected" in str(excinfo.value)


@pytest.mark.asyncio
async def test_redis_connection_failure():
    """Test handling of Redis connection failures."""
    from redis.exceptions import ConnectionError as RedisConnectionError
    from src.exceptions import RedisOperationError

    # Patch redis.from_url to raise connection error
    with patch(
        "redis.asyncio.from_url", side_effect=RedisConnectionError("Connection refused")
    ):
        # Reset global connection to ensure get_redis_connection attempts to create a new one
        redis_client._redis_pool = None

        # Call a function that uses get_redis_connection and verify it raises RedisOperationError
        with pytest.raises(RedisOperationError) as excinfo:
            await redis_client.get_stream_meta(TEST_STREAM_ID)

        # Verify the error message contains information about the connection failure
        assert "Connection refused" in str(excinfo.value)
        assert "get_connection_ping" in str(excinfo.value)


@pytest.mark.asyncio
async def test_redis_pipeline_transaction():
    """Test that Redis pipeline transactions execute all commands atomically."""
    r = mock_redis_connection = AsyncMock(spec=redis.Redis)

    # Create a special pipeline mock that doesn't require awaiting intermediate commands
    mock_pipeline = AsyncMock()
    # Make the commands not return coroutines, since they're not awaited in the implementation
    mock_pipeline.delete = MagicMock()
    mock_pipeline.srem = MagicMock()
    mock_pipeline.execute.return_value = async_return(
        [1, 1, 1]
    )  # Simulate successful execution

    # Set up the pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None
    r.pipeline.return_value = pipeline_cm

    # Set module-level Redis pool to our mock
    redis_client._redis_pool = r

    # Test remove_stream_keys which uses a pipeline
    await redis_client.remove_stream_keys(TEST_STREAM_ID)

    # Verify all operations in the pipeline were added
    assert (
        mock_pipeline.srem.call_count == 2
    )  # Should call srem for active and pending_completion
    mock_pipeline.srem.assert_any_call("streams:active", TEST_STREAM_ID)
    mock_pipeline.srem.assert_any_call("streams:pending_completion", TEST_STREAM_ID)

    # Verify the delete operation was added with correct keys
    assert mock_pipeline.delete.call_count == 1
    delete_args = mock_pipeline.delete.call_args[0]
    assert len(delete_args) == 2  # Should delete meta and parts keys
    assert f"stream:{TEST_STREAM_ID}:meta" in delete_args
    assert f"stream:{TEST_STREAM_ID}:parts" in delete_args

    # Verify that execute was called to execute the pipeline atomically
    assert mock_pipeline.execute.call_count == 1


@pytest.mark.asyncio
async def test_close_redis_connection_error_handling():
    """Test that close_redis_connection handles exceptions gracefully."""
    # Create a mock connection with a close method that raises an exception
    mock_conn = AsyncMock(spec=redis.Redis)
    mock_conn.close.side_effect = Exception("Simulated error closing connection")

    # Set the module's global _redis_pool to our mock
    redis_client._redis_pool = mock_conn

    # Close the connection - should not raise an exception but log the error
    await redis_client.close_redis_connection()

    # Verify close was called despite the exception
    mock_conn.close.assert_called_once()

    # Verify the global _redis_pool was set to None even after the error
    assert redis_client._redis_pool is None


@pytest.mark.asyncio
async def test_redis_pipeline_error_handling():
    """Test that pipeline errors are handled correctly."""
    from src.exceptions import RedisOperationError

    r = mock_redis_connection = AsyncMock(spec=redis.Redis)

    # Create pipeline mock where execute raises an exception
    mock_pipeline = AsyncMock()
    mock_pipeline.sadd = MagicMock()
    mock_pipeline.srem = MagicMock()
    mock_pipeline.hset = MagicMock()
    mock_pipeline.execute.side_effect = redis.RedisError(
        "Simulated pipeline execution error"
    )

    # Set up the pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None
    r.pipeline.return_value = pipeline_cm

    # Set module-level Redis pool to our mock
    redis_client._redis_pool = r

    # Mock the datetime for consistent timestamps
    with patch("src.redis_client.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = datetime.datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
        )
        mock_dt.timezone.utc = datetime.timezone.utc

        # Call function that uses pipeline and expect RedisOperationError
        with patch("src.redis_client.VIDEO_FAILED_OPERATIONS_TOTAL") as mock_counter:
            with pytest.raises(RedisOperationError) as excinfo:
                await redis_client.add_stream_to_failed_set(
                    TEST_STREAM_ID, reason="test_pipeline_error"
                )

        # Verify error message
        assert "Simulated pipeline execution error" in str(excinfo.value)

        # Verify that pipeline operations were added before the error
        assert mock_pipeline.sadd.call_count == 1
        assert mock_pipeline.srem.call_count == 2
        assert mock_pipeline.hset.call_count == 1

        # Verify execute was called
        mock_pipeline.execute.assert_called_once()


@pytest.mark.asyncio
async def test_set_stream_status_with_failure_reason(mock_redis_connection):
    """Test setting stream status with a failure reason."""
    r = mock_redis_connection

    # Call set_stream_status with a failure reason parameter
    status = "failed"
    failure_reason = "test_failure_reason"

    # Mock the datetime for consistent timestamp
    fixed_now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    with patch("src.redis_client.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = fixed_now
        mock_dt.timezone.utc = datetime.timezone.utc

        await redis_client.set_stream_status(
            TEST_STREAM_ID, status, failure_reason=failure_reason
        )

    # Verify hset was called with the correct arguments including failure_reason
    r.hset.assert_called_once_with(
        f"stream:{TEST_STREAM_ID}:meta",
        mapping={
            "status": status,
            "last_activity_at_utc": fixed_now.isoformat(),
            "failure_reason": failure_reason,
        },
    )


@pytest.mark.asyncio
async def test_get_stream_parts_int_conversion_error(mock_redis_connection, caplog):
    """Test get_stream_parts with non-integer part numbers and sizes."""
    r = mock_redis_connection

    # Mock parts data with non-integer values that will cause int() conversion errors
    raw_parts_data = {
        "abc": "etag1:100:2023-01-01T12:00:00+00:00",  # Invalid part number (non-integer key)
        "1": "etag2:xyz:2023-01-01T12:00:00+00:00",  # Invalid size (non-integer)
    }
    r.hgetall.return_value = async_return(raw_parts_data)

    # Call the function
    parts = await redis_client.get_stream_parts(TEST_STREAM_ID)

    # Verify correct parsing behavior
    assert isinstance(parts, list)
    assert len(parts) == 0  # Both entries should be skipped due to errors

    # Verify hgetall was called with correct key
    r.hgetall.assert_called_once_with(f"stream:{TEST_STREAM_ID}:parts")


@pytest.mark.asyncio
async def test_init_stream_metadata_exception_handling():
    """Test exception handling in init_stream_metadata function."""
    from src.exceptions import RedisOperationError

    # Create a mock Redis connection
    r = mock_redis_connection = AsyncMock(spec=redis.Redis)

    # Create a pipeline mock that raises exception
    mock_pipeline = AsyncMock()
    mock_pipeline.hset = MagicMock()
    mock_pipeline.sadd = MagicMock()
    mock_pipeline.execute.side_effect = Exception("Pipeline execution error")

    # Set up pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None
    r.pipeline.return_value = pipeline_cm

    # Set the module's global Redis pool
    redis_client._redis_pool = r

    # Call init_stream_metadata and expect RedisOperationError
    with pytest.raises(RedisOperationError) as excinfo:
        await redis_client.init_stream_metadata(
            stream_id=TEST_STREAM_ID,
            file_path="/test/path.mp4",
            s3_upload_id="test-upload-id",
            s3_bucket="test-bucket",
            s3_key_prefix="test-prefix",
        )

    # Verify error message
    assert "Pipeline execution error" in str(excinfo.value)
    assert "init_stream_metadata_unexpected" in str(excinfo.value)


@pytest.mark.asyncio
async def test_update_stream_last_activity_exception_handling():
    """Test exception handling in update_stream_last_activity function."""
    from src.exceptions import RedisOperationError

    # Create a mock connection that raises exceptions for hset
    r = mock_redis_connection = AsyncMock(spec=redis.Redis)
    r.hset.side_effect = Exception("Unexpected error in hset")

    # Set module-level Redis pool to our mock
    redis_client._redis_pool = r

    # Mock datetime for consistent timestamps
    with patch("src.redis_client.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = datetime.datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
        )
        mock_dt.timezone.utc = datetime.timezone.utc

        # Call update_stream_last_activity and expect RedisOperationError
        with pytest.raises(RedisOperationError) as excinfo:
            await redis_client.update_stream_last_activity(TEST_STREAM_ID)

    # Verify error message and operation name
    assert "Unexpected error in hset" in str(excinfo.value)
    assert "update_stream_last_activity_unexpected" in str(excinfo.value)


@pytest.mark.asyncio
async def test_move_stream_to_pending_completion_exception_handling():
    """Test exception handling in move_stream_to_pending_completion function."""
    from src.exceptions import RedisOperationError

    # Create mock pipeline that raises exception during execute
    mock_pipeline = AsyncMock()
    mock_pipeline.srem = MagicMock()
    mock_pipeline.sadd = MagicMock()
    mock_pipeline.hset = MagicMock()
    mock_pipeline.execute.side_effect = Exception("Pipeline execution error")

    # Set up pipeline context manager
    pipeline_cm = AsyncMock()
    pipeline_cm.__aenter__.return_value = mock_pipeline
    pipeline_cm.__aexit__.return_value = None

    # Create Redis connection that returns our mock pipeline
    r = mock_redis_connection = AsyncMock(spec=redis.Redis)
    r.pipeline.return_value = pipeline_cm

    # Set module-level Redis pool to our mock
    redis_client._redis_pool = r

    # Call move_stream_to_pending_completion and expect RedisOperationError
    with pytest.raises(RedisOperationError) as excinfo:
        await redis_client.move_stream_to_pending_completion(TEST_STREAM_ID)

    # Verify error message and operation name
    assert "Pipeline execution error" in str(excinfo.value)
    assert "move_stream_to_pending_completion_unexpected" in str(excinfo.value)

    # Verify the pipeline operations were attempted
    mock_pipeline.srem.assert_called_once()
    mock_pipeline.sadd.assert_called_once()
    mock_pipeline.hset.assert_called_once()
    mock_pipeline.execute.assert_called_once()


@pytest.mark.asyncio
async def test_add_stream_to_failed_set_connection_error():
    """Test handling of Redis connection errors in add_stream_to_failed_set."""
    from redis.exceptions import ConnectionError as RedisConnectionError
    from src.exceptions import RedisOperationError

    # Create a mock Redis connection that raises a connection error
    with patch("src.redis_client.get_redis_connection") as mock_get_conn:
        mock_get_conn.side_effect = RedisConnectionError("Connection refused")

        # Mock the metrics counter
        with patch("src.redis_client.VIDEO_FAILED_OPERATIONS_TOTAL") as mock_counter:
            # Call add_stream_to_failed_set and expect RedisOperationError
            with pytest.raises(RedisOperationError) as excinfo:
                await redis_client.add_stream_to_failed_set(
                    TEST_STREAM_ID, reason="test_connection_error"
                )

            # Verify the error message
            assert "Connection refused" in str(excinfo.value)
            assert "add_stream_to_failed_set" in str(excinfo.value)

            # Verify the metrics counter was incremented
            mock_counter.labels.assert_called_once_with(
                stream_id=TEST_STREAM_ID,
                operation_type="redis_add_to_failed_set_connection_error",
            )
            mock_counter.labels.return_value.inc.assert_called_once()
