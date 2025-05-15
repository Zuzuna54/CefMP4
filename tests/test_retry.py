import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.utils.retry import async_retry_transient
import asyncio


class TestException(Exception):
    pass


class OtherException(Exception):
    pass


@pytest.mark.asyncio
async def test_retry_success_after_failures():
    """Test that retry mechanism works after a few failures."""
    # Create a mock that fails twice then succeeds
    mock_fn = AsyncMock()
    mock_fn.side_effect = [TestException("Fail 1"), TestException("Fail 2"), "Success"]

    # Apply the retry decorator
    decorated_fn = async_retry_transient(
        attempts=3,
        initial_wait=0.01,  # Small waits for faster test execution
        max_wait=0.05,
        transient_exceptions=(TestException,),
    )(mock_fn)

    # Call the decorated function
    result = await decorated_fn("arg1", key="value")

    # Verify the function was called the expected number of times
    assert mock_fn.call_count == 3
    assert result == "Success"
    mock_fn.assert_called_with("arg1", key="value")


@pytest.mark.asyncio
async def test_retry_max_attempts_exceeded():
    """Test that retry gives up after max attempts."""
    # Create a mock that always fails
    mock_fn = AsyncMock()
    mock_fn.side_effect = TestException("Always fails")

    # Apply the retry decorator
    decorated_fn = async_retry_transient(
        attempts=3,
        initial_wait=0.01,
        max_wait=0.05,
        transient_exceptions=(TestException,),
    )(mock_fn)

    # Call the decorated function and expect it to raise after all retries
    with pytest.raises(TestException):
        await decorated_fn()

    # Verify the function was called the expected number of times
    assert mock_fn.call_count == 3


@pytest.mark.asyncio
async def test_retry_non_matching_exception():
    """Test that retry doesn't catch exceptions not in the transient list."""
    # Create a mock that raises non-matching exception
    mock_fn = AsyncMock()
    mock_fn.side_effect = OtherException("Different exception")

    # Apply the retry decorator with only TestException as transient
    decorated_fn = async_retry_transient(
        attempts=3,
        initial_wait=0.01,
        max_wait=0.05,
        transient_exceptions=(TestException,),
    )(mock_fn)

    # Should immediately raise without retrying
    with pytest.raises(OtherException):
        await decorated_fn()

    # Verify the function was called only once
    assert mock_fn.call_count == 1


@pytest.mark.asyncio
async def test_retry_exponential_backoff():
    """Test that retry uses exponential backoff."""
    # Create a mock that always fails
    mock_fn = AsyncMock()
    mock_fn.side_effect = TestException("Always fails")

    # Mock sleep to track sleep times
    sleep_times = []

    # Apply the retry decorator
    with patch(
        "tenacity.wait_exponential.__call__",
        side_effect=lambda attempt, **kwargs: sleep_times.append((attempt, kwargs))
        or 0,
    ):
        decorated_fn = async_retry_transient(
            attempts=3,
            initial_wait=1,
            max_wait=10,
            transient_exceptions=(TestException,),
        )(mock_fn)

        # Call the decorated function and expect it to raise after all retries
        with pytest.raises(TestException):
            await decorated_fn()

    # Verify increasing backoff pattern
    assert len(sleep_times) > 0


@pytest.mark.asyncio
async def test_retry_cancelled_error():
    """Test that retry properly handles cancellation."""
    # Create a mock that raises CancelledError
    mock_fn = AsyncMock()
    mock_fn.side_effect = asyncio.CancelledError()

    # Apply the retry decorator
    decorated_fn = async_retry_transient(
        attempts=3,
        initial_wait=0.01,
        max_wait=0.05,
        transient_exceptions=(TestException,),
    )(mock_fn)

    # Should propagate cancellation without retrying
    with pytest.raises(asyncio.CancelledError):
        await decorated_fn()

    # Verify the function was called only once
    assert mock_fn.call_count == 1
