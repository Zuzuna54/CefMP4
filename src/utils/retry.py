import structlog
from functools import wraps
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = structlog.get_logger(__name__)


def async_retry_transient(
    attempts=3, initial_wait=1, max_wait=10, transient_exceptions=(Exception,)
):
    """A general retry decorator for async functions using tenacity."""

    def decorator(func):
        @wraps(func)
        @retry(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=1, min=initial_wait, max=max_wait),
            retry=retry_if_exception_type(transient_exceptions),
            reraise=True,  # Reraise the exception if all attempts fail
        )
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Log on the attempt that fails before retry, or only if it's the last attempt?
                # Tenacity logs before retrying if its own logger is configured.
                # For structlog, we can add it manually if needed, or rely on tenacity's internal logging if it uses stdlib.
                # Since we reconfigured stdlib root logger, tenacity's logs should also be structured if it uses stdlib logging.
                # Let's add specific context for our retry log:
                logger.warning(
                    "Retryable operation failed, retrying...",
                    func_name=func.__name__,
                    attempt=wrapper.retry.statistics.get("attempt_number", 0)
                    + 1,  # Next attempt number
                    max_attempts=attempts,
                    error=str(e),
                    # exc_info=e # Add if detailed traceback needed on each retry attempt
                )
                raise  # Important to re-raise for tenacity to catch and retry

        return wrapper

    return decorator
