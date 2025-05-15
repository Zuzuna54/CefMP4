import structlog
import redis.asyncio as redis
from .config import settings
import datetime
from .utils.retry import async_retry_transient
from .exceptions import RedisOperationError
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)
from .metrics import STREAMS_FAILED_TOTAL, VIDEO_FAILED_OPERATIONS_TOTAL

logger = structlog.get_logger(__name__)

# Global Redis connection pool
_redis_pool: redis.Redis | None = None

# Define common transient Redis exceptions
TRANSIENT_REDIS_EXCEPTIONS = (RedisConnectionError, RedisTimeoutError)


async def get_redis_connection() -> redis.Redis:
    global _redis_pool
    if _redis_pool is None:
        try:
            logger.info(
                f"Initializing Redis connection pool for URL: {settings.redis_url}"
            )
            _redis_pool = await redis.from_url(
                settings.redis_url, decode_responses=True
            )
            # Test connection
            await _redis_pool.ping()
            logger.info("Redis connection successful.")
        except (RedisConnectionError, RedisTimeoutError) as e:
            logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            _redis_pool = None
            raise RedisOperationError(operation="get_connection_ping", message=str(e))
        except Exception as e:
            logger.error(
                f"Unexpected error initializing Redis connection: {e}", exc_info=True
            )
            _redis_pool = None
            raise RedisOperationError(
                operation="get_connection_unexpected", message=str(e)
            )
    return _redis_pool


async def close_redis_connection():
    global _redis_pool
    if _redis_pool:
        logger.info("Closing Redis connection pool.")
        try:
            await _redis_pool.close()
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}", exc_info=True)
        finally:
            _redis_pool = None


# --- Stream State Functions ---
@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def init_stream_metadata(
    stream_id: str,
    file_path: str,
    s3_upload_id: str,
    s3_bucket: str,
    s3_key_prefix: str,
):
    try:
        r = await get_redis_connection()
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        stream_meta_key = f"stream:{stream_id}:meta"
        async with r.pipeline(transaction=True) as pipe:
            pipe.hset(
                stream_meta_key,
                mapping={
                    "original_path": file_path,
                    "s3_upload_id": s3_upload_id,
                    "s3_bucket": s3_bucket,
                    "s3_key_prefix": s3_key_prefix,
                    "status": "active",  # CHANGED from "processing" to "active"
                    "started_at_utc": now_iso,
                    "last_activity_at_utc": now_iso,
                    "total_bytes_expected": -1,  # Unknown initially
                    "total_bytes_sent": 0,  # Initialize explicitly
                    "next_part_to_upload": 1,  # Initialize explicitly
                },
            )
            # Add to active streams set
            pipe.sadd("streams:active", stream_id)
            await pipe.execute()
        logger.info(
            f"Initialized metadata for stream {stream_id} (file: {file_path}) in Redis."
        )
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="init_stream_metadata", message=str(e))
    except Exception as e:  # Catch other potential errors
        raise RedisOperationError(
            operation="init_stream_metadata_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def set_stream_status(
    stream_id: str, status: str, failure_reason: str | None = None
):
    try:
        r = await get_redis_connection()
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        meta_key = f"stream:{stream_id}:meta"
        update_mapping = {"status": status, "last_activity_at_utc": now_iso}
        if failure_reason:
            update_mapping["failure_reason"] = failure_reason

        await r.hset(meta_key, mapping=update_mapping)
        logger.info(
            f"Set status for stream {stream_id} to {status}. Reason: {failure_reason if failure_reason else 'N/A'}"
        )
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="set_stream_status", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="set_stream_status_unexpected", message=str(e)
        )


async def add_stream_to_active_set(stream_id: str):
    r = await get_redis_connection()
    await r.sadd("streams:active", stream_id)


async def remove_stream_from_active_set(stream_id: str):
    r = await get_redis_connection()
    await r.srem("streams:active", stream_id)


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def set_stream_next_part(stream_id: str, part_number: int):
    try:
        r = await get_redis_connection()
        await r.hset(f"stream:{stream_id}:meta", "next_part_to_upload", part_number)
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="set_stream_next_part", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="set_stream_next_part_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def get_stream_next_part(stream_id: str) -> int | None:
    try:
        r = await get_redis_connection()
        val = await r.hget(f"stream:{stream_id}:meta", "next_part_to_upload")
        return int(val) if val else None
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="get_stream_next_part", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="get_stream_next_part_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def incr_stream_bytes_sent(stream_id: str, chunk_size: int):
    try:
        r = await get_redis_connection()
        return await r.hincrby(
            f"stream:{stream_id}:meta", "total_bytes_sent", chunk_size
        )
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="incr_stream_bytes_sent", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="incr_stream_bytes_sent_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def get_stream_bytes_sent(stream_id: str) -> int | None:
    try:
        r = await get_redis_connection()
        val = await r.hget(f"stream:{stream_id}:meta", "total_bytes_sent")
        return int(val) if val else 0  # Default to 0 if not set, as no bytes sent yet
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="get_stream_bytes_sent", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="get_stream_bytes_sent_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def add_stream_part_info(
    stream_id: str, part_number: int, etag: str, size_bytes: int
):
    try:
        r = await get_redis_connection()
        parts_key = f"stream:{stream_id}:parts"
        # Store ETag, size, and upload timestamp for each part for later metadata generation
        uploaded_at_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        part_data = (
            f"{etag}:{size_bytes}:{uploaded_at_iso}"  # Format: "etag:size:timestamp"
        )
        await r.hset(parts_key, str(part_number), part_data)
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="add_stream_part_info", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="add_stream_part_info_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def get_stream_parts(stream_id: str) -> list[dict]:
    try:
        r = await get_redis_connection()
        parts_key = f"stream:{stream_id}:parts"
        raw_parts = await r.hgetall(parts_key)
        parsed_parts = []
        for pn_str, data_str in raw_parts.items():
            try:
                # Ensure data_str has at least two colons for splitting
                if data_str.count(":") >= 2:
                    etag, size_str, uploaded_at_iso = data_str.split(":", 2)
                    parsed_parts.append(
                        {
                            "PartNumber": int(pn_str),
                            "ETag": etag,
                            "Size": int(size_str),
                            "UploadedAtUTC": uploaded_at_iso,  # Add the timestamp
                        }
                    )
                else:  # Fallback for old format or corrupted data
                    etag, size_str = data_str.split(":", 1)
                    parsed_parts.append(
                        {
                            "PartNumber": int(pn_str),
                            "ETag": etag,
                            "Size": int(size_str),
                            "UploadedAtUTC": None,  # Indicate missing timestamp
                        }
                    )
                    logger.warning(
                        f"Part data for stream {stream_id}, part {pn_str} has unexpected format: '{data_str}'. Missing UploadedAtUTC."
                    )
            except ValueError as e:
                logger.warning(
                    f"Could not parse part data '{data_str}' for stream {stream_id}, part {pn_str}: {e}"
                )
        # Sort by PartNumber for S3 and consistency
        parsed_parts.sort(key=lambda p: p["PartNumber"])
        return parsed_parts
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="get_stream_parts", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="get_stream_parts_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def update_stream_last_activity(stream_id: str):
    try:
        r = await get_redis_connection()
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        await r.hset(f"stream:{stream_id}:meta", "last_activity_at_utc", now_iso)
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(
            operation="update_stream_last_activity", message=str(e)
        )
    except Exception as e:
        raise RedisOperationError(
            operation="update_stream_last_activity_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def get_stream_meta(stream_id: str) -> dict | None:
    try:
        r = await get_redis_connection()
        # meta_bytes is already a dict[str, str] because decode_responses=True
        meta = await r.hgetall(f"stream:{stream_id}:meta")
        return meta if meta else None  # No further decoding needed
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="get_stream_meta", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="get_stream_meta_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def get_active_stream_ids() -> list[str]:
    try:
        r = await get_redis_connection()
        stream_ids = await r.smembers("streams:active")
        return list(stream_ids)
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="get_active_stream_ids", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="get_active_stream_ids_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def get_pending_completion_stream_ids() -> list[str]:
    try:
        r = await get_redis_connection()
        return list(await r.smembers("streams:pending_completion"))
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(
            operation="get_pending_completion_stream_ids", message=str(e)
        )
    except Exception as e:
        raise RedisOperationError(
            operation="get_pending_completion_stream_ids_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def add_stream_to_failed_set(stream_id: str, reason: str = "unknown"):
    """Marks a stream as failed and moves it to the failed set."""
    try:
        r = await get_redis_connection()
        async with r.pipeline(transaction=True) as pipe:
            pipe.sadd("streams:failed", stream_id)
            pipe.srem("streams:active", stream_id)
            pipe.srem("streams:pending_completion", stream_id)
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            pipe.hset(
                f"stream:{stream_id}:meta",
                mapping={
                    "status": f"failed_{reason}",
                    "last_activity_at_utc": now_iso,
                    "failure_reason": reason,
                },
            )
            await pipe.execute()
        STREAMS_FAILED_TOTAL.inc()
        VIDEO_FAILED_OPERATIONS_TOTAL.labels(
            stream_id=stream_id, operation_type=f"redis_set_failed_{reason}"
        ).inc()
        logger.info(
            "Moved stream to streams:failed set", stream_id=stream_id, reason=reason
        )
    except (RedisConnectionError, RedisTimeoutError) as e:
        VIDEO_FAILED_OPERATIONS_TOTAL.labels(
            stream_id=stream_id,
            operation_type="redis_add_to_failed_set_connection_error",
        ).inc()
        logger.error(
            "Redis connection error while adding stream to failed set",
            stream_id=stream_id,
            reason=reason,
            exc_info=e,
        )
        raise RedisOperationError(operation="add_stream_to_failed_set", message=str(e))
    except Exception as e:
        VIDEO_FAILED_OPERATIONS_TOTAL.labels(
            stream_id=stream_id, operation_type="redis_add_to_failed_set_unexpected"
        ).inc()
        logger.error(
            "Unexpected error while adding stream to failed set",
            stream_id=stream_id,
            reason=reason,
            exc_info=e,
        )
        raise RedisOperationError(
            operation="add_stream_to_failed_set_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def move_stream_to_pending_completion(stream_id: str):
    try:
        r = await get_redis_connection()
        async with r.pipeline(transaction=True) as pipe:
            pipe.srem("streams:active", stream_id)
            pipe.sadd("streams:pending_completion", stream_id)
            # Update status in meta hash
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            pipe.hset(
                f"stream:{stream_id}:meta",
                mapping={
                    "status": "pending_completion",
                    "last_activity_at_utc": now_iso,  # Update last activity to now for this transition
                },
            )
            await pipe.execute()
        logger.info(f"Moved stream {stream_id} from active to pending_completion.")
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(
            operation="move_stream_to_pending_completion", message=str(e)
        )
    except Exception as e:
        raise RedisOperationError(
            operation="move_stream_to_pending_completion_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def remove_stream_from_pending_completion(stream_id: str):
    try:
        r = await get_redis_connection()
        await r.srem("streams:pending_completion", stream_id)
        logger.debug(f"Stream {stream_id} removed from streams:pending_completion set.")
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(
            operation="remove_stream_from_pending_completion", message=str(e)
        )
    except Exception as e:
        raise RedisOperationError(
            operation="remove_stream_from_pending_completion_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def add_stream_to_completed_set(
    stream_id: str,
):  # As per spec, used by finalize_stream
    try:
        r = await get_redis_connection()
        await r.sadd(
            "streams:completed", stream_id
        )  # Phase 5 uses "s3_completed" status, actual "completed" set might be for later
        logger.info(
            f"Added stream {stream_id} to streams:completed set. (Note: Phase 5 aims for 's3_completed' status)"
        )
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(
            operation="add_stream_to_completed_set", message=str(e)
        )
    except Exception as e:
        raise RedisOperationError(
            operation="add_stream_to_completed_set_unexpected", message=str(e)
        )


@async_retry_transient(transient_exceptions=TRANSIENT_REDIS_EXCEPTIONS)
async def remove_stream_keys(stream_id: str):
    """Removes all keys associated with a stream_id from Redis after successful processing or unrecoverable failure."""
    try:
        r = await get_redis_connection()
        keys_to_delete = []
        keys_to_delete.append(f"stream:{stream_id}:meta")
        keys_to_delete.append(f"stream:{stream_id}:parts")
        # Add any other keys that might be associated with the stream_id pattern

        # Remove from general sets as well
        # This should ideally happen when transitioning states, but as a final cleanup:
        async with r.pipeline(transaction=True) as pipe:
            pipe.srem("streams:active", stream_id)
            pipe.srem("streams:pending_completion", stream_id)
            # No need to remove from streams:completed as this is the success state, unless it means "to be archived"
            if keys_to_delete:
                pipe.delete(*keys_to_delete)
            await pipe.execute()
        logger.info(
            f"Cleaned up Redis keys for successfully completed stream {stream_id}."
        )
    except (RedisConnectionError, RedisTimeoutError) as e:
        raise RedisOperationError(operation="remove_stream_keys", message=str(e))
    except Exception as e:
        raise RedisOperationError(
            operation="remove_stream_keys_unexpected", message=str(e)
        )


# More Redis functions will be added in subsequent phases (e.g., for parts, next_part)
