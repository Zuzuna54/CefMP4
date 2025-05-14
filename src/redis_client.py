import logging
import redis.asyncio as redis
from .config import settings
import datetime

logger = logging.getLogger(__name__)  # To be replaced by structlog

# Global Redis connection pool
_redis_pool: redis.Redis | None = None


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
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            _redis_pool = None  # Ensure it's None if connection failed
            raise
    return _redis_pool


async def close_redis_connection():
    global _redis_pool
    if _redis_pool:
        logger.info("Closing Redis connection pool.")
        await _redis_pool.close()
        _redis_pool = None


# --- Stream State Functions ---
async def init_stream_metadata(
    stream_id: str,
    file_path: str,
    s3_upload_id: str,
    s3_bucket: str,
    s3_key_prefix: str,
):
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
            },
        )
        # Add to active streams set
        pipe.sadd("streams:active", stream_id)
        await pipe.execute()
    logger.info(
        f"Initialized metadata for stream {stream_id} (file: {file_path}) in Redis."
    )


async def set_stream_status(stream_id: str, status: str):
    r = await get_redis_connection()
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    await r.hset(
        f"stream:{stream_id}:meta",
        mapping={"status": status, "last_activity_at_utc": now_iso},
    )
    logger.info(f"Set status for stream {stream_id} to {status}")


async def add_stream_to_active_set(stream_id: str):
    r = await get_redis_connection()
    await r.sadd("streams:active", stream_id)


async def remove_stream_from_active_set(stream_id: str):
    r = await get_redis_connection()
    await r.srem("streams:active", stream_id)


async def set_stream_next_part(stream_id: str, part_number: int):
    r = await get_redis_connection()
    await r.hset(f"stream:{stream_id}:meta", "next_part_to_upload", part_number)


async def get_stream_next_part(stream_id: str) -> int | None:
    r = await get_redis_connection()
    val = await r.hget(f"stream:{stream_id}:meta", "next_part_to_upload")
    return int(val) if val else None


async def incr_stream_bytes_sent(stream_id: str, chunk_size: int):
    r = await get_redis_connection()
    new_bytes_sent = await r.hincrby(
        f"stream:{stream_id}:meta", "total_bytes_sent", chunk_size
    )
    # Ensure total_bytes_sent is initialized if it doesn't exist by setting it if new_bytes_sent is same as chunk_size and was 0 before
    # hincrby initializes to the increment value if the field doesn't exist, which is desired.
    return new_bytes_sent


async def get_stream_bytes_sent(stream_id: str) -> int | None:
    r = await get_redis_connection()
    val = await r.hget(f"stream:{stream_id}:meta", "total_bytes_sent")
    return int(val) if val else 0  # Default to 0 if not set, as no bytes sent yet


async def add_stream_part_info(
    stream_id: str, part_number: int, etag: str, size_bytes: int
):
    r = await get_redis_connection()
    parts_key = f"stream:{stream_id}:parts"
    # Store ETag, size, and upload timestamp for each part for later metadata generation
    uploaded_at_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    part_data = (
        f"{etag}:{size_bytes}:{uploaded_at_iso}"  # Format: "etag:size:timestamp"
    )
    await r.hset(parts_key, str(part_number), part_data)


async def get_stream_parts(stream_id: str) -> list[dict]:
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


async def update_stream_last_activity(stream_id: str):
    r = await get_redis_connection()
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    await r.hset(f"stream:{stream_id}:meta", "last_activity_at_utc", now_iso)


async def get_stream_meta(stream_id: str) -> dict | None:
    r = await get_redis_connection()
    # meta_bytes is already a dict[str, str] because decode_responses=True
    meta = await r.hgetall(f"stream:{stream_id}:meta")
    return meta if meta else None  # No further decoding needed


async def get_active_stream_ids() -> list[str]:
    r = await get_redis_connection()
    stream_ids = await r.smembers("streams:active")
    return list(stream_ids)


async def get_pending_completion_stream_ids() -> list[str]:
    r = await get_redis_connection()
    return list(await r.smembers("streams:pending_completion"))


async def add_stream_to_failed_set(stream_id: str, reason: str = "unknown"):
    """Marks a stream as failed and moves it to the failed set."""
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
    logger.info(f"Moved stream {stream_id} to streams:failed set. Reason: {reason}")


async def move_stream_to_pending_completion(stream_id: str):
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


async def remove_stream_from_pending_completion(stream_id: str):
    r = await get_redis_connection()
    await r.srem("streams:pending_completion", stream_id)
    logger.debug(f"Stream {stream_id} removed from streams:pending_completion set.")


async def add_stream_to_completed_set(
    stream_id: str,
):  # As per spec, used by finalize_stream
    r = await get_redis_connection()
    await r.sadd(
        "streams:completed", stream_id
    )  # Phase 5 uses "s3_completed" status, actual "completed" set might be for later
    logger.info(
        f"Added stream {stream_id} to streams:completed set. (Note: Phase 5 aims for 's3_completed' status)"
    )


async def remove_stream_keys(stream_id: str):
    """Removes all keys associated with a stream_id from Redis after successful processing or unrecoverable failure."""
    r = await get_redis_connection()
    keys_to_delete = []
    keys_to_delete.append(f"stream:{stream_id}:meta")
    keys_to_delete.append(f"stream:{stream_id}:parts")
    # Add any other keys that might be associated with the stream_id pattern

    # Remove from general sets as well
    # This should ideally happen when transitioning states, but as a final cleanup:
    await r.srem("streams:active", stream_id)
    await r.srem("streams:pending_completion", stream_id)
    # No need to remove from streams:completed as this is the success state, unless it means "to be archived"

    if keys_to_delete:
        deleted_count = await r.delete(*keys_to_delete)
        logger.info(
            f"Cleaned up {deleted_count} Redis keys for successfully completed stream {stream_id}."
        )
    else:
        logger.info(
            f"No specific stream keys found to delete for {stream_id}, sets handled."
        )


# More Redis functions will be added in subsequent phases (e.g., for parts, next_part)
