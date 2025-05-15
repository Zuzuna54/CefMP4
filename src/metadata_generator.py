import datetime
import structlog
from pathlib import Path
from src.utils.ffprobe_utils import get_video_duration
from src.redis_client import (
    get_stream_meta,
    get_stream_parts,
)  # Assuming get_stream_parts returns part details

logger = structlog.get_logger(__name__)


async def generate_metadata_json(
    stream_id: str, original_file_path: Path
) -> dict | None:
    logger.info(
        f"[{stream_id}] Generating metadata JSON for {original_file_path.name}."
    )

    stream_meta_redis = await get_stream_meta(stream_id)
    if not stream_meta_redis:
        logger.error(
            f"[{stream_id}] Could not retrieve stream metadata from Redis to generate JSON."
        )
        return None

    stream_parts_info_redis = await get_stream_parts(
        stream_id
    )  # This must return richer part info

    duration_seconds = await get_video_duration(str(original_file_path))
    if duration_seconds is None:
        logger.warning(
            f"[{stream_id}] Could not determine video duration for {original_file_path.name}. Duration will be null in metadata."
        )

    total_size_bytes = sum(part.get("Size", 0) for part in stream_parts_info_redis)

    metadata = {
        "stream_id": stream_id,
        "original_file_path": str(original_file_path),
        "s3_bucket": stream_meta_redis.get("s3_bucket"),
        "s3_key_prefix": stream_meta_redis.get("s3_key_prefix"),
        "total_size_bytes": total_size_bytes,
        "duration_seconds": duration_seconds,
        "processed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "stream_started_at_utc": stream_meta_redis.get("started_at_utc"),
        "stream_completed_at_utc": stream_meta_redis.get("last_activity_at_utc"),
        "chunks": [
            {
                "part_number": part.get("PartNumber"),
                "size_bytes": part.get("Size"),
                "etag": part.get("ETag"),
                "uploaded_at_utc": part.get("UploadedAtUTC"),
            }
            for part in sorted(
                stream_parts_info_redis, key=lambda x: x.get("PartNumber", 0)
            )
        ],
    }
    logger.info(f"[{stream_id}] Metadata JSON generated for {original_file_path.name}.")
    return metadata
