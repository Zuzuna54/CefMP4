# src/metrics.py
import structlog  # Use structlog if already configured
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from src.config import settings

logger = structlog.get_logger(__name__)

# --- Define Metrics ---
VIDEO_CHUNKS_UPLOADED_TOTAL = Counter(
    "video_chunks_uploaded_total",
    "Total number of video chunks successfully uploaded.",
    ["stream_id"],  # Add stream_id label
)

VIDEO_BYTES_UPLOADED_TOTAL = Counter(
    "video_bytes_uploaded_total",
    "Total number of bytes successfully uploaded for video streams.",
    ["stream_id"],
)

VIDEO_STREAM_DURATION_SECONDS = Histogram(
    "video_stream_duration_seconds",
    "Duration of processed video streams in seconds. Obtained from ffprobe during metadata generation.",
    ["stream_id"],
    buckets=(5, 10, 30, 60, 120, 300, 600, 1800, 3600),  # Example buckets (seconds)
)

VIDEO_PROCESSING_TIME_SECONDS = Histogram(
    "video_processing_time_seconds",
    "Time taken to process a video stream from initial detection to successful metadata upload, in seconds.",
    ["stream_id"],
    buckets=(10, 30, 60, 120, 300, 600, 1800, 3600, 7200),
)

VIDEO_FAILED_OPERATIONS_TOTAL = Counter(
    "video_failed_operations_total",
    "Total number of failed operations related to video processing.",
    [
        "stream_id",
        "operation_type",
    ],  # e.g., s3_upload, redis_update, ffprobe, metadata_gen
)

ACTIVE_STREAMS_GAUGE = Gauge(
    "active_streams_gauge", "Current number of actively processing video streams."
)

STREAMS_COMPLETED_TOTAL = Counter(
    "streams_completed_total",
    "Total number of streams successfully processed and completed.",
)

STREAMS_FAILED_TOTAL = Counter(
    "streams_failed_total", "Total number of streams that ended in a failed state."
)
# --- End Define Metrics ---


def start_metrics_server():
    """Starts the Prometheus metrics HTTP server."""
    try:
        port = settings.prom_port
        start_http_server(port)
        logger.info("Prometheus metrics server started", port=port)
    except Exception as e:
        logger.error("Could not start Prometheus metrics server", exc_info=e)
