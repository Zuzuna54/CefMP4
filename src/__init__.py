# This file makes 'src' a Python package

from .config import settings
from .events import StreamEvent, WatcherChangeType
from .redis_client import (
    init_stream_metadata,
    close_redis_connection,
    get_redis_connection,
)
from .s3_client import create_s3_multipart_upload, close_s3_resources
from .stream_processor import StreamProcessor

__all__ = [
    "settings",
    "StreamEvent",
    "WatcherChangeType",
    "init_stream_metadata",
    "close_redis_connection",
    "get_redis_connection",
    "create_s3_multipart_upload",
    "close_s3_resources",
    "StreamProcessor",
]
