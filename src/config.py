from pydantic_settings import BaseSettings, SettingsConfigDict
import os
import sys

# Check if running in test mode
IN_TEST_MODE = "pytest" in sys.modules


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        # Skip validation on error in test mode
        validate_default=not IN_TEST_MODE,
    )

    watch_dir: str = "local_watch_dir"
    chunk_size_bytes: int = 10 * 1024 * 1024  # 10 MB
    stream_timeout_seconds: int = (
        30  # Time to wait for more file writes before considering a stream IDLE or STALE
    )

    s3_endpoint_url: str | None = "http://minio:9000"  # For MinIO in Docker
    s3_access_key_id: str = "MINIO_ACCESS_KEY"  # Default for local MinIO
    s3_secret_access_key: str = "MINIO_SECRET_KEY"  # Default for local MinIO
    s3_bucket_name: str = "video-streams"
    s3_region_name: str = "us-east-1"  # Default, can be anything for MinIO
    s3_default_content_type: str = "application/octet-stream"

    redis_url: str = "redis://redis:6379/0"

    ffprobe_path: str | None = None  # Path to ffprobe executable if not in PATH

    log_level: str = "INFO"
    app_env: str = "development"  # "development" or "production"

    prom_port: int = 8000


# In test mode, use default values without validation
if IN_TEST_MODE:
    settings = Settings(
        _ignore_env=True,
        watch_dir="test_watch_dir",
        chunk_size_bytes=10 * 1024 * 1024,
        stream_timeout_seconds=30,
        s3_access_key_id="TEST_ACCESS_KEY",
        s3_secret_access_key="TEST_SECRET_KEY",
        s3_bucket_name="test-bucket",
        app_env="test",
    )
else:
    # Normal settings loading for non-test environments
    settings = Settings()
