from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    watch_dir: str = "local_watch_dir"
    chunk_size_bytes: int = 10 * 1024 * 1024  # 10 MB
    stream_timeout_seconds: int = (
        30  # Time to wait for more file writes before considering a stream IDLE or STALE
    )

    s3_endpoint_url: str | None = None  # For MinIO, e.g., "http://localhost:9000"
    s3_access_key_id: str = "MINIO_ACCESS_KEY"  # Default for local MinIO
    s3_secret_access_key: str = "MINIO_SECRET_KEY"  # Default for local MinIO
    s3_bucket_name: str = "video-streams"
    s3_region_name: str = "us-east-1"  # Default, can be anything for MinIO
    s3_default_content_type: str = "application/octet-stream"

    redis_url: str = "redis://localhost:6379/0"

    ffprobe_path: str | None = None  # Path to ffprobe executable if not in PATH

    log_level: str = "INFO"
    app_env: str = "development"  # "development" or "production"

    prom_port: int = 8000


settings = Settings()
