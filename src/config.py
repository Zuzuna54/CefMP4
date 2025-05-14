from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    watch_dir: str = "/input-videos"
    chunk_size_mb: int = Field(10, gt=0, description="Chunk size in MB")
    stream_timeout_seconds: int = Field(
        30, gt=0, description="Stream idle timeout in seconds"
    )

    s3_endpoint_url: str = "http://minio:9000"
    s3_access_key_id: str = "minioadmin"
    s3_secret_access_key: str = "minioadmin"
    s3_bucket_name: str = "video-streams"
    s3_region_name: str | None = None
    s3_default_content_type: str = "application/octet-stream"

    redis_url: str = "redis://redis:6379/0"

    log_level: str = "INFO"
    prom_port: int = 8000

    ffprobe_path: str | None = None  # Default to ffprobe in PATH

    @property
    def chunk_size_bytes(self) -> int:
        return self.chunk_size_mb * 1024 * 1024


settings = Settings()
