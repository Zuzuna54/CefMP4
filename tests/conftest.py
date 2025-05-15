import os
import tempfile
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# Create a mock Settings class to avoid loading real configuration
class MockSettings:
    watch_dir = "local_watch_dir"
    chunk_size_bytes = 10 * 1024 * 1024  # 10 MB
    stream_timeout_seconds = 30
    s3_endpoint_url = None
    s3_access_key_id = "TEST_ACCESS_KEY"
    s3_secret_access_key = "TEST_SECRET_KEY"
    s3_bucket_name = "test-bucket"
    s3_region_name = "us-east-1"
    s3_default_content_type = "application/octet-stream"
    redis_url = "redis://localhost:6379/0"
    ffprobe_path = None
    log_level = "INFO"
    app_env = "development"
    prom_port = 8000


# Apply the mock settings
@pytest.fixture(autouse=True)
def mock_settings():
    """Mock the settings import for all tests."""
    with patch("src.config.settings", MockSettings()):
        yield MockSettings()


@pytest.fixture
def sample_mp4_path():
    """Fixture providing a path to a sample MP4 file for testing."""
    return os.path.join(os.path.dirname(__file__), "test_data", "sample_valid.mp4")


@pytest.fixture
def corrupted_mp4_path():
    """Fixture providing a path to a corrupted MP4 file for testing."""
    return os.path.join(os.path.dirname(__file__), "test_data", "sample_corrupted.mp4")


@pytest.fixture
def non_mp4_file_path():
    """Fixture providing a path to a non-MP4 file with MP4 extension for testing."""
    return os.path.join(os.path.dirname(__file__), "test_data", "non_mp4_file.mp4")


@pytest.fixture
def temp_dir():
    """Fixture providing a temporary directory for testing file operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_subprocess():
    """Fixture providing a mocked subprocess for ffprobe testing."""
    mock = MagicMock()
    mock.run = MagicMock()
    return mock


@pytest.fixture
def mock_redis_client():
    """Fixture providing a mocked Redis client for testing."""
    mock = AsyncMock()
    mock.get_stream_meta = AsyncMock()
    mock.get_stream_parts = AsyncMock()
    mock.add_stream_to_completed_set = AsyncMock()
    mock.add_stream_to_failed_set = AsyncMock()
    mock.remove_stream_from_pending_completion = AsyncMock()
    return mock


@pytest.fixture
def mock_s3_client():
    """Fixture providing a mocked S3 client for testing."""
    mock = AsyncMock()
    mock.complete_s3_multipart_upload = AsyncMock()
    mock.abort_s3_multipart_upload = AsyncMock()
    mock.upload_json_to_s3 = AsyncMock()
    return mock
