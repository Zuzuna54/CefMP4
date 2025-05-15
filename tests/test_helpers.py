"""Test helper functions and mocks."""

import pytest
from unittest.mock import patch


# Create a mock Settings class for testing
class MockSettings:
    watch_dir = "test_watch_dir"
    chunk_size_bytes = 10 * 1024 * 1024
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
    app_env = "test"
    prom_port = 8000


# Create a patch for settings that can be used in tests
mock_settings_patch = patch("src.config.settings", MockSettings())
