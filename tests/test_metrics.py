import pytest
from unittest.mock import patch, MagicMock
import prometheus_client


@pytest.fixture
def clean_registry():
    """Create a clean registry for tests."""
    registry = prometheus_client.CollectorRegistry()
    yield registry


def test_counter_creation_and_increment(clean_registry):
    """Test that counters can be created and incremented."""
    counter = prometheus_client.Counter(
        "test_counter", "Test counter", registry=clean_registry
    )

    # Increment the counter
    counter.inc()
    counter.inc(2)

    # Get the value using generate_latest which is a stable API
    output = prometheus_client.generate_latest(clean_registry).decode("utf-8")

    # Verify counter was incremented (total value should be 3.0)
    assert "test_counter_total 3.0" in output


def test_gauge_operations(clean_registry):
    """Test gauge set, increment, and decrement operations."""
    gauge = prometheus_client.Gauge("test_gauge", "Test gauge", registry=clean_registry)

    # Set, inc, dec operations
    gauge.set(5)
    output1 = prometheus_client.generate_latest(clean_registry).decode("utf-8")
    assert "test_gauge 5.0" in output1

    gauge.inc(2)
    output2 = prometheus_client.generate_latest(clean_registry).decode("utf-8")
    assert "test_gauge 7.0" in output2

    gauge.dec(3)
    output3 = prometheus_client.generate_latest(clean_registry).decode("utf-8")
    assert "test_gauge 4.0" in output3


def test_histogram_observe(clean_registry):
    """Test histogram observe functionality."""
    hist = prometheus_client.Histogram(
        "test_histogram",
        "Test histogram",
        buckets=[5, 10, 25, 50],
        registry=clean_registry,
    )

    # Observe some values
    hist.observe(15)
    hist.observe(45)

    # Generate output and verify
    output = prometheus_client.generate_latest(clean_registry).decode("utf-8")

    # Count should be 2
    assert "test_histogram_count 2.0" in output

    # Sum should be 60
    assert "test_histogram_sum 60.0" in output


def test_expected_metrics_structure():
    """Test that our expected metrics have the right naming structure.

    Note: This test doesn't check if metrics are registered, only checks
    the correct naming conventions for metric types.
    """
    # Create test metrics with expected names
    registry = prometheus_client.CollectorRegistry()

    # Sample of each metric type with the same naming convention as our application metrics
    counter = prometheus_client.Counter(
        "video_chunks_uploaded", "Test counter", registry=registry
    )
    gauge = prometheus_client.Gauge("active_streams", "Test gauge", registry=registry)
    histogram = prometheus_client.Histogram(
        "video_stream_duration_seconds", "Test histogram", registry=registry
    )

    # Generate the output containing all metrics
    output = prometheus_client.generate_latest(registry).decode("utf-8")

    # Check the metric name structure matches what we expect
    # Counter metric gets _total suffix
    assert "video_chunks_uploaded_total" in output
    # Gauge has no suffix
    assert "active_streams " in output
    # Histogram has _bucket, _count, and _sum suffixes
    assert "video_stream_duration_seconds_bucket" in output
    assert "video_stream_duration_seconds_count" in output
    assert "video_stream_duration_seconds_sum" in output


def test_metrics_server_logging():
    """Test that the metrics server startup gets logged."""
    # We'll skip testing the actual http server startup
    # and just verify logging occurs
    mock_logger = MagicMock()
    mock_settings = MagicMock(prom_port=9090)

    # Create a mock for prometheus_client that includes start_http_server
    mock_prom_client = MagicMock()
    mock_start_http = MagicMock()
    mock_prom_client.start_http_server = mock_start_http

    # Using patch to replace imports
    with (
        patch("src.metrics.logger", mock_logger),
        patch("src.metrics.settings", mock_settings),
        patch("src.metrics.start_http_server", mock_start_http),
    ):
        # Import after patching
        from src.metrics import start_metrics_server

        # Call the function
        start_metrics_server()

        # Verify logger was called
        mock_logger.info.assert_called_once()
        mock_start_http.assert_called_once_with(9090)


def test_metrics_server_error_handling():
    """Test that errors are handled when starting the metrics server."""
    # Use module patch to catch imported function
    with patch(
        "prometheus_client.start_http_server", side_effect=Exception("Test error")
    ) as mock_start:
        # Import the metrics module after patching
        from src.metrics import start_metrics_server

        with (
            patch("src.metrics.settings", MagicMock(prom_port=9090)),
            patch("src.metrics.logger", MagicMock()) as mock_logger,
        ):

            # Call should not raise exception
            start_metrics_server()

            # Verify error was logged
            mock_logger.error.assert_called_once()
