# Phase 1: Foundational Unit Tests

This first phase focuses on implementing missing unit tests for foundational components and utilities. These tests provide the basic coverage needed for core functionality before moving on to more complex scenarios.

## Goal

Establish solid unit test coverage for utility components and previously untested modules that other components depend on.

## Components to Test

1. `utils/ffprobe_utils.py`
2. `utils/retry.py`
3. `metadata_generator.py`
4. `metrics.py`
5. `logging_config.py`
6. `exceptions.py`

## Implementation Steps

### Step 1: Set up Test Data and Fixtures

**Description:** Create the necessary test files, fixtures, and mocking patterns needed for utility tests.

1. Create `tests/test_data/` directory to store sample MP4 files for testing
2. Create sample valid MP4 file (small, ~1MB) in test_data
3. Create sample corrupted MP4 file in test_data
4. Create non-MP4 file with MP4 extension in test_data
5. Define common test fixtures in `tests/conftest.py` for shared test resources

**Expected outcome:** Directory structure and test data ready for utility tests

### Step 2: Implement Tests for ffprobe_utils.py

**Description:** Test the video metadata extraction functionality.

1. Create `tests/test_ffprobe_utils.py` file
2. Implement test for successful duration extraction using a valid MP4 file
   - Mock subprocess execution to return a valid duration
   - Verify the parsed duration is correct
3. Implement test for handling missing ffprobe command
   - Mock subprocess to raise FileNotFoundError
   - Verify FFprobeError is raised with appropriate message
4. Implement test for handling invalid file paths
   - Provide a non-existent file path
   - Verify FFprobeError is raised
5. Implement test for handling non-video or corrupted files
   - Mock subprocess to return error code
   - Verify FFprobeError is raised
6. Implement test for cancellation during execution
   - Inject asyncio.CancelledError during execution
   - Verify cancellation is propagated
7. Add log capture to verify correct log messages are emitted

**Expected outcome:** Complete test coverage for the ffprobe utility including happy path and all error scenarios

### Step 3: Implement Tests for retry.py

**Description:** Test the retry decorator functionality with different scenarios.

1. Create `tests/test_retry.py` file
2. Create mock async function that fails a specified number of times before succeeding
3. Implement test for successful retry after transient failures
   - Verify function completes after retries
   - Verify retry count matches expected
4. Implement test for retry exhaustion
   - Configure function to fail more times than max retries
   - Verify final exception is raised
5. Test backoff behavior
   - Mock time.sleep or use time tracking to verify delays increase
   - Verify exponential backoff pattern
6. Test custom exception filtering
   - Create function that raises different exception types
   - Verify only specified exceptions trigger retry
7. Test cancellation during retry cycles
   - Inject asyncio.CancelledError during retry
   - Verify cancellation is propagated

**Expected outcome:** Complete test coverage for retry decorator with verification of retry logic, backoff patterns, and exception handling

### Step 4: Implement Tests for metadata_generator.py

**Description:** Test the JSON metadata generation functionality.

1. Create `tests/test_metadata_generator.py` file
2. Mock Redis responses (get_stream_meta, get_stream_parts)
3. Mock ffprobe responses (get_video_duration)
4. Implement test for successful metadata generation
   - Provide valid Redis data and duration
   - Verify JSON structure is correct
   - Verify all fields are populated correctly
5. Implement test for handling missing stream data
   - Mock get_stream_meta to return None
   - Verify function returns None
6. Implement test for handling missing parts data
   - Mock get_stream_parts to return empty list
   - Verify total_size_bytes is 0
7. Implement test for ffprobe failure
   - Mock get_video_duration to return None
   - Verify duration_seconds is None in output
8. Test with various input combinations
   - Different part counts
   - Different timestamps
   - Different file path structures

**Expected outcome:** Complete test coverage for metadata generation with verification of JSON structure and error handling

### Step 5: Implement Tests for metrics.py

**Description:** Test the Prometheus metrics registration and usage.

1. Create `tests/test_metrics.py` file
2. Implement test for metric initialization
   - Mock prometheus_client.start_http_server
   - Verify metrics are registered correctly
3. Test counter metrics
   - Verify VIDEO_CHUNKS_UPLOADED_TOTAL, STREAMS_COMPLETED_TOTAL, etc.
   - Test increment operations with various labels
4. Test gauge metrics
   - Verify ACTIVE_STREAMS_GAUGE
   - Test set, increment, decrement operations
5. Test histogram metrics
   - Verify VIDEO_STREAM_DURATION_SECONDS, VIDEO_PROCESSING_TIME_SECONDS
   - Test observe operations with different values
6. Test metrics server startup
   - Verify start_http_server is called with correct port
   - Test error handling when port is in use

**Expected outcome:** Complete test coverage for metrics registration, updating, and server initialization

### Step 6: Implement Tests for logging_config.py

**Description:** Test the structured logging configuration.

1. Create `tests/test_logging_config.py` file
2. Test structured log formatting
   - Mock settings configuration
   - Call setup_logging()
   - Verify logger configuration (processors, renderer)
3. Test different environment configurations
   - Test development mode (console renderer)
   - Test production mode (JSON renderer)
4. Test log level configuration
   - Set different log levels
   - Verify logger is configured with correct level
5. Test noisy module silencing
   - Verify log levels for watchfiles, botocore, etc. are set appropriately

**Expected outcome:** Verification that logging is properly configured for both development and production

### Step 7: Implement Tests for exceptions.py

**Description:** Test the custom exception hierarchy.

1. Create `tests/test_exceptions.py` file
2. Test exception inheritance
   - Verify all exceptions inherit from CefMp4ProcessorError
   - Verify specialized exceptions inherit correctly
3. Test exception attributes
   - Verify S3OperationError contains operation name
   - Verify RedisOperationError contains operation name
4. Test exception messages
   - Verify formatted messages are constructed correctly
   - Test with various inputs

**Expected outcome:** Verification that exception hierarchy is correct and exception instances carry appropriate context

## Success Criteria

1. All new test files are created with appropriate test cases
2. Tests achieve at least 90% line coverage for the targeted files
3. All tests pass consistently
4. Test data files are properly organized in the test_data directory

## Dependencies

- pytest
- pytest-asyncio
- pytest-mock
- pytest-cov (for coverage reporting)
- Small sample MP4 file(s) for ffprobe tests
