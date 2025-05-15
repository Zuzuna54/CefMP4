# Phase 4: End-to-End Testing

This phase focuses on full end-to-end tests that verify the complete application workflow in realistic environments. These tests ensure that the entire system functions correctly as a cohesive whole.

## Goal

Validate that the complete CefMP4 application functions correctly in real-world scenarios, including Docker-based deployments with all required services.

## Implementation Steps

### Step 1: Create End-to-End Test Environment

**Description:** Set up a complete test environment with Docker Compose.

1. Create `tests/e2e` directory for end-to-end test files and resources
2. Create `tests/e2e/docker-compose.test.yml` that extends the main docker-compose.yml:
   - Use the same application container setup
   - Add Redis and MinIO services
   - Add test-specific volume mounts
   - Configure test-specific environment variables
3. Create helper scripts for test environment setup and teardown:
   - `tests/e2e/setup_test_env.sh` - Build containers and set up environment
   - `tests/e2e/teardown_test_env.sh` - Clean up containers and test artifacts
4. Create Python test controller:
   - `tests/e2e/e2e_controller.py` - Manages test execution, file placement, result verification
   - Implement functions for:
     - Starting/stopping test environment with Docker Compose
     - Placing test files in watch directory
     - Monitoring S3 for uploaded files
     - Checking Redis for expected state
     - Monitoring application logs

**Expected outcome:** Complete end-to-end test environment ready for automated testing

### Step 2: Implement Happy Path Tests

**Description:** Test the complete flow with valid input files and verify correct output.

1. Create `tests/e2e/test_happy_paths.py`
2. Implement test for small file processing:
   - Place small MP4 file (<5MB) in watch directory
   - Wait for processing to complete
   - Verify file is uploaded to S3
   - Verify metadata JSON file is created
   - Verify Redis has expected state
   - Verify metrics show successful processing
3. Implement test for medium file processing:
   - Place medium MP4 file (~20MB) in watch directory
   - Verify multiple chunks are processed
   - Verify complete file is assembled in S3
   - Verify metrics show correct chunk count
4. Implement test for large file processing:
   - Place large MP4 file (>100MB) in watch directory
   - Verify system handles large files correctly
   - Monitor memory usage during processing
   - Verify all chunks are uploaded
5. Implement test for multiple concurrent files:
   - Place several MP4 files in watch directory
   - Verify all are processed correctly
   - Verify concurrency controls work properly
6. Implement test for files with different characteristics:
   - Files with different codecs
   - Files with different durations
   - Files with embedded metadata

**Expected outcome:** Verification that the application correctly processes valid input files from start to finish

### Step 3: Implement Fault Tolerance and Recovery Tests

**Description:** Test how the system handles failures and recovers from them.

1. Create `tests/e2e/test_fault_tolerance.py`
2. Implement test for application restart during processing:
   - Start processing large file
   - Kill application container mid-processing
   - Restart application container
   - Verify processing resumes from last checkpoint
   - Verify complete file is eventually uploaded
3. Implement test for Redis failure:
   - Start processing file
   - Kill Redis container mid-processing
   - Verify application handles Redis outage gracefully
   - Restore Redis container
   - Verify processing resumes correctly
4. Implement test for S3 failure:
   - Start processing file
   - Kill MinIO container mid-processing
   - Verify application handles S3 outage gracefully
   - Restore MinIO container
   - Verify processing resumes correctly
5. Implement test for file removal during processing:
   - Start processing file
   - Remove file from watch directory before completion
   - Verify application handles file removal gracefully
   - Verify appropriate error state in Redis
6. Implement test for disk space exhaustion:
   - Configure container with limited disk space
   - Process file that would exceed available space
   - Verify appropriate error handling
   - Verify no data corruption

**Expected outcome:** Verification that the application handles various failure scenarios gracefully and can recover from them

### Step 4: Implement Configuration and Limits Tests

**Description:** Test the application with different configuration settings and verify behavior at boundary conditions.

1. Create `tests/e2e/test_configuration.py`
2. Implement test for different chunk sizes:
   - Test with very small chunk size (1MB)
   - Test with medium chunk size (10MB)
   - Test with large chunk size (50MB)
   - Verify processing works correctly in all cases
   - Measure performance impacts
3. Implement test for different stream timeout values:
   - Test with short timeout (5 seconds)
   - Test with medium timeout (30 seconds)
   - Test with long timeout (5 minutes)
   - Verify idle detection works correctly in all cases
4. Implement test for different S3 endpoints:
   - Use different MinIO configurations
   - Use mock S3 service with different characteristics
   - Verify application adapts to different S3 implementations
5. Implement test for resource limits:
   - Test with memory limits on container
   - Test with CPU limits on container
   - Test with network bandwidth limits
   - Verify application degrades gracefully under constraints

**Expected outcome:** Verification that the application works correctly with different configuration settings and resource constraints

### Step 5: Implement Performance and Load Tests

**Description:** Test the application's performance characteristics under various loads.

1. Create `tests/e2e/test_performance.py`
2. Implement test for processing speed measurement:
   - Process files of different sizes
   - Measure time from detection to completion
   - Calculate throughput (MB/s)
   - Verify performance meets expected thresholds
3. Implement test for concurrent file processing:
   - Place 5, 10, 20 files simultaneously
   - Measure total processing time
   - Measure individual file completion times
   - Verify performance degradation is acceptable
4. Implement test for memory usage:
   - Monitor memory usage during processing
   - Verify memory usage scales appropriately with file size
   - Verify no memory leaks during extended operation
5. Implement test for CPU utilization:
   - Monitor CPU usage during processing
   - Verify CPU usage is reasonable
   - Identify potential optimization points
6. Implement test for sustained operation:
   - Run system continuously for extended period (1+ hours)
   - Periodically add new files
   - Verify stability over time
   - Monitor resource usage trends

**Expected outcome:** Performance metrics and confirmation that the application performs well under various loads

### Step 6: Implement Observability Tests

**Description:** Test the application's logs, metrics, and monitoring capabilities.

1. Create `tests/e2e/test_observability.py`
2. Implement test for log output validation:
   - Verify structured JSON logs are produced in production mode
   - Verify console logs are produced in development mode
   - Verify log levels are respected
   - Verify important events are logged
   - Verify error details are captured
3. Implement test for metrics validation:
   - Hit the `/metrics` endpoint
   - Verify all expected metrics are present
   - Verify metric values match actual processing activity
   - Test histogram metrics for appropriate buckets
   - Verify labels are correctly applied
4. Implement test for tracing span validation (if implemented):
   - Verify correct spans are created
   - Verify parent-child relationships
   - Verify important attributes are captured
5. Create test for metrics in error scenarios:
   - Trigger various error conditions
   - Verify error counters increment correctly
   - Verify failed operations are tracked

**Expected outcome:** Verification that the application provides comprehensive observability

### Step 7: Implement End-to-End Documentation and Reporting

**Description:** Create comprehensive documentation and reporting for end-to-end tests.

1. Create `tests/e2e/README.md`:
   - Document test setup requirements
   - Provide instructions for running tests manually
   - Explain test environment variables
   - List known limitations or environment-specific considerations
2. Implement automated test report generation:
   - Record test results in structured format
   - Generate HTML report with test coverage
   - Include performance metrics where applicable
   - Capture screenshots/logs of failures
3. Create CI integration scripts:
   - Script to run in CI environment
   - Configure resource requirements
   - Manage test artifacts and results

**Expected outcome:** Comprehensive documentation and reporting for end-to-end tests

## Success Criteria

1. All end-to-end tests pass consistently in Docker environment
2. Application correctly processes files from detection through completion
3. Application demonstrates resilience to failures and restarts
4. Performance meets expected thresholds
5. Observability features provide accurate insights into application behavior
6. Test documentation and reports are clear and comprehensive

## Dependencies

- Docker and Docker Compose
- Sample MP4 files of various sizes and types
- Sufficient system resources to run Docker containers
- Network access to download container images
- Test controller script dependencies (Python libraries for Docker control, S3 access, etc.)
- CI environment configuration (if running in CI)
