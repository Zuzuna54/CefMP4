# Phase 2: Core Component Tests

This phase focuses on expanding test coverage for core application components that already have some basic tests but need more comprehensive coverage, particularly for edge cases, error handling, and advanced functionality.

## Goal

Complete and enhance test coverage for the main application components that handle file watching, stream processing, and data storage operations.

## Components to Test

1. `watcher.py` (expand existing tests)
2. `stream_processor.py` (focus on finalize_stream method and error handling)
3. `redis_client.py` (add tests for missing functions and edge cases)
4. `s3_client.py` (add tests for missing functions and error scenarios)
5. `main.py` (focus on signal handling and shutdown)

## Implementation Steps

### Step 1: Expand Watcher Tests

**Description:** Enhance the existing watcher tests to cover more scenarios and edge cases.

1. Update `tests/test_watcher.py` to include DELETE events
   - Similar to existing CREATE/WRITE tests
   - Create file, then delete it
   - Verify DELETE event is generated
2. Add test for file rename/move scenarios
   - Create a file, rename/move it within the watch directory
   - Verify appropriate events are generated (typically DELETE followed by CREATE)
3. Add test for non-MP4 files filtering
   - Create files with non-MP4 extensions
   - Verify no events are generated for these files
4. Add test for duplicate events handling
   - Rapidly create/modify a file to generate potential duplicate events
   - Verify the implementation correctly handles/deduplicates rapid changes
5. Add test for multiple files being watched simultaneously
   - Create multiple files in rapid succession
   - Verify events for all files are correctly generated

**Expected outcome:** Comprehensive testing of the watcher component with all file event types and edge cases covered.

### Step 2: Expand Stream Processor Tests for Finalization

**Description:** Add tests for the `finalize_stream` method of StreamProcessor which completes S3 uploads and generates metadata.

1. Update `tests/test_stream_processor.py` with tests for `finalize_stream`:
   - Test successful finalization flow:
     - Mock successful S3 completion
     - Mock successful metadata generation
     - Verify correct status updates in Redis
   - Test handling of empty parts list:
     - Mock empty parts from Redis
     - Verify abort is called instead of complete
     - Verify appropriate status updates
   - Test S3 completion failure:
     - Mock S3 complete_s3_multipart_upload failure
     - Verify appropriate status updates
     - Verify error handling
   - Test metadata generation failure:
     - Mock S3 complete success but metadata generation failure
     - Verify appropriate status updates
2. Add tests for CancelledError during finalization:
   - Inject cancellation during finalization
   - Verify clean handling without corrupting state
3. Add tests for lock mechanism:
   - Run multiple operations concurrently
   - Verify operations are properly serialized
4. Add tests for metrics integration:
   - Verify metrics are updated during processing and finalization
   - Check all relevant counters, gauges, histograms

**Expected outcome:** Complete testing of the StreamProcessor finalization logic with verification of S3 completion and metadata generation.

### Step 3: Complete Redis Client Tests

**Description:** Ensure full coverage of Redis client functions, particularly those related to stream management.

1. Update `tests/test_redis_client.py` to add tests for:
   - `remove_stream_from_pending_completion`
   - `add_stream_to_completed_set`
   - `add_stream_to_failed_set`
   - Any other stream state management functions
2. Add test for retrieving pending completion streams
   - Mock Redis smembers response for "streams:pending_completion"
   - Verify correct parsing and return
3. Add test for Redis connection failures:
   - Mock connection failures
   - Verify appropriate error handling
4. Test transient Redis errors with retry mechanism:
   - Mock temporary Redis unavailability
   - Verify retry behavior
   - Verify success after retries
5. Test Redis pipeline transactions:
   - Verify pipeline is used in appropriate multi-key operations
   - Test pipeline error handling

**Expected outcome:** Comprehensive test coverage for all Redis operations with verification of error handling and recovery mechanisms.

### Step 4: Complete S3 Client Tests

**Description:** Ensure full coverage of S3 client functions, particularly for multi-part uploads and JSON operations.

1. Update `tests/test_s3_client.py` to add additional tests for:
   - `upload_json_to_s3` with various payloads
   - Multiple part upload scenario (simulate multi-part workflow)
   - Test with different content types
2. Add tests for S3 connection failures:
   - Mock connection errors from boto
   - Verify appropriate error handling
3. Add tests for transient S3 errors with retry mechanism:
   - Mock temporary S3 unavailability
   - Verify retry behavior
   - Verify success after retries
4. Test concurrent S3 operations:
   - Simulate multiple concurrent uploads
   - Verify operations proceed correctly
5. Test handling of already completed/aborted uploads:
   - Mock S3 errors for operations on finalized uploads
   - Verify appropriate error handling

**Expected outcome:** Comprehensive test coverage for all S3 operations with verification of error handling and recovery mechanisms.

### Step 5: Expand Main Application Tests

**Description:** Focus on the main application flow, particularly signal handling and graceful shutdown.

1. Update `tests/test_main.py` to add tests for:
   - SIGINT/SIGTERM handling:
     - Mock signal delivery
     - Verify shutdown sequence is initiated
   - Graceful shutdown with active tasks:
     - Create mock active tasks
     - Trigger shutdown
     - Verify all resources are cleaned up properly
   - Redis connection failure at startup:
     - Mock Redis connection failure
     - Verify appropriate error and exit
   - S3 connection failure at startup:
     - Mock S3 connection failure
     - Verify appropriate error and exit
2. Test resume logic for interrupted streams:
   - Mock Redis to return active streams on startup
   - Verify processors are created for existing streams
   - Verify processing continues from last checkpoint
3. Test concurrency control with semaphore:
   - Mock multiple simultaneous file events
   - Verify semaphore limits concurrent processing
   - Verify all events are eventually processed

**Expected outcome:** Complete testing of the main application flow including startup, signal handling, resumption, and shutdown.

### Step 6: Integration Points in Core Components

**Description:** Test how core components interact when used together.

1. Create `tests/test_core_integration.py` for focused integration tests
2. Test watcher events flowing to StreamProcessor:
   - Mock file system events
   - Verify main.py correctly routes to StreamProcessor
   - Verify appropriate processor methods are called
3. Test StreamProcessor finalization triggering metadata generation:
   - Mock S3 completion
   - Verify metadata generator is called with correct parameters
   - Verify success/failure paths

**Expected outcome:** Verification that core components interact correctly at their boundaries.

## Success Criteria

1. All expanded test files provide at least 90% line coverage for their target modules
2. All core component behaviors are correctly tested including edge cases
3. Error paths and recovery mechanisms are thoroughly tested
4. Signal handling and graceful shutdown are confirmed working

## Dependencies

- Existing test fixtures and mocks
- pytest
- pytest-asyncio
- pytest-mock
- pytest-cov (for coverage reporting)
