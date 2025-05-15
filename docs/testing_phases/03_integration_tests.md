# Phase 3: Integration Tests

This phase focuses on implementing integration tests that verify how different components of the system work together. These tests bridge the gap between unit tests and full end-to-end tests by focusing on specific component interactions.

## Goal

Ensure that components interact correctly with each other and with external services (Redis, S3) when used together in realistic scenarios.

## Key Integration Points to Test

1. Watcher → Main → StreamProcessor pipeline
2. StreamProcessor → S3 Client → S3 Service
3. StreamProcessor → Redis Client → Redis Service
4. Metadata Generation → ffprobe → File System
5. Periodic Stale Stream Check → Stream Finalization

## Implementation Steps

### Step 1: Set up Integration Test Infrastructure

**Description:** Create the foundations needed for integration testing.

1. Create `tests/integration` directory for integration-focused tests
2. Create `tests/integration/conftest.py` with specialized fixtures:
   - Setup/teardown for running local Redis instance (if needed)
   - Setup/teardown for running MinIO (S3-compatible storage)
   - Integration test configuration loader
   - Helper functions for verifying state in Redis/S3
3. Create mini-framework for integration test flow control:
   - Test event emitter and collector
   - Integration test context manager
   - Timeout handling for async operations

**Expected outcome:** Infrastructure ready to support component integration tests

### Step 2: Implement Watcher to Processor Integration Tests

**Description:** Test the full flow from file system events to StreamProcessor operations.

1. Create `tests/integration/test_watcher_processor_integration.py`
2. Implement test for CREATE event flow:
   - Create a file in the watch directory
   - Verify StreamProcessor is created with correct parameters
   - Verify S3 initialization is triggered
   - Verify Redis initialization is triggered
3. Implement test for WRITE event flow:
   - Modify a file in the watch directory
   - Verify correct StreamProcessor.process_file_write is called
   - Verify chunk processing is triggered
4. Implement test for DELETE event flow:
   - Delete a file from the watch directory
   - Verify processor is removed from active_processors
5. Test error propagation through the pipeline:
   - Inject errors at different points (S3 failure, Redis failure)
   - Verify errors are handled properly and don't crash the pipeline
   - Verify appropriate error states are recorded

**Expected outcome:** Verification that file system events correctly trigger the appropriate processing functions

### Step 3: Implement S3 Integration Tests

**Description:** Test the integration between StreamProcessor and S3 services.

1. Create `tests/integration/test_s3_integration.py`
2. Use a running MinIO instance for realistic S3 testing
3. Implement test for full multipart upload flow:
   - Create a multipart upload
   - Upload multiple parts
   - Complete the upload
   - Verify the object exists in S3 with correct content
4. Implement test for upload abort flow:
   - Start a multipart upload
   - Upload some parts
   - Abort the upload
   - Verify no object remains in S3
   - Verify no orphaned parts remain
5. Implement test for concurrent uploads:
   - Start multiple uploads simultaneously
   - Verify all complete correctly
6. Test reconnection scenarios:
   - Start an upload
   - Simulate S3 service restart
   - Verify upload can continue
7. Test resuming interrupted uploads:
   - Start an upload and upload some parts
   - Store upload_id and ETag information
   - Restart the test client
   - Resume the upload using stored information
   - Complete successfully

**Expected outcome:** Verification that S3 multipart upload workflows function correctly end-to-end

### Step 4: Implement Redis Integration Tests

**Description:** Test the integration between the application and Redis for state management.

1. Create `tests/integration/test_redis_integration.py`
2. Use a running Redis instance for realistic testing
3. Implement test for stream state lifecycle:
   - Initialize stream metadata
   - Update stream status through various states
   - Add parts information
   - Track bytes sent
   - Move through sets (active → pending → completed)
   - Verify all state changes are reflected correctly
4. Test Redis reconnection scenarios:
   - Start operations
   - Simulate Redis restart
   - Verify operations can continue
5. Test concurrent Redis operations:
   - Simulate multiple streams updating Redis simultaneously
   - Verify no race conditions or data corruption
6. Test Redis data integrity:
   - Verify stream state is consistent (meta hash, parts hash, set membership)
   - Verify cleanup operations work correctly
7. Test Redis retry mechanism:
   - Simulate transient Redis errors
   - Verify retry mechanism works
   - Verify operations succeed after retries

**Expected outcome:** Verification that Redis state management is robust throughout the application lifecycle

### Step 5: Implement Metadata Generation Integration Tests

**Description:** Test the integration between metadata generation, ffprobe, and the file system.

1. Create `tests/integration/test_metadata_integration.py`
2. Use real MP4 files and actual ffprobe executable
3. Implement test for metadata generation flow:
   - Create Redis state (mocked or real)
   - Process a real MP4 file
   - Generate metadata using ffprobe
   - Verify metadata structure and content is correct
   - Verify duration extraction works
4. Test various MP4 file types:
   - Different durations
   - Different encodings
   - Different metadata within the MP4
5. Test error scenarios:
   - Corrupted MP4 files
   - Missing ffprobe executable
   - File disappears during processing

**Expected outcome:** Verification that metadata generation works correctly with real files and ffprobe

### Step 6: Implement Stale Stream Detection Tests

**Description:** Test the integration between periodic stale stream checking and stream finalization.

1. Create `tests/integration/test_stale_detection_integration.py`
2. Implement test for idle stream detection:
   - Create an active stream in Redis
   - Set last_activity_at timestamp in the past
   - Run the periodic_stale_stream_check function
   - Verify stream is moved to pending_completion
   - Verify finalization is triggered
3. Test stream finalization flow:
   - Create an active stream with parts recorded
   - Mark as idle
   - Verify S3 multipart upload is completed
   - Verify metadata is generated
   - Verify stream is moved to completed state
4. Test edge cases:
   - No parts recorded (should abort instead of complete)
   - Missing file during finalization
   - Missing upload_id

**Expected outcome:** Verification that stale stream detection and finalization work correctly together

### Step 7: Implement Resume Logic Integration Tests

**Description:** Test the integration between application startup and resuming interrupted operations.

1. Create `tests/integration/test_resume_integration.py`
2. Implement test for resuming active streams:
   - Set up Redis with active streams
   - Initialize application
   - Verify StreamProcessors are created for existing streams
   - Verify processing continues from last checkpoints
3. Test resuming streams in different states:
   - Active streams (continue processing)
   - Pending completion streams (trigger finalization)
   - Streams with missing files (should be marked failed)
4. Test recovery from various failure points:
   - After S3 multipart initiated but no parts uploaded
   - After some parts uploaded
   - After all parts uploaded but before completion

**Expected outcome:** Verification that resumption logic correctly recovers streams in various states

## Success Criteria

1. All integration tests pass consistently
2. Component interactions work correctly in realistic scenarios
3. Error handling across component boundaries is robust
4. External service interactions (Redis, S3) are reliable
5. State transitions across the entire application flow are correct

## Dependencies

- Running Redis instance (real or containerized)
- Running MinIO instance (for S3-compatible storage)
- Sample MP4 files of various types and sizes
- ffprobe executable
- pytest-asyncio
- Integration test helper framework
