import pytest
from src.exceptions import (
    CefMp4ProcessorError,
    StreamInitializationError,
    ChunkProcessingError,
    StreamFinalizationError,
    FFprobeError,
    S3OperationError,
    RedisOperationError,
)


def test_cefmp4processor_error_base():
    """Test base exception class initialization and message formatting."""
    error_msg = "Something went wrong"
    error = CefMp4ProcessorError(error_msg)

    assert str(error) == f"CefMp4Processor error: {error_msg}"
    assert isinstance(error, Exception)


def test_stream_initialization_error():
    """Test StreamInitializationError initialization and inheritance."""
    error_msg = "Failed to initialize multipart upload"
    error = StreamInitializationError(error_msg)

    assert (
        str(error) == f"CefMp4Processor error: Stream initialization error: {error_msg}"
    )
    assert isinstance(error, CefMp4ProcessorError)
    assert isinstance(error, Exception)


def test_chunk_processing_error():
    """Test ChunkProcessingError initialization and inheritance."""
    error_msg = "Failed to read chunk"
    error = ChunkProcessingError(error_msg)

    assert str(error) == f"CefMp4Processor error: Chunk processing error: {error_msg}"
    assert isinstance(error, CefMp4ProcessorError)
    assert isinstance(error, Exception)


def test_stream_finalization_error():
    """Test StreamFinalizationError initialization and inheritance."""
    error_msg = "Failed to complete multipart upload"
    error = StreamFinalizationError(error_msg)

    assert (
        str(error) == f"CefMp4Processor error: Stream finalization error: {error_msg}"
    )
    assert isinstance(error, CefMp4ProcessorError)
    assert isinstance(error, Exception)


def test_ffprobe_error():
    """Test FFprobeError initialization and inheritance."""
    error_msg = "ffprobe executable not found"
    error = FFprobeError(error_msg)

    assert str(error) == f"CefMp4Processor error: FFprobe error: {error_msg}"
    assert isinstance(error, CefMp4ProcessorError)
    assert isinstance(error, Exception)


def test_s3_operation_error():
    """Test S3OperationError initialization, inheritance and attributes."""
    operation = "GetObject"
    error_msg = "Access denied"
    error = S3OperationError(operation, error_msg)

    assert (
        str(error)
        == f"CefMp4Processor error: S3 operation '{operation}' failed: {error_msg}"
    )
    assert error.operation == operation
    assert isinstance(error, CefMp4ProcessorError)
    assert isinstance(error, Exception)


def test_redis_operation_error():
    """Test RedisOperationError initialization, inheritance and attributes."""
    operation = "HGET"
    error_msg = "Connection refused"
    error = RedisOperationError(operation, error_msg)

    assert (
        str(error)
        == f"CefMp4Processor error: Redis operation '{operation}' failed: {error_msg}"
    )
    assert error.operation == operation
    assert isinstance(error, CefMp4ProcessorError)
    assert isinstance(error, Exception)


def test_exception_hierarchy():
    """Test the entire exception hierarchy relationships."""
    # All custom exceptions should be instances of CefMp4ProcessorError
    assert issubclass(StreamInitializationError, CefMp4ProcessorError)
    assert issubclass(ChunkProcessingError, CefMp4ProcessorError)
    assert issubclass(StreamFinalizationError, CefMp4ProcessorError)
    assert issubclass(FFprobeError, CefMp4ProcessorError)
    assert issubclass(S3OperationError, CefMp4ProcessorError)
    assert issubclass(RedisOperationError, CefMp4ProcessorError)

    # All should ultimately derive from base Exception
    assert issubclass(CefMp4ProcessorError, Exception)

    # Verify they're not accidentally subclassing each other
    assert not issubclass(StreamInitializationError, ChunkProcessingError)
    assert not issubclass(FFprobeError, S3OperationError)
    assert not issubclass(RedisOperationError, StreamFinalizationError)
