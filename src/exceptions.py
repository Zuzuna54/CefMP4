class CefMp4ProcessorError(Exception):
    """Base exception for this application."""

    def __init__(self, message: str):
        super().__init__(f"CefMp4Processor error: {message}")


class StreamInitializationError(CefMp4ProcessorError):
    """Error during stream initialization (e.g., S3 multipart init, Redis init)."""

    def __init__(self, message: str):
        super().__init__(f"Stream initialization error: {message}")


class ChunkProcessingError(CefMp4ProcessorError):
    """Error during the chunk reading or S3 upload part phase."""

    def __init__(self, message: str):
        super().__init__(f"Chunk processing error: {message}")


class StreamFinalizationError(CefMp4ProcessorError):
    """Error during stream finalization (e.g., S3 complete, metadata generation/upload)."""

    def __init__(self, message: str):
        super().__init__(f"Stream finalization error: {message}")


class FFprobeError(CefMp4ProcessorError):
    """Error related to ffprobe execution or parsing."""

    def __init__(self, message: str):
        super().__init__(f"FFprobe error: {message}")


class S3OperationError(CefMp4ProcessorError):
    """General error during an S3 operation not covered by others."""

    def __init__(self, operation: str, message: str):
        self.operation = operation
        super().__init__(f"S3 operation '{operation}' failed: {message}")


class RedisOperationError(CefMp4ProcessorError):
    """General error during a Redis operation."""

    def __init__(self, operation: str, message: str):
        self.operation = operation
        super().__init__(f"Redis operation '{operation}' failed: {message}")
