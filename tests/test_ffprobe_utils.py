import asyncio
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from src.utils.ffprobe_utils import get_video_duration
from src.exceptions import FFprobeError


class MockProcess:
    def __init__(self, returncode, stdout=None, stderr=None):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    async def communicate(self):
        return self._stdout, self._stderr


@pytest.mark.asyncio
async def test_get_video_duration_success():
    """Test successful extraction of video duration."""
    mock_process = MockProcess(0, stdout=b"10.5\n")

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_process)):
        duration = await get_video_duration("/path/to/video.mp4")

    assert duration == 10.5


@pytest.mark.asyncio
async def test_get_video_duration_ffprobe_not_found():
    """Test handling when ffprobe command is not available."""
    with patch(
        "asyncio.create_subprocess_exec", AsyncMock(side_effect=FileNotFoundError())
    ):
        with pytest.raises(FFprobeError) as exc_info:
            await get_video_duration("/path/to/video.mp4")

    assert "ffprobe command not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_video_duration_invalid_output():
    """Test handling when ffprobe returns non-numeric output."""
    mock_process = MockProcess(0, stdout=b"invalid\n")

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_process)):
        with pytest.raises(FFprobeError) as exc_info:
            await get_video_duration("/path/to/video.mp4")

    assert "not a valid float" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_video_duration_error_return_code():
    """Test handling when ffprobe returns an error code."""
    mock_process = MockProcess(1, stderr=b"File not found\n")

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_process)):
        with pytest.raises(FFprobeError) as exc_info:
            await get_video_duration("/path/to/video.mp4")

    assert "return code 1" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_video_duration_cancelled():
    """Test handling when the operation is cancelled."""
    with patch(
        "asyncio.create_subprocess_exec",
        AsyncMock(side_effect=asyncio.CancelledError()),
    ):
        with pytest.raises(asyncio.CancelledError):
            await get_video_duration("/path/to/video.mp4")


@pytest.mark.asyncio
async def test_get_video_duration_unexpected_error():
    """Test handling of unexpected errors during ffprobe execution."""
    with patch(
        "asyncio.create_subprocess_exec",
        AsyncMock(side_effect=RuntimeError("Unexpected error")),
    ):
        with pytest.raises(FFprobeError) as exc_info:
            await get_video_duration("/path/to/video.mp4")

    assert "Unexpected error running ffprobe" in str(exc_info.value)
