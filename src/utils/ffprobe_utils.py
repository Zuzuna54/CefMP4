import asyncio
import json
import logging
from src.config import settings

logger = logging.getLogger(__name__)  # To be replaced by structlog


async def get_video_duration(file_path: str) -> float | None:
    """Runs ffprobe to get the video duration in seconds."""
    ffprobe_cmd = settings.ffprobe_path if settings.ffprobe_path else "ffprobe"
    args = [
        ffprobe_cmd,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        file_path,
    ]
    logger.debug(f"Running ffprobe for duration: {' '.join(args)}")
    try:
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0 and stdout:
            duration_str = stdout.decode().strip()
            logger.debug(f"ffprobe stdout for duration: {duration_str}")
            return float(duration_str)
        else:
            stderr_str = stderr.decode().strip()
            logger.error(
                f"ffprobe error for {file_path} (return code {process.returncode}): {stderr_str}"
            )
            return None
    except FileNotFoundError:
        logger.error(
            f"ffprobe command not found (path: '{ffprobe_cmd}'). Ensure ffmpeg is installed and ffprobe is in PATH or FFPROBE_PATH is set correctly."
        )
        return None
    except Exception as e:
        logger.error(f"Error running ffprobe for {file_path}: {e}", exc_info=True)
        return None
