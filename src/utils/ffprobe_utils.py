import asyncio
import structlog
from src.config import settings
from src.exceptions import FFprobeError

logger = structlog.get_logger(__name__)


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
    logger.debug(
        "Running ffprobe for duration", command=" ".join(args), file_path=file_path
    )
    try:
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0 and stdout:
            duration_str = stdout.decode().strip()
            logger.debug(
                "ffprobe stdout for duration", output=duration_str, file_path=file_path
            )
            try:
                return float(duration_str)
            except ValueError as ve:
                err_msg = f"ffprobe output is not a valid float: {duration_str}"
                logger.error(
                    err_msg,
                    file_path=file_path,
                    raw_output=duration_str,
                    error_details=str(ve),
                )
                raise FFprobeError(f"{err_msg}. Error: {ve}")
        else:
            stderr_str = stderr.decode().strip()
            err_msg = f"ffprobe error (return code {process.returncode})"
            logger.error(
                err_msg,
                file_path=file_path,
                return_code=process.returncode,
                stderr=stderr_str,
            )
            raise FFprobeError(f"{err_msg}: {stderr_str}")
    except FileNotFoundError:
        err_msg = f"ffprobe command not found. Ensure ffmpeg is installed or FFPROBE_PATH is set."
        logger.error(err_msg, ffprobe_command=ffprobe_cmd)
        raise FFprobeError(f"{err_msg} Command: '{ffprobe_cmd}'")
    except asyncio.CancelledError:
        logger.info("ffprobe execution cancelled", file_path=file_path)
        raise
    except Exception as e:
        logger.error(
            "Unexpected error running ffprobe", file_path=file_path, exc_info=e
        )
        raise FFprobeError(f"Unexpected error running ffprobe for {file_path}: {e}")
