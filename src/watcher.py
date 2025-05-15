import asyncio
import structlog
from pathlib import Path
from typing import AsyncGenerator

from watchfiles import awatch, Change

from .events import StreamEvent, WatcherChangeType

logger = structlog.get_logger(__name__)

# Keep track of files and their last modification times and active status
# Key: file_path (str), Value: last_event_is_write (bool)
# This helps to only emit CREATE once, then subsequent changes are WRITEs until IDLE
# And to only emit IDLE if the last actual filesystem event was a write (not an add then idle)
_active_files_last_event_write: dict[str, bool] = {}


async def video_file_watcher(
    watch_dir: Path,
    stream_timeout_seconds: float,  # Accepted but primarily used by stale-scan task later
    stop_event: asyncio.Event,
) -> AsyncGenerator[StreamEvent, None]:
    logger.info(
        f"Starting watcher on {watch_dir} for .mp4 files. Timeout: {stream_timeout_seconds}s (for future IDLE detection)"
    )

    async for changes in awatch(
        watch_dir,
        stop_event=stop_event,
        watch_filter=lambda change, filename: filename.endswith(".mp4"),
    ):
        for change_type_raw, file_path_str in changes:
            file_path = Path(file_path_str)
            logger.debug(f"Raw change detected: {change_type_raw.name} for {file_path}")

            if change_type_raw == Change.added:
                if file_path_str not in _active_files_last_event_write:
                    logger.info(
                        "New file detected",
                        event_type="CREATE",
                        file_path=str(file_path),
                    )
                    _active_files_last_event_write[file_path_str] = (
                        False  # Initial event is add, not write
                    )
                    yield StreamEvent(
                        change_type=WatcherChangeType.CREATE, file_path=file_path
                    )
                # else: file re-appeared after deletion, or a duplicate add event, treat as modified if already known.
                # For simplicity, we only trigger CREATE once. Subsequent adds could be treated as modifies if needed.

            elif change_type_raw == Change.modified:
                if file_path_str not in _active_files_last_event_write:
                    # File appeared and was immediately modified (e.g. SCP)
                    logger.info(
                        "New file detected",
                        event_type="CREATE",
                        file_path=str(file_path),
                    )
                    _active_files_last_event_write[file_path_str] = True
                    yield StreamEvent(
                        change_type=WatcherChangeType.CREATE, file_path=file_path
                    )
                    # Then immediately yield a WRITE as well, as it was modified
                    yield StreamEvent(
                        change_type=WatcherChangeType.WRITE, file_path=file_path
                    )
                else:
                    logger.info(
                        "File modified", event_type="WRITE", file_path=str(file_path)
                    )
                    _active_files_last_event_write[file_path_str] = True
                    yield StreamEvent(
                        change_type=WatcherChangeType.WRITE, file_path=file_path
                    )

            elif change_type_raw == Change.deleted:
                logger.info(
                    "File deleted", event_type="DELETE", file_path=str(file_path)
                )
                if file_path_str in _active_files_last_event_write:
                    del _active_files_last_event_write[file_path_str]
                yield StreamEvent(
                    change_type=WatcherChangeType.DELETE, file_path=file_path
                )
                # Further processing for DELETE (e.g. cleanup) would be handled by the main loop

    logger.info("Video file watcher stopped.")
