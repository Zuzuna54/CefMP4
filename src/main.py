import asyncio
import logging
from pathlib import Path
import uuid
import signal
import datetime

from .config import settings
from .watcher import video_file_watcher
from .events import StreamEvent, WatcherChangeType
from .redis_client import (
    init_stream_metadata,
    close_redis_connection,
    get_redis_connection,
    get_active_stream_ids,
    get_stream_meta,
    move_stream_to_pending_completion,
    remove_stream_from_active_set,
    remove_stream_from_pending_completion,
    set_stream_status,
    update_stream_last_activity,
)
from .s3_client import create_s3_multipart_upload, close_s3_resources
from .stream_processor import StreamProcessor

# Basic logging setup for now, will be replaced by structlog in a later phase
logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Dictionary to keep track of active stream processors
# Key: file_path (Path object), Value: StreamProcessor instance
active_processors: dict[Path, StreamProcessor] = {}


async def manage_new_stream_creation(event: StreamEvent):
    """Handles the creation and registration of a new stream processor."""
    file_path = event.file_path
    # Check if a processor for this file_path already exists.
    # This simple check might need refinement if files can be rapidly replaced with the same name.
    if file_path in active_processors:
        # This could happen if CREATE events are emitted rapidly or if a previous processor wasn't cleaned up.
        # For now, log and potentially re-initialize or ignore based on state.
        # A more robust solution might involve checking the existing processor's stream_id against a new one
        # or verifying its state in Redis if it's expected to be a replacement.
        logger.warning(
            f"[{file_path.name}] Received CREATE for an already tracked file path. Current processor stream ID: {active_processors[file_path].stream_id}. Re-evaluating or ignoring."
        )
        # For this phase, we'll overwrite if it exists, assuming the watcher detected a truly new file instance.
        # A more sophisticated check would involve checking if the existing processor is still valid or if the underlying file has changed significantly (e.g. inode).
        # If overwriting, ensure old processor is cleaned up if it had active tasks.
        # For now, to keep Phase 4 focused, we'll proceed to create a new one, which might orphan the old one if not handled carefully in shutdown/cleanup.
        # A better approach for Phase 4 might be to simply return if file_path in active_processors, and rely on DELETE/re-CREATE for replacements.
        # Let's stick to the plan's intent: "if file_path in active_processors: ... return" (simplified for now)
        # The plan stated: "For this phase, we keep it simple: one processor per file_path."
        # However, the Phase 4 main.py had "if file_path in active_processors: ... return" which is safer.
        # Let's stick with the safer return for now.``
        # To allow re-processing if a file is deleted and re-added, we should remove it on DELETE.
        # if file_path in active_processors:
        #    logger.warning(f"Processor for {file_path} already exists. Ignoring new CREATE event.")
        #    return
        # The above was the original thought, but if a file is deleted then re-added, we DO want a new processor.
        # The key is that active_processors should be cleaned up on DELETE or on processor completion/failure.
        pass  # Allow creation, will overwrite if key exists. Cleanup of old one is an advanced topic.

    stream_id = str(uuid.uuid4())
    logger.info(
        f"[{file_path.name}] New stream detected (CREATE). Stream ID: {stream_id}"
    )

    s3_object_key_prefix = f"streams/{stream_id}/{file_path.name}"

    try:
        s3_upload_id = await create_s3_multipart_upload(
            bucket_name=settings.s3_bucket_name, object_key=s3_object_key_prefix
        )
        if not s3_upload_id:
            logger.error(
                f"[{stream_id}] Failed to initiate S3 multipart upload for {file_path.name}. Aborting."
            )
            return

        await init_stream_metadata(
            stream_id=stream_id,
            file_path=str(file_path),
            s3_upload_id=s3_upload_id,
            s3_bucket=settings.s3_bucket_name,
            s3_key_prefix=s3_object_key_prefix,
        )

        processor = StreamProcessor(
            stream_id=stream_id,
            file_path=file_path,
            s3_upload_id=s3_upload_id,
            s3_bucket=settings.s3_bucket_name,
            s3_key_prefix=s3_object_key_prefix,
        )
        await processor._initialize_from_checkpoint()  # For consistency, though likely no checkpoint for new stream

        active_processors[file_path] = processor
        logger.info(
            f"[{stream_id}] StreamProcessor created and registered for {file_path.name}."
        )

        # Initial data might exist, so trigger a process_file_write immediately after creation.
        asyncio.create_task(
            processor.process_file_write(), name=f"initial_write_{stream_id[:8]}"
        )

    except Exception as e:
        logger.error(
            f"[{stream_id}] Error during new stream setup for {file_path.name}: {e}",
            exc_info=True,
        )
        # Cleanup for S3/Redis for failed init is for Phase 8.


async def handle_stream_event(event: StreamEvent):
    logger.debug(
        f"Received event: Type={event.change_type.name}, Path={event.file_path}"
    )

    if event.change_type == WatcherChangeType.CREATE:
        asyncio.create_task(
            manage_new_stream_creation(event),
            name=f"manage_create_{event.file_path.name}",
        )

    elif event.change_type == WatcherChangeType.WRITE:
        processor = active_processors.get(event.file_path)
        if processor:
            asyncio.create_task(
                processor.process_file_write(),
                name=f"process_write_{processor.stream_id[:8]}",
            )
        else:
            logger.warning(
                f"[{event.file_path.name}] WRITE event for untracked file. Was CREATE missed or file appeared suddenly?"
            )
            # As per Phase 4 plan, watcher should emit CREATE first if file was new then modified.
            # If this still happens, could re-evaluate or trigger CREATE flow. For now, just log.

    elif event.change_type == WatcherChangeType.DELETE:
        logger.info(f"[{event.file_path.name}] DELETE event received.")
        processor = active_processors.pop(event.file_path, None)
        if processor:
            logger.info(
                f"[{processor.stream_id}] Removed processor for deleted file {event.file_path.name}."
            )
            # TODO: Add cancellation of ongoing tasks for this processor if any (Phase 8)
            # processor.cancel_processing() # Hypothetical method
        else:
            logger.warning(f"[{event.file_path.name}] DELETE event for untracked file.")

    # IDLE events handled by stale stream checker (Phase 5)


async def run_finalization_and_cleanup(processor: StreamProcessor, processor_key: Path):
    """Helper to run finalization and then remove processor from active_processors."""
    try:
        logger.info(
            f"[{processor.stream_id}] Attempting to finalize stream for {processor.file_path} via helper."
        )
        await processor.finalize_stream()
    except Exception as e_finalize:
        logger.error(
            f"[{processor.stream_id}] Error during finalization task for {processor.file_path}: {e_finalize}",
            exc_info=True,
        )
        # Stream status should already be set to a failed state by finalize_stream itself
    finally:
        # Remove the processor from active tracking after finalization attempt
        # Check if the processor at that key is still the same one we intended to finalize
        if (
            processor_key in active_processors
            and active_processors[processor_key].stream_id == processor.stream_id
        ):
            del active_processors[processor_key]
            logger.info(
                f"[{processor.stream_id}] Processor for {processor.file_path} removed from active tracking after finalization attempt."
            )
        else:
            # This could happen if the processor was already removed, or replaced (e.g. file deleted and re-added quickly)
            logger.warning(
                f"[{processor.stream_id}] Processor for {processor.file_path} (key: {processor_key}) not found or stream_id mismatch during cleanup. Current in dict for key: {active_processors.get(processor_key)}"
            )


async def periodic_stale_stream_check(stop_event: asyncio.Event):
    """Periodically checks for streams that have become idle."""
    global active_processors  # Allow modification of the global active_processors dict
    while not stop_event.is_set():
        try:
            logger.debug("Running periodic stale stream check...")
            active_stream_ids_in_redis = await get_active_stream_ids()
            now = datetime.datetime.now(datetime.timezone.utc)

            for stream_id in active_stream_ids_in_redis:
                meta = await get_stream_meta(stream_id)
                if (
                    meta
                    and meta.get("last_activity_at_utc")
                    and meta.get("status") == "active"
                ):  # Process only if status is active
                    try:
                        last_activity_str = meta["last_activity_at_utc"]
                        last_activity_dt = datetime.datetime.fromisoformat(
                            last_activity_str
                        )

                        if (
                            last_activity_dt.tzinfo is None
                        ):  # Ensure offset-aware for comparison
                            last_activity_dt = last_activity_dt.replace(
                                tzinfo=datetime.timezone.utc
                            )

                        if (
                            now - last_activity_dt
                        ).total_seconds() > settings.stream_timeout_seconds:
                            logger.info(
                                f"Stream {stream_id} is idle by last_activity: {last_activity_str}. Checking if fully processed..."
                            )

                            file_path_str = meta.get("original_path")
                            total_bytes_sent_str = meta.get("total_bytes_sent")
                            total_bytes_sent = (
                                int(total_bytes_sent_str)
                                if total_bytes_sent_str
                                and total_bytes_sent_str.isdigit()
                                else 0
                            )

                            if not file_path_str:
                                logger.warning(
                                    f"Stream {stream_id} is idle but has no original_path in metadata. Cannot verify disk size. Skipping finalization attempt for now."
                                )
                                # Potentially move to an error state or log for investigation
                                continue

                            p_file_path = Path(file_path_str)
                            try:
                                disk_file_size = p_file_path.stat().st_size
                            except FileNotFoundError:
                                logger.warning(
                                    f"Stale stream {stream_id} original file {p_file_path} NOT FOUND. Aborting S3 upload if possible."
                                )
                                s3_upload_id = meta.get("s3_upload_id")
                                s3_bucket = meta.get("s3_bucket")
                                s3_key_prefix = meta.get("s3_key_prefix")
                                if s3_upload_id:  # Abort S3 upload
                                    from src.s3_client import (
                                        abort_s3_multipart_upload,
                                    )  # Local import

                                    await abort_s3_multipart_upload(
                                        s3_bucket, s3_key_prefix, s3_upload_id
                                    )
                                await set_stream_status(
                                    stream_id, "failed_file_missing"
                                )  # Update Redis status
                                await remove_stream_from_pending_completion(
                                    stream_id
                                )  # Clean up from this set if it was moved
                                await remove_stream_from_active_set(
                                    stream_id
                                )  # Also remove from active set
                                # Remove from in-memory active_processors if it exists
                                if (
                                    p_file_path in active_processors
                                    and active_processors[p_file_path].stream_id
                                    == stream_id
                                ):
                                    del active_processors[p_file_path]
                                    logger.info(
                                        f"[{stream_id}] Processor for missing file {p_file_path} removed from active tracking."
                                    )
                                continue  # Move to next stream

                            if total_bytes_sent == disk_file_size:
                                logger.info(
                                    f"Stream {stream_id} confirmed STALE and FULLY PROCESSED (processed: {total_bytes_sent}, disk: {disk_file_size}). Proceeding with finalization."
                                )
                                # Existing finalization logic (get processor or create temp_processor, then finalize)
                                s3_upload_id = meta.get(
                                    "s3_upload_id"
                                )  # Re-fetch for temp_processor case
                                s3_bucket = meta.get("s3_bucket")
                                s3_key_prefix = meta.get("s3_key_prefix")
                                processor = active_processors.get(p_file_path)

                                if processor and processor.stream_id == stream_id:
                                    logger.info(
                                        f"[{stream_id}] Found active processor for stale stream. Moving to pending_completion and scheduling finalization."
                                    )
                                    await move_stream_to_pending_completion(stream_id)
                                    asyncio.create_task(
                                        run_finalization_and_cleanup(
                                            processor, p_file_path
                                        ),
                                        name=f"idle_finalize_{stream_id[:8]}",
                                    )
                                elif (
                                    processor
                                ):  # Processor for path exists, but different stream_id
                                    logger.warning(
                                        f"Stale stream {stream_id} for path {p_file_path}, but existing processor has different ID ({processor.stream_id}). Stale check won't finalize this specific processor instance via this path."
                                    )
                                else:  # No processor in memory, reconstruct temp one
                                    logger.warning(
                                        f"Stale stream {stream_id} (path: {p_file_path}) is active in Redis but no in-memory processor found. Reconstructing temporary processor for finalization."
                                    )
                                    if s3_upload_id and s3_bucket and s3_key_prefix:
                                        temp_processor = StreamProcessor(
                                            stream_id=stream_id,
                                            file_path=p_file_path,
                                            s3_upload_id=s3_upload_id,
                                            s3_bucket=s3_bucket,
                                            s3_key_prefix=s3_key_prefix,
                                        )
                                        await temp_processor._initialize_from_checkpoint()
                                        await move_stream_to_pending_completion(
                                            stream_id
                                        )
                                        asyncio.create_task(
                                            temp_processor.finalize_stream(),
                                            name=f"idle_temp_finalize_{stream_id[:8]}",
                                        )
                                    else:
                                        logger.error(
                                            f"Stale stream {stream_id} (path: {p_file_path}) has insufficient metadata to reconstruct for finalization. Manual intervention likely needed."
                                        )
                            elif total_bytes_sent < disk_file_size:
                                logger.warning(
                                    f"Stream {stream_id} is idle by last_activity, but disk file size ({disk_file_size}) > processed bytes ({total_bytes_sent}). File may still be growing or processing lagged. Re-triggering processing and updating last_activity."
                                )
                                await update_stream_last_activity(
                                    stream_id
                                )  # Update to reset idle timer for the next check

                                # Proactively schedule another processing pass
                                processor = active_processors.get(p_file_path)
                                if processor and processor.stream_id == stream_id:
                                    if (
                                        not processor.is_processing
                                    ):  # Optional: check if already processing from another trigger
                                        logger.info(
                                            f"[{stream_id}] Stale checker re-triggering process_file_write for path {p_file_path}."
                                        )
                                        asyncio.create_task(
                                            processor.process_file_write(),
                                            name=f"stale_reprocess_{stream_id[:8]}",
                                        )
                                    else:
                                        logger.info(
                                            f"[{stream_id}] Stale checker sees pending processing for path {p_file_path}, not re-triggering immediately."
                                        )
                                elif (
                                    processor
                                ):  # Processor for path exists, different stream_id
                                    logger.warning(
                                        f"Stream {stream_id} needs reprocessing for path {p_file_path}, but existing processor has different ID ({processor.stream_id}). Cannot re-trigger."
                                    )
                                else:  # No processor in memory - this case is complex, _initialize_from_checkpoint would be needed for a new one
                                    logger.warning(
                                        f"Stream {stream_id} needs reprocessing for path {p_file_path}, but no active processor found. A new processor would need to be created and initialized from checkpoint."
                                    )
                                    # For Phase 5, we primarily focus on existing processors. Phase 7 (Resume) would handle creating new ones robustly.

                            else:  # total_bytes_sent > disk_file_size (file truncated?)
                                logger.error(
                                    f"Stream {stream_id} is idle by last_activity, but processed bytes ({total_bytes_sent}) > disk file size ({disk_file_size}). File may have been truncated. Aborting S3 upload."
                                )
                                s3_upload_id = meta.get("s3_upload_id")
                                s3_bucket = meta.get("s3_bucket")
                                s3_key_prefix = meta.get("s3_key_prefix")
                                if s3_upload_id:  # Abort S3 upload
                                    from src.s3_client import (
                                        abort_s3_multipart_upload,
                                    )  # Local import

                                    await abort_s3_multipart_upload(
                                        s3_bucket, s3_key_prefix, s3_upload_id
                                    )
                                await set_stream_status(
                                    stream_id, "failed_file_truncated"
                                )  # New status, if we want to track this state
                                await remove_stream_from_pending_completion(stream_id)
                                await remove_stream_from_active_set(stream_id)
                                if (
                                    p_file_path in active_processors
                                    and active_processors[p_file_path].stream_id
                                    == stream_id
                                ):
                                    del active_processors[p_file_path]
                                    logger.info(
                                        f"[{stream_id}] Processor for truncated file {p_file_path} removed from active tracking."
                                    )
                    except ValueError as ve:
                        logger.error(
                            f"Error parsing last_activity_at_utc for stream {stream_id}: {meta.get('last_activity_at_utc')} - {ve}"
                        )
                    except Exception as e_inner:
                        logger.error(
                            f"Error processing stale check for stream {stream_id}: {e_inner}",
                            exc_info=True,
                        )
                elif meta and meta.get("status") != "active":
                    logger.debug(
                        f"Stream {stream_id} found by stale checker but status is '{meta.get("status")}'. Skipping idle check."
                    )
                elif meta:
                    logger.warning(
                        f"Stream {stream_id} is active but has no 'last_activity_at_utc' in meta. Updating last activity to now."
                    )
                    await update_stream_last_activity(stream_id)
                # If meta is None, stream_id was in active set but key stream:<id>:meta disappeared. Could log this.

            await asyncio.sleep(
                settings.stream_timeout_seconds / 2
            )  # Or a fixed sensible interval e.g. 10-15s
        except asyncio.CancelledError:
            logger.info("Periodic stale stream checker task cancelled.")
            break
        except Exception as e:
            logger.error(
                f"Critical error in periodic_stale_stream_check loop: {e}",
                exc_info=True,
            )
            await asyncio.sleep(
                settings.stream_timeout_seconds
            )  # Avoid fast error loop


async def main():
    logger.info("Application starting...")
    watch_dir_str = settings.watch_dir  # Use a local var for clarity
    stream_timeout = settings.stream_timeout_seconds  # Use a local var

    logger.info(f"Watching directory: {watch_dir_str}")
    logger.info(
        f"Stream timeout for IDLE detection: {stream_timeout} seconds (used by stale checker - Phase 5)"
    )

    watch_dir = Path(watch_dir_str)
    if not watch_dir.exists():
        logger.warning(f"Watch directory {watch_dir} does not exist. Creating it.")
        watch_dir.mkdir(parents=True, exist_ok=True)
    elif not watch_dir.is_dir():
        logger.error(f"Watch path {watch_dir} exists but is not a directory. Exiting.")
        return

    # Use a global event for stopping all main loops if needed, or specific ones.
    # Let's use one for watcher and one for stale_checker for clarity.
    watcher_stop_event = asyncio.Event()
    stale_check_stop_event = asyncio.Event()
    # Removed global managed_tasks, tasks are managed by their respective stop_events and gather calls.

    try:
        await get_redis_connection()  # Initialize Redis connection pool

        logger.info("Starting periodic stale stream checker...")
        stale_check_task = asyncio.create_task(
            periodic_stale_stream_check(stale_check_stop_event),
            name="stale_stream_checker",
        )

        logger.info("Starting video file watcher...")
        watcher_coro = video_file_watcher(watch_dir, stream_timeout, watcher_stop_event)

        async for event in watcher_coro:
            await handle_stream_event(event)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Signaling services to stop...")
    except Exception as e:
        logger.critical(f"Unhandled exception in main event loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down services...")
        watcher_stop_event.set()  # Signal watcher to stop
        stale_check_stop_event.set()  # Signal stale checker to stop

        # Wait for the stale_check_task to finish
        if (
            "stale_check_task" in locals()
            and stale_check_task
            and not stale_check_task.done()
        ):
            logger.info("Waiting for stale stream checker to complete...")
            try:
                await asyncio.wait_for(
                    stale_check_task, timeout=settings.stream_timeout_seconds + 5
                )  # Give it some time
            except asyncio.TimeoutError:
                logger.warning(
                    "Stale stream checker did not complete in time, cancelling."
                )
                stale_check_task.cancel()
            except asyncio.CancelledError:
                logger.info("Stale stream checker was cancelled during shutdown.")
            except Exception as e_sc_wait:
                logger.error(
                    f"Error waiting for stale_check_task: {e_sc_wait}", exc_info=True
                )

        # Cancel any other outstanding tasks (e.g., from handle_stream_event, run_finalization_and_cleanup)
        # This is a broad cancellation; more targeted cancellation is for Phase 8.
        current_tasks = [
            t
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and not t.done()
        ]
        if current_tasks:
            logger.info(
                f"Cancelling {len(current_tasks)} other outstanding background tasks..."
            )
            for task in current_tasks:
                task.cancel()
            try:
                await asyncio.gather(*current_tasks, return_exceptions=True)
                logger.info("Other outstanding background tasks cancelled/finished.")
            except asyncio.CancelledError:  # Can happen if main itself is cancelled
                logger.info("Task gathering was cancelled.")
            except Exception as e_gather:
                logger.error(
                    f"Error during gathering of cancelled tasks: {e_gather}",
                    exc_info=True,
                )
        else:
            logger.info("No other outstanding background tasks found.")

        await close_redis_connection()
        await close_s3_resources()
        logger.info("Application stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application launch interrupted by user.")
