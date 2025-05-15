import asyncio
import structlog
from pathlib import Path
import uuid
import signal
import os
import datetime
from functools import partial

from .logging_config import setup_logging

setup_logging()

from .config import settings
from .watcher import video_file_watcher
from .events import StreamEvent, WatcherChangeType
from .redis_client import (
    init_stream_metadata,
    close_redis_connection,
    get_redis_connection,
    get_active_stream_ids,
    get_pending_completion_stream_ids,
    get_stream_meta,
    move_stream_to_pending_completion,
    set_stream_status,
    update_stream_last_activity,
    add_stream_to_failed_set,
)
from .s3_client import create_s3_multipart_upload, close_s3_resources
from .stream_processor import StreamProcessor
from .exceptions import (
    StreamInitializationError,
    ChunkProcessingError,
    StreamFinalizationError,
    S3OperationError,
    RedisOperationError,
)
from .metrics import start_metrics_server, ACTIVE_STREAMS_GAUGE

# Replace basicConfig if setup_logging handles everything
# logging.basicConfig(
#     level=settings.log_level.upper(),
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
# )
logger = structlog.get_logger(__name__)

# Dictionary to keep track of active stream processors
# Key: file_path (Path object), Value: StreamProcessor instance
active_processors: dict[Path, StreamProcessor] = {}
active_stream_tasks: dict[str, asyncio.Task] = {}

shutdown_signal_event = asyncio.Event()

MAX_CONCURRENT_STREAMS = int(os.getenv("MAX_CONCURRENT_STREAMS", "5"))
stream_processing_semaphore = asyncio.Semaphore(MAX_CONCURRENT_STREAMS)


def signal_handler(sig, frame):
    logger.info(f"Signal {sig.name} received. Initiating graceful shutdown...")
    shutdown_signal_event.set()


async def _create_and_manage_processor_task(
    processor_coro: asyncio.coroutines, stream_id: str, task_name: str
):
    """Helper to run processor coroutine with semaphore and manage task registration."""
    async with stream_processing_semaphore:
        if shutdown_signal_event.is_set():
            logger.warning(
                f"[{stream_id}] Shutdown in progress. Not starting new task: {task_name}"
            )
            return
        try:
            task = asyncio.create_task(processor_coro, name=task_name)
            active_stream_tasks[stream_id] = task
            await task
        except asyncio.CancelledError:
            logger.info(f"[{stream_id}] Task {task_name} was cancelled externally.")
        except (
            StreamInitializationError,
            ChunkProcessingError,
            StreamFinalizationError,
        ) as e:
            logger.error(
                f"[{stream_id}] Stream processing error in task {task_name}: {e}",
                exc_info=False,
            )
        except Exception as e:
            logger.error(
                f"[{stream_id}] Unexpected error in task {task_name}: {e}",
                exc_info=True,
            )
            try:
                await add_stream_to_failed_set(
                    stream_id, reason="task_unexpected_error"
                )
            except Exception as redis_e:
                logger.critical(
                    f"[{stream_id}] CRITICAL: Failed to mark stream as failed in Redis after unexpected task error: {redis_e}"
                )
        finally:
            active_stream_tasks.pop(stream_id, None)
            logger.debug(
                "Task completed and removed from active_stream_tasks.",
                stream_id=stream_id,
                task_name=task_name,
            )


async def manage_new_stream_creation(event: StreamEvent):
    """Handles the creation and registration of a new stream processor."""
    file_path = event.file_path
    if file_path in active_processors and active_processors[file_path].stream_id:
        logger.warning(
            f"[{file_path.name}] Create event for already tracked path. Current processor stream ID: {active_processors[file_path].stream_id}. Ignoring."
        )
        return

    stream_id = str(uuid.uuid4())
    logger.info(
        f"[{file_path.name}] New stream detected (CREATE). Stream ID: {stream_id}"
    )
    s3_object_key_prefix = f"streams/{stream_id}/{file_path.name}"

    try:
        s3_upload_id = await create_s3_multipart_upload(
            settings.s3_bucket_name, s3_object_key_prefix
        )
        if not s3_upload_id:
            logger.error(
                f"[{stream_id}] Failed to initiate S3 multipart upload (no S3UploadId returned). Aborting."
            )
            return

        await init_stream_metadata(
            stream_id,
            str(file_path),
            s3_upload_id,
            settings.s3_bucket_name,
            s3_object_key_prefix,
        )

        processor = StreamProcessor(
            stream_id,
            file_path,
            s3_upload_id,
            settings.s3_bucket_name,
            s3_object_key_prefix,
            shutdown_signal_event,
        )
        await processor._initialize_from_checkpoint()
        active_processors[file_path] = processor
        ACTIVE_STREAMS_GAUGE.inc()
        logger.info(
            "StreamProcessor created.", stream_id=stream_id, file_path=str(file_path)
        )

        initial_processing_coro = processor.process_file_write()
        asyncio.create_task(
            _create_and_manage_processor_task(
                initial_processing_coro, stream_id, f"initial_write_{stream_id[:8]}"
            )
        )

    except (S3OperationError, RedisOperationError, StreamInitializationError) as e:
        logger.error(
            "Error during new stream setup",
            stream_id=stream_id,
            file_path=str(file_path),
            exc_info=e,
        )
        if stream_id:
            try:
                await add_stream_to_failed_set(
                    stream_id, reason="new_stream_unexpected_setup_failure"
                )
            except Exception:
                pass


async def handle_stream_event(event: StreamEvent, event_queue: asyncio.Queue):
    logger.debug(
        f"Received event: Type={event.change_type.name}, Path={event.file_path}"
    )
    if shutdown_signal_event.is_set():
        logger.info(
            f"Shutdown in progress, not processing new event: {event.change_type.name} for {event.file_path}"
        )
        return

    if event.change_type == WatcherChangeType.CREATE:
        asyncio.create_task(manage_new_stream_creation(event))

    elif event.change_type == WatcherChangeType.WRITE:
        processor = active_processors.get(event.file_path)
        if processor:
            processing_coro = processor.process_file_write()
            asyncio.create_task(
                _create_and_manage_processor_task(
                    processing_coro,
                    processor.stream_id,
                    f"process_write_{processor.stream_id[:8]}",
                )
            )
        else:
            logger.warning(
                f"[{event.file_path.name}] WRITE event for untracked file. Re-queueing as CREATE."
            )
            new_event = StreamEvent(
                change_type=WatcherChangeType.CREATE, file_path=event.file_path
            )
            await event_queue.put(new_event)

    elif event.change_type == WatcherChangeType.DELETE:
        logger.info("DELETE event received.", file_path=str(event.file_path))
        processor = active_processors.pop(event.file_path, None)
        if processor:
            ACTIVE_STREAMS_GAUGE.dec()
            logger.info(
                "Removed processor for deleted file. Signalling task to cancel if active.",
                stream_id=processor.stream_id,
            )
            task = active_stream_tasks.get(processor.stream_id)
            if task and not task.done():
                task.cancel()
            try:
                await set_stream_status(processor.stream_id, "deleted_locally")
            except Exception as e:
                logger.warning(
                    f"[{processor.stream_id}] Failed to set status to deleted_locally: {e}"
                )
        else:
            logger.warning(
                "DELETE event for untracked file.", file_path=str(event.file_path)
            )


async def run_finalization_and_cleanup(processor: StreamProcessor, processor_key: Path):
    try:
        logger.info(
            f"[{processor.stream_id}] Attempting to finalize stream for {processor.file_path} via helper."
        )
        finalization_coro = processor.finalize_stream()
        await _create_and_manage_processor_task(
            finalization_coro,
            processor.stream_id,
            f"finalize_stream_{processor.stream_id[:8]}",
        )
    except Exception as e_finalize_wrapper:
        logger.error(
            f"[{processor.stream_id}] Error in finalization task wrapper for {processor.file_path}: {e_finalize_wrapper}",
            exc_info=True,
        )
    finally:
        if (
            processor_key in active_processors
            and active_processors.get(processor_key) == processor
        ):
            del active_processors[processor_key]
            ACTIVE_STREAMS_GAUGE.dec()
            logger.info(
                "Processor removed from active_processors after finalization attempt.",
                stream_id=processor.stream_id,
            )


async def periodic_stale_stream_check():
    while not shutdown_signal_event.is_set():
        try:
            logger.debug("Running periodic stale stream check...")
            active_redis_stream_ids = await get_active_stream_ids()
            now_utc = datetime.datetime.now(datetime.timezone.utc)

            for stream_id in active_redis_stream_ids:
                if shutdown_signal_event.is_set():
                    break
                meta = await get_stream_meta(stream_id)
                if not meta or meta.get("status") != "active":
                    continue

                last_activity_str = meta.get("last_activity_at_utc")
                if not last_activity_str:
                    await update_stream_last_activity(stream_id)
                    continue

                last_activity_dt = datetime.datetime.fromisoformat(last_activity_str)
                if last_activity_dt.tzinfo is None:
                    last_activity_dt = last_activity_dt.replace(
                        tzinfo=datetime.timezone.utc
                    )

                if (
                    now_utc - last_activity_dt
                ).total_seconds() > settings.stream_timeout_seconds:
                    logger.info(
                        f"[{stream_id}] Stream is STALE by last_activity: {last_activity_str}. Checking for completion."
                    )
                    file_path_str = meta.get("original_path")
                    if not file_path_str:
                        await add_stream_to_failed_set(stream_id, "stale_no_path")
                        continue

                    p_file_path = Path(file_path_str)
                    processor = active_processors.get(p_file_path)

                    if not p_file_path.exists():
                        logger.warning(
                            f"[{stream_id}] Stale stream original file {p_file_path} NOT FOUND. Aborting S3 (if processor known) & marking failed."
                        )
                        if processor and processor.s3_upload_id:
                            from .s3_client import abort_s3_multipart_upload

                            await abort_s3_multipart_upload(
                                processor.s3_bucket,
                                processor.s3_key_prefix,
                                processor.s3_upload_id,
                            )
                        await add_stream_to_failed_set(stream_id, "stale_file_missing")
                        if processor:
                            active_processors.pop(p_file_path, None)
                            ACTIVE_STREAMS_GAUGE.dec()
                        continue

                    disk_file_size = p_file_path.stat().st_size
                    total_bytes_sent = int(meta.get("total_bytes_sent", 0))

                    if total_bytes_sent >= disk_file_size:
                        logger.info(
                            f"[{stream_id}] Stale stream appears fully uploaded (sent: {total_bytes_sent}, disk: {disk_file_size}). Moving to pending completion."
                        )
                        if processor and processor.stream_id == stream_id:
                            await move_stream_to_pending_completion(stream_id)
                            asyncio.create_task(
                                run_finalization_and_cleanup(processor, p_file_path)
                            )
                        elif processor:
                            logger.warning(
                                f"[{stream_id}] Stale stream {stream_id} found processor for path {p_file_path}, but stream ID mismatch ({processor.stream_id}). Orphaned? Marking failed."
                            )
                            await add_stream_to_failed_set(
                                stream_id, "stale_processor_mismatch"
                            )
                        else:
                            logger.warning(
                                f"[{stream_id}] Stale stream {stream_id} for {p_file_path} is fully uploaded but has no active processor. Attempting to resume for finalization."
                            )
                            asyncio.create_task(
                                resume_stream_processing(
                                    stream_id, for_finalization_only=True
                                )
                            )
                    else:
                        logger.info(
                            f"[{stream_id}] Stale stream data on disk ({disk_file_size}) > sent ({total_bytes_sent}). Re-triggering write processing."
                        )
                        if processor and processor.stream_id == stream_id:
                            processing_coro = processor.process_file_write()
                            asyncio.create_task(
                                _create_and_manage_processor_task(
                                    processing_coro,
                                    stream_id,
                                    f"stale_reprocess_{stream_id[:8]}",
                                )
                            )
                        else:
                            logger.warning(
                                f"[{stream_id}] Stale stream needs reprocessing for {p_file_path}, but no matching processor. Resuming."
                            )
                            asyncio.create_task(resume_stream_processing(stream_id))

            await asyncio.sleep(
                settings.stream_timeout_seconds / 2
                if settings.stream_timeout_seconds > 2
                else 1
            )

        except (RedisOperationError, S3OperationError) as e:
            logger.error(
                f"Client error in periodic_stale_stream_check: {e}", exc_info=False
            )
            await asyncio.sleep(settings.stream_timeout_seconds)
        except asyncio.CancelledError:
            logger.info("Periodic stale stream checker task cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in periodic_stale_stream_check: {e}", exc_info=True)
            await asyncio.sleep(settings.stream_timeout_seconds)


async def resume_stream_processing(stream_id: str, for_finalization_only: bool = False):
    logger.info(f"Attempting to resume processing for stream_id: {stream_id}")
    meta = await get_stream_meta(stream_id)
    if not meta:
        logger.error(f"No metadata for stream_id: {stream_id} during resume. Skipping.")
        await add_stream_to_failed_set(stream_id, "resume_no_meta")
        return

    file_path_str = meta.get("original_path")
    s3_upload_id = meta.get("s3_upload_id")
    s3_bucket = meta.get("s3_bucket")
    s3_key_prefix = meta.get("s3_key_prefix")
    status = meta.get("status", "unknown")

    if not all([file_path_str, s3_upload_id, s3_bucket, s3_key_prefix]):
        logger.error(
            f"Incomplete metadata for stream_id: {stream_id} during resume. Meta: {meta}"
        )
        await add_stream_to_failed_set(stream_id, "resume_incomplete_meta")
        return

    file_path = Path(file_path_str)
    if file_path in active_processors:
        logger.warning(
            "Processor for file already exists. Skipping resume action.",
            stream_id=stream_id,
            file_path=str(file_path),
        )
        return

    processor = StreamProcessor(
        stream_id,
        file_path,
        s3_upload_id,
        s3_bucket,
        s3_key_prefix,
        shutdown_signal_event,
    )
    try:
        await processor._initialize_from_checkpoint()
    except FileNotFoundError:
        logger.error(
            f"Resume failed for stream {stream_id}: original file {file_path} not found. Already marked failed by init."
        )
        return
    except StreamInitializationError as e_init:
        logger.error(
            f"Resume failed for stream {stream_id}: init error {e_init}. Already marked failed by init."
        )
        return
    except Exception as e_init_unexpected:
        logger.error(
            f"Resume failed for stream {stream_id}: unexpected init error {e_init_unexpected}",
            exc_info=True,
        )
        await add_stream_to_failed_set(
            stream_id, reason="resume_init_unexpected_failure"
        )
        return

    active_processors[file_path] = processor
    ACTIVE_STREAMS_GAUGE.inc()
    logger.info(
        "Processor resumed.",
        stream_id=stream_id,
        file_path=str(file_path),
        status=status,
    )

    task_coro = None
    task_name_suffix = ""
    if (
        status
        in [
            "pending_completion",
            "s3_completed",
            "failed_meta_upload",
            "failed_meta_generation",
        ]
        or for_finalization_only
    ):
        logger.info(f"[{stream_id}] Resuming: Triggering finalize_stream.")
        task_coro = processor.finalize_stream()
        task_name_suffix = f"resume_finalize_{stream_id[:8]}"
    elif (
        status
        in [
            "active",
            "processing",
            "interrupted_shutdown_write",
            "interrupted_cancelled_write",
            "unknown",
        ]
        and not for_finalization_only
    ):
        logger.info(f"[{stream_id}] Resuming: Triggering process_file_write.")
        task_coro = processor.process_file_write()
        task_name_suffix = f"resume_write_{stream_id[:8]}"
    else:
        logger.info(
            f"[{stream_id}] Resuming: Stream status '{status}'. No immediate action taken by resume_stream_processing."
        )

    if task_coro:
        asyncio.create_task(
            _create_and_manage_processor_task(task_coro, stream_id, task_name_suffix)
        )


async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler, sig, None)

    # Start Prometheus metrics server in a separate thread
    await loop.run_in_executor(None, partial(start_metrics_server))

    logger.info("Application starting...")
    event_queue = asyncio.Queue()
    stale_check_task = None
    watcher_task_manager = None

    try:
        await get_redis_connection()
        logger.info("--- Attempting to resume interrupted streams ---")
        active_ids = await get_active_stream_ids()
        pending_ids = await get_pending_completion_stream_ids()
        all_ids_to_resume = set(active_ids + pending_ids)

        if all_ids_to_resume:
            logger.info(
                f"Found {len(all_ids_to_resume)} streams to resume: {all_ids_to_resume}"
            )
            resume_futures = [
                resume_stream_processing(sid) for sid in all_ids_to_resume
            ]
            await asyncio.gather(*resume_futures, return_exceptions=True)
        else:
            logger.info("No interrupted streams found to resume.")
        logger.info("--- Resume attempt finished ---")

        watch_dir_path = Path(settings.watch_dir)
        if not watch_dir_path.is_dir():
            logger.info(
                f"Watch directory {watch_dir_path} does not exist or is not a directory. Creating it."
            )
            watch_dir_path.mkdir(parents=True, exist_ok=True)

        async def run_watcher_loop():
            async for event_item in video_file_watcher(
                watch_dir_path, settings.stream_timeout_seconds, shutdown_signal_event
            ):
                if shutdown_signal_event.is_set():
                    break
                await event_queue.put(event_item)
            logger.info("Watcher loop has terminated.")

        watcher_task_manager = asyncio.create_task(
            run_watcher_loop(), name="video_file_watcher_manager"
        )

        stale_check_task = asyncio.create_task(
            periodic_stale_stream_check(), name="stale_stream_checker"
        )
        logger.info("Watcher and stale stream checker started.")

        while not shutdown_signal_event.is_set() or not event_queue.empty():
            try:
                event = await asyncio.wait_for(event_queue.get(), timeout=1.0)
                await handle_stream_event(event, event_queue)
                event_queue.task_done()
            except asyncio.TimeoutError:
                if shutdown_signal_event.is_set() and event_queue.empty():
                    break
                continue
            except asyncio.CancelledError:
                logger.info("Main event loop cancelled.")
                break

        logger.info(
            "Main event loop finished. Waiting for shutdown signal to be fully processed by tasks..."
        )
        if not shutdown_signal_event.is_set():
            await shutdown_signal_event.wait()

    except asyncio.CancelledError:
        logger.info("Main application task cancelled.")
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)
    finally:
        logger.info("Graceful shutdown sequence initiated in main finally block...")
        if not shutdown_signal_event.is_set():
            shutdown_signal_event.set()

        tasks_to_await = []
        if stale_check_task and not stale_check_task.done():
            stale_check_task.cancel()
            tasks_to_await.append(stale_check_task)
        if watcher_task_manager and not watcher_task_manager.done():
            watcher_task_manager.cancel()
            tasks_to_await.append(watcher_task_manager)

        active_task_ids = list(active_stream_tasks.keys())
        for stream_id in active_task_ids:
            task = active_stream_tasks.get(stream_id)
            if task and not task.done():
                logger.info(
                    f"[{stream_id}] Cancelling active stream task during shutdown."
                )
                task.cancel()
                tasks_to_await.append(task)

        if not event_queue.empty():
            logger.info(
                f"Event queue has {event_queue.qsize()} items. Attempting to join..."
            )
            try:
                await asyncio.wait_for(event_queue.join(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "Timeout waiting for event queue to join during shutdown."
                )

        if tasks_to_await:
            logger.info(
                f"Waiting for {len(tasks_to_await)} background tasks to complete shutdown..."
            )
            await asyncio.gather(*tasks_to_await, return_exceptions=True)

        # Ensure any remaining processors are cleared and gauge decremented if not already
        remaining_processors = list(active_processors.keys())
        for proc_path in remaining_processors:
            processor = active_processors.pop(proc_path, None)
            if processor:
                ACTIVE_STREAMS_GAUGE.dec()
                logger.info(
                    "Decremented active streams gauge for remaining processor during final shutdown",
                    stream_id=processor.stream_id,
                )
        logger.info("All tasks awaited. Closing resources.")
        await close_redis_connection()
        await close_s3_resources()
        logger.info("Application stopped gracefully.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info(
            "Application interrupted by user (KeyboardInterrupt outside asyncio loop)."
        )
    except Exception as e:
        logger.critical(
            f"Application exited with unhandled exception: {e}", exc_info=True
        )
