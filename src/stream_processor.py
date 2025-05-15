import asyncio
import structlog
from pathlib import Path
import datetime  # For timestamping parts later if needed directly here
from structlog.contextvars import bind_contextvars

from .config import settings
from .exceptions import (
    ChunkProcessingError,
    StreamFinalizationError,
    S3OperationError,
    RedisOperationError,
    FFprobeError,
)
from .metrics import (
    ACTIVE_STREAMS_GAUGE,
    VIDEO_CHUNKS_UPLOADED_TOTAL,
    VIDEO_BYTES_UPLOADED_TOTAL,
    VIDEO_STREAM_DURATION_SECONDS,
    VIDEO_PROCESSING_TIME_SECONDS,
    VIDEO_FAILED_OPERATIONS_TOTAL,
    STREAMS_COMPLETED_TOTAL,
    STREAMS_FAILED_TOTAL,
)

# Import Redis and S3 client functions will be done dynamically inside methods
# to avoid circular dependencies at module import time, especially if those
# clients might one day import StreamProcessor or related types.

# logger = structlog.get_logger(__name__)  # Global logger removed, will use bound logger in methods


class StreamProcessor:
    def __init__(
        self,
        stream_id: str,
        file_path: Path,
        s3_upload_id: str,
        s3_bucket: str,
        s3_key_prefix: str,
        shutdown_event: asyncio.Event,
    ):
        self.stream_id = stream_id
        self.file_path = file_path
        self.s3_upload_id = s3_upload_id
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix
        self.shutdown_event = shutdown_event

        self.current_file_offset = 0  # Bytes already read and processed
        self.next_part_number = 1  # S3 part numbers are 1-indexed
        self.uploaded_parts_info = (
            []
        )  # List of {"PartNumber": int, "ETag": str, "Size": int}
        self.is_processing_write = (
            False  # To indicate if a process_file_write is active (conceptual)
        )
        self.is_finalizing = False  # To prevent re-entrant finalization
        self.lock = (
            asyncio.Lock()
        )  # To prevent concurrent processing of the same file path by this instance
        self.logger = structlog.get_logger(self.__class__.__name__).bind(
            stream_id=self.stream_id, file_path=str(self.file_path)
        )  # Bound logger
        self.stream_start_time_utc: datetime.datetime | None = (
            None  # For processing time metric
        )
        self.logger.debug("StreamProcessor initialized")

    async def _set_stream_start_time_from_redis(self):
        """Helper to fetch and set the initial stream start time from Redis meta."""
        from src.redis_client import get_stream_meta

        if self.stream_start_time_utc is None:
            try:
                meta = await get_stream_meta(self.stream_id)
                if meta and meta.get("started_at_utc"):
                    self.stream_start_time_utc = datetime.datetime.fromisoformat(
                        meta["started_at_utc"]
                    )
            except Exception as e:
                self.logger.warning(
                    "Failed to get stream start time from Redis for metrics", exc_info=e
                )

    async def _handle_processor_error(
        self, operation: str, e: Exception, reason_code: str
    ):
        """Helper to log error, set stream to failed in Redis."""
        from src.redis_client import add_stream_to_failed_set

        VIDEO_FAILED_OPERATIONS_TOTAL.labels(
            stream_id=self.stream_id, operation_type=reason_code
        ).inc()

        self.logger.error(
            f"Error during {operation}",  # No need for f-string stream_id here
            exc_info=e,  # Pass exception directly for structlog to handle
            operation=operation,
            reason_code=reason_code,
        )
        try:
            await add_stream_to_failed_set(self.stream_id, reason=reason_code)
        except RedisOperationError as redis_e:
            self.logger.error(
                "CRITICAL: Failed to mark stream as failed in Redis during error handling",
                exc_info=redis_e,
            )
            VIDEO_FAILED_OPERATIONS_TOTAL.labels(
                stream_id=self.stream_id, operation_type="redis_mark_failed_critical"
            ).inc()

    async def _initialize_from_checkpoint(self):
        """Loads the processor's state from Redis based on its stream_id."""
        self.logger.info("Attempting to initialize from checkpoint.")
        await self._set_stream_start_time_from_redis()  # Ensure start time is loaded
        from src.redis_client import (
            get_stream_meta,
            get_stream_next_part,
            get_stream_bytes_sent,
            get_stream_parts,
            add_stream_to_failed_set,
        )

        try:
            meta = await get_stream_meta(self.stream_id)
            if not meta:
                self.logger.warning(
                    "No metadata found in Redis. Cannot initialize from checkpoint. Assuming new stream (problematic if called for resume)."
                )
                return

            self.file_path = Path(meta.get("original_path", str(self.file_path)))
            self.s3_upload_id = meta.get("s3_upload_id", self.s3_upload_id)
            self.s3_bucket = meta.get("s3_bucket", self.s3_bucket)
            self.s3_key_prefix = meta.get("s3_key_prefix", self.s3_key_prefix)

            next_part_redis = await get_stream_next_part(self.stream_id)
            bytes_sent_redis = await get_stream_bytes_sent(self.stream_id)
            parts_redis = await get_stream_parts(self.stream_id)

            if parts_redis:
                self.uploaded_parts_info = parts_redis
                # Derive next_part_number if not explicitly set or if parts_redis is more current
                self.next_part_number = (
                    max(p["PartNumber"] for p in self.uploaded_parts_info) + 1
                )
                # Derive current_file_offset from sum of part sizes if not explicitly set or if parts_redis is more current
                self.current_file_offset = sum(
                    p["Size"] for p in self.uploaded_parts_info
                )
            else:
                self.uploaded_parts_info = []
                self.next_part_number = 1
                self.current_file_offset = 0

            # Override with explicit Redis values if they exist, as they might be more authoritative
            # for a partially resumed upload where parts_redis might not yet reflect the very last action.
            if next_part_redis is not None:
                self.next_part_number = int(next_part_redis)
            if bytes_sent_redis is not None:
                self.current_file_offset = int(bytes_sent_redis)

            # Final consistency check if parts_redis was empty but explicit values were also not there
            if not parts_redis and next_part_redis is None:
                self.next_part_number = 1
            if not parts_redis and bytes_sent_redis is None:
                self.current_file_offset = 0

            self.logger.info(
                f"Initialized from checkpoint: File='{self.file_path}', next_part={self.next_part_number}, offset={self.current_file_offset}, known_parts={len(self.uploaded_parts_info)}, S3 UploadID='{self.s3_upload_id}'"
            )

            if not self.file_path.exists():
                self.logger.warning(
                    f"Original file {self.file_path} not found during checkpoint initialization. Stream cannot be processed further."
                )
                await add_stream_to_failed_set(
                    self.stream_id, reason="file_missing_on_resume"
                )
                raise FileNotFoundError(
                    f"Original file {self.file_path} missing for stream {self.stream_id}"
                )

        except FileNotFoundError:
            raise
        except (RedisOperationError, S3OperationError) as e:
            await self._handle_processor_error(
                "_initialize_from_checkpoint (client op)",
                e,
                "init_checkpoint_client_failure",
            )
            raise StreamFinalizationError(
                f"Failed to initialize from checkpoint due to client error: {e}"
            ) from e
        except Exception as e:
            await self._handle_processor_error(
                "_initialize_from_checkpoint", e, "init_checkpoint_failure"
            )
            raise StreamFinalizationError(
                f"Unexpected error initializing from checkpoint: {e}"
            ) from e

    async def process_file_write(self):
        """Handles a WRITE event, reads new data, chunks, and uploads."""
        if self.is_processing_write:
            self.logger.debug("process_file_write already in progress. Skipping.")
            return

        async with self.lock:
            from src.s3_client import upload_s3_part
            from src.redis_client import (
                add_stream_part_info,
                incr_stream_bytes_sent,
                set_stream_next_part,
                update_stream_last_activity,
                set_stream_status,
            )

            if self.is_processing_write:
                self.logger.debug(
                    "process_file_write acquired lock but another instance was already processing. Skipping."
                )
                return
            self.is_processing_write = True

            await self._set_stream_start_time_from_redis()  # Ensure start time is available

            try:
                if not self.file_path.exists():
                    self.logger.warning(
                        f"File {self.file_path} no longer exists. Stopping processing."
                    )
                    return

                with open(self.file_path, "rb") as f:
                    f.seek(self.current_file_offset)
                    processed_in_this_pass = False
                    while not self.shutdown_event.is_set():
                        current_disk_file_size = self.file_path.stat().st_size
                        if current_disk_file_size <= self.current_file_offset:
                            if not processed_in_this_pass:
                                self.logger.debug(
                                    f"No new data for {self.file_path} (disk size: {current_disk_file_size}, offset: {self.current_file_offset})."
                                )
                            break

                        if not processed_in_this_pass:
                            self.logger.info(
                                f"Processing WRITE for {self.file_path}. Current disk size: {current_disk_file_size}, Offset: {self.current_file_offset}"
                            )

                        bytes_to_read = min(
                            settings.chunk_size_bytes,
                            current_disk_file_size - self.current_file_offset,
                        )

                        if bytes_to_read <= 0:
                            break

                        chunk = f.read(bytes_to_read)
                        if not chunk:
                            break

                        processed_in_this_pass = True
                        chunk_len = len(chunk)
                        self.logger.debug(
                            f"Read chunk of size {chunk_len}, part num {self.next_part_number}"
                        )

                        part_etag = await upload_s3_part(
                            bucket_name=self.s3_bucket,
                            object_key=self.s3_key_prefix,
                            upload_id=self.s3_upload_id,
                            part_number=self.next_part_number,
                            data=chunk,
                        )

                        if part_etag:
                            VIDEO_CHUNKS_UPLOADED_TOTAL.labels(
                                stream_id=self.stream_id
                            ).inc()
                            VIDEO_BYTES_UPLOADED_TOTAL.labels(
                                stream_id=self.stream_id
                            ).inc(chunk_len)
                            part_info = {
                                "PartNumber": self.next_part_number,
                                "ETag": part_etag,
                                "Size": chunk_len,
                            }
                            self.uploaded_parts_info.append(part_info)

                            await add_stream_part_info(
                                self.stream_id,
                                self.next_part_number,
                                part_etag,
                                chunk_len,
                            )
                            await incr_stream_bytes_sent(self.stream_id, chunk_len)
                            await set_stream_next_part(
                                self.stream_id, self.next_part_number + 1
                            )
                            await update_stream_last_activity(self.stream_id)

                            self.current_file_offset += chunk_len
                            self.next_part_number += 1
                            self.logger.info(
                                f"Uploaded part {self.next_part_number-1}, ETag: {part_etag}, new offset: {self.current_file_offset}"
                            )
                        else:
                            self.logger.error(
                                f"Failed to upload part {self.next_part_number}. Halting stream for this pass."
                            )
                            VIDEO_FAILED_OPERATIONS_TOTAL.labels(
                                stream_id=self.stream_id,
                                operation_type="s3_upload_part",
                            ).inc()
                            self.is_processing_write = False
                            return

                    if self.shutdown_event.is_set():
                        self.logger.info(
                            f"Shutdown signaled during process_file_write. Loop terminated."
                        )
                        await set_stream_status(
                            self.stream_id, "interrupted_shutdown_write"
                        )
                        return

                    if processed_in_this_pass:
                        self.logger.info(
                            f"Finished processing pass for {self.file_path}, current offset {self.current_file_offset}"
                        )

            except FileNotFoundError:
                self.logger.warning(
                    f"File {self.file_path} disappeared during processing."
                )
                VIDEO_FAILED_OPERATIONS_TOTAL.labels(
                    stream_id=self.stream_id, operation_type="process_file_disappeared"
                ).inc()
                # TODO: Mark stream as failed/aborted (Phase 8)
            except (S3OperationError, RedisOperationError) as e:
                await self._handle_processor_error(
                    "process_file_write (client op)", e, "chunk_processing_failure"
                )
                raise ChunkProcessingError(
                    f"Failed to process chunk for {self.stream_id}: {e}"
                ) from e
            except asyncio.CancelledError:
                self.logger.info(f"process_file_write task cancelled.")
                await set_stream_status(self.stream_id, "interrupted_cancelled_write")
                raise
            except Exception as e:
                await self._handle_processor_error(
                    "process_file_write", e, "chunk_processing_unexpected"
                )
                raise ChunkProcessingError(
                    f"Unexpected error processing chunk for {self.stream_id}: {e}"
                ) from e
            finally:
                self.is_processing_write = False

    async def finalize_stream(self):
        """Handles stream finalization: complete S3 multipart upload and then generate/upload metadata JSON."""
        if self.is_finalizing:
            self.logger.info("Finalization already in progress. Skipping.")
            return
        self.is_finalizing = True

        self.logger.info("Finalizing stream.")
        async with self.lock:
            from src.s3_client import (
                complete_s3_multipart_upload,
                abort_s3_multipart_upload,
                upload_json_to_s3,
            )
            from src.redis_client import (
                get_stream_parts,
                set_stream_status,
                remove_stream_from_pending_completion,
                remove_stream_keys,
                add_stream_to_failed_set,
            )
            from src.metadata_generator import generate_metadata_json

            s3_completed_successfully = False
            try:
                s3_parts_for_completion = await get_stream_parts(self.stream_id)
                s3_parts_for_completion = [
                    p for p in s3_parts_for_completion if p.get("ETag")
                ]

                if not self.file_path.exists():
                    self.logger.warning(
                        f"Original file {self.file_path} not found during finalization. Aborting S3 upload."
                    )
                    await abort_s3_multipart_upload(
                        self.s3_bucket, self.s3_key_prefix, self.s3_upload_id
                    )
                    await add_stream_to_failed_set(
                        self.stream_id, reason="finalize_file_missing"
                    )
                    raise StreamFinalizationError(
                        "Original file missing for finalization"
                    )

                if not s3_parts_for_completion:
                    self.logger.warning(
                        f"No parts for {self.file_path}. Aborting S3 upload."
                    )
                    await abort_s3_multipart_upload(
                        self.s3_bucket, self.s3_key_prefix, self.s3_upload_id
                    )
                    await set_stream_status(self.stream_id, "aborted_no_parts")
                    return

                s3_completed_successfully = await complete_s3_multipart_upload(
                    bucket_name=self.s3_bucket,
                    object_key=self.s3_key_prefix,
                    upload_id=self.s3_upload_id,
                    parts=s3_parts_for_completion,
                )

                if s3_completed_successfully:
                    self.logger.info(
                        f"S3 multipart upload completed for {self.file_path}."
                    )
                    await set_stream_status(self.stream_id, "s3_completed")

                    metadata_object_key = f"{self.s3_key_prefix}.metadata.json"
                    metadata = await generate_metadata_json(
                        self.stream_id, self.file_path
                    )
                    if metadata:
                        duration = metadata.get("format", {}).get("duration")
                        if duration:
                            try:
                                VIDEO_STREAM_DURATION_SECONDS.labels(
                                    stream_id=self.stream_id
                                ).observe(float(duration))
                            except ValueError:
                                self.logger.warning(
                                    "Could not parse duration for metric",
                                    duration=duration,
                                )

                        meta_upload_success = await upload_json_to_s3(
                            self.s3_bucket, metadata_object_key, metadata
                        )
                        if meta_upload_success:
                            await set_stream_status(
                                self.stream_id, "completed_with_meta"
                            )
                            await remove_stream_keys(self.stream_id)
                            STREAMS_COMPLETED_TOTAL.inc()
                            if self.stream_start_time_utc:
                                processing_duration = (
                                    datetime.datetime.now(datetime.timezone.utc)
                                    - self.stream_start_time_utc
                                ).total_seconds()
                                VIDEO_PROCESSING_TIME_SECONDS.labels(
                                    stream_id=self.stream_id
                                ).observe(processing_duration)

                            self.logger.info(
                                f"Stream fully completed and metadata uploaded."
                            )
                        else:
                            await add_stream_to_failed_set(
                                self.stream_id, reason="failed_meta_upload"
                            )
                            raise StreamFinalizationError(
                                "Metadata JSON upload to S3 failed."
                            )
                    else:
                        await add_stream_to_failed_set(
                            self.stream_id, reason="failed_meta_generation"
                        )
                        raise StreamFinalizationError(
                            "Metadata JSON generation failed."
                        )
                else:
                    self.logger.error(
                        f"S3 complete_multipart_upload returned False. Marking as failed_s3_complete."
                    )
                    await add_stream_to_failed_set(
                        self.stream_id, reason="failed_s3_complete_internal"
                    )
                    raise StreamFinalizationError(
                        "S3 complete_multipart_upload indicated failure internally."
                    )

            except (S3OperationError, RedisOperationError, FFprobeError) as e:
                reason = (
                    "finalize_s3_failure"
                    if isinstance(e, S3OperationError)
                    else (
                        "finalize_redis_failure"
                        if isinstance(e, RedisOperationError)
                        else (
                            "finalize_ffprobe_failure"
                            if isinstance(e, FFprobeError)
                            else "finalize_client_failure"
                        )
                    )
                )
                await self._handle_processor_error(
                    "finalize_stream (client op)", e, reason
                )
                if (
                    not s3_completed_successfully
                    and isinstance(e, S3OperationError)
                    and e.operation == "complete_multipart_upload"
                ):
                    self.logger.info(
                        f"Attempting to abort S3 upload due to completion failure: {e}"
                    )
                    await abort_s3_multipart_upload(
                        self.s3_bucket, self.s3_key_prefix, self.s3_upload_id
                    )
                raise StreamFinalizationError(
                    f"Failed to finalize stream {self.stream_id} due to client error: {e}"
                ) from e
            except asyncio.CancelledError:
                self.logger.info(f"finalize_stream task cancelled.")
                await set_stream_status(
                    self.stream_id, "interrupted_cancelled_finalize"
                )
                raise
            except Exception as e:
                await self._handle_processor_error(
                    "finalize_stream", e, "finalize_unexpected_failure"
                )
                raise StreamFinalizationError(
                    f"Unexpected error finalizing stream {self.stream_id}: {e}"
                ) from e
            finally:
                await remove_stream_from_pending_completion(self.stream_id)
                self.is_finalizing = False

    async def cancel_processing(self):
        """Cancels ongoing processing tasks for this stream."""
        # ... existing code ...
