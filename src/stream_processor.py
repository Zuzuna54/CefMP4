import asyncio
import logging
import os  # For file_path.stat() if needed, though pathlib.Path handles it
from pathlib import Path
import datetime  # For timestamping parts later if needed directly here

from .config import settings

# Import Redis and S3 client functions will be done dynamically inside methods
# to avoid circular dependencies at module import time, especially if those
# clients might one day import StreamProcessor or related types.

logger = logging.getLogger(__name__)  # To be replaced by structlog


class StreamProcessor:
    def __init__(
        self,
        stream_id: str,
        file_path: Path,
        s3_upload_id: str,
        s3_bucket: str,
        s3_key_prefix: str,
    ):
        self.stream_id = stream_id
        self.file_path = file_path
        self.s3_upload_id = s3_upload_id
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix

        self.current_file_offset = 0  # Bytes already read and processed
        self.next_part_number = 1  # S3 part numbers are 1-indexed
        self.uploaded_parts_info = (
            []
        )  # List of {"PartNumber": int, "ETag": str, "Size": int}
        self.is_processing = (
            False  # To indicate if a process_file_write is active (conceptual)
        )
        self.lock = (
            asyncio.Lock()
        )  # To prevent concurrent processing of the same file path by this instance

    async def _initialize_from_checkpoint(self):
        # This method will be fully implemented in Phase 7 (Resume Logic) to load state from Redis.
        # For Phase 4 (new streams), it is called but will likely find no existing state in Redis.
        from src.redis_client import (
            get_stream_next_part,
            get_stream_bytes_sent,
            get_stream_parts,
        )

        next_part_redis = await get_stream_next_part(self.stream_id)
        if next_part_redis is not None:
            self.next_part_number = next_part_redis

        bytes_sent_redis = await get_stream_bytes_sent(self.stream_id)
        if bytes_sent_redis is not None:
            self.current_file_offset = bytes_sent_redis

        parts_redis = await get_stream_parts(self.stream_id)
        if parts_redis:
            self.uploaded_parts_info = parts_redis

        logger.info(
            f"[{self.stream_id}] Initialized processor state (or defaults): next_part={self.next_part_number}, offset={self.current_file_offset}, known_parts={len(self.uploaded_parts_info)}"
        )

    async def process_file_write(self):
        """Handles a WRITE event, reads new data, chunks, and uploads."""
        async with self.lock:
            if not self.file_path.exists():
                logger.warning(
                    f"[{self.stream_id}] File {self.file_path} no longer exists. Stopping processing."
                )
                return

            try:
                self.is_processing = True
                from src.s3_client import upload_s3_part
                from src.redis_client import (
                    set_stream_next_part,
                    incr_stream_bytes_sent,
                    add_stream_part_info,
                    update_stream_last_activity,
                )

                with open(self.file_path, "rb") as f:
                    f.seek(self.current_file_offset)
                    processed_in_this_pass = False
                    while True:
                        current_disk_file_size = self.file_path.stat().st_size
                        if current_disk_file_size <= self.current_file_offset:
                            if not processed_in_this_pass:
                                logger.debug(
                                    f"[{self.stream_id}] No new data for {self.file_path} (disk size: {current_disk_file_size}, offset: {self.current_file_offset})."
                                )
                            break

                        if not processed_in_this_pass:
                            logger.info(
                                f"[{self.stream_id}] Processing WRITE for {self.file_path}. Current disk size: {current_disk_file_size}, Offset: {self.current_file_offset}"
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
                        logger.debug(
                            f"[{self.stream_id}] Read chunk of size {chunk_len}, part num {self.next_part_number}"
                        )

                        part_etag = await upload_s3_part(
                            bucket_name=self.s3_bucket,
                            object_key=self.s3_key_prefix,
                            upload_id=self.s3_upload_id,
                            part_number=self.next_part_number,
                            data=chunk,
                        )

                        if part_etag:
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
                            logger.info(
                                f"[{self.stream_id}] Uploaded part {self.next_part_number-1}, ETag: {part_etag}, new offset: {self.current_file_offset}"
                            )
                        else:
                            logger.error(
                                f"[{self.stream_id}] Failed to upload part {self.next_part_number}. Halting stream for this pass."
                            )
                            self.is_processing = False
                            return

                    if processed_in_this_pass:
                        logger.info(
                            f"[{self.stream_id}] Finished processing pass for {self.file_path}, current offset {self.current_file_offset}"
                        )

            except FileNotFoundError:
                logger.warning(
                    f"[{self.stream_id}] File {self.file_path} disappeared during processing."
                )
                # TODO: Mark stream as failed/aborted (Phase 8)
            except Exception as e:
                logger.error(
                    f"[{self.stream_id}] Error processing file write for {self.file_path}: {e}",
                    exc_info=True,
                )
                # TODO: Mark stream as failed (Phase 8)
            finally:
                self.is_processing = False

    async def finalize_stream(self):
        """Handles stream finalization: complete S3 multipart upload and then generate/upload metadata JSON."""
        logger.info(f"[{self.stream_id}] Finalizing stream for {self.file_path}.")
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
                add_stream_to_completed_set,
                remove_stream_keys,
            )
            from src.metadata_generator import generate_metadata_json

            try:
                s3_parts_for_completion = await get_stream_parts(self.stream_id)
                s3_parts_for_completion = [
                    p for p in s3_parts_for_completion if p.get("ETag")
                ]

                if not s3_parts_for_completion:
                    logger.warning(
                        f"[{self.stream_id}] No parts for {self.file_path}. Aborting upload."
                    )
                    await abort_s3_multipart_upload(
                        self.s3_bucket, self.s3_key_prefix, self.s3_upload_id
                    )
                    await set_stream_status(self.stream_id, "aborted_no_parts")
                    await remove_stream_from_pending_completion(self.stream_id)
                    return

                s3_upload_success = await complete_s3_multipart_upload(
                    bucket_name=self.s3_bucket,
                    object_key=self.s3_key_prefix,
                    upload_id=self.s3_upload_id,
                    parts=s3_parts_for_completion,
                )

                if s3_upload_success:
                    logger.info(
                        f"[{self.stream_id}] S3 multipart upload completed for {self.file_path}."
                    )
                    await set_stream_status(self.stream_id, "s3_completed")

                    metadata_obj = await generate_metadata_json(
                        self.stream_id, self.file_path
                    )
                    if metadata_obj:
                        metadata_s3_key = f"{self.s3_key_prefix}.metadata.json"
                        metadata_upload_success = await upload_json_to_s3(
                            bucket_name=self.s3_bucket,
                            object_key=metadata_s3_key,
                            data=metadata_obj,
                        )
                        if metadata_upload_success:
                            logger.info(
                                f"[{self.stream_id}] Metadata JSON uploaded to S3: {metadata_s3_key}"
                            )
                            await set_stream_status(self.stream_id, "completed")
                            await add_stream_to_completed_set(self.stream_id)
                            await remove_stream_keys(self.stream_id)
                        else:
                            logger.error(
                                f"[{self.stream_id}] Failed to upload metadata JSON for {self.file_path}."
                            )
                            await set_stream_status(
                                self.stream_id, "failed_metadata_upload"
                            )
                    else:
                        logger.error(
                            f"[{self.stream_id}] Failed to generate metadata JSON for {self.file_path}."
                        )
                        await set_stream_status(
                            self.stream_id, "failed_metadata_generation"
                        )
                else:
                    logger.error(
                        f"[{self.stream_id}] Failed S3 complete for {self.file_path}."
                    )
                    await set_stream_status(self.stream_id, "failed_s3_complete")

                await remove_stream_from_pending_completion(self.stream_id)

            except Exception as e:
                logger.error(
                    f"[{self.stream_id}] Error during stream finalization (Phase 6 extensions) for {self.file_path}: {e}",
                    exc_info=True,
                )
                await set_stream_status(self.stream_id, "failed_finalization_meta")
                await remove_stream_from_pending_completion(self.stream_id)

    async def cancel_processing(self):
        """Cancels ongoing processing tasks for this stream."""
        # ... existing code ...
