import logging
import aioboto3
from botocore.exceptions import ClientError
from .config import settings
import asyncio
import json
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)  # To be replaced by structlog

# Global S3 session
_s3_session: aioboto3.Session | None = None


def get_s3_session() -> aioboto3.Session:
    global _s3_session
    if _s3_session is None:
        _s3_session = aioboto3.Session()
    return _s3_session


async def create_s3_multipart_upload(bucket_name: str, object_key: str) -> str | None:
    session = get_s3_session()
    async with session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        region_name=settings.s3_region_name,
    ) as s3:
        try:
            response = await s3.create_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                ContentType=settings.s3_default_content_type,
                # TODO: Consider adding Metadata like original filename, content_type if known
                # ContentType='video/mp4' # If known
            )
            upload_id = response.get("UploadId")
            logger.info(
                f"Initiated S3 multipart upload for {bucket_name}/{object_key}. Upload ID: {upload_id}"
            )
            return upload_id
        except ClientError as e:
            logger.error(
                f"Error creating S3 multipart upload for {bucket_name}/{object_key}: {e}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error during S3 multipart upload initiation for {bucket_name}/{object_key}: {e}",
                exc_info=True,
            )
            return None


async def close_s3_resources():
    # aioboto3 clients are typically managed with context managers (async with)
    # Explicitly closing a global session isn't standard if clients are short-lived.
    # If a long-lived client were used, it would be closed here.
    logger.info(
        "S3 resources are managed by context managers; no explicit global close needed for session."
    )
    pass


async def upload_s3_part(
    bucket_name: str, object_key: str, upload_id: str, part_number: int, data: bytes
) -> str | None:
    session = get_s3_session()
    async with session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        region_name=settings.s3_region_name,
    ) as s3:
        try:
            response = await s3.upload_part(
                Bucket=bucket_name,
                Key=object_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=data,
            )
            etag = response.get("ETag")
            # ETag from S3 often has quotes around it, which should be preserved for CompleteMultipartUpload
            logger.debug(
                f"Uploaded part {part_number} for {object_key} (UploadID: {upload_id}), ETag: {etag}"
            )
            return etag
        except ClientError as e:
            logger.error(
                f"Error uploading S3 part {part_number} for {object_key} (Upload ID: {upload_id}): {e}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error during S3 part upload for {object_key}, part {part_number}: {e}",
                exc_info=True,
            )
            return None


async def complete_s3_multipart_upload(
    bucket_name: str, object_key: str, upload_id: str, parts: list[dict]
) -> bool:
    """Completes a multipart upload. parts should be a list of {'PartNumber': int, 'ETag': str, 'Size': int, 'UploadedAtUTC': str}."""
    session = get_s3_session()
    async with session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        region_name=settings.s3_region_name,
    ) as s3:
        if not parts:
            logger.warning(
                f"No parts provided for completing multipart upload {upload_id} for {object_key}. Aborting instead."
            )
            await abort_s3_multipart_upload(bucket_name, object_key, upload_id)
            return False

        # Format parts for S3 complete_multipart_upload API call
        # It expects a list of dicts, each with only 'PartNumber' and 'ETag'
        s3_api_parts = [
            {"PartNumber": p["PartNumber"], "ETag": p["ETag"]}
            for p in parts
            if "PartNumber" in p and "ETag" in p  # Ensure keys exist
        ]

        if not s3_api_parts:
            logger.warning(
                f"No valid parts with PartNumber and ETag found for completing multipart upload {upload_id} for {object_key} after filtering. Original parts: {parts}. Aborting."
            )
            await abort_s3_multipart_upload(bucket_name, object_key, upload_id)
            return False

        try:
            logger.info(
                f"Completing S3 multipart upload for {object_key}, Upload ID: {upload_id} with {len(s3_api_parts)} parts."
            )
            await s3.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": s3_api_parts},
            )
            logger.info(
                f"Successfully completed S3 multipart upload for {object_key}, Upload ID: {upload_id}."
            )
            return True
        except ClientError as e:
            logger.error(
                f"Error completing S3 multipart upload {upload_id} for {object_key}: {e}",
                exc_info=True,
            )
            return False
        except Exception as e:  # Catch any other unexpected errors
            logger.error(
                f"Unexpected error completing S3 multipart upload {upload_id} for {object_key}: {e}",
                exc_info=True,
            )
            return False


async def abort_s3_multipart_upload(
    bucket_name: str, object_key: str, upload_id: str
) -> bool:
    session = get_s3_session()
    async with session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        region_name=settings.s3_region_name,
    ) as s3:
        try:
            logger.info(
                f"Aborting S3 multipart upload for {object_key}, Upload ID: {upload_id}."
            )
            await s3.abort_multipart_upload(
                Bucket=bucket_name, Key=object_key, UploadId=upload_id
            )
            logger.info(
                f"Successfully aborted S3 multipart upload for {object_key}, Upload ID: {upload_id}."
            )
            return True
        except ClientError as e:
            logger.error(
                f"Error aborting S3 multipart upload {upload_id} for {object_key}: {e}",
                exc_info=True,
            )
            return False
        except Exception as e:  # Catch any other unexpected errors
            logger.error(
                f"Unexpected error aborting S3 multipart upload {upload_id} for {object_key}: {e}",
                exc_info=True,
            )
            return False


async def upload_s3_json_data(
    bucket_name: str, object_key: str, json_data_str: str
) -> bool:
    logger.warning(f"[NOT IMPLEMENTED] upload_s3_json_data called for {object_key}")
    # TODO: Implement in Phase 6
    return True  # Placeholder


async def list_s3_parts(
    bucket_name: str, object_key: str, upload_id: str
) -> list[dict] | None:
    logger.warning(f"[NOT IMPLEMENTED] list_s3_parts called for {object_key}")
    # TODO: Implement in Phase 7 (Resume Logic)
    return []  # Placeholder, return empty list


async def upload_json_to_s3(bucket_name: str, object_key: str, data: dict) -> bool:
    session = get_s3_session()
    async with session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        region_name=settings.s3_region_name,
    ) as s3:
        try:
            json_bytes = json.dumps(data, indent=2).encode("utf-8")
            await s3.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=json_bytes,
                ContentType="application/json",
            )
            logger.info(
                f"Successfully uploaded JSON metadata to s3://{bucket_name}/{object_key}"
            )
            return True
        except ClientError as e:
            logger.error(
                f"Error uploading JSON to S3 for {object_key}: {e}", exc_info=True
            )
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error during JSON S3 upload for {object_key}: {e}",
                exc_info=True,
            )
            return False


# Other S3 functions (upload_part, complete_multipart_upload, etc.) will be added in later phases.
