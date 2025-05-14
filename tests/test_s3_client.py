# Unit tests for S3 client functionality
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import aioboto3  # For type hinting if needed, and for potential exceptions

# Module to test
from src import s3_client
from src.config import settings

# Constants for testing
TEST_BUCKET_NAME = "test-s3-bucket"
TEST_OBJECT_KEY = "streams/test-stream-s3/video.mp4"
TEST_UPLOAD_ID = "test-upload-id-s3"


@pytest.fixture
def mock_aioboto3_session():
    with patch("aioboto3.Session") as mock_session_constructor:
        mock_session_instance = MagicMock()  # This is the session object
        mock_s3_client_instance = AsyncMock()  # This is the S3 client object

        # Configure the session instance to return our mock_s3_client when s3_client_instance() is called
        mock_session_instance.client.return_value.__aenter__.return_value = (
            mock_s3_client_instance
        )
        mock_session_constructor.return_value = mock_session_instance
        yield mock_s3_client_instance


@pytest.mark.asyncio
async def test_create_s3_multipart_upload_success(mock_aioboto3_session: AsyncMock):
    s3 = mock_aioboto3_session
    expected_response = {"UploadId": TEST_UPLOAD_ID}
    s3.create_multipart_upload.return_value = expected_response

    upload_id = await s3_client.create_s3_multipart_upload(
        TEST_BUCKET_NAME, TEST_OBJECT_KEY
    )

    s3.create_multipart_upload.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME,
        Key=TEST_OBJECT_KEY,
        # Any other default params like ContentType if set by the client
        ContentType=settings.s3_default_content_type,
    )
    assert upload_id == TEST_UPLOAD_ID


@pytest.mark.asyncio
async def test_create_s3_multipart_upload_failure(
    mock_aioboto3_session: AsyncMock, caplog
):
    s3 = mock_aioboto3_session
    s3.create_multipart_upload.side_effect = Exception("S3 Create Upload Error")

    upload_id = await s3_client.create_s3_multipart_upload(
        TEST_BUCKET_NAME, TEST_OBJECT_KEY
    )

    assert upload_id is None
    assert (
        f"Error creating multipart upload for {TEST_OBJECT_KEY} in bucket {TEST_BUCKET_NAME}: S3 Create Upload Error"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_upload_s3_part_success(mock_aioboto3_session: AsyncMock):
    s3 = mock_aioboto3_session
    part_number = 1
    data_chunk = b"test data part"
    expected_etag = '"test-etag"'  # S3 ETags are often quoted
    s3.upload_part.return_value = {"ETag": expected_etag}

    etag = await s3_client.upload_s3_part(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
        part_number=part_number,
        data=data_chunk,
    )

    s3.upload_part.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME,
        Key=TEST_OBJECT_KEY,
        UploadId=TEST_UPLOAD_ID,
        PartNumber=part_number,
        Body=data_chunk,
    )
    assert etag == expected_etag.strip(
        '"'
    )  # Ensure quotes are stripped as per client code


@pytest.mark.asyncio
async def test_upload_s3_part_failure(mock_aioboto3_session: AsyncMock, caplog):
    s3 = mock_aioboto3_session
    part_number = 1
    data_chunk = b"test data part failure"
    s3.upload_part.side_effect = Exception("S3 Upload Part Error")

    etag = await s3_client.upload_s3_part(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
        part_number=part_number,
        data=data_chunk,
    )

    assert etag is None
    assert (
        f"Error uploading part {part_number} for {TEST_OBJECT_KEY} (Upload ID: {TEST_UPLOAD_ID}): S3 Upload Part Error"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_complete_s3_multipart_upload_success(mock_aioboto3_session: AsyncMock):
    s3 = mock_aioboto3_session
    parts_list = [
        {"PartNumber": 1, "ETag": "etag1"},
        {"PartNumber": 2, "ETag": "etag2"},
    ]
    s3.complete_multipart_upload.return_value = {
        "Location": f"s3://{TEST_BUCKET_NAME}/{TEST_OBJECT_KEY}"
    }

    success = await s3_client.complete_s3_multipart_upload(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
        parts=parts_list,
    )

    s3.complete_multipart_upload.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME,
        Key=TEST_OBJECT_KEY,
        UploadId=TEST_UPLOAD_ID,
        MultipartUpload={"Parts": parts_list},
    )
    assert success is True


@pytest.mark.asyncio
async def test_complete_s3_multipart_upload_failure(
    mock_aioboto3_session: AsyncMock, caplog
):
    s3 = mock_aioboto3_session
    parts_list = [{"PartNumber": 1, "ETag": "etag1"}]
    s3.complete_multipart_upload.side_effect = Exception("S3 Complete Upload Error")

    success = await s3_client.complete_s3_multipart_upload(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
        parts=parts_list,
    )

    assert success is False
    assert (
        f"Error completing multipart upload for {TEST_OBJECT_KEY} (Upload ID: {TEST_UPLOAD_ID}): S3 Complete Upload Error"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_abort_s3_multipart_upload_success(mock_aioboto3_session: AsyncMock):
    s3 = mock_aioboto3_session
    s3.abort_multipart_upload.return_value = (
        {}
    )  # Successful abort returns 204 No Content

    success = await s3_client.abort_s3_multipart_upload(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
    )

    s3.abort_multipart_upload.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME, Key=TEST_OBJECT_KEY, UploadId=TEST_UPLOAD_ID
    )
    assert success is True


@pytest.mark.asyncio
async def test_abort_s3_multipart_upload_failure(
    mock_aioboto3_session: AsyncMock, caplog
):
    s3 = mock_aioboto3_session
    s3.abort_multipart_upload.side_effect = Exception("S3 Abort Error")

    success = await s3_client.abort_s3_multipart_upload(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
    )

    assert success is False
    assert (
        f"Error aborting multipart upload for {TEST_OBJECT_KEY} (Upload ID: {TEST_UPLOAD_ID}): S3 Abort Error"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_upload_s3_json_data_success(mock_aioboto3_session: AsyncMock):
    s3 = mock_aioboto3_session
    json_data = '{"key": "value"}'

    s3.put_object.return_value = {}

    success = await s3_client.upload_s3_json_data(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY + ".json",  # typical for metadata
        json_data_str=json_data,
    )

    s3.put_object.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME,
        Key=TEST_OBJECT_KEY + ".json",
        Body=json_data.encode("utf-8"),
        ContentType="application/json",
    )
    assert success is True


@pytest.mark.asyncio
async def test_upload_s3_json_data_failure(mock_aioboto3_session: AsyncMock, caplog):
    s3 = mock_aioboto3_session
    json_data = '{"key": "value"}'
    object_key_json = TEST_OBJECT_KEY + ".json"
    s3.put_object.side_effect = Exception("S3 Put JSON Error")

    success = await s3_client.upload_s3_json_data(
        bucket_name=TEST_BUCKET_NAME,
        object_key=object_key_json,
        json_data_str=json_data,
    )

    assert success is False
    assert (
        f"Error uploading JSON data to {object_key_json} in bucket {TEST_BUCKET_NAME}: S3 Put JSON Error"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_close_s3_resources():
    # This test primarily ensures that close_s3_resources can be called
    # and attempts to close the session if it exists.
    mock_session_instance = AsyncMock()
    s3_client.S3_SESSION = mock_session_instance  # Assign a mock session

    await s3_client.close_s3_resources()

    mock_session_instance.close.assert_called_once()
    assert s3_client.S3_SESSION is None  # Should be reset

    # Test idempotency
    await s3_client.close_s3_resources()  # Should not raise error
    mock_session_instance.close.assert_called_once()  # Still called only once

    # Cleanup
    s3_client.S3_SESSION = None


# TODO: Add tests for list_s3_parts if it's complex enough or used directly
# (Currently list_s3_parts is not implemented in the provided s3_client.py but planned for phase 7)
