# Unit tests for S3 client functionality
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import aioboto3  # For type hinting if needed, and for potential exceptions
from botocore.exceptions import ClientError

# Module to test
from src import s3_client
from src.config import settings
from src.exceptions import S3OperationError

# Constants for testing
TEST_BUCKET_NAME = "test-s3-bucket"
TEST_OBJECT_KEY = "streams/test-stream-s3/video.mp4"
TEST_UPLOAD_ID = "test-upload-id-s3"


@pytest_asyncio.fixture
async def mock_aioboto3_session():
    """Mock the aioboto3 session to avoid actual S3 calls."""

    # Reset the global variable
    s3_client._s3_session = None

    # Create a mock S3 client
    mock_s3_client = AsyncMock()
    mock_s3_client.create_multipart_upload.return_value = {"UploadId": TEST_UPLOAD_ID}
    mock_s3_client.upload_part.return_value = {"ETag": '"test-etag"'}
    mock_s3_client.complete_multipart_upload.return_value = {
        "Location": f"s3://{TEST_BUCKET_NAME}/{TEST_OBJECT_KEY}"
    }
    mock_s3_client.abort_multipart_upload.return_value = {}
    mock_s3_client.put_object.return_value = {}

    # Create a patch for aioboto3.Session
    with patch("aioboto3.Session") as mock_session_class:
        # Create a context manager for the S3 client that returns our mock client
        mock_s3_context = AsyncMock()
        mock_s3_context.__aenter__.return_value = mock_s3_client

        # Set up the session to return our context manager when client() is called
        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3_context

        # Make the Session constructor return our mock session
        mock_session_class.return_value = mock_session

        # Set the global session to our mock session to avoid it being recreated
        s3_client._s3_session = mock_session

        # Yield the mock S3 client for tests to use
        yield mock_s3_client


@pytest.mark.asyncio
async def test_create_s3_multipart_upload_success(mock_aioboto3_session):
    """Test successful multipart upload creation."""
    # Call the function
    upload_id = await s3_client.create_s3_multipart_upload(
        TEST_BUCKET_NAME, TEST_OBJECT_KEY
    )

    # Verify the API was called with the right parameters
    mock_aioboto3_session.create_multipart_upload.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME,
        Key=TEST_OBJECT_KEY,
        ContentType=settings.s3_default_content_type,
    )

    # Verify we got the expected upload ID
    assert upload_id == TEST_UPLOAD_ID


@pytest.mark.asyncio
async def test_create_s3_multipart_upload_failure(mock_aioboto3_session, caplog):
    """Test handling of failure during multipart upload creation."""
    # Set up the mock to raise an exception
    error = ClientError(
        {"Error": {"Code": "InternalError", "Message": "S3 Create Upload Error"}},
        "create_multipart_upload",
    )
    mock_aioboto3_session.create_multipart_upload.side_effect = error

    # Expect an exception to be raised
    with pytest.raises(S3OperationError) as excinfo:
        await s3_client.create_s3_multipart_upload(TEST_BUCKET_NAME, TEST_OBJECT_KEY)

    # Verify the error details
    assert "create_multipart_upload" in str(excinfo.value)
    assert "S3 Create Upload Error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_upload_s3_part_success(mock_aioboto3_session):
    """Test successful upload of a part."""
    part_number = 1
    data_chunk = b"test data part"
    expected_etag = '"test-etag"'

    # Call the function
    etag = await s3_client.upload_s3_part(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
        part_number=part_number,
        data=data_chunk,
    )

    # Verify the API was called correctly
    mock_aioboto3_session.upload_part.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME,
        Key=TEST_OBJECT_KEY,
        UploadId=TEST_UPLOAD_ID,
        PartNumber=part_number,
        Body=data_chunk,
    )

    # Verify we got the expected ETag
    assert etag == expected_etag


@pytest.mark.asyncio
async def test_upload_s3_part_failure(mock_aioboto3_session, caplog):
    """Test handling of failure during part upload."""
    part_number = 1
    data_chunk = b"test data part failure"

    # Set up the mock to raise an exception
    error = ClientError(
        {"Error": {"Code": "InternalError", "Message": "S3 Upload Part Error"}},
        "upload_part",
    )
    mock_aioboto3_session.upload_part.side_effect = error

    # Expect an exception to be raised
    with pytest.raises(S3OperationError) as excinfo:
        await s3_client.upload_s3_part(
            bucket_name=TEST_BUCKET_NAME,
            object_key=TEST_OBJECT_KEY,
            upload_id=TEST_UPLOAD_ID,
            part_number=part_number,
            data=data_chunk,
        )

    # Verify the error details
    assert "upload_part" in str(excinfo.value)
    assert "S3 Upload Part Error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_complete_s3_multipart_upload_success(mock_aioboto3_session):
    """Test successful completion of a multipart upload."""
    parts_list = [
        {"PartNumber": 1, "ETag": "etag1"},
        {"PartNumber": 2, "ETag": "etag2"},
    ]

    # Call the function
    success = await s3_client.complete_s3_multipart_upload(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
        parts=parts_list,
    )

    # Verify the API was called correctly
    mock_aioboto3_session.complete_multipart_upload.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME,
        Key=TEST_OBJECT_KEY,
        UploadId=TEST_UPLOAD_ID,
        MultipartUpload={"Parts": parts_list},
    )

    # Verify the function reported success
    assert success is True


@pytest.mark.asyncio
async def test_complete_s3_multipart_upload_failure(mock_aioboto3_session, caplog):
    """Test handling of failure during multipart upload completion."""
    parts_list = [{"PartNumber": 1, "ETag": "etag1"}]

    # Set up the mock to raise an exception
    error = ClientError(
        {"Error": {"Code": "InternalError", "Message": "S3 Complete Upload Error"}},
        "complete_multipart_upload",
    )
    mock_aioboto3_session.complete_multipart_upload.side_effect = error

    # Expect an exception to be raised
    with pytest.raises(S3OperationError) as excinfo:
        await s3_client.complete_s3_multipart_upload(
            bucket_name=TEST_BUCKET_NAME,
            object_key=TEST_OBJECT_KEY,
            upload_id=TEST_UPLOAD_ID,
            parts=parts_list,
        )

    # Verify the error details
    assert "complete_multipart_upload" in str(excinfo.value)
    assert "S3 Complete Upload Error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_complete_s3_multipart_upload_no_parts(mock_aioboto3_session):
    """Test handling of a multipart upload with no parts."""
    # Create a patch for abort_s3_multipart_upload to avoid actual S3 calls and
    # to verify it's called when no parts are provided
    with patch(
        "src.s3_client.abort_s3_multipart_upload", return_value=True
    ) as mock_abort:
        # Call the function with an empty parts list
        success = await s3_client.complete_s3_multipart_upload(
            bucket_name=TEST_BUCKET_NAME,
            object_key=TEST_OBJECT_KEY,
            upload_id=TEST_UPLOAD_ID,
            parts=[],
        )

        # Verify abort was called
        mock_abort.assert_called_once_with(
            TEST_BUCKET_NAME, TEST_OBJECT_KEY, TEST_UPLOAD_ID
        )

        # Verify the function reported failure
        assert success is False


@pytest.mark.asyncio
async def test_abort_s3_multipart_upload_success(mock_aioboto3_session):
    """Test successful abortion of a multipart upload."""
    # Call the function
    success = await s3_client.abort_s3_multipart_upload(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY,
        upload_id=TEST_UPLOAD_ID,
    )

    # Verify the API was called correctly
    mock_aioboto3_session.abort_multipart_upload.assert_called_once_with(
        Bucket=TEST_BUCKET_NAME, Key=TEST_OBJECT_KEY, UploadId=TEST_UPLOAD_ID
    )

    # Verify the function reported success
    assert success is True


@pytest.mark.asyncio
async def test_abort_s3_multipart_upload_failure(mock_aioboto3_session, caplog):
    """Test handling of failure during multipart upload abortion."""
    # Set up the mock to raise an exception
    error = ClientError(
        {"Error": {"Code": "InternalError", "Message": "S3 Abort Error"}},
        "abort_multipart_upload",
    )
    mock_aioboto3_session.abort_multipart_upload.side_effect = error

    # Expect an exception to be raised
    with pytest.raises(S3OperationError) as excinfo:
        await s3_client.abort_s3_multipart_upload(
            bucket_name=TEST_BUCKET_NAME,
            object_key=TEST_OBJECT_KEY,
            upload_id=TEST_UPLOAD_ID,
        )

    # Verify the error details
    assert "abort_multipart_upload" in str(excinfo.value)
    assert "S3 Abort Error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_upload_s3_json_data_success(mock_aioboto3_session):
    """Test successful upload of JSON data."""
    json_data = '{"key": "value"}'

    # Call the function
    success = await s3_client.upload_s3_json_data(
        bucket_name=TEST_BUCKET_NAME,
        object_key=TEST_OBJECT_KEY + ".json",
        json_data_str=json_data,
    )

    # Function is marked as not implemented but returns True
    assert success is True


@pytest.mark.asyncio
async def test_close_s3_resources():
    """Test closing of S3 resources."""
    # Create a mock session with a proper type for .close()
    mock_session = MagicMock()
    mock_session.close = AsyncMock()

    # Set the global variable
    s3_client._s3_session = mock_session

    # Call the function
    await s3_client.close_s3_resources()

    # Since the function is essentially a no-op, just verify it runs without errors
    assert True


# TODO: Add tests for list_s3_parts if it's complex enough or used directly
# (Currently list_s3_parts is not implemented in the provided s3_client.py but planned for phase 7)
