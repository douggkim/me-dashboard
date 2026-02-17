"""Unit tests for screen time lambda."""

import importlib.util
import json
import os
import sys
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest

# dynamic import for hyphenated file
MODULE_PATH = Path(__file__).parent / "../../../lambda_code/screen-time-collection.py"
MODULE_NAME = "screen_time_collection"

spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
screen_time_collection = importlib.util.module_from_spec(spec)
sys.modules[MODULE_NAME] = screen_time_collection
spec.loader.exec_module(screen_time_collection)


@pytest.fixture
def mock_env() -> None:
    """
    Mock environment variables.

    Yields
    ------
    None
        Context manager yield.
    """
    # Patch the module-level variable bucket_name
    with (
        mock.patch.object(screen_time_collection, "bucket_name", "test-bucket"),
        mock.patch.dict(
            os.environ,
            {
                "S3_BUCKET": "test-bucket",
                "AUTH_TOKEN": "test-token",
                "STORAGE_LOCATION": "test-location",
            },
        ),
    ):
        yield


@pytest.fixture
def mock_s3() -> MagicMock:
    """
    Mock S3 client.

    Yields
    ------
    MagicMock
        The mocked S3 client.
    """
    with mock.patch("screen_time_collection.s3_client") as mock_s3:
        yield mock_s3


def test_authenticate_request_success(mock_env: None) -> None:
    """Test successful authentication."""
    _ = mock_env  # Unused arg
    headers = {"Authorization": "Bearer test-token"}
    response = screen_time_collection._authenticate_request(headers)
    assert response is None


def test_authenticate_request_missing_header(mock_env: None) -> None:
    """Test missing authorization header."""
    _ = mock_env
    headers = {}
    response = screen_time_collection._authenticate_request(headers)
    assert response["statusCode"] == 401
    assert json.loads(response["body"]) == {"error": "Missing Authorization header"}


def test_authenticate_request_invalid_token(mock_env: None) -> None:
    """Test invalid token."""
    _ = mock_env
    headers = {"Authorization": "Bearer wrong-token"}
    response = screen_time_collection._authenticate_request(headers)
    assert response["statusCode"] == 401
    assert json.loads(response["body"]) == {"error": "Invalid token"}


def test_handle_mac_data() -> None:
    """Test Mac data processing."""
    raw_data = {
        "device_type": "mac",
        "data": [
            {"total_usage_seconds": 100},
            {"total_usage_seconds": 200},
        ],
    }
    meta = screen_time_collection._handle_mac_data(raw_data)
    assert meta["device_class"] == "mac"
    assert meta["total_seconds"] == 300
    assert meta["app_count"] == 2


def test_handle_iphone_data() -> None:
    """Test iPhone data processing."""
    raw_data = {"device_type": "iphone", "usage_seconds": 500}
    meta = screen_time_collection._handle_iphone_data(raw_data)
    assert meta["device_class"] == "iphone"
    assert meta["total_seconds"] == 500
    assert meta["app_count"] == 1


def test_lambda_handler_success_mac(mock_env: None, mock_s3: MagicMock) -> None:
    """Test successful lambda execution for Mac data."""
    _ = mock_env
    event = {
        "headers": {"Authorization": "Bearer test-token"},
        "body": json.dumps({
            "device_type": "mac",
            "usage_date": "2023-01-01",
            "device_id": "test-device",
            "data": [{"total_usage_seconds": 60}],
        }),
    }
    context = MagicMock()
    context.aws_request_id = "test-request-id"

    response = screen_time_collection.lambda_handler(event, context)

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["result"] == "ok"
    assert body["device"] == "mac"
    assert body["total_seconds"] == 60

    mock_s3.put_object.assert_called_once()
    call_args = mock_s3.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    assert "test-location/mac/2023_01_01/mac-test-request-id.json" in call_args["Key"]


def test_lambda_handler_success_iphone(mock_env: None, mock_s3: MagicMock) -> None:
    """Test successful lambda execution for iPhone data."""
    _ = mock_env
    event = {
        "headers": {"Authorization": "Bearer test-token"},
        "body": json.dumps({
            "device_type": "iphone",
            "usage_date": "2023-01-02",
            "device_id": "test-iphone",
            "usage_seconds": 120,
        }),
    }
    context = MagicMock()
    context.aws_request_id = "test-req-2"

    response = screen_time_collection.lambda_handler(event, context)

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["result"] == "ok"
    assert body["device"] == "iphone"
    assert body["total_seconds"] == 120

    mock_s3.put_object.assert_called_once()
    call_args = mock_s3.put_object.call_args[1]
    assert "test-location/iphone/2023_01_02/iphone-test-req-2.json" in call_args["Key"]


def test_lambda_handler_auth_fail(mock_env: None, mock_s3: MagicMock) -> None:
    """Test lambda handler returns 401 on auth failure."""
    _ = mock_env
    event = {"headers": {"Authorization": "Bearer wrong-token"}}
    response = screen_time_collection.lambda_handler(event, None)
    assert response["statusCode"] == 401
    mock_s3.put_object.assert_not_called()


def test_lambda_handler_exception(mock_env: None, mock_s3: MagicMock) -> None:
    """Test general exception handling."""
    _ = (mock_env, mock_s3)
    event = {
        "headers": {"Authorization": "Bearer test-token"},
        "body": "invalid-json",
    }
    response = screen_time_collection.lambda_handler(event, None)
    assert response["statusCode"] == 500
    assert "error" in json.loads(response["body"])
