"""Lambda code for collecting and saving health/workout data to S3."""

import base64
import csv
import io
import json
import logging
import os
from datetime import UTC, datetime

import boto3

# Initialize S3 client
s3_client = boto3.client("s3")
bucket_name = os.environ.get("S3_BUCKET")

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _authenticate_request(headers: dict[str, str]) -> dict[str, any] | None:
    """
    Authenticate the incoming request using Bearer token.

    Parameters
    ----------
    headers : dict[str, str]
        HTTP headers from the incoming request, potentially containing
        authorization information.

    Returns
    -------
    dict[str, any] | None
        Returns None if authentication is successful. Returns a dict containing
        error response with statusCode, headers, and body if authentication fails.

    Raises
    ------
    KeyError
        If expected environment variables are not set.
    """
    # API Gateway might lowercase headers, so check both cases
    auth_header = headers.get("authorization") or headers.get("Authorization", "")

    # Check for Bearer token
    if not auth_header.startswith("Bearer "):
        logger.warning("Missing or invalid Authorization header")
        return {
            "statusCode": 401,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "error": "Missing Authorization header",
                "expected": "Authorization: Bearer <token>",
            }),
        }

    # Extract the token
    token = auth_header.split(" ")[1] if len(auth_header.split(" ")) > 1 else ""

    # Validate token
    expected_token = os.getenv("HEALTH_TOKEN")

    if token != expected_token:
        logger.warning("Invalid token received")
        return {
            "statusCode": 403,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "error": "Invalid token",
            }),
        }

    logger.info("✅ Authentication successful")
    return None


def _log_csv_data(csv_content: str, data_type: str) -> tuple[list[str], int]:
    """
    Log CSV data summary.

    Parameters
    ----------
    csv_content : str
        CSV content as string.
    data_type : str
        Type of data (health, workout, etc.).

    Returns
    -------
    tuple[list[str], int]
        Tuple of (column_names, row_count).
    """
    try:
        csv_reader = csv.reader(io.StringIO(csv_content))
        rows = list(csv_reader)

        if not rows:
            logger.warning("📊 Empty CSV data received")
            return [], 0

        # Headers start from the 4th row (index 3)
        if len(rows) < 5:
            logger.warning("📊 CSV has fewer than 4 rows - no headers found")
            return [], 0

        headers = rows[4]  # 4th row contains headers
        data_rows = rows[5:]  # Data starts from 5th row

        logger.info(f"📊 {data_type.title()} CSV Data received:")
        logger.info(f"  📋 Columns ({len(headers)}): {', '.join(headers)}")
        logger.info(f"  📈 Data rows: {len(data_rows)}")

        return headers, len(data_rows)

    except Exception:
        logger.exception("❌ Failed to parse CSV")
        return [], 0


def lambda_handler(event: dict[str, any], context: any) -> dict[str, any]:
    """
    AWS Lambda handler for processing health/workout CSV data.

    Receives CSV data via HTTP POST request (base64 encoded), authenticates the request,
    processes the CSV data based on data_type header, and stores it to S3.

    Parameters
    ----------
    event : dict[str, any]
        AWS Lambda event object containing HTTP request data including headers,
        body with base64-encoded CSV data, and other request metadata.
    context : any
        AWS Lambda context object containing runtime information including
        request ID and execution details.

    Returns
    -------
    dict[str, any]
        HTTP response dictionary containing statusCode, headers, and JSON body.
        Returns 200 on success with processing summary, 401 for authentication
        errors, 400 for invalid CSV/base64, or 500 for unexpected errors.

    Raises
    ------
    Exception
        For any unexpected errors during processing or S3 storage.
    ValueError
        The header 'data_type' was not provided properly.
    KeyError
        When required environment variables (S3_BUCKET, HEALTH_TOKEN) are missing.
    boto3.exceptions.Boto3Error
        When S3 operations fail due to permissions or connectivity issues.
    """
    try:
        # Log the entire incoming event for debugging
        logger.info("Received data collection event:")
        logger.info(json.dumps(event, indent=2, default=str))

        # Extract headers and authenticate
        headers = event.get("headers", {})
        auth_error = _authenticate_request(headers)
        if auth_error:
            return auth_error

        # Extract data_type from headers
        data_type = headers.get("data_type") or headers.get("Data-Type")
        if data_type is None:
            raise ValueError("The data has to include 'data_type' header. Please check the message.")
        logger.info(f"📊 Processing {data_type} data")

        # Parse the CSV data - expect base64 encoded
        body = event.get("body", "")

        # Decode base64 encoded body (always expected for CSV)
        logger.info("📦 Decoding base64 encoded CSV body")
        try:
            csv_content = base64.b64decode(body).decode("utf-8")
        except Exception:
            logger.exception("❌ Failed to decode base64 body")
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Failed to decode base64 body"}),
            }

        # Log CSV data summary
        columns, row_count = _log_csv_data(csv_content, data_type)

        # Store to S3
        timestamp = datetime.now(UTC)
        file_name = f"{timestamp.strftime('%H%M%S')}-{context.aws_request_id}.csv"
        base_storage_location = os.getenv("STORAGE_LOCATION", "bronze")
        date_directory = f"{timestamp.strftime('%Y_%m_%d')}"

        file_key = f"{base_storage_location}/{data_type}/{date_directory}/{file_name}"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_content,
            ContentType="text/csv",
            Metadata={
                "source": f"{data_type}-app",
                "data_type": data_type,
                "columns": str(len(columns)),
                "rows": str(row_count),
                "timestamp": timestamp.isoformat(),
            },
        )

        logger.info(f"✅ Stored {row_count} {data_type} data rows to s3://{bucket_name}/{file_key}")

        # Return success response
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "result": "ok",
                "data_type": data_type,
                "columns_processed": len(columns),
                "rows_processed": row_count,
                "timestamp": datetime.now(UTC).isoformat(),
            }),
        }

    except Exception:
        logger.exception("❌ Unexpected error")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({"error": "Internal server error"}),
        }
