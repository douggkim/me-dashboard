"""Lambda code for collecting and saving health data to S3."""

import base64
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

    logger.info(f"Authorization header: {auth_header}")

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


def _log_health_metrics(health_data: dict[str, any]) -> int:
    """
    Log health metrics summary.

    Parameters
    ----------
    health_data : dict[str, any]
        Parsed health data containing metrics information.

    Returns
    -------
    int
        Total number of metric data points processed.
    """
    metrics = health_data.get("data", {}).get("metrics", [])
    total_data_points = 0

    logger.info(f"📊 Health Data received with {len(metrics)} metric types:")

    for metric in metrics:
        metric_name = metric.get("name", "unknown")
        metric_units = metric.get("units", "unknown")
        metric_data = metric.get("data", [])
        
        logger.info(f"  📈 {metric_name} ({metric_units}): {len(metric_data)} data points")
        total_data_points += len(metric_data)

    return total_data_points


def lambda_handler(event: dict[str, any], context: any) -> dict[str, any]:
    """
    AWS Lambda handler for processing health data.

    Receives health metrics data via HTTP POST request, authenticates the request,
    processes the health data (handling base64 encoding if present), and stores it to S3.

    Parameters
    ----------
    event : dict[str, any]
        AWS Lambda event object containing HTTP request data including headers,
        body with health metrics, and other request metadata.
    context : any
        AWS Lambda context object containing runtime information including
        request ID and execution details.

    Returns
    -------
    dict[str, any]
        HTTP response dictionary containing statusCode, headers, and JSON body.
        Returns 200 on success with processing summary, 401 for authentication
        errors, 400 for invalid JSON/base64, or 500 for unexpected errors.

    Raises
    ------
    json.JSONDecodeError
        When the request body contains invalid JSON data.
    Exception
        For any unexpected errors during processing or S3 storage.
    KeyError
        When required environment variables (S3_BUCKET, HEALTH_TOKEN) are missing.
    boto3.exceptions.Boto3Error
        When S3 operations fail due to permissions or connectivity issues.
    """
    try:
        # Log the entire incoming event for debugging
        logger.info("Received health data event:")
        logger.info(json.dumps(event, indent=2, default=str))

        # Extract headers and authenticate
        headers = event.get("headers", {})
        auth_error = _authenticate_request(headers)
        if auth_error:
            return auth_error

        # Parse the health data
        body = event.get("body", "{}")
        
        # Handle base64 encoded body if present
        if event.get("isBase64Encoded", False):
            logger.info("📦 Decoding base64 encoded body")
            try:
                body = base64.b64decode(body).decode('utf-8')
            except Exception as e:
                logger.error(f"❌ Failed to decode base64 body: {e}")
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "Failed to decode base64 body", "details": str(e)}),
                }

        # Parse JSON
        health_data: dict[str, any] = json.loads(body) if isinstance(body, str) else body

        # Log health metrics summary
        total_data_points = _log_health_metrics(health_data)

        # Store to S3
        timestamp = datetime.now(UTC)
        file_name = f"{timestamp.strftime('%H%M%S')}-{context.aws_request_id}.json"
        storage_location = os.getenv("STORAGE_LOCATION")
        date_directory = f"{timestamp.strftime('%Y_%m_%d')}"

        file_key = f"{storage_location}/{date_directory}/{file_name}"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(health_data, indent=2),
            ContentType="application/json",
            Metadata={
                "source": "health-app",
                "metric_types": str(len(health_data.get("data", {}).get("metrics", []))),
                "data_points": str(total_data_points),
                "timestamp": timestamp.isoformat(),
            },
        )

        logger.info(f"✅ Stored {total_data_points} health data points to s3://{bucket_name}/{file_key}")

        # Return success response
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "result": "ok",
                "metric_types_processed": len(health_data.get("data", {}).get("metrics", [])),
                "data_points_processed": total_data_points,
                "timestamp": datetime.now(UTC).isoformat(),
            }),
        }

    except json.JSONDecodeError as e:
        logger.exception("❌ JSON parsing error")
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({"error": "Invalid JSON in request body", "details": str(e)}),
        }

    except Exception as e:
        logger.exception("❌ Unexpected error")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({"error": "Internal server error", "details": str(e)}),
        }