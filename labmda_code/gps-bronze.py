"""Lambda code for collecting and saving location data to S3."""

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
    expected_token = os.getenv("OVERLAND_TOKEN")

    if token != expected_token:
        logger.warning(f"Invalid token received: {token}")
        return {
            "statusCode": 401,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "error": "Invalid token",
                "received_token": token,  # Remove this in production!
            }),
        }

    logger.info("✅ Authentication successful")
    return None


def lambda_handler(event: dict[str, any], context: any) -> dict[str, any]:
    """
    AWS Lambda handler for processing location data from Overland app.

    Receives GPS location data via HTTP POST request, authenticates the request,
    processes the location data, and stores it to S3 in a structured format.

    Parameters
    ----------
    event : dict[str, any]
        AWS Lambda event object containing HTTP request data including headers,
        body with GPS coordinates, and other request metadata.
    context : any
        AWS Lambda context object containing runtime information including
        request ID and execution details.

    Returns
    -------
    dict[str, any]
        HTTP response dictionary containing statusCode, headers, and JSON body.
        Returns 200 on success with processing summary, 401 for authentication
        errors, 400 for invalid JSON, or 500 for unexpected errors.

    Raises
    ------
    json.JSONDecodeError
        When the request body contains invalid JSON data.
    Exception
        For any unexpected errors during processing or S3 storage.
    KeyError
        When required environment variables (S3_BUCKET, OVERLAND_TOKEN) are missing.
    boto3.exceptions.Boto3Error
        When S3 operations fail due to permissions or connectivity issues.
    """
    try:
        # Log the entire incoming event for debugging
        logger.info("Received event:")
        logger.info(json.dumps(event, indent=2, default=str))

        # Extract headers and authenticate
        headers = event.get("headers", {})
        auth_error = _authenticate_request(headers)
        if auth_error:
            return auth_error

        # Parse the GPS data
        body = event.get("body", "{}")
        gps_data: dict[str, any] = json.loads(body) if isinstance(body, str) else body

        logger.info("📍 GPS Data received:")
        logger.info(json.dumps(gps_data, indent=2, default=str))

        # Extract and log location details
        locations: list[dict[str, any]] = gps_data.get("locations", [])
        logger.info(f"Number of locations in batch: {len(locations)}")

        for location in locations:
            if location.get("geometry") and location.get("properties"):
                coords: list[float] = location["geometry"].get("coordinates", [])

                logger.info("Location information found.")
                logger.info(
                    f"  📍 Lat/Lng: {coords[1] if len(coords) > 1 else 'N/A'}, {coords[0] if len(coords) > 0 else 'N/A'}"
                )

        # Log trip info if present
        if "trip" in gps_data:
            logger.info("🚗 Trip information found.")

        # Log current location if present
        if "current" in gps_data:
            logger.info("📍 Current location data included")

        # Store to S3
        # Build target location
        timestamp = datetime.now(UTC)

        file_name = f"{timestamp.strftime('%H%M%S')}-{context.aws_request_id}.json"
        storage_location = os.getenv("STORAGE_LOCATION")
        date_directory = f"{timestamp.strftime('%Y_%m_%d')}"

        file_key = f"{storage_location}/{date_directory}/{file_name}"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(gps_data, indent=2),
            ContentType="application/json",
            Metadata={
                "source": "overland-app",
                "locations_count": str(len(locations)),
                "timestamp": timestamp.isoformat(),
            },
        )

        logger.info(f"✅ Stored {len(locations)} locations to s3://{bucket_name}/{file_key}")

        # Return the response Overland expects
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "result": "ok",
                "locations_processed": len(locations),
                "timestamp": datetime.utcnow().isoformat(),
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
