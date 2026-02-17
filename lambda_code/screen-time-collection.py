"""Lambda code for collecting and saving screen time data to S3."""

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

    Returns
    -------
    dict[str, any] | None
        Error response if authentication fails, None otherwise.
    """
    auth_header = headers.get("authorization") or headers.get("Authorization", "")

    if not auth_header.startswith("Bearer "):
        logger.warning("Missing or invalid Authorization header")
        return {
            "statusCode": 401,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Missing Authorization header"}),
        }

    token = auth_header.split(" ")[1] if len(auth_header.split(" ")) > 1 else ""
    expected_token = os.getenv("AUTH_TOKEN")  # Matches AUTH_TOKEN in your Mac script

    if token != expected_token:
        logger.warning("Invalid token received")
        return {
            "statusCode": 401,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Invalid token"}),
        }

    logger.info("✅ Authentication successful")
    return None


def _handle_mac_data(raw_data: dict[str, any]) -> dict[str, any]:
    """
    Process aggregated per-app usage from the Mac SQL scraper.

    Returns
    -------
    dict[str, any]
        Metadata about the processed data.
    """
    usage_list = raw_data.get("data", [])
    total_sec = sum(item.get("total_usage_seconds", 0) for item in usage_list)

    logger.info(f"💻 Mac Data: {len(usage_list)} apps, Total: {total_sec}s")

    return {"device_class": "mac", "total_seconds": total_sec, "app_count": len(usage_list)}


def _handle_iphone_data(raw_data: dict[str, any]) -> dict[str, any]:
    """
    Process daily aggregate usage from the iPhone Shortcut.

    Returns
    -------
    dict[str, any]
        Metadata about the processed data.
    """
    usage_seconds = raw_data.get("usage_seconds", 0)

    logger.info(f"📱 iPhone Data: Total: {usage_seconds}s")

    return {
        "device_class": "iphone",
        "total_seconds": usage_seconds,
        "app_count": 1,  # Aggregated single value
    }


def lambda_handler(event: dict[str, any], context: any) -> dict[str, any]:
    """
    Handle screen time ingestion.

    Returns
    -------
    dict[str, any]
        API Gateway response.
    """
    try:
        # 1. Authenticate
        headers = event.get("headers", {})
        auth_error = _authenticate_request(headers)
        if auth_error:
            return auth_error

        # 2. Parse Body
        body = event.get("body", "{}")
        raw_data = json.loads(body) if isinstance(body, str) else body

        device_type = raw_data.get("device_type", "iphone").lower()
        # Use the usage_date from the scraper if provided, otherwise today
        usage_date_str = raw_data.get("usage_date", datetime.now(UTC).strftime("%Y-%m-%d"))
        usage_date = datetime.strptime(usage_date_str, "%Y-%m-%d").replace(tzinfo=UTC)

        # 3. Route to Helpers
        meta = _handle_mac_data(raw_data) if device_type == "mac" else _handle_iphone_data(raw_data)

        # 4. Partitioned S3 Storage
        # S3 Key Structure: screen_time/YYYY/MM/DD/device_type-request_id.json
        storage_location = os.getenv("STORAGE_LOCATION", "screen_time")
        date_partition = usage_date.strftime("%Y_%m_%d")
        file_name = f"{device_type}-{context.aws_request_id}.json"

        file_key = f"{storage_location}/{device_type}/{date_partition}/{file_name}"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps({**raw_data, "processed_at": datetime.now(UTC).isoformat()}, indent=2),
            ContentType="application/json",
            Metadata={
                "device_id": str(raw_data.get("device_id")),
                "total_seconds": str(meta["total_seconds"]),
                "usage_date": usage_date_str,
            },
        )

        logger.info(f"✅ Data stored to s3://{bucket_name}/{file_key}")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "result": "ok",
                "device": device_type,
                "date": usage_date_str,
                "total_seconds": meta["total_seconds"],
            }),
        }

    except Exception as e:
        logger.exception("❌ Processing Error")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)}),
        }
