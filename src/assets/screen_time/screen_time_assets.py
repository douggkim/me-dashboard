"""Assets for processing Screen Time data."""

import hashlib
import json
from datetime import UTC
from datetime import datetime as dt
from urllib.parse import urlparse

import dagster as dg
import fsspec
import polars as pl

from src.utils.aws import AWSCredentialFormat, get_aws_storage_options
from src.utils.data_loaders import get_storage_path
from src.validation.schemas.screen_time_schema import (
    ScreenTimeIphoneSilverDagsterType,
    ScreenTimeMacSilverDagsterType,
)


@dg.asset(
    name="screen_time_iphone",
    key_prefix=["silver", "work"],
    io_manager_key="io_manager_pl",
    dagster_type=ScreenTimeIphoneSilverDagsterType,
    description="Processed iPhone screen time data",
    group_name="work",
    kinds={"polars", "silver"},
    tags={"domain": "biometrics", "source": "iphone"},
    metadata={
        "primary_keys": ["id"],
        "partition_cols": ["usage_date"],
    },
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 6 * * *"),  # Run daily at 6 AM
)
def screen_time_iphone(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Process iPhone screen time data from Bronze to Silver.

    Reads raw JSON files, deduplicates by device_id and usage_date (keeping latest updated_at),
    and formats for the silver layer.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context.

    Returns
    -------
    pl.DataFrame
        The processed Polars DataFrame.
    """
    partition_date = context.partition_key.replace("-", "_")
    base_path = get_storage_path("screen_time", "iphone_bronze")
    bronze_path = f"{base_path}/{partition_date}"

    raw_data = load_bronze_json_files(context, bronze_path)

    empty_schema = {
        "id": pl.Utf8,
        "device_id": pl.Utf8,
        "usage_date": pl.Utf8,
        "total_usage_seconds": pl.Int64,
        "updated_at": pl.Datetime(time_zone="UTC"),
        "device_name": pl.Utf8,
        "device_type": pl.Utf8,
    }

    if not raw_data:
        context.log.warning("No raw data found for iPhone screen time.")
        return pl.DataFrame(schema=empty_schema)

    processed_data = _process_iphone_records(context, raw_data)

    if not processed_data:
        context.log.warning("No valid records found after processing.")
        return pl.DataFrame(schema=empty_schema)

    iphone_df = pl.DataFrame(processed_data)

    # Deduplication: Sort by updated_at descending, then group by id and take first
    return iphone_df.sort("updated_at", descending=True).unique(subset=["id"], keep="first")


@dg.asset(
    name="screen_time_mac",
    key_prefix=["silver", "work"],
    io_manager_key="io_manager_pl",
    dagster_type=ScreenTimeMacSilverDagsterType,
    description="Processed Mac screen time data",
    group_name="work",
    kinds={"polars", "silver"},
    tags={"domain": "biometrics", "source": "mac"},
    metadata={
        "primary_keys": ["id"],
        "partition_cols": ["usage_date"],
    },
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 6 * * *"),  # Run daily at 6 AM
)
def screen_time_mac(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Process Mac screen time data from Bronze to Silver.

    Reads raw JSON files, deduplicates by device_id and usage_date (keeping latest updated_at),
    explodes the 'data' list (per app usage), and formats for the silver layer.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context.

    Returns
    -------
    pl.DataFrame
        The processed Polars DataFrame.
    """
    partition_date = context.partition_key.replace("-", "_")
    base_path = get_storage_path("screen_time", "mac_bronze")
    bronze_path = f"{base_path}/{partition_date}"

    raw_data = load_bronze_json_files(context, bronze_path)

    empty_schema = {
        "id": pl.Utf8,
        "device_id": pl.Utf8,
        "usage_date": pl.Utf8,
        "bundle_id": pl.Utf8,
        "app_name": pl.Utf8,
        "total_usage_seconds": pl.Int64,
        "updated_at": pl.Datetime(time_zone="UTC"),
        "device_name": pl.Utf8,
        "device_type": pl.Utf8,
    }

    if not raw_data:
        context.log.warning("No raw data found for Mac screen time.")
        return pl.DataFrame(schema=empty_schema)

    processed_data = _process_mac_records(context, raw_data)

    if not processed_data:
        return pl.DataFrame(schema=empty_schema)

    mac_df = pl.DataFrame(processed_data)

    # Deduplication: Sort by updated_at descending, then group by id and take first
    return mac_df.sort("updated_at", descending=True).unique(subset=["id"], keep="first")


def load_bronze_json_files(context: dg.AssetExecutionContext, path: str) -> list[dict]:
    """
    Load all JSON files from the specified path.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context.
    path : str
        The path to the bronze data directory.

    Returns
    -------
    list[dict]
        A list of loaded JSON objects.
    """
    parsed = urlparse(path)
    storage_options = get_aws_storage_options(return_credential_type=AWSCredentialFormat.UTILIZE_ENV_VARS)
    fs = (
        fsspec.filesystem("file")
        if parsed.scheme in {"", "file"}
        else fsspec.filesystem(parsed.scheme, **storage_options)
    )

    if not fs.exists(path):
        context.log.warning(f"Path does not exist: {path}")
        return []

    # Recursive glob to find all json files
    try:
        json_files = fs.glob(f"{path}/**/*.json")
    except Exception:
        context.log.exception(f"Failed to glob files in {path}")
        return []

    data_list = []
    context.log.info(f"Found {len(json_files)} files in {path}")

    for file_path in json_files:
        try:
            with fs.open(file_path, "r") as f:
                data = json.load(f)
                data_list.append(data)
        except Exception:  # noqa: BLE001
            context.log.warning(f"Failed to read file: {file_path}")

    return data_list


def parse_updated_at(date_str: str) -> dt:
    """
    Parse timestamp string to UTC datetime.

    Handles format: "Feb 16, 2026 at 6:57 PM" -> UTC datetime
    Also handles ISO format if present.

    Parameters
    ----------
    date_str : str
        The date string to parse.

    Returns
    -------
    dt
        The parsed UTC datetime.

    Raises
    ------
    ValueError
        If the date string cannot be parsed.
    """
    try:
        # Try ISO format first
        return dt.fromisoformat(date_str).replace(tzinfo=UTC)
    except ValueError:
        pass

    try:
        # Clean up string: remove " at " and parse
        clean_str = date_str.replace(" at ", " ")
        return dt.strptime(clean_str, "%b %d, %Y %I:%M %p").replace(tzinfo=UTC)
    except ValueError as err:
        # Better to fail loud or return None?
        raise ValueError(f"Could not parse date: {date_str}") from err


def _process_iphone_records(context: dg.AssetExecutionContext, raw_data: list[dict]) -> list[dict]:
    """
    Process and deduplicate iPhone screen time records.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context.
    raw_data : list[dict]
        List of raw iPhone screen time records.

    Returns
    -------
    list[dict]
        Processed and deduplicated records.
    """
    processed_data = []

    for item in raw_data:
        try:
            device_id = item.get("device_id")
            updated_at_str = item.get("updated_at")
            # usage_seconds in sample could be string
            usage_seconds = int(item.get("usage_seconds", 0))
            device_name = item.get("device_name")

            # Derive usage_date from updated_at if not present
            updated_at = parse_updated_at(updated_at_str)
            usage_date_str = item.get("usage_date")
            if not usage_date_str:
                usage_date_str = updated_at.strftime("%Y-%m-%d")

            # Create ID
            hash_input = f"{device_id}_{usage_date_str}"
            record_id = hashlib.md5(hash_input.encode()).hexdigest()  # noqa: S324

            processed_data.append({
                "id": record_id,
                "device_id": device_id,
                "usage_date": usage_date_str,
                "total_usage_seconds": usage_seconds,
                "updated_at": updated_at,
                "device_name": device_name,
                "device_type": "iphone",
            })
        except Exception as e:  # noqa: BLE001
            context.log.warning(f"Skipping record due to error: {e}, Record: {item}")

    return processed_data


def _process_mac_records(context: dg.AssetExecutionContext, raw_data: list[dict]) -> list[dict]:
    """
    Process and deduplicate Mac screen time records.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context.
    raw_data : list[dict]
        List of raw Mac screen time records.

    Returns
    -------
    list[dict]
        Processed and deduplicated records.
    """
    reports_map = {}

    for item in raw_data:
        try:
            device_id = item.get("device_id")
            usage_date = item.get("usage_date")
            updated_at_str = item.get("updated_at")
            updated_at = parse_updated_at(updated_at_str)
            item["updated_at_dt"] = updated_at  # Store parsed dt for comparison

            key = f"{device_id}_{usage_date}"

            if key not in reports_map or updated_at > reports_map[key]["updated_at_dt"]:
                reports_map[key] = item
        except Exception as e:  # noqa: BLE001
            context.log.warning(f"Error processing mac report header: {e}")

    processed_data = []

    for report in reports_map.values():
        device_id = report.get("device_id")
        usage_date = report.get("usage_date")
        updated_at = report.get("updated_at_dt")
        device_name = report.get("device_name")
        apps_data = report.get("data", [])

        for app in apps_data:
            try:
                bundle_id = app.get("bundle_id")
                # usage_seconds in app data might be int or float? Sample says int/float.
                usage_seconds = int(app.get("total_usage_seconds", 0))

                # App Name logic
                parts = bundle_id.split(".")
                app_name = parts[-1] if parts else bundle_id

                # ID = hash(device_id, usage_date, bundle_id)
                hash_input = f"{device_id}_{usage_date}_{bundle_id}"
                record_id = hashlib.md5(hash_input.encode()).hexdigest()  # noqa: S324

                processed_data.append({
                    "id": record_id,
                    "device_id": device_id,
                    "usage_date": usage_date,
                    "bundle_id": bundle_id,
                    "app_name": app_name,
                    "total_usage_seconds": usage_seconds,
                    "updated_at": updated_at,
                    "device_name": device_name,
                    "device_type": "mac",
                })
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"Error processing app record: {e}")

    return processed_data
