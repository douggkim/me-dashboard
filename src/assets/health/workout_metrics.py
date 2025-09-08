"""Asset for processing workout metrics data from bronze to silver layer."""

from datetime import UTC, datetime
from urllib.parse import urlparse

import dagster as dg
import fsspec
import polars as pl

from src.utils.aws import AWSCredentialFormat, get_aws_storage_options
from src.utils.data_loaders import get_storage_path
from src.utils.health_data import detect_duplicates, generate_workout_id, parse_health_csv_content


def parse_workout_datetime(date_str: str) -> datetime:
    """
    Parse workout datetime from M/D/YY H:MM format to ISO datetime.

    Parameters
    ----------
    date_str : str
        Date string in format like "9/6/25 16:45"

    Returns
    -------
    datetime
        Parsed datetime object in UTC
    """
    try:
        # Handle the M/D/YY H:MM format
        dt = datetime.strptime(date_str, "%m/%d/%y %H:%M")
        # Convert to UTC timezone
        dt = dt.replace(tzinfo=UTC)
        return dt
    except ValueError:
        # Try alternative formats if needed
        for fmt in ["%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.replace(tzinfo=UTC)
            except ValueError:
                continue
        raise ValueError(f"Could not parse datetime: {date_str}")


@dg.asset(
    name="workout_metrics_silver",
    key_prefix=["silver", "health"],
    io_manager_key="io_manager_polars_delta",
    description="Processed workout metrics data with standardized timestamps and duration parsing",
    group_name="health",
    kinds={"python", "delta", "silver"},
    tags={"domain": "health", "layer": "silver"},
    owners=["doug@randomplace.com"],
    metadata={"partition_cols": ["date"], "primary_keys": ["workout_id"]},
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-09-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 8 * * *"),  # Run daily at 8 AM
)
def workout_metrics_silver(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """
    Process raw workout CSV files into structured silver layer data.

    This asset processes workout metrics data from the bronze layer by:
    1. Loading CSV files from the partition directory (created by Lambda)
    2. Parsing CSV files and skipping metadata headers
    3. Converting column names to snake_case
    4. Converting M/D/YY datetime format to ISO format
    5. Parsing duration strings and validating workout sessions
    6. Generating unique workout IDs and duplicate detection flags
    7. Adding source file tracking for data lineage

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context with partition information

    Returns
    -------
    pl.LazyFrame
        Processed workout metrics data with standardized schema

    Schema
    ------
    - workout_id: str - Unique hash-based workout identifier
    - date: Date - Partition date (YYYY-MM-DD)
    - type: str - Type of workout (e.g., "Traditional Strength Training")
    - start_time: Datetime - Workout start timestamp (UTC)
    - end_time: Datetime - Workout end timestamp (UTC)
    - duration: str - Workout duration (e.g., "1:29:44")
    - total_energy_kcal: Float64 - Total energy burned
    - active_energy_kcal: Float64 - Active energy burned
    - max_heart_rate_bpm: Float64 - Maximum heart rate
    - avg_heart_rate_bpm: Float64 - Average heart rate
    - distance_km: Float64 - Distance covered
    - step_count_count: Float64 - Number of steps
    - ... (additional workout metrics)
    - source_file: str - Original CSV filename for tracking
    - is_duplicate: bool - Flag indicating potential duplicate records
    """
    partition_date = context.partition_key
    partition_date_formatted = partition_date.replace("-", "_")
    context.log.info(f"Processing workout metrics for date: {partition_date}")

    # Get storage path for bronze workout data
    base_path = get_storage_path(dataset_name="workout", table_name="bronze")
    partition_path = f"{base_path}/{partition_date_formatted}"

    # Setup filesystem
    parsed = urlparse(partition_path)
    storage_options = get_aws_storage_options(return_credential_type=AWSCredentialFormat.UTILIZE_ENV_VARS)
    fs = (
        fsspec.filesystem("file")
        if parsed.scheme in {"", "file"}
        else fsspec.filesystem(parsed.scheme, **storage_options)
    )

    context.log.info(f"Reading workout bronze data from {partition_path}")

    # Check if partition directory exists
    if not fs.exists(partition_path):
        context.log.warning(f"No workout data directory found for partition {partition_date}")
        return pl.LazyFrame(
            schema={
                "workout_id": pl.Utf8,
                "date": pl.Date,
                "type": pl.Utf8,
                "start_time": pl.Datetime,
                "end_time": pl.Datetime,
                "duration": pl.Utf8,
                "source_file": pl.Utf8,
                "is_duplicate": pl.Boolean,
            }
        )

    # List all CSV files in the partition directory
    try:
        csv_files = fs.glob(f"{partition_path}/*.csv")
    except Exception:
        context.log.exception(f"Failed to list files in {partition_path}")
        return pl.LazyFrame(
            schema={
                "workout_id": pl.Utf8,
                "date": pl.Date,
                "type": pl.Utf8,
                "start_time": pl.Datetime,
                "source_file": pl.Utf8,
                "is_duplicate": pl.Boolean,
            }
        )

    if not csv_files:
        context.log.warning(f"No CSV files found in {partition_path}")
        return pl.LazyFrame(
            schema={
                "workout_id": pl.Utf8,
                "date": pl.Date,
                "type": pl.Utf8,
                "start_time": pl.Datetime,
                "source_file": pl.Utf8,
                "is_duplicate": pl.Boolean,
            }
        )

    processed_dataframes = []

    for csv_file_path in csv_files:
        try:
            # Extract source filename from path
            source_file = csv_file_path.split("/")[-1]
            context.log.debug(f"Processing file: {source_file}")

            # Read CSV file content
            with fs.open(csv_file_path, "r") as f:
                csv_content = f.read()

            # Parse CSV content, skipping metadata lines
            df = parse_health_csv_content(csv_content, source_file)

            if df.height == 0:
                context.log.warning(f"Empty DataFrame from {source_file}")
                continue

            # Filter out empty rows (all columns null except Type)
            non_empty_mask = pl.any_horizontal([
                pl.col(col).is_not_null() for col in df.columns if col != "source_file"
            ])
            df = df.filter(non_empty_mask)

            if df.height == 0:
                context.log.info(f"No valid workout records found in {source_file} after filtering")
                continue

            # Expected columns mapping (update based on actual data structure)
            expected_columns = {"type": "type", "start": "start_time", "end": "end_time", "duration": "duration"}

            # Rename columns to standardized names where they exist
            for old_col, new_col in expected_columns.items():
                if old_col in df.columns:
                    df = df.rename({old_col: new_col})

            # Check if we have required columns
            if "start_time" not in df.columns or "type" not in df.columns:
                context.log.warning(f"Missing required columns in {source_file}")
                continue

            # Parse start and end times, handle the M/D/YY format
            df = df.with_columns([
                pl.col("start_time").map_elements(
                    lambda x: parse_workout_datetime(x) if x else None, return_dtype=pl.Datetime
                ),
            ])

            # Parse end time if it exists
            if "end_time" in df.columns:
                df = df.with_columns([
                    pl.col("end_time").map_elements(
                        lambda x: parse_workout_datetime(x) if x else None, return_dtype=pl.Datetime
                    ),
                ])

            # Add date column for partitioning (extract from start_time)
            df = df.with_columns([pl.col("start_time").dt.date().alias("date")])

            # Generate unique workout IDs
            df = df.with_columns([
                pl.struct(["date", "start_time", "type", "duration"])
                .map_elements(
                    lambda x: generate_workout_id(
                        str(x["date"]), str(x["start_time"]), x["type"] or "", x["duration"] or ""
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("workout_id")
            ])

            processed_dataframes.append(df)
            context.log.info(f"Processed {df.height} workout records from {source_file}")

        except Exception as e:
            context.log.exception(f"Failed to process workout file {source_file}: {e}")
            continue

    if not processed_dataframes:
        context.log.warning("No valid workout data processed")
        return pl.LazyFrame(
            schema={
                "workout_id": pl.Utf8,
                "date": pl.Date,
                "type": pl.Utf8,
                "start_time": pl.Datetime,
                "source_file": pl.Utf8,
                "is_duplicate": pl.Boolean,
            }
        )

    # Combine all processed DataFrames
    combined_df = pl.concat(processed_dataframes, how="vertical_relaxed")

    # Add duplicate detection
    combined_df = detect_duplicates(combined_df, "workout_id")

    # Ensure date column is properly typed for partitioning
    combined_df = combined_df.with_columns([pl.col("date").cast(pl.Date)])

    context.log.info(f"Final processed workout metrics: {combined_df.height} records")

    duplicate_count = combined_df.filter(pl.col("is_duplicate")).height
    if duplicate_count > 0:
        context.log.warning(f"Found {duplicate_count} potential duplicate workout records")

    return combined_df.lazy()
