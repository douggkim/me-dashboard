"""Asset for processing health metrics data from bronze to silver layer."""

from urllib.parse import urlparse

import dagster as dg
import fsspec
import polars as pl

from src.utils.aws import AWSCredentialFormat, get_aws_storage_options
from src.utils.data_loaders import get_storage_path
from src.utils.health_data import detect_duplicates, generate_health_record_id, parse_health_csv_content


@dg.asset(
    name="health_metrics_silver",
    key_prefix=["silver", "health"],
    io_manager_key="io_manager_polars_delta",
    description="Processed health metrics data with clean timestamps and snake_case columns",
    group_name="health",
    kinds={"python", "delta", "silver"},
    tags={"domain": "health", "layer": "silver"},
    owners=["doug@randomplace.com"],
    metadata={"partition_cols": ["date"], "primary_keys": ["health_record_id"]},
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-09-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 8 * * *"),  # Run daily at 8 AM
)
def health_metrics_silver(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """
    Process raw health CSV files into structured silver layer data.

    This asset processes health metrics data from the bronze layer by:
    1. Loading CSV files from the partition directory (created by Lambda)
    2. Parsing CSV files and skipping metadata headers
    3. Converting column names to snake_case
    4. Filtering out empty rows (all null metrics)
    5. Parsing timestamps to proper datetime format
    6. Generating unique record IDs and duplicate detection flags
    7. Adding source file tracking for data lineage

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context with partition information

    Returns
    -------
    pl.LazyFrame
        Processed health metrics data with standardized schema

    Schema
    ------
    - health_record_id: str - Unique hash-based record identifier
    - date: Date - Partition date (YYYY-MM-DD)
    - timestamp: Datetime - Health measurement timestamp (UTC)
    - active_energy_kcal: Float64 - Active energy in kilocalories
    - apple_exercise_time_min: Float64 - Exercise time in minutes
    - heart_rate_min_bpm: Float64 - Minimum heart rate in BPM
    - heart_rate_max_bpm: Float64 - Maximum heart rate in BPM
    - step_count_steps: Float64 - Number of steps
    - ... (additional health metrics)
    - source_file: str - Original CSV filename for tracking
    - is_duplicate: bool - Flag indicating potential duplicate records
    """
    partition_date = context.partition_key
    partition_date_formatted = partition_date.replace("-", "_")
    context.log.info(f"Processing health metrics for date: {partition_date}")

    # Get storage path for bronze health data
    base_path = get_storage_path(dataset_name="health", table_name="bronze")
    partition_path = f"{base_path}/{partition_date_formatted}"

    # Setup filesystem
    parsed = urlparse(partition_path)
    storage_options = get_aws_storage_options(return_credential_type=AWSCredentialFormat.UTILIZE_ENV_VARS)
    fs = (
        fsspec.filesystem("file")
        if parsed.scheme in {"", "file"}
        else fsspec.filesystem(parsed.scheme, **storage_options)
    )

    context.log.info(f"Reading health bronze data from {partition_path}")

    # Check if partition directory exists
    if not fs.exists(partition_path):
        context.log.warning(f"No health data directory found for partition {partition_date}")
        return pl.LazyFrame(
            schema={
                "health_record_id": pl.Utf8,
                "date": pl.Date,
                "timestamp": pl.Datetime,
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
                "health_record_id": pl.Utf8,
                "date": pl.Date,
                "timestamp": pl.Datetime,
                "source_file": pl.Utf8,
                "is_duplicate": pl.Boolean,
            }
        )

    if not csv_files:
        context.log.warning(f"No CSV files found in {partition_path}")
        return pl.LazyFrame(
            schema={
                "health_record_id": pl.Utf8,
                "date": pl.Date,
                "timestamp": pl.Datetime,
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

            # Check if Date column exists (first column should be Date/timestamp)
            date_col = df.columns[0]
            if "date" not in date_col.lower():
                context.log.warning(f"Expected date column, found: {date_col}")
                continue

            # Rename the date column to standardized name
            df = df.rename({date_col: "timestamp"})

            # Filter out rows where all metric columns are null
            metric_columns = [col for col in df.columns if col not in ["timestamp", "source_file"]]

            if metric_columns:
                # Keep rows that have at least one non-null metric value
                df = df.filter(pl.any_horizontal([pl.col(col).is_not_null() for col in metric_columns]))

            if df.height == 0:
                context.log.info(f"No valid health records found in {source_file} after filtering")
                continue

            # Parse timestamp to datetime and extract date
            df = df.with_columns([
                pl.col("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S").dt.replace_time_zone("UTC"),
            ])

            # Add date column for partitioning
            df = df.with_columns([pl.col("timestamp").dt.date().alias("date")])

            # Generate unique record IDs
            df = df.with_columns([
                pl.struct(["date", "timestamp", "source_file"])
                .map_elements(
                    lambda x: generate_health_record_id(str(x["date"]), str(x["timestamp"]), x["source_file"]),
                    return_dtype=pl.Utf8,
                )
                .alias("health_record_id")
            ])

            processed_dataframes.append(df)
            context.log.info(f"Processed {df.height} health records from {source_file}")

        except Exception as e:
            context.log.exception(f"Failed to process health file {source_file}: {e}")
            continue

    if not processed_dataframes:
        context.log.warning("No valid health data processed")
        return pl.LazyFrame(
            schema={
                "health_record_id": pl.Utf8,
                "date": pl.Date,
                "timestamp": pl.Datetime,
                "source_file": pl.Utf8,
                "is_duplicate": pl.Boolean,
            }
        )

    # Combine all processed DataFrames
    combined_df = pl.concat(processed_dataframes, how="vertical_relaxed")

    # Add duplicate detection
    combined_df = detect_duplicates(combined_df, "health_record_id")

    # Ensure date column is properly typed for partitioning
    combined_df = combined_df.with_columns([pl.col("date").cast(pl.Date)])

    context.log.info(f"Final processed health metrics: {combined_df.height} records")

    duplicate_count = combined_df.filter(pl.col("is_duplicate")).height
    if duplicate_count > 0:
        context.log.warning(f"Found {duplicate_count} potential duplicate records")

    return combined_df.lazy()
