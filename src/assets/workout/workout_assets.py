"""Dagster assets for Workout data processing."""

import hashlib
import io
import re
from datetime import datetime as dt
from datetime import timedelta
from urllib.parse import urlparse

import dagster as dg
import fsspec
import polars as pl

from src.utils.aws import AWSCredentialFormat, get_aws_storage_options
from src.utils.data_loaders import get_storage_path
from src.validation.schemas.workout_schema import WorkoutSilverDagsterType

WORKOUT_CSV_SCHEMA = {
    "Type": pl.Utf8,
    "Start": pl.Utf8,
    "End": pl.Utf8,
    "Duration": pl.Utf8,
    "Total Energy (kcal)": pl.Float64,
    "Active Energy (kcal)": pl.Float64,
    "Max Heart Rate (bpm)": pl.Float64,
    "Avg Heart Rate (bpm)": pl.Float64,
    "Distance (km)": pl.Float64,
    "Avg Speed(km/hr)": pl.Float64,
    "Step Count (count)": pl.Float64,
    "Step Cadence (spm)": pl.Float64,
    "Swimming Stroke Count (count)": pl.Float64,
    "Swim Stoke Cadence (spm)": pl.Float64,
    "Flights Climbed (count)": pl.Float64,
    "Elevation Ascended (m)": pl.Float64,
    "Elevation Descended (m)": pl.Float64,
}


@dg.asset(
    name="workout_silver",
    key_prefix=["silver", "workout"],
    io_manager_key="io_manager_pl",
    dagster_type=WorkoutSilverDagsterType,
    description="""Processed Apple Workout metrics data.
    Refresh Cadence:
    - Data for partition date D arrives in folder D+1 due to iOS upload timing.
    - end_offset=0 ensures we read from D+1 when processing partition D.
    """,
    group_name="health",
    kinds={"polars", "silver"},
    tags={"domain": "biometrics", "source": "apple_workout"},
    metadata={
        "primary_keys": ["workout_activity_id"],
        "partition_cols": ["activity_date"],
    },
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-09-01", end_offset=0, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 10 * * *"),
)
def workout_silver(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Process Apple Workout data from Bronze to Silver.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context.

    Returns
    -------
    pl.DataFrame
        The processed Polars DataFrame.
    """
    partition_dt = dt.fromisoformat(context.partition_key.replace("_", "-"))
    folder_dt = partition_dt + timedelta(days=1)
    folder_date = folder_dt.strftime("%Y_%m_%d")

    base_path = get_storage_path("workout", "bronze")
    bronze_path = f"{base_path}/{folder_date}"

    raw_data = load_bronze_csv_files(context, bronze_path)

    empty_schema = {
        "workout_activity_id": pl.Utf8,
        "type": pl.Utf8,
        "start_pst": pl.Datetime(),
        "end_pst": pl.Datetime(),
        "duration": pl.Utf8,
        "activity_date": pl.Utf8,
        "total_energy_kcal": pl.Float64,
        "active_energy_kcal": pl.Float64,
        "max_heart_rate_bpm": pl.Float64,
        "avg_heart_rate_bpm": pl.Float64,
        "distance_km": pl.Float64,
        "avg_speed_km_hr": pl.Float64,
        "step_count_count": pl.Float64,
        "step_cadence_spm": pl.Float64,
        "swimming_stroke_count_count": pl.Float64,
        "swim_stoke_cadence_spm": pl.Float64,
        "flights_climbed_count": pl.Float64,
        "elevation_ascended_m": pl.Float64,
        "elevation_descended_m": pl.Float64,
    }

    if not raw_data:
        context.log.warning("No raw data found for Workout.")
        context.add_output_metadata({"row_count": 0})
        return pl.DataFrame(schema=empty_schema)

    dfs = []
    for csv_str in raw_data:
        clean_csv = extract_csv_from_multipart(csv_str)
        if not clean_csv:
            continue
        try:
            chunk_df = pl.read_csv(
                io.StringIO(clean_csv),
                null_values=[""],
                schema_overrides=WORKOUT_CSV_SCHEMA,
                truncate_ragged_lines=True,
            )
            dfs.append(chunk_df)
        except pl.exceptions.ComputeError as e:
            context.log.warning(f"Failed to parse CSV chunk into DataFrame due to a compute error: {e}")
        except Exception:
            context.log.exception("Unexpected error when parsing CSV chunk.")

    if not dfs:
        context.log.warning("No valid records found after processing.")
        context.add_output_metadata({"row_count": 0})
        return pl.DataFrame(schema=empty_schema)

    combined_df = pl.concat(dfs, how="diagonal_relaxed")
    mapping = rename_columns(combined_df.columns)
    combined_df = combined_df.rename(mapping)

    # Cast datetimes: format is often "%m/%d/%y %H:%M" such as "9/6/25 16:45"
    # We can try to specify a format string, or use the strptime with `%m/%d/%y %H:%M`
    combined_df = combined_df.with_columns([
        pl.col("start_pst").str.strptime(pl.Datetime, "%m/%d/%y %H:%M", strict=False),
        pl.col("end_pst").str.strptime(pl.Datetime, "%m/%d/%y %H:%M", strict=False),
    ])

    # Add activity_date partition col
    combined_df = combined_df.with_columns([pl.col("start_pst").dt.strftime("%Y-%m-%d").alias("activity_date")])

    # Deduplicate / Hash PK
    def hash_id(s: str) -> str:
        return hashlib.md5(s.encode()).hexdigest()  # noqa: S324

    combined_df = combined_df.with_columns(
        pl.col("start_pst")
        .dt.to_string("%Y-%m-%d %H:%M:%S")
        .map_elements(hash_id, return_dtype=pl.Utf8)
        .alias("workout_activity_id")
    )

    combined_df = combined_df.drop_nulls("start_pst")
    workout_df = combined_df.unique(subset=["workout_activity_id"], maintain_order=True)

    min_dt = workout_df["start_pst"].min()
    max_dt = workout_df["start_pst"].max()

    context.add_output_metadata({
        "min_date_pst": str(min_dt) if min_dt else "N/A",
        "max_date_pst": str(max_dt) if max_dt else "N/A",
        "row_count": len(workout_df),
        "source_folder": folder_date,
    })

    return workout_df


def load_bronze_csv_files(context: dg.AssetExecutionContext, path: str) -> list[str]:
    """
    Load all CSV files from the specified bronze path.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context.
    path : str
        The path to the bronze data directory.

    Returns
    -------
    list[str]
        A list of loaded CSV file contents as strings.
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

    try:
        csv_files = fs.glob(f"{path}/**/*.csv")
    except Exception:
        context.log.exception(f"Failed to glob files in {path}")
        return []

    data_list = []
    context.log.info(f"Found {len(csv_files)} files in {path}")

    for file_path in csv_files:
        try:
            with fs.open(file_path, "r") as f:
                content = f.read()
                data_list.append(content)
        except Exception:  # noqa: BLE001
            context.log.warning(f"Failed to read file: {file_path}")

    return data_list


def extract_csv_from_multipart(raw_string: str) -> str:
    """
    Extract the clean CSV payload from iOS multipart form-data structure.

    Parameters
    ----------
    raw_string : str
        The raw string payload containing multipart boundary headers and footers.

    Returns
    -------
    str
        The cleaned CSV content.
    """
    lines = raw_string.splitlines()
    start_idx = 0
    for i, line in enumerate(lines):
        if line.startswith(("Date,", "Type,")):
            start_idx = i
            break

    end_idx = len(lines)
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].startswith("--Boundary"):
            end_idx = i
            break

    return "\n".join(lines[start_idx:end_idx]).strip()


def rename_columns(cols: list[str]) -> dict[str, str]:
    """
    Generate a mapping of clean snake_case column names based on raw names.

    Parameters
    ----------
    cols : list[str]
        List of raw column names.

    Returns
    -------
    dict[str, str]
        A mapping from raw column to cleaned column name.
    """
    mapping = {}
    for col in cols:
        name = col.lower().strip()
        name = re.sub(r"[ ()/\-\[\]]", "_", name)
        name = re.sub(r"_+", "_", name)
        name = name.strip("_")

        if name == "date":
            name = "date_time_pst"
        elif name == "start":
            name = "start_pst"
        elif name == "end":
            name = "end_pst"

        mapping[col] = name
    return mapping
