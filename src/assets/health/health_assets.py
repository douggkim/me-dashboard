"""Dagster assets for Health data processing."""

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
from src.validation.schemas.health_schema import HealthSilverDagsterType

HEALTH_CSV_SCHEMA = {
    "Date": pl.Utf8,
    "Active Energy (kcal)": pl.Float64,
    "Apple Exercise Time (min)": pl.Float64,
    "Apple Move Time (min)": pl.Float64,
    "Apple Stand Hour (hours)": pl.Float64,
    "Apple Stand Time (min)": pl.Float64,
    "Body Temperature (ºF)": pl.Float64,
    "Breathing Disturbances (count)": pl.Float64,
    "Environmental Audio Exposure (dBASPL)": pl.Float64,
    "Flights Climbed (count)": pl.Float64,
    "Headphone Audio Exposure (dBASPL)": pl.Float64,
    "Heart Rate [Min] (bpm)": pl.Float64,
    "Heart Rate [Max] (bpm)": pl.Float64,
    "Heart Rate [Avg] (bpm)": pl.Float64,
    "Heart Rate Variability (ms)": pl.Float64,
    "Number of Times Fallen (falls)": pl.Float64,
    "Physical Effort (MET)": pl.Float64,
    "Respiratory Rate (count/min)": pl.Float64,
    "Resting Energy (kcal)": pl.Float64,
    "Resting Heart Rate (bpm)": pl.Float64,
    "Running Power (watts)": pl.Float64,
    "Running Speed (mi/hr)": pl.Float64,
    "Step Count (steps)": pl.Float64,
    "Walking Speed (mi/hr)": pl.Float64,
    "Walking Step Length (in)": pl.Float64,
}


@dg.asset(
    name="health_silver",
    key_prefix=["silver", "health"],
    io_manager_key="io_manager_pl",
    dagster_type=HealthSilverDagsterType,
    description="""Processed Apple Health metrics data.
    Refresh Cadence:
    - Data for partition date D arrives in folder D+1 due to iOS upload timing.
    - end_offset=0 ensures we read from D+1 when processing partition D.
    """,
    group_name="health",
    kinds={"polars", "silver"},
    tags={"domain": "biometrics", "source": "apple_health"},
    metadata={
        "primary_keys": ["health_activity_id"],
        "partition_cols": ["activity_date"],
    },
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-09-01", end_offset=0, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 10 * * *"),
)
def health_silver(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Process Apple Health data from Bronze to Silver.

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

    base_path = get_storage_path("health", "bronze")
    bronze_path = f"{base_path}/{folder_date}"

    raw_data = load_bronze_csv_files(context, bronze_path)

    empty_schema = {
        "health_activity_id": pl.Utf8,
        "date_time_pst": pl.Datetime(),
        "activity_date": pl.Utf8,
        "active_energy_kcal": pl.Float64,
        "apple_exercise_time_min": pl.Float64,
        "apple_move_time_min": pl.Float64,
        "apple_stand_hour_hours": pl.Float64,
        "apple_stand_time_min": pl.Float64,
        "body_temperature_f": pl.Float64,
        "breathing_disturbances_count": pl.Float64,
        "environmental_audio_exposure_dbaspl": pl.Float64,
        "flights_climbed_count": pl.Float64,
        "headphone_audio_exposure_dbaspl": pl.Float64,
        "heart_rate_min_bpm": pl.Float64,
        "heart_rate_max_bpm": pl.Float64,
        "heart_rate_avg_bpm": pl.Float64,
        "heart_rate_variability_ms": pl.Float64,
        "number_of_times_fallen_falls": pl.Float64,
        "physical_effort_met": pl.Float64,
        "respiratory_rate_count_min": pl.Float64,
        "resting_energy_kcal": pl.Float64,
        "resting_heart_rate_bpm": pl.Float64,
        "running_power_watts": pl.Float64,
        "running_speed_mi_hr": pl.Float64,
        "step_count_steps": pl.Float64,
        "walking_speed_mi_hr": pl.Float64,
        "walking_step_length_in": pl.Float64,
    }

    if not raw_data:
        context.log.warning("No raw data found for Health.")
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
                schema_overrides=HEALTH_CSV_SCHEMA,
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

    # Cast datetimes
    combined_df = combined_df.with_columns([
        pl.col("date_time_pst").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
    ])

    # Add activity_date partition col
    combined_df = combined_df.with_columns([pl.col("date_time_pst").dt.strftime("%Y-%m-%d").alias("activity_date")])

    # Deduplicate / Hash PK
    def hash_id(s: str) -> str:
        return hashlib.md5(s.encode()).hexdigest()  # noqa: S324

    combined_df = combined_df.with_columns(
        pl.col("date_time_pst")
        .dt.to_string("%Y-%m-%d %H:%M:%S")
        .map_elements(hash_id, return_dtype=pl.Utf8)
        .alias("health_activity_id")
    )

    combined_df = combined_df.drop_nulls("date_time_pst")
    health_df = combined_df.unique(subset=["health_activity_id"], maintain_order=True)

    min_dt = health_df["date_time_pst"].min()
    max_dt = health_df["date_time_pst"].max()

    context.add_output_metadata({
        "min_date_pst": str(min_dt) if min_dt else "N/A",
        "max_date_pst": str(max_dt) if max_dt else "N/A",
        "row_count": len(health_df),
        "source_folder": folder_date,
    })

    return health_df


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
        name = name.replace("º", "")
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
