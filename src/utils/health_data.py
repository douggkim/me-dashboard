"""Utility functions for processing health and workout data."""

import hashlib
import re
from io import StringIO

import polars as pl


def to_snake_case(text: str) -> str:
    """
    Convert text to snake_case format.

    Handles spaces, parentheses, brackets, and other special characters commonly
    found in health metric column names.

    Parameters
    ----------
    text : str
        Input text to convert

    Returns
    -------
    str
        Text converted to snake_case format

    Examples
    --------
    >>> to_snake_case("Active Energy (kcal)")
    'active_energy_kcal'
    >>> to_snake_case("Heart Rate [Max] (bpm)")
    'heart_rate_max_bpm'
    >>> to_snake_case("Apple Stand Hour (hours)")
    'apple_stand_hour_hours'
    """
    # Remove content within parentheses and brackets, but keep the content
    text = re.sub(r"\[([^\]]+)\]", r"_\1", text)  # [Max] -> _Max
    text = re.sub(r"\(([^)]+)\)", r"_\1", text)  # (kcal) -> _kcal

    # Replace spaces, hyphens, and other separators with underscores
    text = re.sub(r"[\s\-\.]+", "_", text)

    # Convert to lowercase
    text = text.lower()

    # Remove multiple consecutive underscores
    text = re.sub(r"_+", "_", text)

    # Remove leading/trailing underscores
    text = text.strip("_")

    return text


def parse_health_csv_content(csv_text: str, source_file: str = "") -> pl.DataFrame:
    """
    Parse health CSV content, skipping the 4-line metadata header.

    Health CSV files have a specific format with metadata lines:
    Line 1: --Boundary-UUID
    Line 2: Content-Disposition header
    Line 3: Content-Type header
    Line 4: Empty line
    Line 5+: Actual CSV data

    Parameters
    ----------
    csv_text : str
        Raw CSV text content
    source_file : str, optional
        Source filename for tracking, by default ""

    Returns
    -------
    pl.DataFrame
        Parsed DataFrame with snake_case column names and source tracking

    Raises
    ------
    ValueError
        If CSV content cannot be parsed or has invalid structure
    """
    lines = csv_text.strip().split("\n")

    if len(lines) < 5:
        raise ValueError(f"Invalid CSV format: expected at least 5 lines, got {len(lines)}")

    # Skip first 4 metadata lines and get CSV content
    csv_content = "\n".join(lines[4:])

    if not csv_content.strip():
        raise ValueError("No CSV data found after metadata lines")

    try:
        # Parse CSV content with Polars
        df = pl.read_csv(StringIO(csv_content))

        # Convert column names to snake_case
        column_mapping = {col: to_snake_case(col) for col in df.columns}
        df = df.rename(column_mapping)

        # Add source tracking column
        df = df.with_columns(source_file=pl.lit(source_file))

        return df

    except Exception as e:
        raise ValueError(f"Failed to parse CSV content: {e}") from e


def generate_health_record_id(date_str: str, timestamp_str: str, source_file: str) -> str:
    """
    Generate a unique health record ID using hash of key fields.

    Parameters
    ----------
    date_str : str
        Date string (YYYY-MM-DD format)
    timestamp_str : str
        Timestamp string
    source_file : str
        Source filename

    Returns
    -------
    str
        Unique hash-based record ID
    """
    hash_input = f"{date_str}_{timestamp_str}_{source_file}"
    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]


def generate_workout_id(date_str: str, start_time: str, workout_type: str, duration: str) -> str:
    """
    Generate a unique workout ID using hash of key fields.

    Parameters
    ----------
    date_str : str
        Date string (YYYY-MM-DD format)
    start_time : str
        Workout start timestamp
    workout_type : str
        Type of workout
    duration : str
        Workout duration

    Returns
    -------
    str
        Unique hash-based workout ID
    """
    hash_input = f"{date_str}_{start_time}_{workout_type}_{duration}"
    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]


def detect_duplicates(df: pl.DataFrame, id_column: str) -> pl.DataFrame:
    """
    Add duplicate detection flag to DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame with unique ID column
    id_column : str
        Name of the ID column to check for duplicates

    Returns
    -------
    pl.DataFrame
        DataFrame with added 'is_duplicate' boolean column
    """
    # Count occurrences of each ID
    id_counts = df.group_by(id_column).agg(pl.len().alias("count"))

    # Join back to original DataFrame and add flag
    df_with_duplicates = df.join(id_counts, on=id_column)
    df_with_duplicates = df_with_duplicates.with_columns(is_duplicate=pl.col("count") > 1).drop("count")

    return df_with_duplicates
