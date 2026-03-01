"""Unit tests for Health assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
import polars as pl
import pytest

from src.assets.health.health_assets import (
    HEALTH_CSV_SCHEMA,
    extract_csv_from_multipart,
    health_silver,
    load_bronze_csv_files,
    rename_columns,
)


@pytest.fixture
def mock_get_storage_path() -> MagicMock:
    """
    Mock get_storage_path utility.

    Yields
    ------
    MagicMock
        The mocked get_storage_path function.
    """
    with patch("src.assets.health.health_assets.get_storage_path") as mock:
        yield mock


@pytest.fixture
def mock_load_csv() -> MagicMock:
    """
    Mock load_bronze_csv_files utility.

    Yields
    ------
    MagicMock
        The mocked load_bronze_csv_files function.
    """
    with patch("src.assets.health.health_assets.load_bronze_csv_files") as mock:
        yield mock


def test_extract_csv_from_multipart() -> None:
    """Test extracting CSV from iOS multipart payload."""
    raw_payload = (
        "--Boundary-123\n"
        "Content-Disposition: form-data\n"
        "Content-Type: text/csv\n"
        "\n"
        "Date,Active Energy (kcal),Apple Exercise Time (min)\n"
        "2026-02-16 12:00:00,10.5,\n"
        "--Boundary-123--\n"
    )
    result = extract_csv_from_multipart(raw_payload)
    assert result == "Date,Active Energy (kcal),Apple Exercise Time (min)\n2026-02-16 12:00:00,10.5,"


def test_rename_columns() -> None:
    """Test column renaming logic."""
    cols = ["Date", "Active Energy (kcal)", "Running Speed (mi/hr)"]
    mapping = rename_columns(cols)
    assert mapping["Date"] == "date_time_pst"
    assert mapping["Active Energy (kcal)"] == "active_energy_kcal"
    assert mapping["Running Speed (mi/hr)"] == "running_speed_mi_hr"


def test_health_silver(mock_get_storage_path: MagicMock, mock_load_csv: MagicMock) -> None:
    """Test Health silver asset processing and deduplication."""
    mock_get_storage_path.return_value = "dummy/path"

    header = (
        "Date,Active Energy (kcal),Apple Exercise Time (min),Apple Move Time (min),"
        "Apple Stand Hour (hours),Apple Stand Time (min),Body Temperature (ºF),"
        "Breathing Disturbances (count),Environmental Audio Exposure (dBASPL),"
        "Flights Climbed (count),Headphone Audio Exposure (dBASPL),Heart Rate [Min] (bpm),"
        "Heart Rate [Max] (bpm),Heart Rate [Avg] (bpm),Heart Rate Variability (ms),"
        "Number of Times Fallen (falls),Physical Effort (MET),Respiratory Rate (count/min),"
        "Resting Energy (kcal),Resting Heart Rate (bpm),Running Power (watts),"
        "Running Speed (mi/hr),Step Count (steps),Walking Speed (mi/hr),Walking Step Length (in)"
    )
    mock_load_csv.return_value = [
        f"{header}\n2026-02-16 12:00:00,15.5,,,,,,,,,,,,,,,,,,,,,,,\n2026-02-16 13:00:00,20.0,,,,,,,,,,,,,,,,,,,,,,,",
        f"{header}\n2026-02-16 12:00:00,15.5,,,,,,,,,,,,,,,,,,,,,,,",  # duplicate
    ]

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        health_df = health_silver(context=context)

        # Partition 2026-02-16 reads from 2026_02_17 folder
        mock_load_csv.assert_called_with(context, "dummy/path/2026_02_17")

    assert health_df.height == 2

    rows = health_df.to_dicts()
    assert rows[0]["active_energy_kcal"] == 15.5
    assert rows[0]["activity_date"] == "2026-02-16"


def test_health_silver_empty_data(mock_get_storage_path: MagicMock, mock_load_csv: MagicMock) -> None:
    """Test Health silver asset with no raw data."""
    mock_get_storage_path.return_value = "dummy/path"
    mock_load_csv.return_value = []

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        health_df = health_silver(context=context)

    assert health_df.height == 0
    assert "health_activity_id" in health_df.columns


@patch("src.assets.health.health_assets.pl.read_csv")
def test_health_silver_read_csv_errors(
    mock_read_csv: MagicMock, mock_get_storage_path: MagicMock, mock_load_csv: MagicMock
) -> None:
    """Test Health silver asset handling of Polars CSV parsing errors."""
    mock_get_storage_path.return_value = "dummy/path"

    # 1 valid, 1 compute error, 1 general error, 1 empty clean_csv
    valid_csv = "Date,Active Energy (kcal)\n2026-02-16 12:00:00,15.5"
    mock_load_csv.return_value = [
        "Date,Active Energy (kcal)\n2026-02-16 12:00:00,10.0",  # Trigger compute error
        "Date,Active Energy (kcal)\n2026-02-16 13:00:00,20.0",  # Trigger exception
        valid_csv,  # Valid
        "No,Valid,Headers",  # Empty clean CSV after extraction
    ]

    valid_dicts = {k: [None] for k in HEALTH_CSV_SCHEMA}
    valid_dicts["Date"] = ["2026-02-16 12:00:00"]
    valid_dicts["Active Energy (kcal)"] = [15.5]

    mock_read_csv.side_effect = [
        pl.exceptions.ComputeError("Mock Compute Error"),
        Exception("Mock General Error"),
        pl.DataFrame(valid_dicts, schema=HEALTH_CSV_SCHEMA),
    ]

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        health_df = health_silver(context=context)

    # Should only contain the valid one
    assert health_df.height == 1


@patch("src.assets.health.health_assets.get_aws_storage_options")
@patch("src.assets.health.health_assets.fsspec.filesystem")
def test_load_bronze_csv_files(mock_filesystem: MagicMock, mock_aws: MagicMock) -> None:
    """Test the load_bronze_csv_files utility under various conditions."""
    mock_fs = MagicMock()
    mock_filesystem.return_value = mock_fs

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        mock_aws.return_value = {}
        mock_fs.exists.return_value = False
        assert load_bronze_csv_files(context, "s3://dummy/path") == []

        # Test 2: Glob raises exception
        mock_fs.exists.return_value = True
        mock_fs.glob.side_effect = Exception("Glob Error")
        assert load_bronze_csv_files(context, "s3://dummy/path") == []

        # Test 3: fs.open raises exception for one file, succeeds for another
        mock_fs.glob.side_effect = None
        mock_fs.glob.return_value = ["file1.csv", "file2.csv"]

        mock_file_1 = MagicMock()
        mock_file_1.__enter__.return_value.read.return_value = "csv_data_1"

        mock_file_2 = MagicMock()
        mock_file_2.__enter__.side_effect = Exception("Read Error")

        mock_fs.open.side_effect = [mock_file_1, mock_file_2]

        result = load_bronze_csv_files(context, "s3://dummy/path")
        assert result == ["csv_data_1"]
