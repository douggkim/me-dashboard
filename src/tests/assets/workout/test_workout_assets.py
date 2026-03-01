"""Unit tests for Workout assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
import polars as pl
import pytest

from src.assets.workout.workout_assets import (
    WORKOUT_CSV_SCHEMA,
    extract_csv_from_multipart,
    load_bronze_csv_files,
    rename_columns,
    workout_silver,
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
    with patch("src.assets.workout.workout_assets.get_storage_path") as mock:
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
    with patch("src.assets.workout.workout_assets.load_bronze_csv_files") as mock:
        yield mock


def test_extract_csv_from_multipart() -> None:
    """Test extracting CSV from iOS multipart payload."""
    raw_payload = (
        "--Boundary-123\n"
        "Content-Disposition: form-data\n"
        "Content-Type: text/csv\n"
        "\n"
        "Type,Start,End,Duration\n"
        "Running,2/16/26 12:00,2/16/26 13:00,60:00\n"
        "--Boundary-123--\n"
    )
    result = extract_csv_from_multipart(raw_payload)
    assert result == "Type,Start,End,Duration\nRunning,2/16/26 12:00,2/16/26 13:00,60:00"


def test_rename_columns() -> None:
    """Test column renaming logic."""
    cols = ["Type", "Start", "End", "Total Energy (kcal)"]
    mapping = rename_columns(cols)
    assert mapping["Type"] == "type"
    assert mapping["Start"] == "start_pst"
    assert mapping["End"] == "end_pst"
    assert mapping["Total Energy (kcal)"] == "total_energy_kcal"


def test_workout_silver(mock_get_storage_path: MagicMock, mock_load_csv: MagicMock) -> None:
    """Test Workout silver asset processing and deduplication."""
    mock_get_storage_path.return_value = "dummy/path"

    header = (
        "Type,Start,End,Duration,Total Energy (kcal),Active Energy (kcal),"
        "Max Heart Rate (bpm),Avg Heart Rate (bpm),Distance (km),Avg Speed(km/hr),"
        "Step Count (count),Step Cadence (spm),Swimming Stroke Count (count),"
        "Swim Stoke Cadence (spm),Flights Climbed (count),Elevation Ascended (m),"
        "Elevation Descended (m)"
    )
    mock_load_csv.return_value = [
        f"{header}\nRunning,2/16/26 12:00,2/16/26 12:30,30:00"
        ",,,,,,,,,,,,,\nWalking,2/16/26 15:00,2/16/26 15:15,15:00,,,,,,,,,,,,,",
        f"{header}\nRunning,2/16/26 12:00,2/16/26 12:30,30:00,,,,,,,,,,,,,",  # duplicate
    ]

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        workout_df = workout_silver(context=context)

        mock_load_csv.assert_called_with(context, "dummy/path/2026_02_17")

    assert workout_df.height == 2

    rows = workout_df.to_dicts()
    assert rows[0]["type"] == "Running"
    assert rows[0]["activity_date"] == "2026-02-16"


def test_workout_silver_empty_data(mock_get_storage_path: MagicMock, mock_load_csv: MagicMock) -> None:
    """Test Workout silver asset with no raw data."""
    mock_get_storage_path.return_value = "dummy/path"
    mock_load_csv.return_value = []

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        workout_df = workout_silver(context=context)

    assert workout_df.height == 0
    assert "workout_activity_id" in workout_df.columns


@patch("src.assets.workout.workout_assets.pl.read_csv")
def test_workout_silver_read_csv_errors(
    mock_read_csv: MagicMock, mock_get_storage_path: MagicMock, mock_load_csv: MagicMock
) -> None:
    """Test Workout silver asset handling of Polars CSV parsing errors."""
    mock_get_storage_path.return_value = "dummy/path"

    mock_load_csv.return_value = [
        "Type,Start,End\nRunning,2/16/26 12:00,2/16/26 13:00",  # trigger compute error
        "Type,Start,End\nWalking,2/16/26 13:00,2/16/26 14:00",  # trigger exception
        "Type,Start,End\nSwimming,2/16/26 14:00,2/16/26 15:00",  # valid
        "No,Valid,Headers",  # clean_csv empty
    ]

    valid_dicts = {k: [None] for k in WORKOUT_CSV_SCHEMA}
    valid_dicts["Type"] = ["Swimming"]
    valid_dicts["Start"] = ["02/16/26 14:00"]
    valid_dicts["End"] = ["02/16/26 15:00"]

    mock_read_csv.side_effect = [
        pl.exceptions.ComputeError("Mock Compute Error"),
        Exception("Mock General Error"),
        pl.DataFrame(valid_dicts, schema=WORKOUT_CSV_SCHEMA),
    ]

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        workout_df = workout_silver(context=context)

    # Should only contain the valid one
    assert workout_df.height == 1


@patch("src.assets.workout.workout_assets.get_aws_storage_options")
@patch("src.assets.workout.workout_assets.fsspec.filesystem")
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
