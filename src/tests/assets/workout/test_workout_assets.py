"""Unit tests for Workout assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from src.assets.workout.workout_assets import (
    extract_csv_from_multipart,
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
        f"{header}\nRunning,2/16/26 12:00,2/16/26 12:30,30:00,,,,,,,,,,,,,\nWalking,2/16/26 15:00,2/16/26 15:15,15:00,,,,,,,,,,,,,",
        f"{header}\nRunning,2/16/26 12:00,2/16/26 12:30,30:00,,,,,,,,,,,,,",  # duplicate
    ]

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        workout_df = workout_silver(context=context)

        mock_load_csv.assert_called_with(context, "dummy/path/2026_02_17")

    assert workout_df.height == 2

    rows = workout_df.to_dicts()
    assert rows[0]["type"] == "Running"
    assert rows[0]["activity_date"] == "2026-02-16"
