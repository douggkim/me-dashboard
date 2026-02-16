"""Unit tests for movement_data assets and helpers."""

import datetime
import io
import json
from unittest.mock import MagicMock

import dagster as dg
import polars as pl
import pytest

from src.assets.location.movement_data import (
    clean_location_dataframe,
    load_raw_location_data,
    transform_geojson_to_records,
)


@pytest.fixture
def mock_log() -> MagicMock:
    """
    Fixture for mock DagsterLogManager.

    Returns
    -------
    MagicMock
        A mock of the Dagster log manager.
    """
    return MagicMock(spec=dg.DagsterLogManager)


@pytest.fixture
def raw_geojson_data() -> list[dict]:
    """
    Fixture for raw GeoJSON location data.

    Returns
    -------
    list[dict]
        A list of raw GeoJSON location data records.
    """
    return [
        {
            "geometry": {"coordinates": [-122.4194, 37.7749]},
            "properties": {
                "timestamp": "2024-01-01T12:00:00Z",
                "speed": 5.5,
                "motion": ["walking"],
                "battery_level": 0.8,
                "device_id": "test_device",
            },
        }
    ]


def test_load_raw_location_data(mock_log: MagicMock) -> None:
    """Test loading raw location data from JSON files."""
    fs = MagicMock()
    json_files = ["file1.json"]
    mock_data = {"locations": [{"id": 1}]}

    # Need to mock the json.load as well or use a real file handle
    with MagicMock() as mock_file:
        mock_file.__enter__.return_value = mock_file
        fs.open.return_value = mock_file
        # Instead of mocking json.load, let's just use it with a string
        mock_file.read.return_value = json.dumps(mock_data)
        # We need to mock the behavior of json.load(f)
        fs.open.return_value.__enter__.return_value = io.StringIO(json.dumps(mock_data))

        result = load_raw_location_data(fs, json_files, mock_log)

    assert len(result) == 1
    assert result[0] == {"id": 1}


def test_transform_geojson_to_records(raw_geojson_data: list[dict], mock_log: MagicMock) -> None:
    """Test transforming GeoJSON data to flattened records."""
    result = transform_geojson_to_records(raw_geojson_data, mock_log)

    assert len(result) == 1
    record = result[0]
    assert record["latitude"] == 37.7749
    assert record["longitude"] == -122.4194
    assert record["speed"] == 5.5
    assert record["motion"] == "walking"
    assert record["device_id"] == "test_device"


def test_transform_geojson_to_records_invalid_coords(mock_log: MagicMock) -> None:
    """Test handling invalid coordinates during transformation."""
    invalid_data = [{"geometry": {"coordinates": [1]}, "properties": {"timestamp": "2024-01-01T12:00:00Z"}}]
    result = transform_geojson_to_records(invalid_data, mock_log)

    assert len(result) == 0
    mock_log.warning.assert_called()


def test_clean_location_dataframe() -> None:
    """Test cleaning and validating location DataFrame."""
    records = [
        {
            "timestamp": "2024-01-01T12:00:00Z",
            "latitude": 37.7749,
            "longitude": -122.4194,
            "speed": 5.5,
            "motion": "walking",
            "battery_level": 0.8,
            "device_id": "test_device",
            "battery_state": "charging",
            "altitude": 10.0,
            "horizontal_accuracy": 5.0,
            "vertical_accuracy": 3.0,
            "speed_accuracy": 1.0,
            "course": 90.0,
            "course_accuracy": 2.0,
            "wifi": "home",
        }
    ]

    location_df = clean_location_dataframe(records)

    assert len(location_df) == 1
    assert location_df["latitude"][0] == 37.7749
    assert location_df["longitude"][0] == -122.4194
    assert location_df["timestamp"].dtype == pl.Datetime
    # Use item() on the Series element to get a Python date
    assert location_df["date"][0] == datetime.date(2024, 1, 1)


def test_clean_location_dataframe_invalid_coords() -> None:
    """Test filtering invalid coordinates in DataFrame."""
    records = [
        {
            "timestamp": "2024-01-01T12:00:00Z",
            "latitude": 100.0,
            "longitude": -122.4194,
            "speed": 0.0,
            "battery_level": 0.5,
            "altitude": 0.0,
            "horizontal_accuracy": 0.0,
            "vertical_accuracy": 0.0,
            "speed_accuracy": 0.0,
            "course": 0.0,
            "course_accuracy": 0.0,
        },  # Invalid lat
        {
            "timestamp": "2024-01-01T12:00:00Z",
            "latitude": 37.7749,
            "longitude": -200.0,
            "speed": 1.0,
            "battery_level": 0.5,
            "altitude": 0.0,
            "horizontal_accuracy": 0.0,
            "vertical_accuracy": 0.0,
            "speed_accuracy": 0.0,
            "course": 0.0,
            "course_accuracy": 0.0,
        },  # Invalid lng
        {
            "timestamp": "2024-01-01T12:00:00Z",
            "latitude": None,
            "longitude": -122.4194,
            "speed": 2.0,
            "battery_level": 0.5,
            "altitude": 0.0,
            "horizontal_accuracy": 0.0,
            "vertical_accuracy": 0.0,
            "speed_accuracy": 0.0,
            "course": 0.0,
            "course_accuracy": 0.0,
        },  # Null lat
    ]

    location_df = clean_location_dataframe(records)

    assert len(location_df) == 0


def test_clean_location_dataframe_empty() -> None:
    """Test handling of empty records in DataFrame."""
    location_df = clean_location_dataframe([])
    assert len(location_df) == 0
    assert "timestamp" in location_df.columns
    assert "device_id" in location_df.columns
