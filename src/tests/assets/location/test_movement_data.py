"""Unit tests for movement_data assets and helpers."""

import datetime
import io
import json
import logging
from unittest.mock import MagicMock

import dagster as dg
import polars as pl
import pytest
from pydantic import PrivateAttr

from src.assets.location.movement_data import (
    clean_location_dataframe,
    fetch_bronze_location_data,
    load_raw_location_data,
    transform_geojson_to_records,
)
from src.resources.geo_encoder import GeoEncoderResource


@pytest.fixture
def mock_log() -> MagicMock:
    """Fixture for mock DagsterLogManager."""
    return MagicMock(spec=dg.DagsterLogManager)


@pytest.fixture
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fixture to mock environment variables."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test_access_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test_secret_key")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("AWS_S3_USE_EMULATOR", "1")
    monkeypatch.setenv("ENVIRONMENT", "dev")


@pytest.fixture
def raw_geojson_data() -> list[dict]:
    """Fixture for raw GeoJSON location data."""
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


@pytest.fixture
def mock_context() -> MagicMock:
    """Fixture for mock AssetExecutionContext."""
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.partition_key = "2025-06-01"
    context.log = MagicMock()
    return context


class MockGeoEncoder(GeoEncoderResource):
    """Mock GeoEncoderResource for testing."""

    # Use a PrivateAttr to hold the result or exception to raise
    # This bypasses Pydantic model validation issues
    _enrich_result: object = PrivateAttr(default=None)
    _mock_calls: list = PrivateAttr(default_factory=list)

    def set_result(self, result):
        self._enrich_result = result

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Override to avoid real client initialization."""

    def enrich_dataframe(self, df: pl.DataFrame, **kwargs) -> pl.DataFrame:
        """Mock behavior for enrich_dataframe."""
        self._mock_calls.append((df, kwargs))
        if isinstance(self._enrich_result, Exception):
            raise self._enrich_result
        if self._enrich_result is not None:
            return self._enrich_result
        return df


@pytest.fixture
def mock_geo_encoder() -> MockGeoEncoder:
    """Fixture for mock GeoEncoderResource."""
    return MockGeoEncoder(google_maps_api_key="test_key", s3_bucket="test_bucket")


def test_load_raw_location_data(mock_log: MagicMock) -> None:
    """Test loading raw location data from JSON files."""
    fs = MagicMock()
    json_files = ["file1.json"]
    mock_data = {"locations": [{"id": 1}]}

    # Need to mock the json.load as well or use a real file handle
    with MagicMock() as mock_file:
        mock_file.__enter__.return_value = mock_file
        fs.open.return_value = mock_file
        mock_file.read.return_value = json.dumps(mock_data)
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
            "timestamp_utc": "2024-01-01T12:00:00Z",
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


def test_load_raw_location_data_missing_key(mock_log: MagicMock) -> None:
    """Test handling of JSON files without 'locations' key."""
    fs = MagicMock()
    json_files = ["file1.json"]
    mock_data = {"other_key": []}
    fs.open.return_value.__enter__.return_value = io.StringIO(json.dumps(mock_data))

    result = load_raw_location_data(fs, json_files, mock_log)

    assert len(result) == 0
    mock_log.warning.assert_called_with("No 'locations' key found in file1.json")


def test_load_raw_location_data_json_error(mock_log: MagicMock) -> None:
    """Test handling of JSON decode errors."""
    fs = MagicMock()
    json_files = ["file1.json"]
    fs.open.return_value.__enter__.return_value = io.StringIO("invalid json")

    result = load_raw_location_data(fs, json_files, mock_log)

    assert len(result) == 0
    mock_log.exception.assert_called()


def test_load_raw_location_data_read_error(mock_log: MagicMock) -> None:
    """Test handling of general read errors."""
    fs = MagicMock()
    json_files = ["file1.json"]
    fs.open.side_effect = Exception("Read failed")

    result = load_raw_location_data(fs, json_files, mock_log)

    assert len(result) == 0
    mock_log.exception.assert_called_with("Failed to read file file1.json")


def test_transform_geojson_to_records_process_error(mock_log: MagicMock) -> None:
    """Test handling of general processing errors in GeoJSON transformation."""
    invalid_data = [None]
    result = transform_geojson_to_records(invalid_data, mock_log)

    assert len(result) == 0
    mock_log.exception.assert_called_with("Failed to process location record")


def test_fetch_bronze_location_data_not_exists(mock_context: MagicMock, mock_env: None) -> None:
    """Test fetch_bronze_location_data when path does not exist."""
    with MagicMock() as mock_fs:
        mock_fs.exists.return_value = False
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("fsspec.filesystem", lambda *args, **kwargs: mock_fs)
            result = fetch_bronze_location_data(mock_context, "/dummy/path")

    assert result == []
    mock_context.log.warning.assert_called()


def test_fetch_bronze_location_data_glob_error(mock_context: MagicMock, mock_env: None) -> None:
    """Test fetch_bronze_location_data when glob fails."""
    with MagicMock() as mock_fs:
        mock_fs.exists.return_value = True
        mock_fs.glob.side_effect = Exception("Glob failed")
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("fsspec.filesystem", lambda *args, **kwargs: mock_fs)
            result = fetch_bronze_location_data(mock_context, "/dummy/path")

    assert result == []
    mock_context.log.exception.assert_called()


def test_fetch_bronze_location_data_no_files(mock_context: MagicMock, mock_env: None) -> None:
    """Test fetch_bronze_location_data when no JSON files found."""
    with MagicMock() as mock_fs:
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = []
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("fsspec.filesystem", lambda *args, **kwargs: mock_fs)
            result = fetch_bronze_location_data(mock_context, "/dummy/path")

    assert result == []
    mock_context.log.warning.assert_called()


def test_location_data_silver_no_data(mock_geo_encoder: MockGeoEncoder, caplog: pytest.LogCaptureFixture) -> None:
    """Test location_data_silver asset when no data is found."""
    caplog.set_level(logging.WARNING)
    from src.assets.location.movement_data import location_data_silver

    context = dg.build_asset_context(partition_key="2025-06-01")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("src.assets.location.movement_data.get_storage_path", lambda **kwargs: "/dummy/path")
        mp.setattr("src.assets.location.movement_data.fetch_bronze_location_data", lambda *args: [])
        result = location_data_silver(context, mock_geo_encoder)

    assert len(result) == 0


def test_location_data_silver_no_records_after_transform(
    mock_geo_encoder: MockGeoEncoder, caplog: pytest.LogCaptureFixture
) -> None:
    """Test location_data_silver when transform returns no records."""
    caplog.set_level(logging.WARNING)
    from src.assets.location.movement_data import location_data_silver

    context = dg.build_asset_context(partition_key="2025-06-01")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("src.assets.location.movement_data.get_storage_path", lambda **kwargs: "/dummy/path")
        mp.setattr("src.assets.location.movement_data.fetch_bronze_location_data", lambda *args: [{"raw": "data"}])
        mp.setattr("src.assets.location.movement_data.transform_geojson_to_records", lambda *args: [])
        result = location_data_silver(context, mock_geo_encoder)

    assert len(result) == 0
