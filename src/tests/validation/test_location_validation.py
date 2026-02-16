"""Unit tests for location data validation schema."""

from datetime import date, datetime

import pandera.polars as pa
import polars as pl
import pytest

from src.validation.schemas.location_schema import LocationSilverSchema


def test_location_silver_schema_valid() -> None:
    """Test validation of valid location data."""
    valid_df = pl.DataFrame({
        "timestamp": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "timestamp_utc": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "latitude": [37.7749],
        "longitude": [-122.4194],
        "speed": [10.5],
        "battery_level": [0.85],
        "device_id": ["iPhone_15"],
        "date": [date(2025, 1, 1)],
        "formatted_address": ["123 Main St, San Francisco, CA"],
    })

    # Should not raise
    LocationSilverSchema.validate(valid_df)


def test_location_silver_schema_invalid_latitude() -> None:
    """Test validation of out-of-bounds latitude."""
    invalid_df = pl.DataFrame({
        "timestamp": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "timestamp_utc": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "latitude": [95.0],  # Invalid: > 90
        "longitude": [-122.4194],
        "speed": [10.5],
        "battery_level": [0.85],
        "device_id": ["iPhone_15"],
        "date": [date(2025, 1, 1)],
        "formatted_address": ["123 Main St"],
    })

    with pytest.raises(pa.errors.SchemaError, match="latitude"):
        LocationSilverSchema.validate(invalid_df)


def test_location_silver_schema_missing_required_field() -> None:
    """Test validation with missing required field (device_id)."""
    invalid_df = pl.DataFrame({
        "timestamp": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "timestamp_utc": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "latitude": [37.7749],
        "longitude": [-122.4194],
        "speed": [10.5],
        "battery_level": [0.85],
        "date": [date(2025, 1, 1)],
        "formatted_address": ["123 Main St"],
    })

    with pytest.raises(pa.errors.SchemaError, match="column 'device_id' not in dataframe"):
        LocationSilverSchema.validate(invalid_df)


def test_location_silver_schema_null_where_not_allowed() -> None:
    """Test validation with null in a non-nullable field."""
    invalid_df = pl.DataFrame({
        "timestamp": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "timestamp_utc": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "latitude": pl.Series([None], dtype=pl.Float64),  # Invalid: nullable=False
        "longitude": [-122.4194],
        "speed": [10.5],
        "battery_level": [0.85],
        "device_id": ["iPhone_15"],
        "date": [date(2025, 1, 1)],
        "formatted_address": ["123 Main St"],
    })

    with pytest.raises(pa.errors.SchemaError, match="non-nullable column 'latitude' contains null values"):
        LocationSilverSchema.validate(invalid_df)


def test_location_silver_schema_optional_fields_null() -> None:
    """Test validation with nulls in optional fields."""
    valid_df = pl.DataFrame({
        "timestamp": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "timestamp_utc": [datetime(2025, 1, 1, 12, 0, 0)],  # noqa: DTZ001
        "latitude": [37.7749],
        "longitude": [-122.4194],
        "speed": pl.Series([None], dtype=pl.Float64),  # Allowed
        "battery_level": pl.Series([None], dtype=pl.Float64),  # Allowed
        "device_id": ["iPhone_15"],
        "date": [date(2025, 1, 1)],
        "formatted_address": pl.Series([None], dtype=pl.String),  # Allowed
    })

    # Should not raise
    LocationSilverSchema.validate(valid_df)
