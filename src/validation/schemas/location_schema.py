"""
Schema definitions for location data validation.

This module defines the Pandera schemas for location-related data assets,
specifically the silver layer movement data.
"""

import pandera.engines.polars_engine as pa_pl
import pandera.polars as pa
import polars as pl
from dagster_pandera import pandera_schema_to_dagster_type


class LocationSilverSchema(pa.DataFrameModel):
    """Schema for location_data_silver asset (Parquet/Polars)."""

    timestamp: pa_pl.DateTime(time_zone="UTC") = pa.Field(
        nullable=False, description="Local timestamp when location was recorded."
    )
    timestamp_utc: pa_pl.DateTime(time_zone="UTC") = pa.Field(
        nullable=False, description="UTC timestamp when location was recorded."
    )
    latitude: float = pa.Field(
        nullable=False,
        ge=-90,
        le=90,
        description="Decimal degrees latitude (WGS84).",
    )
    longitude: float = pa.Field(
        nullable=False,
        ge=-180,
        le=180,
        description="Decimal degrees longitude (WGS84).",
    )
    speed: float = pa.Field(
        nullable=True,
        ge=-1.0,
        description="Movement speed in meters per second. -1 typically indicates unavailable.",
    )
    battery_level: float = pa.Field(
        nullable=True,
        ge=0.0,
        le=1.0,
        description="Device battery level as a percentage (0.0 to 1.0).",
    )
    device_id: str = pa.Field(nullable=False, description="Identifier for the source mobile device.")
    date: pl.Date = pa.Field(nullable=False, description="Partition date in YYYY-MM-DD format.")
    formatted_address: str = pa.Field(nullable=True, description="Enriched human-readable address from geocoding.")

    class Config:
        """Pandera configuration."""

        strict = False  # Allow extra columns (e.g., geocoding detail columns)
        description = "Schema for processed location movement data."


# Export Dagster type for asset return type validation
LocationSilverDagsterType = pandera_schema_to_dagster_type(LocationSilverSchema)
