"""Assets for processing GPS location and movement tracking data."""

import json
from urllib.parse import urlparse

import dagster as dg
import fsspec
import polars as pl
from loguru import logger

from src.resources.geo_encoder import GeoEncoderResource
from src.utils.aws import AWSCredentialFormat, get_aws_storage_options
from src.utils.data_loaders import get_storage_path


@dg.asset(
    name="location_data_silver",
    key_prefix=["silver", "location"],
    description="Processed GPS location data enriched with geocoding information",
    group_name="location",
    kinds={"polars", "silver"},
    tags={"domain": "location", "source": "mobile_device"},
    metadata={
        "partition_cols": ["date"],
        "primary_keys": [
            "timestamp",
            "device_id",
        ],  # Since it's for one user, this should suffice. But might need revisit if scaling
    },
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-06-01", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 6 * * *"),  # Run daily at 6 AM
)
def location_data_silver(  # noqa: C901, PLR0912, PLR0915
    context: dg.AssetExecutionContext,
    geo_encoder: GeoEncoderResource,
) -> pl.DataFrame:
    """
    Transform and enrich location data with geocoding information.

    This asset processes raw GPS location data by:
    1. Loading JSON files from the partition directory
    2. Extracting coordinates and device metadata from GeoJSON format
    3. Flattening nested properties into columns
    4. Enriching coordinates with address information using Google Maps API with S3/MinIO caching
    5. Applying data cleaning and type validation
    6. Adding derived columns like date partitions

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context with partition information
    geo_encoder : GeoEncoderResource
        Resource for geocoding coordinates to addresses with caching

    Returns
    -------
    pl.DataFrame
        Enriched DataFrame with columns:
        - timestamp: ISO timestamp when location was recorded
        - latitude: Decimal degrees latitude
        - longitude: Decimal degrees longitude
        - speed: Movement speed (m/s, -1 if unavailable)
        - motion: Type of detected motion (walking, driving, etc.)
        - battery_state: Device battery charging state
        - battery_level: Battery percentage (0.0-1.0)
        - altitude: Elevation in meters
        - horizontal_accuracy: GPS accuracy in meters
        - vertical_accuracy: Altitude accuracy in meters
        - device_id: Device identifier
        - date: Partition date (YYYY-MM-DD)
        - formatted_address: Human-readable address from geocoding
        - country: Country from geocoding
        - state: State/province from geocoding
        - county: County from geocoding
        - city: City from geocoding
        - postal_code: ZIP/postal code from geocoding
        - place_id: Google Maps place identifier

    Raises
    ------
    FileNotFoundError
        If the partition directory or JSON files don't exist
    ValueError
        If location data is malformed or missing required fields
    Exception
        If geocoding API calls fail
    """
    partition_date = context.partition_key.replace("-", "_")
    base_path = get_storage_path(dataset_name="location", table_name="raw_data")
    partition_path = f"{base_path}/{partition_date}"

    # Get filesystem following existing pattern
    parsed = urlparse(partition_path)
    storage_options = get_aws_storage_options(return_credential_type=AWSCredentialFormat.UTILIZE_ENV_VARS)
    fs = (
        fsspec.filesystem("file")
        if parsed.scheme in {"", "file"}
        else fsspec.filesystem(parsed.scheme, **storage_options)
    )

    logger.info(f"Reading location bronze data from {parsed}")

    # Check if partition directory exists
    if not fs.exists(partition_path):
        context.log.warning(f"No location data directory found for partition {context.partition_key}")
        return pl.DataFrame()

    # List all JSON files in the partition directory
    try:
        json_files = fs.glob(f"{partition_path}/*.json")
    except Exception:
        context.log.exception(f"Failed to list files in {partition_path}")
        return pl.DataFrame()

    if not json_files:
        context.log.warning(f"No JSON files found in {partition_path}")
        return pl.DataFrame()

    # Load all JSON files from partition directory
    all_locations = []

    for json_file_path in json_files:
        try:
            with fs.open(json_file_path, "r") as f:
                data = json.load(f)
                if "locations" in data:
                    all_locations.extend(data["locations"])
                else:
                    context.log.warning(f"No 'locations' key found in {json_file_path}")
        except json.JSONDecodeError:
            context.log.exception(f"Failed to parse JSON file {json_file_path}")
            continue
        except Exception:
            context.log.exception(f"Failed to read file {json_file_path}")
            continue

    if not all_locations:
        context.log.warning("No location data found")
        return pl.DataFrame()

    context.log.info(f"Loaded {len(all_locations)} location records from {len(json_files)} files")

    # Extract coordinates and properties from GeoJSON format
    records = []

    for location in all_locations:
        try:
            geometry = location.get("geometry", {})
            properties = location.get("properties", {})

            # Extract coordinates (GeoJSON format is [lng, lat])
            coordinates = geometry.get("coordinates", [None, None])
            if len(coordinates) >= 2:
                longitude, latitude = coordinates[0], coordinates[1]
            else:
                context.log.warning(f"Invalid coordinates format: {coordinates}")
                continue

            # Build record with flattened properties
            record = {
                "timestamp": properties.get("timestamp"),
                "latitude": latitude,
                "longitude": longitude,
                "speed": properties.get("speed", -1),
                "motion": ",".join(properties.get("motion", []))
                if isinstance(properties.get("motion"), list)
                else properties.get("motion"),
                "battery_state": properties.get("battery_state"),
                "battery_level": properties.get("battery_level"),
                "altitude": properties.get("altitude"),
                "horizontal_accuracy": properties.get("horizontal_accuracy"),
                "vertical_accuracy": properties.get("vertical_accuracy"),
                "speed_accuracy": properties.get("speed_accuracy", -1),
                "course": properties.get("course", -1),
                "course_accuracy": properties.get("course_accuracy", -1),
                "device_id": properties.get("device_id"),
                "wifi": properties.get("wifi", ""),
            }

            records.append(record)

        except Exception:
            context.log.exception("Failed to process location record")
            continue

    if not records:
        context.log.warning("No valid location records after processing")
        return pl.DataFrame()

    # Create DataFrame with proper schema
    location_df = pl.DataFrame(records)

    # Data type conversions and validation
    location_df = location_df.with_columns([
        pl.col("timestamp").str.to_datetime(),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("speed").cast(pl.Float64),
        pl.col("battery_level").cast(pl.Float64),
        pl.col("altitude").cast(pl.Float64),
        pl.col("horizontal_accuracy").cast(pl.Float64),
        pl.col("vertical_accuracy").cast(pl.Float64),
        pl.col("speed_accuracy").cast(pl.Float64),
        pl.col("course").cast(pl.Float64),
        pl.col("course_accuracy").cast(pl.Float64),
    ])

    # Add partition date column using preferred syntax
    location_df = location_df.with_columns(date=pl.col("timestamp").dt.date())

    # Filter out invalid coordinates
    location_df = location_df.filter(
        pl.col("latitude").is_not_null()
        & pl.col("longitude").is_not_null()
        & pl.col("latitude").is_between(-90, 90)
        & pl.col("longitude").is_between(-180, 180)
    )

    context.log.info(f"Processing {len(location_df)} valid location records for geocoding")

    # Enrich with geocoding data using the geo_encoder resource with S3/MinIO caching
    if len(location_df) > 0:
        try:
            df_enriched = geo_encoder.enrich_dataframe(
                location_df,
                lat_col="latitude",
                lng_col="longitude",
                enrich_columns=[
                    "formatted_address",
                    "country",
                    "administrative_area_level_1",  # state
                    "administrative_area_level_2",  # county
                    "locality",  # city
                    "postal_code",
                    "place_id",
                ],
            )

            # Rename geocoding columns to be more readable
            df_enriched = df_enriched.rename({
                "administrative_area_level_1": "state",
                "administrative_area_level_2": "county",
                "locality": "city",
            })

            context.log.info(f"Successfully enriched {len(df_enriched)} location records with geocoding data")
            return df_enriched

        except Exception:
            context.log.exception("Geocoding enrichment failed")
            # Return DataFrame without enrichment if geocoding fails
            context.log.warning("Returning location data without geocoding enrichment")
            return location_df

    return location_df
