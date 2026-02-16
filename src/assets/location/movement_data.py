"""Assets for processing GPS location and movement tracking data."""

import json
from urllib.parse import urlparse

import dagster as dg
import fsspec
import polars as pl

from src.resources.geo_encoder import GeoEncoderResource
from src.utils.aws import AWSCredentialFormat, get_aws_storage_options
from src.utils.data_loaders import get_storage_path
from src.validation.schemas.location_schema import LocationSilverDagsterType


@dg.asset(
    name="location_data_silver",
    key_prefix=["silver", "location"],
    dagster_type=LocationSilverDagsterType,
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
def location_data_silver(
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
        Enriched DataFrame with columns defined in the schema.
    """
    partition_date = context.partition_key.replace("-", "_")
    base_path = get_storage_path(dataset_name="location", table_name="bronze")
    partition_path = f"{base_path}/{partition_date}"

    empty_df = pl.DataFrame(schema={"timestamp": pl.Datetime, "device_id": pl.String})

    # Fetch raw data from bronze storage
    all_locations = fetch_bronze_location_data(context, partition_path)

    if not all_locations:
        context.log.warning("No location data found")
        return empty_df

    context.log.info(f"Loaded {len(all_locations)} location records for partition {context.partition_key}")

    # Extract coordinates and properties from GeoJSON format
    records = transform_geojson_to_records(all_locations, context.log)

    if not records:
        context.log.warning("No valid location records after processing")
        return empty_df

    # Create and clean DataFrame
    location_df = clean_location_dataframe(records)

    context.log.info(f"Processing {len(location_df)} valid location records for geocoding")

    # Enrich with geocoding data using the geo_encoder resource.
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
            return df_enriched.rename({
                "administrative_area_level_1": "state",
                "administrative_area_level_2": "county",
                "locality": "city",
            })

        except Exception:
            context.log.exception("Geocoding enrichment failed")
            # Return DataFrame without enrichment if geocoding fails
            context.log.warning("Returning location data without geocoding enrichment")
            return location_df

    return location_df


def fetch_bronze_location_data(context: dg.AssetExecutionContext, partition_path: str) -> list[dict]:
    """
    Fetch raw location data from bronze storage for a given partition.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context for logging and partition info.
    partition_path : str
        The storage path for the partition.

    Returns
    -------
    list[dict]
        A list of raw location records.
    """
    # Get filesystem following existing pattern
    parsed = urlparse(partition_path)
    storage_options = get_aws_storage_options(return_credential_type=AWSCredentialFormat.UTILIZE_ENV_VARS)
    fs = (
        fsspec.filesystem("file")
        if parsed.scheme in {"", "file"}
        else fsspec.filesystem(parsed.scheme, **storage_options)
    )

    context.log.info(f"Reading location bronze data from {parsed.geturl()}")

    # Check if partition directory exists
    if not fs.exists(partition_path):
        context.log.warning(f"No location data directory found for partition {partition_path}")
        return []

    # List all JSON files in the partition directory
    try:
        json_files = fs.glob(f"{partition_path}/*.json")
    except Exception:
        context.log.exception(f"Failed to list files in {partition_path}")
        return []

    if not json_files:
        context.log.warning(f"No JSON files found in {partition_path}")
        return []

    # Load all JSON files from partition directory
    return load_raw_location_data(fs, json_files, context.log)


def load_raw_location_data(
    fs: fsspec.AbstractFileSystem, json_files: list[str], log: dg.DagsterLogManager
) -> list[dict]:
    """
    Load raw location data from a list of JSON files.

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem
        The filesystem to use for reading files.
    json_files : list[str]
        A list of paths to JSON files.
    log : dg.DagsterLogManager
        The Dagster log manager for logging information and errors.

    Returns
    -------
    list[dict]
        A list of raw location data records.
    """
    all_locations = []
    for json_file_path in json_files:
        try:
            with fs.open(json_file_path, "r") as f:
                data = json.load(f)
                if "locations" in data:
                    all_locations.extend(data["locations"])
                else:
                    log.warning(f"No 'locations' key found in {json_file_path}")
        except json.JSONDecodeError:
            log.exception(f"Failed to parse JSON file {json_file_path}")
            continue
        except Exception:
            log.exception(f"Failed to read file {json_file_path}")
            continue
    return all_locations


def transform_geojson_to_records(all_locations: list[dict], log: dg.DagsterLogManager) -> list[dict]:
    """
    Transform raw GeoJSON location data into flattened records.

    Parameters
    ----------
    all_locations : list[dict]
        A list of raw location data records in GeoJSON format.
    log : dg.DagsterLogManager
        The Dagster log manager for logging information and errors.

    Returns
    -------
    list[dict]
        A list of flattened location records.
    """
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
                log.warning(f"Invalid coordinates format: {coordinates}")
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
            log.exception("Failed to process location record")
            continue
    return records


def clean_location_dataframe(records: list[dict]) -> pl.DataFrame:
    """
    Create and clean a Polars DataFrame from flattened records.

    Parameters
    ----------
    records : list[dict]
        A list of flattened location records.

    Returns
    -------
    pl.DataFrame
        A cleaned and validated Polars DataFrame.
    """
    if not records:
        return pl.DataFrame(schema={"timestamp": pl.Datetime, "device_id": pl.String})

    location_df = pl.DataFrame(records, schema_overrides={"timestamp": pl.String})

    # Data type conversions and validation
    location_df = location_df.with_columns(
        timestamp=pl.col("timestamp").str.to_datetime(),
        timestamp_utc=pl.col("timestamp").str.to_datetime().dt.replace_time_zone("UTC"),
        latitude=pl.col("latitude").cast(pl.Float64),
        longitude=pl.col("longitude").cast(pl.Float64),
        speed=pl.col("speed").cast(pl.Float64),
        battery_level=pl.col("battery_level").cast(pl.Float64),
        altitude=pl.col("altitude").cast(pl.Float64),
        horizontal_accuracy=pl.col("horizontal_accuracy").cast(pl.Float64),
        vertical_accuracy=pl.col("vertical_accuracy").cast(pl.Float64),
        speed_accuracy=pl.col("speed_accuracy").cast(pl.Float64),
        course=pl.col("course").cast(pl.Float64),
        course_accuracy=pl.col("course_accuracy").cast(pl.Float64),
    )

    # Add partition date column
    location_df = location_df.with_columns(date=pl.col("timestamp_utc").dt.date())

    # Filter out invalid coordinates
    return location_df.filter(
        pl.col("latitude").is_not_null()
        & pl.col("longitude").is_not_null()
        & pl.col("latitude").is_between(-90, 90)
        & pl.col("longitude").is_between(-180, 180)
    )
