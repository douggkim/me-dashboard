"""Assets loading and processing Spotify play history."""

import datetime
import hashlib
import json
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
from furl import furl
from loguru import logger

from src.resources.spotify_resource import SpotifyResource
from src.utils.data_loaders import get_storage_path
from src.utils.date import datetime_to_epoch_ms


@dg.asset(
    name="spotify_play_history_bronze",
    key_prefix=["entertainment", "spotify"],
    description="Raw Json files retrieved from requesting Spotify recent-history API",
    group_name="entertainment",
    kinds={"python", "file", "bronze"},
    tags={"domain": "entertainment", "source": "spotify"},
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
)
def spotify_play_history_bronze(context: dg.AssetExecutionContext, spotify_resource: SpotifyResource) -> None:
    """
    Retrieve and save recent Spotify play history as JSON files.

    This asset retrieves the user's recently played tracks from the Spotify API
    and saves the raw response as a JSON file. It queries for tracks played
    within the last 3 hours with a limit of 50 tracks.

    Parameters
    ----------
    spotify_resource : SpotifyResource
        Resource for making authenticated calls to the Spotify API.

    Notes
    -----
    The JSON file is saved with the naming format:
    YYYYMMDD_HHMM_play_History.json
    """
    processed_date = context.partition_key.replace("-", "_")
    target_base_path = furl(get_storage_path(dataset_name="spotify", table_name="raw_play_history"))

    # Add the processed_date to the path
    target_base_path.path.add(processed_date)

    play_history_raw = spotify_resource.call_api(
        endpoint="me/player/recently-played",
        params={
            "after": datetime_to_epoch_ms(datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=3)),
            "offset": 0,
            "limit": 50,
        },
    )

    # Get the current datetime for the filename
    now = datetime.datetime.now(datetime.UTC)
    filename = now.strftime("%Y%m%d_%H%M_play_History.json")

    # Add the filename to the path
    target_base_path.path.add(filename)

    # Convert the furl path to string for use with Path
    file_path_str = str(target_base_path.path)

    # Ensure the directory exists using Path
    Path(file_path_str).parent.mkdir(parents=True, exist_ok=True)

    # Save the JSON file
    with open(file_path_str, "w", encoding="utf-8") as f:
        json.dump(play_history_raw, f, indent=2)


@dg.asset(
    name="spotify_play_history_silver",
    key_prefix=["entertainment", "spotify"],
    description="Processed version of the Spotify Playhistory",
    group_name="entertainment",
    kinds={"polars", "silver"},
    tags={"domain": "entertainment", "source": "spotify"},
    metadata={"partition_cols": ["played_date"], "primary_keys": ["played_date", "play_history_id"]},
    deps=["spotify_play_history_bronze"],
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", timezone="Etc/UTC", end_offset=1),
)
def spotify_play_history_silver(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Process Spotify play history data into a tabular format from all JSON files in the date directory.

    This asset reads all JSON files from the partition directory and transforms them
    into a structured DataFrame with properly typed columns.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context with partition information.

    Returns
    -------
    pl.DataFrame
        Processed DataFrame with extracted play history information.

    Raises
    ------
    ValueError
        If no file is found at the location.
    """
    processed_date = context.partition_key.replace("-", "_")

    # Get the base directory for the date partition
    target_data_path = furl(get_storage_path(dataset_name="spotify", table_name="raw_play_history"))
    target_data_path = target_data_path.path.add(processed_date)
    target_data_path = str(target_data_path)

    # Find all JSON files in the date directory
    json_files = list(Path(target_data_path).glob("*.json"))

    if not json_files:
        raise ValueError(f"No JSON files found in directory: {target_data_path}")

    processed_history_data = []

    for json_file in json_files:
        raw_history_data = load_json_file(file_path=str(json_file))

        # Process each item in the file
        for item in raw_history_data.get("items", []):
            track = item["track"]
            album = track["album"]
            played_at = item["played_at"]
            song_id = track["id"]

            # Create play_history_id by hashing played_at and song_id
            hash_input = f"{played_at}_{song_id}"
            play_history_id = hashlib.md5(hash_input.encode()).hexdigest()  # noqa: S324

            # Convert duration from ms to seconds
            duration_seconds = track["duration_ms"] / 1000

            # Parse played_at to datetime
            played_at_dt = datetime.datetime.fromisoformat(played_at)

            # Extract date for partitioning
            played_date = played_at_dt.date()

            processed_item = {
                "play_history_id": play_history_id,
                "played_at": played_at,
                "played_date": played_date,
                "duration_seconds": duration_seconds,
                "duration_ms": track["duration_ms"],
                "artist_names": [artist["name"] for artist in track["artists"]],
                "song_id": song_id,
                "song_name": track["name"],
                "album_name": album["name"],
                "popularity_points_by_spotify": track["popularity"],
                "is_explicit": track["explicit"],
                "song_release_date": album["release_date"],
                "no_of_available_markets": len(album["available_markets"]),
                "album_type": album["album_type"],
                "total_tracks": album["total_tracks"],
            }
            processed_history_data.append(processed_item)

    # If no data was processed, return an empty DataFrame with the correct schema
    if not processed_history_data:
        raise ValueError(
            f"No play history data processed for files for date: {processed_date}. Please check the data quality and the schema"
        )

    play_history_df = pl.from_dicts(
        processed_history_data,
        schema={
            "play_history_id": pl.Utf8,
            "played_at": pl.Utf8,
            "played_date": pl.Date,
            "duration_seconds": pl.Float64,
            "duration_ms": pl.Int64,
            "artist_names": pl.List(pl.Utf8),
            "song_id": pl.Utf8,
            "song_name": pl.Utf8,
            "album_name": pl.Utf8,
            "popularity_points_by_spotify": pl.Int64,
            "is_explicit": pl.Boolean,
            "song_release_date": pl.Utf8,
            "no_of_available_markets": pl.Int64,
            "album_type": pl.Utf8,
            "total_tracks": pl.Int64,
        },
    )

    # Add deduplication logic - keep only the first occurrence of each play_history_id
    deduplicated_df = play_history_df.unique(subset=["play_history_id"])

    if len(deduplicated_df) < len(play_history_df):
        context.log.info(f"Removed {len(play_history_df) - len(deduplicated_df)} duplicate records")

    return deduplicated_df.with_columns(
        pl.col("played_at").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.fZ", time_unit="ms")
    )


# TODO: Should be deleted later and be replaced with IO manager
def load_json_file(file_path: str | Path) -> dict[str, Any]:
    """
    Load a JSON file as a Python dictionary.

    Parameters
    ----------
    file_path : str or Path
        Path to the JSON file containing Spotify play history data.

    Returns
    -------
    dict
        Dictionary containing the parsed JSON data.

    Raises
    ------
    FileNotFoundError
        If the specified file doesn't exist.
    json.JSONDecodeError
        If the file contains invalid JSON.
    """
    try:
        # Convert to Path object for better path handling
        path = Path(file_path)

        # Open and read the file
        with open(path, encoding="utf-8") as file:
            return json.load(file)

    except FileNotFoundError:
        logger.critical(f"Error: File '{file_path}' not found.")
        raise
    except json.JSONDecodeError as e:
        logger.critical(f"Error: Invalid JSON in file '{file_path}': {e}")
        raise
    except Exception as e:
        logger.critical(f"Unexpected error loading '{file_path}': {e}")
        raise
