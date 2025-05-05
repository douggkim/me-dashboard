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
)
def spotify_play_history_bronze(spotify_resource: SpotifyResource) -> None:
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
    target_path = get_storage_path(dataset_name="spotify", table_name="raw_play_history")
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

    # Use furl to build the path
    file_path = furl(target_path)
    file_path.path.segments.append(filename)
    full_path = file_path.path

    # Ensure the directory exists using Path
    Path(str(full_path)).parent.mkdir(parents=True, exist_ok=True)

    # Save the JSON file
    with open(str(full_path), "w", encoding="utf-8") as f:
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
)
def spotify_play_history_silver() -> pl.DataFrame:
    """
    Process Spotify play history data into a tabular format.

    Parameters
    ----------
    history_data : Dict[str, Any]
        Dictionary containing Spotify play history data.

    Returns
    -------
    pl.DataFrame
        Processed DataFrame with extracted play history information.
    """
    # TODO: Devise the right algorithm to load files -> Partition? Just by Date?
    target_path = furl(get_storage_path(dataset_name="spotify", table_name="raw_play_history"))
    raw_history_data = load_json_file(file_path=str(target_path.path.add("20250505_0511_play_History.json")))
    processed_history_data = []

    for item in raw_history_data["items"]:
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
        played_at_dt = datetime.datetime.fromisoformat(played_at.replace("Z", "+00:00"))

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

    # Create Polars DataFrame
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

    return play_history_df.with_columns(
        pl.col("played_at").str.to_datetime(format="%Y-%m-%dT%H:%M:%S.%fZ", time_unit="ms")
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
