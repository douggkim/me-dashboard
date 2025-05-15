"""Assets loading and processing Spotify play history."""

import datetime
import hashlib

import dagster as dg
import polars as pl
from furl import furl

from src.resources.spotify_resource import SpotifyResource
from src.utils.data_loaders import get_storage_path
from src.utils.date import datetime_to_epoch_ms


@dg.asset(
    name="spotify_play_history_bronze",
    key_prefix=["entertainment", "spotify"],
    io_manager_key="io_manager_json_txt",
    description="Raw Json files retrieved from requesting Spotify recent-history API",
    group_name="entertainment",
    kinds={"python", "file", "bronze"},
    tags={"domain": "entertainment", "source": "spotify"},
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 */3 * * *"),  # Run every 3 hours
)
def spotify_play_history_bronze(context: dg.AssetExecutionContext, spotify_resource: SpotifyResource) -> dict:
    """
    Retrieve and save recent Spotify play history as JSON files.

    This asset retrieves the user's recently played tracks from the Spotify API
    and saves the raw response as a JSON file. It queries for tracks played
    within the last 3 hours with a limit of 50 tracks.

    Parameters
    ----------
    spotify_resource : SpotifyResource
        Resource for making authenticated calls to the Spotify API.

    Returns
    -------
    dict
        dictionary containing raw responses from spotify Playhistory API

    Notes
    -----
    The JSON file is saved with the naming format:
    YYYYMMDD_HHMM_play_History.json
    """
    processed_date = context.partition_key.replace("-", "_")
    target_base_path = furl(get_storage_path(dataset_name="spotify", table_name="raw_play_history"))

    # Add the processed_date to the path
    target_base_path.path.add(processed_date)

    return spotify_resource.call_api(
        endpoint="me/player/recently-played",
        params={
            "after": datetime_to_epoch_ms(datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=6)),
            "offset": 0,
            "limit": 50,
        },
    )


@dg.asset(
    name="spotify_play_history_silver",
    key_prefix=["entertainment", "spotify"],
    description="Processed version of the Spotify Playhistory",
    group_name="entertainment",
    kinds={"polars", "silver"},
    tags={"domain": "entertainment", "source": "spotify"},
    metadata={"partition_cols": ["played_date"], "primary_keys": ["played_date", "play_history_id"]},
    ins={
        "spotify_play_history_bronze": dg.AssetIn(
            key_prefix=["entertainment", "spotify"], input_manager_key="io_manager_json_txt"
        )
    },
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", timezone="Etc/UTC", end_offset=1),
    automation_condition=dg.AutomationCondition.on_cron("0 */12 * * *"),  # Run every 12 hours
)
def spotify_play_history_silver(
    context: dg.AssetExecutionContext, spotify_play_history_bronze: list[dict]
) -> pl.DataFrame:
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
        Returns None if there's no recent play history.
    """
    processed_history_data = []
    schema = {
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
    }

    for json_dict in spotify_play_history_bronze:
        # Process each item in the file
        for item in json_dict.get("items", []):
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

    if not processed_history_data:
        context.log.warning("No additional play history found. Will be returning an empty dataframe")
        return pl.DataFrame(schema=schema)

    play_history_df = pl.from_dicts(
        processed_history_data,
        schema=schema,
    )

    # Add deduplication logic - keep only the first occurrence of each play_history_id
    deduplicated_df = play_history_df.unique(subset=["play_history_id"])

    if len(deduplicated_df) < len(play_history_df):
        context.log.info(f"Removed {len(play_history_df) - len(deduplicated_df)} duplicate records")

    return deduplicated_df.with_columns(
        pl.col("played_at").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.fZ", time_unit="ms")
    )
