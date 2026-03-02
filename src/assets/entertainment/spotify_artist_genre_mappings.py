"""Assets for extracting and processing Spotify Artist data."""

import datetime

import dagster as dg
import polars as pl

from src.resources.data_loader import DataLoaderResource
from src.resources.spotify_resource import SpotifyResource


@dg.asset(
    name="spotify_artist_genre_mapping_bronze",
    key_prefix=["bronze", "entertainment", "spotify"],
    io_manager_key="io_manager_json_txt",
    description="Raw JSON responses for Spotify Artists from unique artists found in daily play history",
    group_name="entertainment",
    kinds={"python", "file", "bronze"},
    tags={"domain": "entertainment", "source": "spotify"},
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.any_deps_updated(),
    ins={
        "spotify_play_history_bronze": dg.AssetIn(
            key_prefix=["bronze", "entertainment", "spotify"], input_manager_key="io_manager_json_txt"
        )
    },
)
def spotify_artist_genre_mapping_bronze(
    context: dg.AssetExecutionContext,
    spotify_play_history_bronze: list[dict],
    spotify_resource: SpotifyResource,
) -> list[dict]:
    """
    Extract unique artists from play history and fetch full artist details.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context with partition information.
    spotify_play_history_bronze : list[dict]
        Raw recent play history data containing simplified artist info.
    spotify_resource : SpotifyResource
        Resource for authenticating and making API calls to Spotify.

    Returns
    -------
    list[dict]
        A list of dictionaries containing raw artist details from Spotify.
    """
    unique_artist_ids = _extract_unique_artist_ids(spotify_play_history_bronze)

    if not unique_artist_ids:
        context.log.info("No artist IDs found in the play history data.")
        return []

    context.log.info(f"Found {len(unique_artist_ids)} unique artist IDs to fetch.")

    all_artists = _fetch_artist_details(unique_artist_ids, spotify_resource, context)

    context.log.info(f"Successfully fetched {len(all_artists)} artist records.")
    return all_artists


@dg.asset(
    name="spotify_artist_genre_mapping_silver",
    key_prefix=["silver", "entertainment", "spotify"],
    description="SCD Type 2 table storing Spotify Artist information (genres, etc).",
    group_name="entertainment",
    kinds={"polars", "silver"},
    tags={"domain": "entertainment", "source": "spotify"},
    metadata={
        "primary_keys": ["artist_id", "start_date_scd"],
        "partition_cols": [],  # Unpartitioned Delta table handles SCD 2 elegantly
    },
    io_manager_key="io_manager_pl",
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    ins={
        "spotify_artist_genre_mapping_bronze": dg.AssetIn(
            key_prefix=["bronze", "entertainment", "spotify"], input_manager_key="io_manager_json_txt"
        )
    },
    automation_condition=dg.AutomationCondition.any_deps_updated(),
    owners=["doug@randomplace.com"],
)
def spotify_artist_genre_mapping_silver(
    context: dg.AssetExecutionContext,
    spotify_artist_genre_mapping_bronze: list[dict],
    data_loader: DataLoaderResource,
) -> pl.DataFrame:
    """
    Process raw artist data into a Slowly Changing Dimension (SCD Type 2) table.

    Updates the end dates of existing artists if their genres name change,
    and inserts new records to track history over time.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context.
    spotify_artist_genre_mapping_bronze : list[dict]
        Raw artist details from the bronze zone.

    Returns
    -------
    pl.DataFrame
        SCD Type 2 formatted DataFrame containing artist details.
    """
    if not spotify_artist_genre_mapping_bronze:
        context.log.info("No artist data to process. Returning empty dataframe.")
        return pl.DataFrame(
            schema={
                "artist_id": pl.Utf8,
                "name": pl.Utf8,
                "genres": pl.List(pl.Utf8),
                "start_date_scd": pl.Date,
                "end_date_scd": pl.Date,
            }
        ).rechunk()

    incoming_df = _parse_artist_data(spotify_artist_genre_mapping_bronze)

    try:
        existing_df = data_loader.load_data("spotify", "artist_silver", pl.DataFrame)
    except Exception as e:  # noqa: BLE001
        context.log.info(f"Existing silver table not found or couldn't be loaded: {e}. Assuming initial load.")
        existing_df = pl.DataFrame(
            schema={
                "artist_id": pl.Utf8,
                "name": pl.Utf8,
                "genres": pl.List(pl.Utf8),
                "start_date_scd": pl.Date,
                "end_date_scd": pl.Date,
            }
        )

    current_date = datetime.datetime.now(datetime.UTC).date()
    updates_df = _apply_scd2_logic(incoming_df, existing_df, current_date)

    context.log.info(f"Processed SCD logic. Total operations pushed to Delta: {updates_df.height}")
    return updates_df.rechunk()


def _extract_unique_artist_ids(play_history: list[dict]) -> list[str]:
    """
    Extract unique artist IDs from the play history data.

    Parameters
    ----------
    play_history : list[dict]
        Raw recent play history data containing simplified artist info.

    Returns
    -------
    list[str]
        A list of unique artist IDs extracted from the play history.
    """
    artist_ids = set()
    for json_dict in play_history:
        for item in json_dict.get("items", []):
            track = item.get("track", {})
            for artist in track.get("artists", []):
                if artist_id := artist.get("id"):
                    artist_ids.add(artist_id)
    return list(artist_ids)


def _fetch_artist_details(
    artist_ids: list[str], spotify_resource: SpotifyResource, context: dg.AssetExecutionContext
) -> list[dict]:
    """
    Fetch full artist details for each artist ID from the Spotify API.

    Parameters
    ----------
    artist_ids : list[str]
        List of Spotify artist IDs.
    spotify_resource : SpotifyResource
        Resource for authenticating and making API calls to Spotify.
    context : dg.AssetExecutionContext
        The execution context for logging.

    Returns
    -------
    list[dict]
        A list of dictionaries containing raw artist details from Spotify.
    """
    all_artists = []

    for artist_id in artist_ids:
        try:
            # Querying GET /artists/{id} as per the remaining endpoint
            response = spotify_resource.call_api(
                endpoint=f"artists/{artist_id}",
            )
            if response:
                all_artists.append(response)
        except Exception as e:  # noqa: BLE001
            context.log.error(f"Failed to fetch artist details for {artist_id}: {e}")  # noqa: TRY400

    return all_artists


def _parse_artist_data(artist_bronze_data: list[dict]) -> pl.DataFrame:
    """
    Parse raw bronze artist data into a standardized Polars DataFrame.

    Parameters
    ----------
    artist_bronze_data : list[dict]
        Raw artist details from the bronze zone.

    Returns
    -------
    pl.DataFrame
        Parsed artist DataFrame with artist_id, name, and genres.
    """
    flat_data = []
    for item in artist_bronze_data:
        if isinstance(item, list):
            flat_data.extend(item)
        else:
            flat_data.append(item)

    parsed_artists = []
    for artist in flat_data:
        artist_id = artist.get("id")
        if not artist_id:
            continue

        parsed_artists.append({
            "artist_id": artist_id,
            "name": artist.get("name", "Unknown"),
            "genres": artist.get("genres", []),
        })

    parsed_df = pl.from_dicts(
        parsed_artists, schema={"artist_id": pl.Utf8, "name": pl.Utf8, "genres": pl.List(pl.Utf8)}
    )

    # Deduplicate keeping the last to ensure only one record per artist in incoming batch
    return parsed_df.unique(subset=["artist_id"], keep="last")


def _apply_scd2_logic(
    incoming_df: pl.DataFrame, existing_df: pl.DataFrame, current_date: datetime.date
) -> pl.DataFrame:
    """
    Apply Slowly Changing Dimension (Type 2) logic comparing incoming and existing data.

    Parameters
    ----------
    incoming_df : pl.DataFrame
        The new data to incorporate.
    existing_df : pl.DataFrame
        The current active state of the Silver table.
    current_date : datetime.date
        The date to use for SCD start/end date logic.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the updates (new rows and closed-out rows) for Delta Merge.
    """
    if existing_df.height == 0:
        return incoming_df.with_columns(
            start_date_scd=pl.lit(datetime.date(1990, 1, 1), dtype=pl.Date),
            end_date_scd=pl.lit(datetime.date(9999, 12, 31), dtype=pl.Date),
        )

    active_existing = existing_df.filter(pl.col("end_date_scd") == pl.lit(datetime.date(9999, 12, 31), dtype=pl.Date))

    comparison_df = incoming_df.join(
        active_existing.select(["artist_id", "name", "genres"]), on="artist_id", how="left", suffix="_existing"
    )

    new_records = (
        comparison_df.filter(pl.col("name_existing").is_null())
        .select(["artist_id", "name", "genres"])
        .with_columns(
            start_date_scd=pl.lit(datetime.date(1990, 1, 1), dtype=pl.Date),
            end_date_scd=pl.lit(datetime.date(9999, 12, 31), dtype=pl.Date),
        )
    )

    changed_records_incoming = comparison_df.filter(
        pl.col("name_existing").is_not_null()
        & (
            (pl.col("name") != pl.col("name_existing"))
            | (pl.col("genres").list.join(",") != pl.col("genres_existing").list.join(","))
        )
    )

    if changed_records_incoming.height == 0 and new_records.height == 0:
        return pl.DataFrame(
            schema={
                "artist_id": pl.Utf8,
                "name": pl.Utf8,
                "genres": pl.List(pl.Utf8),
                "start_date_scd": pl.Date,
                "end_date_scd": pl.Date,
            }
        )

    closed_out_records = (
        changed_records_incoming.select(["artist_id"])
        .join(active_existing, on="artist_id", how="inner")
        .with_columns(end_date_scd=pl.lit(current_date, dtype=pl.Date))
    )

    new_version_records = changed_records_incoming.select(["artist_id", "name", "genres"]).with_columns(
        start_date_scd=pl.lit(current_date, dtype=pl.Date),
        end_date_scd=pl.lit(datetime.date(9999, 12, 31), dtype=pl.Date),
    )

    return pl.concat([closed_out_records, new_records, new_version_records], how="diagonal_relaxed").rechunk()
