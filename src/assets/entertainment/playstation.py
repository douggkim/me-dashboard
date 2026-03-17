"""Assets loading and processing Playstation usage data."""

import hashlib
from datetime import UTC, datetime

import dagster as dg
import polars as pl

from src.resources.psn_resource import PSNResource


@dg.asset(
    name="psn_profile_bronze",
    key_prefix=["bronze", "entertainment", "PSN"],
    io_manager_key="io_manager_json_txt",
    description="Raw Json files retrieved from requesting PSN Profile API",
    group_name="entertainment",
    kinds={"python", "file", "bronze"},
    tags={"domain": "entertainment", "source": "psn"},
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def psn_profile_bronze(psn_resource: PSNResource) -> dict:
    """
    Retrieve raw PSN profile data from the PlayStation Network API.

    Parameters
    ----------
    psn_resource : PSNResource
        Resource for interacting with PlayStation Network APIs.

    Returns
    -------
    dict
        Raw JSON response containing the user's PSN profile information.
    """
    return psn_resource.get_profile()


@dg.asset(
    name="psn_title_stats_bronze",  # Changed name to match function name
    key_prefix=["bronze", "entertainment", "PSN"],
    io_manager_key="io_manager_json_txt",
    description="Raw Json files retrieved from requesting PSN User history API",
    group_name="entertainment",
    kinds={"python", "file", "bronze"},
    tags={"domain": "entertainment", "source": "psn"},
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def psn_title_stats_bronze(psn_resource: PSNResource) -> list[dict]:
    """
    Retrieve play time statistics for all titles played by the user.

    Parameters
    ----------
    psn_resource : PSNResource
        Resource for interacting with PlayStation Network APIs.

    Returns
    -------
    list[dict]
        List of dictionaries containing title play statistics for each game.
        Each dictionary contains details such as play count, playtime, etc.
    """
    return psn_resource.get_title_stats()


@dg.asset(
    name="psn_game_details_bronze",
    key_prefix=["bronze", "entertainment", "PSN"],
    io_manager_key="io_manager_json_txt",
    description="Raw Json files retrieved from requesting PSN Title Search API",
    group_name="entertainment",
    kinds={"python", "file", "bronze"},
    tags={"domain": "entertainment", "source": "psn"},
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def psn_game_details_bronze(psn_title_stats_bronze: list[dict], psn_resource: PSNResource) -> list[dict]:
    """
    Fetch detailed game information for each title in the user's play history.

    Parameters
    ----------
    psn_title_stats_bronze : list[dict]
        List of dictionaries containing basic title information, including title_id.
    psn_resource : PSNResource
        Resource for interacting with PlayStation Network APIs.

    Returns
    -------
    list[dict]
        List of dictionaries containing detailed information for each game.
        Includes comprehensive metadata about each title beyond just play statistics.
    """
    title_ids = set()
    for title in psn_title_stats_bronze:
        title_id = title["title_id"]
        title_ids.add(title_id)

    game_details = []
    for title_id in title_ids:
        game_detail = psn_resource.get_game_details(title_id=title_id)
        # for some reason detail is returned as a list
        # I guess there are cases where there are multiple entries per id?
        game_detail = game_detail[-1]
        # adding title_id for ease of joining later
        # each game has a list of game ids,so wanted to keep a separate string column
        game_detail["title_id"] = title_id

        game_details.append(game_detail)

    return game_details


@dg.asset(
    name="psn_game_play_history_silver",
    key_prefix=["silver", "entertainment", "PSN"],
    io_manager_key="io_manager_pl",
    description="PSN Game play history data processed into a tabular format while joining with game details",
    group_name="entertainment",
    kinds={"polars", "silver"},
    tags={"domain": "entertainment", "source": "psn"},
    ins={
        "psn_game_details_bronze": dg.AssetIn(
            key_prefix=["bronze", "entertainment", "PSN"], input_manager_key="io_manager_json_txt"
        ),
        "psn_title_stats_bronze": dg.AssetIn(
            key_prefix=["bronze", "entertainment", "PSN"], input_manager_key="io_manager_json_txt"
        ),
    },
    metadata={"partition_cols": ["processed_date"], "primary_keys": ["processed_date", "play_history_id"]},
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def psn_game_play_history_silver(
    context: dg.AssetExecutionContext, psn_title_stats_bronze: list[dict], psn_game_details_bronze: list[dict]
) -> pl.DataFrame:
    """
    Process PlayStation Network game play history data into a structured tabular format.

    This asset combines game details and play statistics from PlayStation Network
    to create a comprehensive dataset of user gaming activity. It performs deduplication
    and ensures all datetime fields are properly formatted with UTC timezone.

    Parameters
    ----------
    context : dg.AssetExecutionContext
    The execution context providing partition information and logging capabilities.
    psn_title_stats_bronze : list[dict]
    A list of dictionaries containing raw PSN title statistics data including
    play counts, play durations, and first/last played timestamps.
    psn_game_details_bronze : list[dict]
    A list of dictionaries containing detailed information about PSN games
    including genres, ratings, publishers, and release dates.

    Returns
    -------
    pl.DataFrame
    A Polars DataFrame with processed gaming history data, structured according
    to the defined schema. Each row represents a unique play history record for
    a game title, with all datetime fields standardized to UTC timezone.

    The DataFrame contains the following columns:
    - play_history_id: Unique identifier for each play history record
    - title_id: PSN title identifier
    - title_name: Name of the game
    - country: Country of origin
    - minimum_playable_age: Minimum age requirement for playing
    - content_rating: Content rating (e.g., ESRB rating)
    - is_best_selling: Whether the game is marked as a best seller
    - genres: List of game genres
    - game_release_date: Release date of the game
    - publisher: Game publisher name
    - psn_store_score: Rating score in PSN store
    - play_count: Number of times the game has been played
    - first_played_date_time: First time the game was played (UTC)
    - last_played_date_time: Last time the game was played (UTC)
    - play_duration_in_seconds: Total play duration in seconds
    - processed_date: Date when the data was processed

    Notes
    -----
    - The function creates a unique play_history_id by hashing the combination of
    processed_date and title_id.
    - Duplicate play history records are removed based on play_history_id.
    - All datetime fields are standardized to UTC timezone.
    - If no play history data is found, an empty DataFrame with the defined schema
    is returned.

    Examples
    --------
    >>> context = dg.AssetExecutionContext(partition_key="2025-05-15")
    >>> title_stats = [{"title_id": "CUSA12345", "name": "Game A", "play_count": 10,
    ...                "first_played_date_time": "2025-05-01T12:00:00",
    ...                "last_played_date_time": "2025-05-10T15:30:00",
    ...                "play_duration": 3600}]
    >>> game_details = [{"title_id": "CUSA12345", "country": "US",
    ...                 "defaultProduct": {"minimumAge": 18, "contentRating": "M",
    ...                                   "isBestSelling": True,
    ...                                   "genres": ["Action", "Adventure"],
    ...                                   "releaseDate": "2025-01-15",
    ...                                   "leadPublisherName": "Publisher X"},
    ...                 "starRating": {"score": 4.5}}]
    >>> df = psn_game_play_history_silver(context, title_stats, game_details)
    >>> print(df.shape)
    (1, 16)
    """
    processed_date = context.partition_key
    processed_date_dt = datetime.strptime(processed_date, "%Y-%m-%d").replace(tzinfo=UTC).date()
    processed_game_play_history = []

    schema = {
        "play_history_id": pl.Utf8,
        "title_id": pl.Utf8,
        "title_name": pl.Utf8,
        "country": pl.Utf8,
        "minimum_playable_age": pl.Int64,
        "content_rating": pl.Utf8,
        "is_best_selling": pl.Boolean,
        "genres": pl.List(pl.Utf8),
        "game_release_date": pl.Date,
        "publisher": pl.Utf8,
        "psn_store_score": pl.Float64,
        "play_count": pl.Int64,
        "first_played_date_time": pl.Datetime,
        "last_played_date_time": pl.Datetime,
        "play_duration_in_seconds": pl.Float64,
        "processed_date": pl.Date,
    }

    searchable_game_details = {}

    # for joining with stat histor, we created a dictionary indexed with title_id
    for item in psn_game_details_bronze:
        searchable_game_details[item["title_id"]] = item

    for stat_dict in psn_title_stats_bronze:
        title_id = stat_dict["title_id"]
        hash_input = f"{processed_date}_{title_id}"
        play_history_id = hashlib.md5(hash_input.encode()).hexdigest()  # noqa: S324

        # Extract just the date part from the ISO timestamp
        release_date_str = searchable_game_details[title_id]["defaultProduct"]["releaseDate"]
        release_date_dt = datetime.fromisoformat(release_date_str).date()

        first_played_date_dt = datetime.fromisoformat(stat_dict["first_played_date_time"])
        last_played_date_dt = datetime.fromisoformat(stat_dict["last_played_date_time"])

        processed_item = {
            "play_history_id": play_history_id,
            "title_id": stat_dict["title_id"],
            "title_name": stat_dict["name"],
            "country": searchable_game_details[title_id]["country"],
            "minimum_playable_age": searchable_game_details[title_id]["defaultProduct"]["minimumAge"],
            "content_rating": searchable_game_details[title_id]["defaultProduct"]["contentRating"]["name"],
            "is_best_selling": searchable_game_details[title_id]["defaultProduct"]["isBestSelling"],
            "genres": searchable_game_details[title_id]["defaultProduct"]["genres"],
            "game_release_date": release_date_dt,
            "publisher": searchable_game_details[title_id]["defaultProduct"]["leadPublisherName"],
            "psn_store_score": searchable_game_details[title_id]["starRating"]["score"],
            "play_count": stat_dict["play_count"],
            "first_played_date_time": first_played_date_dt,
            "last_played_date_time": last_played_date_dt,
            "play_duration_in_seconds": stat_dict["play_duration"],
            "processed_date": processed_date_dt,
        }

        processed_game_play_history.append(processed_item)

    if not processed_game_play_history:
        context.log.warning("No additional play history found. Will be returning an empty dataframe")
        return pl.DataFrame(schema=schema)

    play_history_df = pl.from_dicts(
        processed_game_play_history,
        schema=schema,
    )

    # Add deduplication logic - keep only the first occurrence of each play_history_id
    deduplicated_df = play_history_df.unique(subset=["play_history_id"])

    if len(deduplicated_df) < len(play_history_df):
        context.log.info(f"Removed {len(play_history_df) - len(deduplicated_df)} duplicate records")

    # convert datetime columns to UTC
    for col_name in deduplicated_df.columns:
        if "date_time" in col_name.lower() and deduplicated_df[col_name].dtype == pl.Datetime:
            deduplicated_df = deduplicated_df.with_columns(**{col_name: pl.col(col_name).dt.convert_time_zone("UTC")})

    return deduplicated_df


@dg.asset(
    name="psn_profile_silver",
    key_prefix=["silver", "entertainment", "PSN"],
    io_manager_key="io_manager_pl",
    description="PSN Profile data processed into a tabular format",
    group_name="entertainment",
    kinds={"polars", "silver"},
    tags={"domain": "entertainment", "source": "psn"},
    ins={
        "psn_profile_bronze": dg.AssetIn(
            key_prefix=["bronze", "entertainment", "PSN"], input_manager_key="io_manager_json_txt"
        ),
    },
    metadata={"partition_cols": ["processed_date"], "primary_keys": ["processed_date", "profile_id"]},
    owners=["doug@randomplace.com"],
    partitions_def=dg.DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def psn_profile_silver(context: dg.AssetExecutionContext, psn_profile_bronze: list[dict]) -> pl.DataFrame:
    """
    Process PlayStation Network profile data into a structured tabular format.

    This asset extracts profile information from PSN including account details
    and trophy statistics into a standardized tabular format.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        The execution context providing partition information and logging capabilities.
    psn_profile_bronze : list[dict]
        A list of dictionaries containing raw PSN profile data with account and trophy information.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame with processed profile data, including:
        - profile_id: Unique identifier generated from processed_date and account_id
        - online_id: PSN online ID
        - account_id: PSN account ID
        - trophy_level: User's trophy level
        - trophy_progress: Progress to next trophy level
        - platinum_trophies: Number of platinum trophies earned
        - gold_trophies: Number of gold trophies earned
        - silver_trophies: Number of silver trophies earned
        - bronze_trophies: Number of bronze trophies earned
        - processed_date: Date when the data was processed
    """
    processed_date_str = context.partition_key
    processed_date = datetime.strptime(processed_date_str, "%Y-%m-%d").replace(tzinfo=UTC).date()
    processed_profiles = []

    schema = {
        "profile_id": pl.Utf8,
        "online_id": pl.Utf8,
        "account_id": pl.Utf8,
        "trophy_level": pl.Int64,
        "trophy_progress": pl.Int64,
        "platinum_trophies": pl.Int64,
        "gold_trophies": pl.Int64,
        "silver_trophies": pl.Int64,
        "bronze_trophies": pl.Int64,
        "processed_date": pl.Date,
    }

    for profile_info in psn_profile_bronze:
        account_id = profile_info["profile"]["accountId"]
        hash_input = f"{processed_date_str}_{account_id}"
        profile_id = hashlib.md5(hash_input.encode()).hexdigest()  # noqa: S324

        processed_item = {
            "profile_id": profile_id,
            "online_id": profile_info["profile"]["onlineId"],
            "account_id": account_id,
            "trophy_level": profile_info["profile"]["trophySummary"]["level"],
            "trophy_progress": profile_info["profile"]["trophySummary"]["progress"],
            "platinum_trophies": profile_info["profile"]["trophySummary"]["earnedTrophies"]["platinum"],
            "gold_trophies": profile_info["profile"]["trophySummary"]["earnedTrophies"]["gold"],
            "silver_trophies": profile_info["profile"]["trophySummary"]["earnedTrophies"]["silver"],
            "bronze_trophies": profile_info["profile"]["trophySummary"]["earnedTrophies"]["bronze"],
            "processed_date": processed_date,
        }
        processed_profiles.append(processed_item)

    if not processed_profiles:
        context.log.warning("No profile data found. Will be returning an empty dataframe")
        return pl.DataFrame(schema=schema)

    profile_df = pl.from_dicts(
        processed_profiles,
        schema=schema,
    )

    # Add deduplication logic - keep only the first occurrence of each profile_id
    deduplicated_df = profile_df.unique(subset=["profile_id"])

    if len(deduplicated_df) < len(profile_df):
        context.log.info(f"Removed {len(profile_df) - len(deduplicated_df)} duplicate profiles")

    return deduplicated_df
