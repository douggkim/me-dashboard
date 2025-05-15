"""Assets loading and processing Playstation usage data."""

import dagster as dg

from src.resources.psn_resource import PSNResource


@dg.asset(
    name="psn_profile_bronze",
    key_prefix=["entertainment", "PSN"],
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
    key_prefix=["entertainment", "PSN"],
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
    key_prefix=["entertainment", "PSN"],
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
        game_details.append(game_detail[-1])

    return game_details
