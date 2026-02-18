"""Dagster assets for collecting and processing GitHub events and repository statistics."""

import datetime

import dagster as dg
from dagster import AssetExecutionContext, AutomationCondition, DailyPartitionsDefinition, asset

from src.resources.github_resource import GithubResource


@asset(
    group_name="work_github",
    key_prefix=["bronze", "work", "github"],
    io_manager_key="io_manager_json_txt",
    partitions_def=DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def github_events(context: AssetExecutionContext, github_resource: GithubResource) -> list:
    """
    Fetch all events for a given GitHub user for a specific partition date.

    This asset queries the GitHub API for user events and filters them by the
    partition date (created_at). It stores the raw event data.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster asset execution context. Provides `partition_key` for filtering.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.

    Returns
    -------
    list
        A list of dictionaries, where each dictionary represents a GitHub event.
    """
    events = github_resource.get_user_events()

    filtered_events = filter_gh_events_by_date(events, context.partition_key)

    context.log.info(
        f"Found {len(filtered_events)} events for user {github_resource.github_username} "
        f"for partition {context.partition_key}"
    )
    return filtered_events


@asset(
    group_name="work_github",
    key_prefix=["bronze", "work", "github"],
    io_manager_key="io_manager_json_txt",
    partitions_def=DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    ins={"github_events": dg.AssetIn(key_prefix=["bronze", "work", "github"], input_manager_key="io_manager_json_txt")},
    automation_condition=AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def github_repository_stats(
    context: AssetExecutionContext,
    github_resource: GithubResource,
    github_events: list,
) -> list[dict]:
    """
    Fetch repository statistics for all repositories associated with events in the partition.

    This asset takes the unique repositories from the `github_events` list
    (filtered by partition), queries the GitHub API for each repository's statistics,
    and returns them.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster asset execution context.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.
    github_events : list
        A list of dictionaries containing event information.

    Returns
    -------
    list[dict]
        A list of dictionaries containing repository statistics.
    """
    if not github_events:
        context.log.info("No events found for partition, skipping repository stats fetching.")
        return []

    repos = get_unique_repos_from_events(github_events)

    all_stats = fetch_and_process_repo_stats(repos, github_resource)

    context.log.info(f"Found stats for {len(all_stats)} repositories for partition {context.partition_key}")
    return all_stats


def get_unique_repos_from_events(github_events: list) -> list[str]:
    """
    Extract unique repository names from a list of event dictionaries.

    Parameters
    ----------
    github_events : list
        A list of dictionaries, where each dictionary represents a GitHub event.

    Returns
    -------
    list[str]
        A list of unique repository names.
    """
    if not github_events:
        return []

    unique_repos = set()
    for event in github_events:
        repo = event.get("repo", {})
        name = repo.get("name")
        if name:
            unique_repos.add(name)

    return list(unique_repos)


def filter_gh_events_by_date(events: list, partition_key: str) -> list:
    """
    Filter GitHub events by partition date.

    Parameters
    ----------
    events : list
        List of event dictionaries.
    partition_key : str
        Partition key as 'YYYY-MM-DD'.

    Returns
    -------
    list
        Filtered list of events falling within the partition date range.
        Currently set to: [partition_key - 1 day, partition_key).
    """
    partition_date_end = datetime.datetime.strptime(partition_key, "%Y-%m-%d").replace(tzinfo=datetime.UTC)
    partition_date_start = partition_date_end - datetime.timedelta(days=1)

    filtered_events = []
    for event in events:
        created_at_str = event.get("created_at")
        if created_at_str:
            # Assuming Python 3.11+ environment where fromisoformat handles 'Z'
            created_at = datetime.datetime.fromisoformat(created_at_str)
            if partition_date_start <= created_at < partition_date_end:
                filtered_events.append(event)

    return filtered_events


def fetch_and_process_repo_stats(
    repos: list[str],
    github_resource: GithubResource,
) -> list[dict]:
    """
    Fetch and process repository statistics for a list of repositories.

    Parameters
    ----------
    repos : list[str]
        A list of full repository names (e.g., "owner/repo").
    github_resource : GithubResource
        Resource for interacting with the GitHub API.

    Returns
    -------
    list[dict]
        A list of dictionaries, where each dictionary contains processed
        repository statistics.
    """
    all_stats = []
    fetched_at = datetime.datetime.now(datetime.UTC)

    for repo_name_full in repos:
        owner, repo_name = repo_name_full.split("/", 1)
        stats = github_resource.get_repository_stats(owner, repo_name)
        all_stats.append({
            "repository": repo_name_full,
            "stargazers_count": stats.get("stargazers_count"),
            "forks_count": stats.get("forks_count"),
            "open_issues_count": stats.get("open_issues_count"),
            "watchers_count": stats.get("watchers_count"),
            "created_at": stats.get("created_at"),
            "updated_at": stats.get("updated_at"),
            "fetched_at": fetched_at,
        })
    return all_stats
