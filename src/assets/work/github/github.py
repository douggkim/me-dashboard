"""Dagster assets for collecting and processing GitHub commit and repository statistics."""

import datetime

import dagster as dg
import polars as pl
from dagster import AssetExecutionContext, AutomationCondition, DailyPartitionsDefinition, asset

from src.resources.github_resource import GithubResource


@asset(
    group_name="work_github",
    key_prefix=["bronze", "work", "github"],
    io_manager_key="io_manager_json_txt",
    partitions_def=DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    automation_condition=AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def github_commits(context: AssetExecutionContext, github_resource: GithubResource) -> list:
    """
    Fetch all commits for a given GitHub user for a specific partition date.

    This asset queries the GitHub API for user events, filters for push events,
    and then retrieves detailed information for each commit, including additions
    and deletions. It handles cases where the push event payload might not
    directly contain commit details by falling back to the head commit.
    The fetched data is then filtered to match the asset's partition date.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster asset execution context. Provides `partition_key` for filtering.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.

    Returns
    -------
    list
        A list of dictionaries, where each dictionary represents a commit.
        Each dictionary contains:
        - `commit_sha` (str): The unique SHA hash of the commit.
        - `repository` (str): The repository where the commit was made (owner/repo_name).
        - `message` (str): The commit message.
        - `author_name` (str): The name of the commit author.
        - `author_email` (str): The email of the commit author.
        - `committed_at` (str): The timestamp of the commit (ISO 8601 string).
        - `additions` (int): The number of lines added in the commit.
        - `deletions` (int): The number of lines deleted in the commit.
    """
    try:
        return _get_filtered_commits_data(context, github_resource)
    except Exception as e:
        context.log.error(f"Error in github_commits asset: {e}")
        raise e


@asset(
    group_name="work_github",
    key_prefix=["bronze", "work", "github"],
    io_manager_key="io_manager_json_txt",
    partitions_def=DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    ins={
        "github_commits": dg.AssetIn(key_prefix=["bronze", "work", "github"], input_manager_key="io_manager_json_txt")
    },
    automation_condition=AutomationCondition.on_cron("0 0 * * *"),  # Run daily at midnight
)
def github_repository_stats(
    context: AssetExecutionContext,
    github_resource: GithubResource,
    github_commits: list,
) -> list[dict]:
    """
    Fetch repository statistics for all repositories a user has committed to for a specific partition date.

    This asset takes the unique repositories from the `github_commits` list of dictionaries
    (filtered by partition), queries the GitHub API for each repository's statistics
    (stars, forks, issues, etc.), and returns them as a list of dictionaries.
    The `fetched_at` timestamp is set to the partition start time.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster asset execution context. Provides `partition_key` for filtering.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.
    github_commits : list
        A list of dictionaries containing commit information (already filtered by partition),
        used to extract unique repository names.

    Returns
    -------
    list[dict]
        A list of dictionaries containing the following keys:
        - `repository` (str): The name of the repository (owner/repo_name).
        - `stargazers_count` (int): Total number of stars for the repository.
        - `forks_count` (int): Total number of forks for the repository.
        - `open_issues_count` (int): Number of open issues.
        - `watchers_count` (int): Number of watchers.
        - `created_at` (datetime): Timestamp when the repository was created.
        - `updated_at` (datetime): Timestamp when the repository was last updated.
        - `fetched_at` (datetime): Timestamp corresponding to the partition start.
    """
    if not github_commits:  # If the input list is empty
        context.log.info("No commits found for partition, skipping repository stats fetching.")
        return []

    repos = _get_unique_repos_from_commits(github_commits)

    partition_date_str = context.partition_key
    partition_date_dt = datetime.datetime.strptime(partition_date_str, "%Y-%m-%d").replace(tzinfo=datetime.UTC)

    all_stats = _fetch_and_process_repo_stats(repos, github_resource, partition_date_dt)

    context.log.info(f"Found stats for {len(all_stats)} repositories for partition {context.partition_key}")
    return all_stats


def _get_unique_repos_from_commits(github_commits: list) -> list[str]:
    """
    Extract unique repository names from a list of commit dictionaries.

    Parameters
    ----------
    github_commits : list
        A list of dictionaries, where each dictionary represents a commit.

    Returns
    -------
    list[str]
        A list of unique repository names.
    """
    if not github_commits:
        return []
    github_commits_df = pl.DataFrame(github_commits)
    return github_commits_df["repository"].unique().to_list()


def _fetch_and_process_repo_stats(
    repos: list[str],
    github_resource: GithubResource,
    partition_date_dt: datetime.datetime,
) -> list[dict]:
    """
    Fetch and process repository statistics for a list of repositories.

    Parameters
    ----------
    repos : list[str]
        A list of full repository names (e.g., "owner/repo").
    github_resource : GithubResource
        Resource for interacting with the GitHub API.
    partition_date_dt : datetime.datetime
        The datetime object representing the start of the current partition.

    Returns
    -------
    list[dict]
        A list of dictionaries, where each dictionary contains processed
        repository statistics.
    """
    all_stats = []
    for repo_name_full in repos:
        owner, repo_name = repo_name_full.split("/", 1)
        stats = github_resource.get_repository_stats(owner, repo_name)  # type: ignore
        all_stats.append({
            "repository": repo_name_full,
            "stargazers_count": stats.get("stargazers_count"),
            "forks_count": stats.get("forks_count"),
            "open_issues_count": stats.get("open_issues_count"),
            "watchers_count": stats.get("watchers_count"),
            "created_at": stats.get("created_at"),
            "updated_at": stats.get("updated_at"),
            "fetched_at": partition_date_dt,
        })
    return all_stats


def _process_single_push_event(
    event: dict, github_resource: GithubResource, context: AssetExecutionContext
) -> list[dict]:
    """
    Process a single GitHub PushEvent to extract commit details.

    Handles cases where the PushEvent payload might not directly contain a 'commits' array
    by falling back to fetching details for the 'head' commit.

    Parameters
    ----------
    event : dict
        A dictionary representing a single GitHub PushEvent.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.
    context : AssetExecutionContext
        Dagster asset execution context, used for logging warnings.

    Returns
    -------
    list[dict]
        A list of dictionaries, where each dictionary represents a commit from the event.
    """
    repo_name_full = event.get("repo", {}).get("name")
    if not repo_name_full:
        return []
    owner, repo_name = repo_name_full.split("/", 1)

    commits_from_event = []
    commits_in_payload = event.get("payload", {}).get("commits")

    if commits_in_payload:
        for commit_info in commits_in_payload:
            commit_sha = commit_info.get("sha")
            if not commit_sha:
                continue
            commit_details = github_resource.get_commit(owner, repo_name, commit_sha)
            commits_from_event.append({
                "commit_sha": commit_sha,
                "repository": repo_name_full,
                "message": commit_info.get("message"),
                "author_name": commit_info.get("author", {}).get("name"),
                "author_email": commit_info.get("author", {}).get("email"),
                "committed_at": commit_details.get("commit", {}).get("author", {}).get("date"),
                "additions": commit_details.get("stats", {}).get("additions"),
                "deletions": commit_details.get("stats", {}).get("deletions"),
            })
    else:
        commit_sha = event.get("payload", {}).get("head")
        if not commit_sha:
            return []

        context.log.warning(
            f"PushEvent {event.get('id')} for repo {repo_name_full} does not contain a 'commits' array in payload. "
            f"Falling back to 'head' commit SHA: {commit_sha}"
        )
        commit_details = github_resource.get_commit(owner, repo_name, commit_sha)
        commit_data = commit_details.get("commit", {})
        author_data = commit_data.get("author", {})
        stats_data = commit_details.get("stats", {})

        commits_from_event.append({
            "commit_sha": commit_sha,
            "repository": repo_name_full,
            "message": commit_data.get("message"),
            "author_name": author_data.get("name"),
            "author_email": author_data.get("email"),
            "committed_at": author_data.get("date"),
            "additions": stats_data.get("additions"),
            "deletions": stats_data.get("deletions"),
        })
    return commits_from_event


def _get_filtered_commits_data(context: AssetExecutionContext, github_resource: GithubResource) -> list[dict]:
    """
    Fetch all commits for a given GitHub user, processes them, and filters by partition date.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster asset execution context. Provides `partition_key` for filtering.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.

    Returns
    -------
    list[dict]
        A list of dictionaries, where each dictionary represents a commit, filtered by partition.
    """
    events = github_resource.get_user_events()
    push_events = [event for event in events if event["type"] == "PushEvent"]

    all_commits = []
    for event in push_events:
        all_commits.extend(_process_single_push_event(event, github_resource, context))

    # Convert 'committed_at' to datetime object to allow filtering
    for commit in all_commits:
        if commit.get("committed_at"):
            commit["committed_at"] = datetime.datetime.fromisoformat(
                commit["committed_at"].replace("Z", "+00:00")  # Ensure proper ISO format for Python 3.11+
            )

    # Filter by partition date
    partition_date_str = context.partition_key
    partition_date_start = datetime.datetime.strptime(partition_date_str, "%Y-%m-%d").replace(tzinfo=datetime.UTC)
    partition_date_end = partition_date_start + datetime.timedelta(days=1)

    filtered_commits = [
        commit
        for commit in all_commits
        if partition_date_start
        <= commit.get("committed_at", datetime.datetime.min.replace(tzinfo=datetime.UTC))
        < partition_date_end
    ]

    context.log.info(
        f"Found {len(filtered_commits)} commits for user {github_resource.github_username} for partition {context.partition_key}"
    )
    # Convert committed_at back to string for JSON serialization
    for commit in filtered_commits:
        if commit.get("committed_at"):
            commit["committed_at"] = commit["committed_at"].isoformat().replace("+00:00", "Z")

    return filtered_commits
