"""Dagster assets for collecting and processing GitHub events and repository statistics."""

import datetime

import dagster as dg
import polars as pl
import requests
from dagster import AssetExecutionContext, AutomationCondition, DailyPartitionsDefinition, asset

from src.resources.github_resource import GithubResource
from src.validation.schemas.github_schema import github_silver_dagster_type


@asset(
    group_name="work_github",
    key_prefix=["bronze", "work", "github"],
    io_manager_key="io_manager_json_txt",
    partitions_def=DailyPartitionsDefinition(start_date="2025-05-05", end_offset=0, timezone="Etc/UTC"),
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
    partition_date_start = datetime.datetime.strptime(partition_key, "%Y-%m-%d").replace(tzinfo=datetime.UTC)
    partition_date_end = partition_date_start + datetime.timedelta(days=1)

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


def categorize_branch(branch_name: str | None) -> str:
    """
    Categorize a branch based on its name.

    Parameters
    ----------
    branch_name : str or None
        The name of the branch.

    Returns
    -------
    str
        The category: 'feature', 'fix', 'chore', or 'other'.
    """
    if not branch_name:
        return "other"

    name_lower = branch_name.lower()
    if "feat" in name_lower or "feature" in name_lower:
        return "feature"
    if "fix" in name_lower:
        return "fix"
    if "chore" in name_lower:
        return "chore"
    return "other"


def get_branch_name_from_event(event: dict) -> str | None:
    """
    Extract branch name from a GitHub event.

    Parameters
    ----------
    event : dict
        A GitHub event dictionary.

    Returns
    -------
    str or None
        The branch name if found, else None.
    """
    payload = event.get("payload", {})
    # For PushEvent and CreateEvent
    ref = payload.get("ref")
    if ref:
        if ref.startswith("refs/heads/"):
            return ref.replace("refs/heads/", "")
        return ref

    # For PullRequestEvent
    pr = payload.get("pull_request", {})
    head = pr.get("head", {})
    return head.get("ref")


def _fetch_commit_stats(
    owner: str,
    repo_name: str,
    sha: str,
    github_resource: GithubResource,
    context: AssetExecutionContext,
) -> tuple[int, int, int]:
    """
    Fetch additions, deletions, and changed file count for a specific commit.

    Parameters
    ----------
    owner : str
        Repository owner.
    repo_name : str
        Repository name.
    sha : str
        Commit SHA.
    github_resource : GithubResource
        GitHub API resource.
    context : AssetExecutionContext
        Dagster context for logging.

    Returns
    -------
    tuple[int, int, int]
        (additions, deletions, changed_file_count)
    """
    try:
        context.log.info(f"Fetching statistics for commit {sha[:7]} in {owner}/{repo_name}")
        commit_details = github_resource.get_commit(owner, repo_name, sha)
        stats = commit_details.get("stats", {})
        additions = stats.get("additions", 0)
        deletions = stats.get("deletions", 0)
        files = commit_details.get("files", [])
        changed_file_count = len(files)
        context.log.info(f"Commit {sha[:7]} stats: +{additions}, -{deletions}, files: {changed_file_count}")
        return additions, deletions, changed_file_count
    except requests.RequestException as e:
        context.log.warning(f"Failed to fetch stats for commit {sha[:7]} in {owner}/{repo_name}: {e}")
        return 0, 0, 0


def _process_push_event(
    event: dict,
    github_resource: GithubResource,
    context: AssetExecutionContext,
) -> list[dict]:
    """
    Process a PushEvent and return rows for each commit.

    Parameters
    ----------
    event : dict
        The GitHub PushEvent.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.
    context : AssetExecutionContext
        Dagster asset execution context.

    Returns
    -------
    list[dict]
        A list of processed rows, one for each commit.
    """
    rows = []
    payload = event.get("payload", {})
    commits = payload.get("commits", [])
    repo = event.get("repo", {}).get("name")
    event_id = event.get("id")
    created_at_dt = datetime.datetime.fromisoformat(event.get("created_at"))
    date = created_at_dt.date()
    branch = get_branch_name_from_event(event)
    branch_type = categorize_branch(branch)

    owner, repo_name = repo.split("/", 1)

    # Use payload['head'] if commits list is empty (common in some bronze data)
    if not commits:
        head_sha = payload.get("head")
        if head_sha:
            context.log.info(f"PushEvent {event_id} has no commits array, using head SHA: {head_sha[:7]}")
            additions, deletions, file_count = _fetch_commit_stats(owner, repo_name, head_sha, github_resource, context)
            rows.append({
                "date": date,
                "created_at": created_at_dt,
                "id": event_id,
                "event_type": "PushEvent",
                "target_repo": repo,
                "target_branch": branch,
                "branch_type": branch_type,
                "commit_sha": head_sha,
                "code_additions": additions,
                "code_deletions": deletions,
                "number_of_changed_files": file_count,
            })
        return rows

    for commit in commits:
        sha = commit.get("sha")
        if not sha:
            continue

        additions, deletions, file_count = _fetch_commit_stats(owner, repo_name, sha, github_resource, context)
        rows.append({
            "date": date,
            "created_at": created_at_dt,
            "id": event_id,
            "event_type": "PushEvent",
            "target_repo": repo,
            "target_branch": branch,
            "branch_type": branch_type,
            "commit_sha": sha,
            "code_additions": additions,
            "code_deletions": deletions,
            "number_of_changed_files": file_count,
        })
    return rows


def _process_pull_request_event(
    event: dict,
    github_resource: GithubResource,
    context: AssetExecutionContext,
) -> dict:
    """
    Process a PullRequestEvent and return a single row with head commit stats.

    Parameters
    ----------
    event : dict
        The GitHub PullRequestEvent.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.
    context : AssetExecutionContext
        Dagster asset execution context.

    Returns
    -------
    dict
        A processed row for the PR event.
    """
    payload = event.get("payload", {})
    pr_data = payload.get("pull_request", {})
    head = pr_data.get("head", {})
    sha = head.get("sha")
    repo = event.get("repo", {}).get("name")
    event_id = event.get("id")
    created_at_dt = datetime.datetime.fromisoformat(event.get("created_at"))
    date = created_at_dt.date()
    branch = get_branch_name_from_event(event)

    additions, deletions, file_count = 0, 0, 0
    if sha:
        owner, repo_name = repo.split("/", 1)
        additions, deletions, file_count = _fetch_commit_stats(owner, repo_name, sha, github_resource, context)
    else:
        context.log.warning(f"PullRequestEvent {event_id} missing head SHA")

    return {
        "date": date,
        "created_at": created_at_dt,
        "id": event_id,
        "event_type": "PullRequestEvent",
        "target_repo": repo,
        "target_branch": branch,
        "branch_type": categorize_branch(branch),
        "commit_sha": sha,
        "code_additions": additions,
        "code_deletions": deletions,
        "number_of_changed_files": file_count,
    }


def transform_github_events_to_silver(
    github_events: list[dict],
    github_resource: GithubResource,
    context: AssetExecutionContext,
) -> pl.DataFrame:
    """
    Transform raw GitHub events into a structured Polars DataFrame.

    Parameters
    ----------
    github_events : list[dict]
        List of raw GitHub events.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.
    context : AssetExecutionContext
        Dagster asset execution context.

    Returns
    -------
    pl.DataFrame
        Processed GitHub events.
    """
    processed_rows = []

    for event in github_events:
        event_type = event.get("type")
        if event_type == "DeleteEvent":
            continue

        created_at_str = event.get("created_at")
        if not created_at_str:
            continue

        created_at_dt = datetime.datetime.fromisoformat(created_at_str)
        date = created_at_dt.date()
        repo = event.get("repo", {}).get("name")
        branch = get_branch_name_from_event(event)

        if event_type == "PushEvent":
            push_rows = _process_push_event(
                event=event,
                github_resource=github_resource,
                context=context,
            )
            processed_rows.extend(push_rows)
        elif event_type == "PullRequestEvent":
            pr_row = _process_pull_request_event(
                event=event,
                github_resource=github_resource,
                context=context,
            )
            processed_rows.append(pr_row)
        else:
            processed_rows.append({
                "date": date,
                "created_at": created_at_dt,
                "id": event.get("id"),
                "event_type": event_type,
                "target_repo": repo,
                "target_branch": branch,
                "branch_type": categorize_branch(branch),
                "commit_sha": None,
                "code_additions": 0,
                "code_deletions": 0,
                "number_of_changed_files": 0,
            })

    schema = {
        "date": pl.Date,
        "created_at": pl.Datetime,
        "id": pl.Utf8,
        "event_type": pl.Utf8,
        "target_repo": pl.Utf8,
        "target_branch": pl.Utf8,
        "branch_type": pl.Utf8,
        "commit_sha": pl.Utf8,
        "code_additions": pl.Int64,
        "code_deletions": pl.Int64,
        "number_of_changed_files": pl.Int64,
    }

    if not processed_rows:
        return pl.DataFrame(schema={**schema, "primary_key": pl.Utf8})

    # Generate primary_key: hash of date, id, and commit_sha
    # commit_sha can be null for non-push/pr events, so we use fill_null
    github_events_df = (
        pl.from_dicts(processed_rows, schema=schema)
        .with_columns(
            primary_key=(pl.col("date").cast(pl.Utf8) + pl.col("id") + pl.col("commit_sha").fill_null("none")).hash()
        )
        .with_columns(pl.col("primary_key").cast(pl.Utf8))
    )

    # Deduplicate: Keep the row with more recent created_at column if primary_key is the same
    return github_events_df.sort("created_at", descending=True).unique(subset=["primary_key"], keep="first")


@asset(
    name="github_event_silver",
    key_prefix=["silver", "work", "github"],
    group_name="work_github",
    io_manager_key="io_manager_pl",
    dagster_type=github_silver_dagster_type,
    partitions_def=DailyPartitionsDefinition(start_date="2025-05-05", end_offset=1, timezone="Etc/UTC"),
    ins={"github_events": dg.AssetIn(key_prefix=["bronze", "work", "github"], input_manager_key="io_manager_json_txt")},
    metadata={"primary_keys": ["primary_key"], "partition_cols": ["date"]},
)
def github_event_silver(
    context: AssetExecutionContext, github_events: list[dict], github_resource: GithubResource
) -> pl.DataFrame:
    """
    Process raw GitHub events into a silver zone Delta table.

    This asset filters out 'DeleteEvent', extracts branch information, and
    fetches commit statistics (additions/deletions) for PushEvents.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster asset execution context.
    github_events : list[dict]
        List of raw GitHub events from the bronze zone.
    github_resource : GithubResource
        Resource for interacting with the GitHub API.

    Returns
    -------
    pl.DataFrame
        Processed GitHub events with commit statistics and branch categorization.
    """
    return transform_github_events_to_silver(
        github_events=github_events,
        github_resource=github_resource,
        context=context,
    )
