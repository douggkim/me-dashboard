"""Unit tests for GitHub asset helper functions."""

import datetime
from unittest.mock import MagicMock, patch

import dagster as dg
import polars as pl
import pytest

from src.assets.work.github.github import (
    _process_pull_request_event,
    _process_push_event,
    categorize_branch,
    fetch_and_process_repo_stats,
    filter_gh_events_by_date,
    get_branch_name_from_event,
    get_unique_repos_from_events,
    transform_github_events_to_silver,
)


def test_get_unique_repos_from_events(sample_events: list[dict]) -> None:
    """Test extracting unique repos from events."""
    repos = get_unique_repos_from_events(sample_events)
    # Based on gh_event.json content viewed earlier, "douggkim/me-dashboard" is repeated
    assert "douggkim/me-dashboard" in repos
    assert len(repos) > 0


def test_get_unique_repos_from_events_empty() -> None:
    """Test extracting unique repos from empty events list."""
    assert get_unique_repos_from_events([]) == []


def test_filter_gh_events_by_date(sample_events: list[dict]) -> None:
    """Test filtering events by date using data from gh_event.json."""
    # gh_event.json contains events for 2026-02-16
    # Testing commit - below line for some reason is not being uploaded to GH
    partition_key = "2026-02-16"
    filtered = filter_gh_events_by_date(sample_events, partition_key)

    assert len(filtered) > 0
    for event in filtered:
        created_at = datetime.datetime.fromisoformat(event["created_at"])
        assert created_at.date() == datetime.date(2026, 2, 16)

    # Test that partition 2026-02-16 doesn't include 2026-02-16 events anymore
    # because it now looks for 2026-02-15 events.
    filtered_no_match = filter_gh_events_by_date(sample_events, "2026-02-16")
    assert len(filtered_no_match) == 0

    # Test a date that shouldn't match (e.g., 2020-01-01)
    partition_key_empty = "2020-01-01"
    filtered_empty = filter_gh_events_by_date(sample_events, partition_key_empty)
    assert len(filtered_empty) == 0


def test_filter_gh_events_by_date_invalid_format() -> None:
    """Test filtering events with invalid date format raises ValueError."""
    events = [{"id": "1", "created_at": "invalid-date"}]
    with pytest.raises(ValueError, match="Invalid isoformat string"):
        filter_gh_events_by_date(events, "2023-01-01")


def test_fetch_and_process_repo_stats() -> None:
    """Test fetching and processing repo stats."""
    mock_github_resource = MagicMock()
    mock_github_resource.get_repository_stats.side_effect = [
        {
            "stargazers_count": 10,
            "forks_count": 5,
            "open_issues_count": 2,
            "watchers_count": 3,
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2023-01-01T00:00:00Z",
        },
        {
            "stargazers_count": 20,
            "forks_count": 8,
            "open_issues_count": 1,
            "watchers_count": 4,
            "created_at": "2021-01-01T00:00:00Z",
            "updated_at": "2023-01-02T00:00:00Z",
        },
    ]

    repos = ["owner/repo1", "owner/repo2"]

    # Mock datetime to ensure consistent fetched_at
    fixed_now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    with patch("src.assets.work.github.github.datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now

        stats = fetch_and_process_repo_stats(repos, mock_github_resource)

    assert len(stats) == 2

    assert stats[0]["repository"] == "owner/repo1"
    assert stats[0]["stargazers_count"] == 10
    assert stats[0]["fetched_at"] == fixed_now

    assert stats[1]["repository"] == "owner/repo2"
    assert stats[1]["stargazers_count"] == 20
    assert stats[1]["fetched_at"] == fixed_now

    assert mock_github_resource.get_repository_stats.call_count == 2
    mock_github_resource.get_repository_stats.assert_any_call("owner", "repo1")
    mock_github_resource.get_repository_stats.assert_any_call("owner", "repo2")


def test_fetch_and_process_repo_stats_error_handling() -> None:
    """Test fetching repo stats with error handling."""
    mock_github_resource = MagicMock()
    # First call succeeds, second raises exception
    mock_github_resource.get_repository_stats.side_effect = [
        {
            "stargazers_count": 10,
            "forks_count": 5,
            "open_issues_count": 2,
            "watchers_count": 3,
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2023-01-01T00:00:00Z",
        },
        Exception("Repo not found"),
    ]

    repos = ["owner/repo1", "owner/repo2"]

    # We expect the exception to propagate up
    with pytest.raises(Exception, match="Repo not found"):
        fetch_and_process_repo_stats(repos, mock_github_resource)


def test_categorize_branch() -> None:
    """Test branch categorization logic."""
    assert categorize_branch("feat/add-login") == "feature"
    assert categorize_branch("feature/dashboard") == "feature"
    assert categorize_branch("fix/bug-123") == "fix"
    assert categorize_branch("chore/update-deps") == "chore"
    assert categorize_branch("main") == "other"
    assert categorize_branch(None) == "other"


def test_get_branch_name_from_event() -> None:
    """Test branch name extraction from different event types."""
    # Push/Create event
    push_event = {"payload": {"ref": "refs/heads/main"}}
    assert get_branch_name_from_event(push_event) == "main"

    create_event = {"payload": {"ref": "feature/xyz"}}
    assert get_branch_name_from_event(create_event) == "feature/xyz"

    # PR event
    pr_event = {"payload": {"pull_request": {"head": {"ref": "feat-123"}}}}
    assert get_branch_name_from_event(pr_event) == "feat-123"

    # Empty payload
    assert get_branch_name_from_event({}) is None


def test__process_push_event(mock_github_resource: MagicMock) -> None:
    """Test processing a PushEvent with multiple commits."""
    event = {
        "id": "event_1",
        "type": "PushEvent",
        "created_at": "2023-01-01T10:00:00Z",
        "repo": {"name": "owner/repo"},
        "payload": {
            "ref": "refs/heads/feat-1",
            "commits": [{"sha": "sha1"}, {"sha": "sha2"}],
        },
    }
    mock_github_resource.get_commit.side_effect = [
        {"stats": {"additions": 10, "deletions": 5}, "files": [{}, {}]},
        {"stats": {"additions": 20, "deletions": 2}, "files": [{}]},
    ]
    mock_context = MagicMock()

    rows = _process_push_event(event, mock_github_resource, mock_context)

    assert len(rows) == 2
    assert rows[0]["commit_sha"] == "sha1"
    assert rows[0]["created_at"] == datetime.datetime.fromisoformat("2023-01-01T10:00:00Z")
    assert rows[0]["code_additions"] == 10
    assert rows[0]["number_of_changed_files"] == 2
    assert rows[0]["branch_type"] == "feature"
    assert rows[1]["commit_sha"] == "sha2"
    assert rows[1]["created_at"] == datetime.datetime.fromisoformat("2023-01-01T10:00:00Z")
    assert rows[1]["code_additions"] == 20
    assert rows[1]["number_of_changed_files"] == 1


def test__process_push_event_no_commits_fallback(mock_github_resource: MagicMock) -> None:
    """Test PushEvent fallback to head SHA when commits array is empty."""
    event = {
        "id": "event_fallback",
        "type": "PushEvent",
        "created_at": "2023-01-01T10:00:00Z",
        "repo": {"name": "owner/repo"},
        "payload": {
            "ref": "refs/heads/main",
            "head": "fallback_sha",
            "commits": [],
        },
    }
    mock_github_resource.get_commit.return_value = {"stats": {"additions": 5, "deletions": 3}, "files": [{}]}
    mock_context = MagicMock()

    rows = _process_push_event(event, mock_github_resource, mock_context)

    assert len(rows) == 1
    assert rows[0]["commit_sha"] == "fallback_sha"
    assert rows[0]["code_additions"] == 5
    assert rows[0]["number_of_changed_files"] == 1
    mock_github_resource.get_commit.assert_called_with("owner", "repo", "fallback_sha")


def test__process_pull_request_event(mock_github_resource: MagicMock) -> None:
    """Test processing a PullRequestEvent."""
    event = {
        "id": "pr_1",
        "type": "PullRequestEvent",
        "created_at": "2023-01-01T12:00:00Z",
        "repo": {"name": "owner/repo"},
        "payload": {
            "pull_request": {
                "head": {"ref": "feat-pr", "sha": "pr_sha"},
            },
        },
    }
    mock_github_resource.get_commit.return_value = {"stats": {"additions": 15, "deletions": 5}, "files": [{}, {}, {}]}
    mock_context = MagicMock()

    row = _process_pull_request_event(event, mock_github_resource, mock_context)

    assert row["event_type"] == "PullRequestEvent"
    assert row["commit_sha"] == "pr_sha"
    assert row["created_at"] == datetime.datetime.fromisoformat("2023-01-01T12:00:00Z")
    assert row["code_additions"] == 15
    assert row["number_of_changed_files"] == 3
    assert row["branch_type"] == "feature"


def test_transform_github_events_to_silver(mock_github_resource: MagicMock) -> None:
    """Test the transform_github_events_to_silver transformation logic."""
    events = [
        {
            "id": "1",
            "type": "PullRequestEvent",
            "created_at": "2026-02-16T10:00:00Z",
            "repo": {"name": "owner/repo"},
            "payload": {"pull_request": {"head": {"ref": "feat-1", "sha": "sha_pr"}}},
        },
        {
            "id": "2",
            "type": "PushEvent",
            "created_at": "2026-02-16T11:00:00Z",
            "repo": {"name": "owner/repo"},
            "payload": {
                "ref": "refs/heads/fix-1",
                "commits": [{"sha": "sha1"}, {"sha": "sha2"}],
            },
        },
        {
            "id": "3",
            "type": "CreateEvent",
            "created_at": "2026-02-16T12:00:00Z",
            "repo": {"name": "owner/repo"},
            "payload": {"ref": "chore-1"},
        },
    ]

    mock_github_resource.get_commit.return_value = {"stats": {"additions": 5, "deletions": 1}, "files": [{}]}
    context = dg.build_asset_context(partition_key="2026-02-16")

    github_event_df = transform_github_events_to_silver(
        github_events=events,
        github_resource=mock_github_resource,
        context=context,
    )

    # 1 (PR) + 2 (Push commits) + 1 (Create) = 4 rows
    assert len(github_event_df) == 4
    assert "date" in github_event_df.columns
    assert "created_at" in github_event_df.columns
    assert "code_additions" in github_event_df.columns
    assert "number_of_changed_files" in github_event_df.columns
    assert "primary_key" in github_event_df.columns

    # Check that PR event has commit stats and file count
    pr_rows = github_event_df.filter(pl.col("event_type") == "PullRequestEvent")
    assert pr_rows["code_additions"][0] == 5
    assert pr_rows["number_of_changed_files"][0] == 1
    assert pr_rows["created_at"][0] == datetime.datetime.fromisoformat("2026-02-16T10:00:00Z").replace(tzinfo=None)

    # Verify primary_key is unique and not null
    assert github_event_df["primary_key"].n_unique() == 4
    assert github_event_df["primary_key"].is_null().sum() == 0

    assert github_event_df["event_type"].n_unique() == 3
    assert github_event_df.filter(pl.col("event_type") == "PushEvent").height == 2


def test_transform_github_events_to_silver_deduplication(mock_github_resource: MagicMock) -> None:
    """Test that duplicates are removed, keeping the one with the latest created_at."""
    events = [
        {
            "id": "dup_1",
            "type": "CreateEvent",
            "created_at": "2026-02-16T10:00:00Z",
            "repo": {"name": "owner/repo"},
            "payload": {"ref": "main"},
        },
        {
            "id": "dup_1",
            "type": "CreateEvent",
            "created_at": "2026-02-16T12:00:00Z",  # More recent
            "repo": {"name": "owner/repo"},
            "payload": {"ref": "main"},
        },
    ]

    context = dg.build_asset_context(partition_key="2026-02-16")

    github_event_df = transform_github_events_to_silver(
        github_events=events,
        github_resource=mock_github_resource,
        context=context,
    )

    # Should only have 1 row
    assert len(github_event_df) == 1
    # Should be the more recent one
    assert github_event_df["created_at"][0] == datetime.datetime.fromisoformat("2026-02-16T12:00:00Z").replace(
        tzinfo=None
    )
