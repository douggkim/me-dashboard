"""Unit tests for GitHub asset helper functions."""

import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.assets.work.github.github import (
    fetch_and_process_repo_stats,
    filter_gh_events_by_date,
    get_unique_repos_from_events,
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
    partition_key = "2026-02-16"
    filtered = filter_gh_events_by_date(sample_events, partition_key)

    assert len(filtered) > 0
    for event in filtered:
        created_at = datetime.datetime.fromisoformat(event["created_at"])
        assert created_at.date() == datetime.date(2026, 2, 16)

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
