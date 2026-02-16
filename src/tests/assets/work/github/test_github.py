
import datetime
import pytest
from unittest.mock import MagicMock

# Need to make sure we can import from src
# The user's pythonpath likely includes the root, so src.assets... works
from src.assets.work.github.github import (
    _get_unique_repos_from_commits,
    _process_single_push_event,
    _fetch_and_process_repo_stats,
    _get_filtered_commits_data,
)

def test_get_unique_repos_from_commits():
    commits = [
        {"repository": "owner/repo1", "other": "data"},
        {"repository": "owner/repo2", "other": "data"},
        {"repository": "owner/repo1", "other": "data"},
    ]
    repos = _get_unique_repos_from_commits(commits)
    assert sorted(repos) == sorted(["owner/repo1", "owner/repo2"])

    assert _get_unique_repos_from_commits([]) == []


def test_process_single_push_event_with_commits_payload(
    mock_github_resource, mock_context, sample_push_event, sample_commit_details
):
    # Mock get_commit response
    # We can customize the fixture's mock return value for this specific test
    sample_commit_details["stats"] = {"additions": 10, "deletions": 5}
    mock_github_resource.get_commit.return_value = sample_commit_details

    result = _process_single_push_event(sample_push_event, mock_github_resource, mock_context)

    assert len(result) == 1
    commit = result[0]
    assert commit["commit_sha"] == "sha1"
    assert commit["repository"] == "owner/repo"
    assert commit["additions"] == 10
    assert commit["deletions"] == 5
    mock_github_resource.get_commit.assert_called_with("owner", "repo", "sha1")


def test_process_single_push_event_fallback_to_head(
    mock_github_resource, mock_context
):
    mock_github_resource.get_commit.return_value = {
        "commit": {
            "author": {
                "date": "2023-01-01T00:00:00Z",
                "name": "Test User",
                "email": "test@example.com",
            },
            "message": "head commit message",
        },
        "stats": {"additions": 20, "deletions": 10},
    }

    event = {
        "id": "123",
        "repo": {"name": "owner/repo"},
        "payload": {"commits": [], "head": "head_sha"},
    }

    result = _process_single_push_event(event, mock_github_resource, mock_context)

    assert len(result) == 1
    commit = result[0]
    assert commit["commit_sha"] == "head_sha"
    assert commit["message"] == "head commit message"
    # Verify warning was logged
    mock_context.log.warning.assert_called()


def test_fetch_and_process_repo_stats(
    mock_github_resource, sample_repo_stats
):
    partition_date = datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC)

    mock_github_resource.get_repository_stats.return_value = sample_repo_stats

    repos = ["owner/repo"]
    stats = _fetch_and_process_repo_stats(repos, mock_github_resource, partition_date)

    assert len(stats) == 1
    stat = stats[0]
    assert stat["repository"] == "owner/repo"
    assert stat["stargazers_count"] == 100
    assert stat["fetched_at"] == partition_date


def test_get_filtered_commits_data(
    mock_github_resource, mock_context, sample_push_event, sample_commit_details
):
    mock_github_resource.github_username = "testuser"
    mock_context.partition_key = "2023-01-01"

    # Mock user events
    # Add a relevant push event and an irrelevant watch event
    sample_push_event["payload"]["commits"][0]["sha"] = "sha1"
    mock_github_resource.get_user_events.return_value = [
        sample_push_event,
        {"type": "WatchEvent"},  # Should be ignored
    ]

    # Mock commit details for the push event
    # Ensure date is within partition (2023-01-01)
    sample_commit_details["commit"]["author"]["date"] = "2023-01-01T12:00:00Z"
    mock_github_resource.get_commit.return_value = sample_commit_details

    filtered_commits = _get_filtered_commits_data(context=mock_context, github_resource=mock_github_resource)

    assert len(filtered_commits) == 1
    assert filtered_commits[0]["commit_sha"] == "sha1"

    # Test filtering out of range dates
    sample_commit_details["commit"]["author"]["date"] = "2023-01-02T12:00:00Z" # Outside partition
    mock_github_resource.get_commit.return_value = sample_commit_details

    # Re-run with out-of-range date
    filtered_commits_out = _get_filtered_commits_data(context=mock_context, github_resource=mock_github_resource)
    assert len(filtered_commits_out) == 0
