"""Fixtures for GitHub asset tests."""

from unittest.mock import MagicMock

import pytest

from src.resources.github_resource import GithubResource


@pytest.fixture
def sample_events() -> list[dict]:
    """
    Fixture providing sample events hardcoded for testing.

    Based on structure from gh_event.json.

    Returns
    -------
    list[dict]
        A list of sample GitHub event dictionaries.
    """
    return [
        {
            "id": "6600998574",
            "type": "PullRequestEvent",
            "actor": {
                "id": 54276417,
                "login": "douggkim",
                "display_login": "douggkim",
                "url": "https://api.github.com/users/douggkim",
            },
            "repo": {
                "id": 977702641,
                "name": "douggkim/me-dashboard",
                "url": "https://api.github.com/repos/douggkim/me-dashboard",
            },
            "payload": {
                "action": "opened",
                "number": 20,
            },
            "public": True,
            "created_at": "2026-02-16T01:42:27Z",
        },
        {
            "id": "8478208965",
            "type": "CreateEvent",
            "actor": {
                "id": 54276417,
                "login": "douggkim",
            },
            "repo": {
                "id": 977702641,
                "name": "douggkim/me-dashboard",
            },
            "payload": {
                "ref": "feature/github-workflows",
                "ref_type": "branch",
                "master_branch": "main",
                "description": "None",
                "pusher_type": "user",
            },
            "public": True,
            "created_at": "2026-02-16T01:42:09Z",
        },
        {
            "id": "8476769417",
            "type": "PushEvent",
            "actor": {
                "id": 54276417,
                "login": "douggkim",
            },
            "repo": {
                "id": 977702641,
                "name": "douggkim/me-dashboard",
            },
            "payload": {
                "repository_id": 977702641,
                "push_id": 30771734814,
                "ref": "refs/heads/feature/github-integration",
                "head": "9f61841c57271e83112dbfd8240c5c9df3a4cd6a",
            },
            "public": True,
            "created_at": "2026-02-16T00:01:22Z",
        },
        {
            "id": "old_event_1",
            "type": "PushEvent",
            "actor": {"login": "douggkim"},
            "repo": {"name": "douggkim/old-repo"},
            "created_at": "2025-01-01T10:00:00Z",
        },
    ]


@pytest.fixture
def mock_github_resource() -> MagicMock:
    """
    Fixture for mocking GithubResource.

    Configures a default mock object.

    Returns
    -------
    MagicMock
        A MagicMock object configured as a GithubResource.
    """
    return MagicMock(spec=GithubResource)


@pytest.fixture
def sample_push_event() -> dict:
    """
    Fixture providing a sample GitHub PushEvent payload.

    Returns
    -------
    dict
        A dictionary representing a GitHub PushEvent.
    """
    return {
        "id": "1234567890",
        "type": "PushEvent",
        "actor": {
            "id": 12345,
            "login": "testuser",
            "display_login": "testuser",
            "gravatar_id": "",
            "url": "https://api.github.com/users/testuser",
            "avatar_url": "https://avatars.githubusercontent.com/u/12345?",
        },
        "repo": {"id": 123456, "name": "owner/repo", "url": "https://api.github.com/repos/owner/repo"},
        "payload": {
            "commits": [
                {
                    "sha": "sha1",
                    "author": {"email": "test@example.com", "name": "Test User"},
                    "message": "commit message",
                    "distinct": True,
                    "url": "https://api.github.com/repos/owner/repo/commits/sha1",
                }
            ]
        },
        "public": True,
        "created_at": "2023-01-01T00:00:00Z",
    }


@pytest.fixture
def sample_commit_details() -> dict:
    """
    Fixture providing sample commit details as returned by the GitHub API.

    Returns
    -------
    dict
        A dictionary representing commit details.
    """
    return {
        "sha": "sha1",
        "commit": {
            "author": {"name": "Test User", "email": "test@example.com", "date": "2023-01-01T00:00:00Z"},
            "committer": {"name": "Test User", "email": "test@example.com", "date": "2023-01-01T00:00:00Z"},
            "message": "commit message",
            "tree": {"sha": "tree_sha", "url": "https://api.github.com/repos/owner/repo/git/trees/tree_sha"},
            "url": "https://api.github.com/repos/owner/repo/git/commits/sha1",
            "comment_count": 0,
            "verification": {"verified": False, "reason": "unsigned", "signature": None, "payload": None},
        },
        "stats": {"total": 15, "additions": 10, "deletions": 5},
    }


@pytest.fixture
def sample_repo_stats() -> dict:
    """
    Fixture providing sample repository statistics.

    Returns
    -------
    dict
        A dictionary representing repository statistics.
    """
    return {
        "id": 123456,
        "node_id": "MDEwOlJlcG9zaXRvcnkxMjM0NTY=",
        "name": "repo",
        "full_name": "owner/repo",
        "private": False,
        "owner": {
            "login": "owner",
            "id": 12345,
            # ... other owner fields ...
        },
        "html_url": "https://github.com/owner/repo",
        "description": "Test repository",
        "fork": False,
        "url": "https://api.github.com/repos/owner/repo",
        "created_at": "2020-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z",
        "pushed_at": "2023-01-01T00:00:00Z",
        "homepage": "",
        "size": 123,
        "stargazers_count": 100,
        "watchers_count": 100,
        "language": "Python",
        "has_issues": True,
        "has_projects": True,
        "has_downloads": True,
        "has_wiki": True,
        "has_pages": False,
        "forks_count": 50,
        "mirror_url": None,
        "archived": False,
        "disabled": False,
        "open_issues_count": 5,
        "license": None,
        "allow_forking": True,
        "is_template": False,
        "web_commit_signoff_required": False,
        "topics": [],
        "visibility": "public",
        "forks": 50,
        "open_issues": 5,
        "watchers": 100,
        "default_branch": "main",
    }
