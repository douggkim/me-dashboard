
import pytest
from unittest.mock import MagicMock
from src.resources.github_resource import GithubResource
import datetime

@pytest.fixture
def mock_github_resource():
    """
    Fixture for mocking GithubResource.
    Configures a default mock object.
    """
    resource = MagicMock(spec=GithubResource)
    # Default behavior can be set here if needed, or overridden in specific tests
    return resource

@pytest.fixture
def sample_push_event():
    """
    Fixture providing a sample GitHub PushEvent payload.
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
            "avatar_url": "https://avatars.githubusercontent.com/u/12345?"
        },
        "repo": {
            "id": 123456,
            "name": "owner/repo",
            "url": "https://api.github.com/repos/owner/repo"
        },
        "payload": {
            "commits": [
                {
                    "sha": "sha1",
                    "author": {
                        "email": "test@example.com",
                        "name": "Test User"
                    },
                    "message": "commit message",
                    "distinct": True,
                    "url": "https://api.github.com/repos/owner/repo/commits/sha1"
                }
            ]
        },
        "public": True,
        "created_at": "2023-01-01T00:00:00Z"
    }

@pytest.fixture
def sample_commit_details():
    """
    Fixture providing sample commit details as returned by the GitHub API.
    """
    return {
        "sha": "sha1",
        "commit": {
            "author": {
                "name": "Test User",
                "email": "test@example.com",
                "date": "2023-01-01T00:00:00Z"
            },
            "committer": {
                "name": "Test User",
                "email": "test@example.com",
                "date": "2023-01-01T00:00:00Z"
            },
            "message": "commit message",
            "tree": {
                "sha": "tree_sha",
                "url": "https://api.github.com/repos/owner/repo/git/trees/tree_sha"
            },
            "url": "https://api.github.com/repos/owner/repo/git/commits/sha1",
            "comment_count": 0,
            "verification": {
                "verified": False,
                "reason": "unsigned",
                "signature": None,
                "payload": None
            }
        },
        "stats": {
            "total": 15,
            "additions": 10,
            "deletions": 5
        }
    }

@pytest.fixture
def sample_repo_stats():
    """
    Fixture providing sample repository statistics.
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
        "default_branch": "main"
    }
