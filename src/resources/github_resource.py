"""Dagster resource for interacting with the GitHub API."""

import requests
from dagster import ConfigurableResource


class GithubResource(ConfigurableResource):
    """
    A Dagster resource for interacting with the GitHub API.

    Attributes
    ----------
    github_token : str
        Personal access token for GitHub API authentication.
    github_username : str
        The GitHub username for which to fetch data.
    """

    github_token: str
    github_username: str

    def get_user_events(self) -> list:
        """
        Fetch public events for the configured GitHub user.

        Returns
        -------
        list
            A list of dictionaries, where each dictionary represents a public event.
        """
        url = f"https://api.github.com/users/{self.github_username}/events"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.github_token}",
        }
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()

    def get_commit(self, owner: str, repo_name: str, commit_sha: str) -> dict:
        """
        Fetch a specific commit from a repository.

        Parameters
        ----------
        owner : str
            The owner of the repository.
        repo_name : str
            The name of the repository.
        commit_sha : str
            The SHA of the commit to fetch.

        Returns
        -------
        dict
            A dictionary containing the commit details.
        """
        url = f"https://api.github.com/repos/{owner}/{repo_name}/commits/{commit_sha}"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.github_token}",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_repository_stats(self, owner: str, repo_name: str) -> dict:
        """
        Fetch statistics for a given repository.

        Parameters
        ----------
        owner : str
            The owner of the repository.
        repo_name : str
            The name of the repository.

        Returns
        -------
        dict
            A dictionary containing the repository statistics.
        """
        url = f"https://api.github.com/repos/{owner}/{repo_name}"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.github_token}",
        }
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()
