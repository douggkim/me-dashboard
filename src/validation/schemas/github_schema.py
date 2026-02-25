"""
Schema definitions for GitHub data validation.

This module defines the Pandera schemas for GitHub-related data assets,
including raw events and repository statistics.
"""

import dagster_pandera
import pandera.polars as pa
import polars as pl


class GithubEventsSchema(pa.DataFrameModel):
    """Schema for Github Events asset (Bronze/JSON)."""

    id: str = pa.Field(nullable=False, description="Unique event ID.")
    type: str = pa.Field(nullable=False, description="Event type (e.g., PushEvent).")
    created_at: str = pa.Field(nullable=False, description="Event timestamp (ISO 8601 string).")

    # We can add more fields if we want to validte existence of specific keys,
    # but since it's raw JSON, we might want to keep it minimal for now.

    class Config:
        """Pandera configuration."""

        description = "Schema for GitHub raw events data."


class GithubRepositoryStatsSchema(pa.DataFrameModel):
    """Schema for Github Repository Stats asset (Bronze/JSON)."""

    repository: str = pa.Field(nullable=False, unique=True, description="Repository name (owner/repo).")
    stargazers_count: int = pa.Field(ge=0, description="Number of stargazers.", nullable=True)  # nullable just in case
    forks_count: int = pa.Field(ge=0, description="Number of forks.", nullable=True)
    open_issues_count: int = pa.Field(ge=0, description="Number of open issues.", nullable=True)
    watchers_count: int = pa.Field(ge=0, description="Number of watchers.", nullable=True)
    created_at: str = pa.Field(nullable=True, description="Creation timestamp (ISO string).")
    updated_at: str = pa.Field(nullable=True, description="Last update timestamp (ISO string).")
    fetched_at: object = pa.Field(
        nullable=False, description="Fetch timestamp."
    )  # Object because Polars might load as generic object if mixed, but usually Datetime

    class Config:
        """Pandera configuration."""

        description = "Schema for GitHub repository statistics."


class GithubSilverSchema(pa.DataFrameModel):
    """
    Schema for GitHub Silver events.

    This schema validates the structured GitHub event data in the silver zone,
    including commit statistics and branch categorization.
    """

    date: pl.Date = pa.Field(description="The date of the event.")
    created_at: pl.Datetime = pa.Field(description="The exact timestamp of the event.")
    id: str = pa.Field(description="Unique GitHub event ID.")
    event_type: str = pa.Field(description="The type of the GitHub event (e.g., PushEvent, PullRequestEvent).")
    target_repo: str = pa.Field(description="The full name of the repository (owner/repo).")
    target_branch: str = pa.Field(nullable=True, description="The target branch name.")
    branch_type: str = pa.Field(description="Categorized branch type (feature, fix, chore, other).")
    commit_sha: str = pa.Field(nullable=True, description="The SHA of the commit.")
    code_additions: int = pa.Field(ge=0, description="Number of lines added in the commit.")
    code_deletions: int = pa.Field(ge=0, description="Number of lines deleted in the commit.")
    number_of_changed_files: int = pa.Field(ge=0, description="Number of files changed in the commit.")
    primary_key: str = pa.Field(description="Unique hash of date, event ID, and commit SHA for upsert identification.")

    class Config:
        """Pandera configuration."""

        strict = True


github_silver_dagster_type = dagster_pandera.pandera_schema_to_dagster_type(GithubSilverSchema)
