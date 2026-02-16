"""
Schema definitions for GitHub data validation.

This module defines the Pandera schemas for GitHub-related data assets,
including raw events and repository statistics.
"""

import pandera.polars as pa


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
