"""Data quality checks for GitHub assets."""

import dagster as dg
import pandera.polars as pa
import polars as pl

from src.assets.work.github.github import github_events, github_repository_stats
from src.validation.schemas.github_schema import GithubEventsSchema, GithubRepositoryStatsSchema


@dg.asset_check(asset=github_events)
def check_github_events_data_quality(github_events: list[dict]) -> dg.AssetCheckResult:
    """
    Validate data quality for github_events asset using Pandera schema.

    Returns
    -------
    dg.AssetCheckResult
        The result of the check.
    """
    if not github_events:
        return dg.AssetCheckResult(passed=True, metadata={"num_rows": 0, "status": "empty_input"})

    # Convert to Polars DataFrame for Schema Validation
    # We let Polars infer types, which matches standard JSON->Table flow
    github_events_df = pl.DataFrame(github_events)

    try:
        GithubEventsSchema.validate(github_events_df, lazy=True)
        return dg.AssetCheckResult(
            passed=True,
            metadata={"num_rows": len(github_events_df), "schema": "GithubEventsSchema", "status": "passed"},
        )
    except pa.errors.SchemaErrors as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "num_rows": len(github_events_df),
                "schema": "GithubEventsSchema",
                "status": "failed",
                "errors": str(e.failure_cases),
            },
        )


@dg.asset_check(asset=github_repository_stats)
def check_github_repo_stats_data_quality(github_repository_stats: list[dict]) -> dg.AssetCheckResult:
    """
    Validate data quality for github_repository_stats asset using Pandera schema.

    Returns
    -------
    dg.AssetCheckResult
        The result of the check.
    """
    if not github_repository_stats:
        return dg.AssetCheckResult(passed=True, metadata={"num_rows": 0, "status": "empty_input"})

    github_repo_stats_df = pl.DataFrame(github_repository_stats)

    try:
        GithubRepositoryStatsSchema.validate(github_repo_stats_df, lazy=True)
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "num_rows": len(github_repo_stats_df),
                "schema": "GithubRepositoryStatsSchema",
                "status": "passed",
            },
        )
    except pa.errors.SchemaErrors as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "num_rows": len(github_repo_stats_df),
                "schema": "GithubRepositoryStatsSchema",
                "status": "failed",
                "errors": str(e.failure_cases),
            },
        )
