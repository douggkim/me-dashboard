import dagster as dg
import polars as pl
import pandera.polars as pa

from src.assets.work.github.github import github_commits, github_repository_stats
from src.validation.schemas.github_schema import GithubCommitsSchema, GithubRepositoryStatsSchema


@dg.asset_check(asset=github_commits)
def check_github_commits_data_quality(github_commits: list[dict]) -> dg.AssetCheckResult:
    """
    Validate data quality for github_commits asset using Pandera schema.
    """
    if not github_commits:
        return dg.AssetCheckResult(passed=True, metadata={"num_rows": 0, "status": "empty_input"})

    # Convert to Polars DataFrame for Schema Validation
    # We let Polars infer types, which matches standard JSON->Table flow
    # Schema expects Utf8 for dates in Bronze
    df = pl.DataFrame(github_commits)
    
    try:
        GithubCommitsSchema.validate(df, lazy=True)
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "num_rows": len(df),
                "schema": "GithubCommitsSchema",
                "status": "passed"
            }
        )
    except pa.errors.SchemaErrors as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "num_rows": len(df),
                "schema": "GithubCommitsSchema",
                "status": "failed",
                "errors": str(e.failure_cases)
            }
        )
    except Exception as e:
         return dg.AssetCheckResult(
            passed=False,
            metadata={
                "status": "failed",
                "error": str(e)
            }
        )


@dg.asset_check(asset=github_repository_stats)
def check_github_repo_stats_data_quality(github_repository_stats: list[dict]) -> dg.AssetCheckResult:
    """
    Validate data quality for github_repository_stats asset using Pandera schema.
    """
    if not github_repository_stats:
        return dg.AssetCheckResult(passed=True, metadata={"num_rows": 0, "status": "empty_input"})

    df = pl.DataFrame(github_repository_stats)
    
    try:
        GithubRepositoryStatsSchema.validate(df, lazy=True)
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "num_rows": len(df),
                "schema": "GithubRepositoryStatsSchema",
                "status": "passed"
            }
        )
    except pa.errors.SchemaErrors as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "num_rows": len(df),
                "schema": "GithubRepositoryStatsSchema",
                "status": "failed",
                "errors": str(e.failure_cases)
            }
        )
    except Exception as e:
         return dg.AssetCheckResult(
            passed=False,
            metadata={
                "status": "failed",
                "error": str(e)
            }
        )
