import pandera.polars as pa
import polars as pl

class GithubCommitsSchema(pa.DataFrameModel):
    """Schema for Github Commits asset (Bronze/JSON)."""
    commit_sha: str = pa.Field(nullable=False, unique=True, description="Unique SHA hash of the commit.")
    repository: str = pa.Field(nullable=False, description="Repository name (owner/repo).")
    message: str = pa.Field(nullable=True, description="Commit message.")
    author_name: str = pa.Field(nullable=True, description="Author name.")
    author_email: str = pa.Field(nullable=True, description="Author email.")
    committed_at: str = pa.Field(nullable=False, description="Commit timestamp (ISO 8601 string).")
    additions: int = pa.Field(ge=0, description="Number of additions.")
    deletions: int = pa.Field(ge=0, description="Number of deletions.")

    class Config:
        description = "Schema for GitHub commits data."
        strict = True # Ensure no unexpected columns


class GithubRepositoryStatsSchema(pa.DataFrameModel):
    """Schema for Github Repository Stats asset (Bronze/JSON)."""
    repository: str = pa.Field(nullable=False, unique=True, description="Repository name (owner/repo).")
    stargazers_count: int = pa.Field(ge=0, description="Number of stargazers.")
    forks_count: int = pa.Field(ge=0, description="Number of forks.")
    open_issues_count: int = pa.Field(ge=0, description="Number of open issues.")
    watchers_count: int = pa.Field(ge=0, description="Number of watchers.")
    created_at: str = pa.Field(nullable=True, description="Creation timestamp (ISO string).")
    updated_at: str = pa.Field(nullable=True, description="Last update timestamp (ISO string).")
    fetched_at: str = pa.Field(nullable=False, description="Fetch timestamp (ISO string).")

    class Config:
        description = "Schema for GitHub repository statistics."
        # strict = True # JSON loads might have extra fields? Let's be strict for now based on asset definition.
