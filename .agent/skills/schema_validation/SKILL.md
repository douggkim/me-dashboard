---
name: schema_validation
description: Create and manage Pandera-based schema validation for Polars DataFrames.
---

# Schema Validation Skill

This skill guides the creation of Pandera schemas for validating data in Dagster assets.

## Core Principles

1.  **Pandera + Polars**: Use `pandera.polars` for validation. The project uses Polars for data processing, so validation should align with this.
2.  **Rich Schemas**: All schemas MUST define:
    *   **Data Types**: Explicit Polars data types (e.g., `pl.Utf8`, `pl.Int64`).
    *   **Nullability**: Explicit verify `nullable=True` or `nullable=False`.
    *   **Descriptions**: A clear `description` for every field explaining the data's meaning or source.
3.  **Unified Validation**: Use the **same schema** for validating Bronze (JSON) and Silver (Parquet/Delta) layers where possible.

## Validation Strategies

### 1. Bronze Layer (JSON/Dicts) -> *Ad-hoc Validation*
For assets that return `list[dict]` (Bronze layer), do **not** use `pandera_schema_to_dagster_type` as it forces a Dagster Type check that expects a DataFrame, adding unnecessary complexity to the asset signature.

Instead, validate **inside an `@asset_check`**:
1. Load the `list[dict]`.
2. Convert to `pl.DataFrame` found in memory.
3. call `MySchema.validate(df)`.

### 2. Silver/Gold Layers (Polars/Pandas) -> *Dagster Type System*
For assets that already return DataFrames, you *can* use `dagster_pandera.pandera_schema_to_dagster_type` to create a Dagster Type. This integrates validation into the input/output type system.

However, using the **`@asset_check`** pattern (Strategy 1) is often preferred even here, as it decouples validation failure from downstream execution (soft failures) and provides better visibility in the UI.

## Workflow

1.  **Define Schema**: Create a `DataFrameModel` in `src/validation/schemas/`.
2.  **Implement Validation**:
    *   **Asset Checks**: Import the schema and use `MySchema.validate(df)` inside an `@asset_check`.

## Example Schema

```python
import pandera.polars as pa
import polars as pl
from pandera.typing import DataFrame

class GithubRepositoryStatsSchema(pa.DataFrameModel):
    repository: str = pa.Field(dtype=pl.Utf8, nullable=False, description="Full repository name (owner/repo).")
    stargazers_count: int = pa.Field(dtype=pl.Int64, ge=0, description="Number of stars. Must be non-negative.")
    fetched_at: pl.Datetime = pa.Field(dtype=pl.Datetime, description="Timestamp of data fetch.")

    class Config:
        strict = True  # Reject unknown columns
```

## Example Usage (Bronze/JSON)

```python
# In src/validation/asset_checks/your_check.py
import polars as pl
import dagster as dg
from src.validation.schemas.your_schema import GithubRepositoryStatsSchema

@dg.asset_check(asset=github_repository_stats)
def check_schema_compliance(github_repository_stats: list[dict]):
    # Frictionless validation: Convert to Polars to use the common schema
    df = pl.DataFrame(github_repository_stats)
    
    try:
        GithubRepositoryStatsSchema.validate(df)
        return dg.AssetCheckResult(passed=True)
    except pa.errors.SchemaError as e:
        return dg.AssetCheckResult(passed=False, metadata={"error": str(e)})
```
