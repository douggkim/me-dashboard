---
name: data_quality
description: Create and manage Dagster data quality checks.
---

# Data Quality Checks Skill

This skill guides the creation of data quality checks in Dagster.

## Workflow

1.  **Discovery**: Ask the user to identify the critical asset(s) they want to validate or identify critical assets yourself (e.g. bronze/silver layer tables).
2.  **Proposal**:
    *   Query the asset data to understand its shape and common values (use `duckdb` or `polars`).
    *   List potential quality checks (e.g., "column `id` should be unique", "column `status` should be one of ['active', 'inactive']", "no null values in `timestamp`").
    *   Present this list to the user for confirmation.
3.  **Implementation**:
    *   Create a new Python file in `src/validation/asset_checks/` (create directories if needed).
    *   Implement checks using the `@asset_check` decorator.
    *   Ensure the new module is discoverable by `src/main.py`. This means ensuring it's imported in `src/validation/asset_checks/__init__.py` or that `load_asset_checks_from_package_module` scans it recursively.

## Coding Standards

*   Use `polars` for data processing within checks if possible.
*   Return `AssetCheckResult` with metadata (e.g., number of failing rows).
*   Follow the project's linting rules.

## Example

```python
import dagster as dg
import polars as pl
from src.resources.io_managers import PolarsDeltaIOManager

@dg.asset_check(asset=dg.AssetKey(["bronze", "work", "github", "github_repository_stats"]))
def check_repo_stats_positive(context, io_manager: PolarsDeltaIOManager):
    df = io_manager.load_input(context)
    # logic
    return dg.AssetCheckResult(passed=True)
```
