# Contributing Guidelines

Thank you for your interest in contributing to the `doug-dashboard` project! We appreciate your help in making this project better. To ensure a smooth and collaborative development process, please adhere to the following guidelines.

## Table of Contents

1.  [Coding Conventions](#coding-conventions)
2.  [Polars Usage](#polars-usage)
3.  [Docstring Format](#docstring-format)
4.  [Type Hinting](#type-hinting)
5.  [Pipeline Development](#pipeline-development)
    *   [Helper Functions](#helper-functions)
    *   [Aggregations and Column Creation](#aggregations-and-column-creation)
6.  [Unit Testing](#unit-testing)

---

## 1. Coding Conventions

All Python code must adhere to the [Ruff](https://beta.ruff.rs/docs/) coding style and conventions.
Please ensure your code is formatted and linted correctly before submitting a Pull Request.

To check your code:
```bash
ruff check .
ruff format .
```

## 2. Polars Usage

For all data manipulation and transformation tasks, please use [Polars](https://pola.rs/). Avoid using Pandas or other data manipulation libraries unless there's a compelling reason approved by the maintainers.

## 3. Docstring Format

All functions, classes, and modules should follow the [NumPy Docstring Format](https://numpydoc.readthedocs.io/en/latest/format.html). This ensures consistency and clarity in our documentation.

Example:

```python
def example_function(param1: str, param2: int) -> bool:
    """
    Brief description of what the function does.

    Parameters
    ----------
    param1 : str
        Description of the first parameter.
    param2 : int
        Description of the second parameter.

    Returns
    -------
    bool
        Description of the return value.

    Raises
    ------
    ValueError
        If param2 is negative.
    """
    if param2 < 0:
        raise ValueError("param2 cannot be negative")
    # Function logic
    return True
```

## 4. Type Hinting

We use native Python type hints (`str`, `int`, `list`, `dict`, etc.) for all function signatures and variable annotations. Please avoid importing types from the `typing` module (e.g., `List`, `Dict`) unless absolutely necessary for advanced typing features not available in native types.

*   **Polars Datetime Handling**: When working with datetime strings in Polars, especially those from external APIs, use `pl.col(...).str.to_datetime(time_unit="ns")`. Polars will correctly infer UTC for Z-formatted (ISO 8601 with 'Z' suffix) strings. Avoid the deprecated `utc=True` parameter.

## 5. Pipeline Development

When developing data pipelines (especially Dagster assets), we emphasize readability, maintainability, and testability.

### Dagster Asset Best Practices

*   **Asset Naming and Grouping**:
    *   Use `key_prefix` to define the full asset key, clearly reflecting the data layer and domain (e.g., `key_prefix=["bronze", "work", "github"]`).
    *   `group_name` is primarily for UI organization within Dagster UI and should use valid Python identifier characters (e.g., `work_github`, not `work/github`).
*   **Asset Discovery**:
    *   For discoverability, ensure `__init__.py` files within asset directories (e.g., `src/assets/bronze/work/__init__.py`) are correctly structured. While `load_assets_from_package_module` can recursively find assets, explicit imports in higher-level `__init__.py` files may be necessary for deeply nested assets. Alternatively, directly list asset modules in `src/main.py` using `dg.load_assets_from_modules([...])` for explicit control and to prevent discovery issues.
*   **Bronze Layer Output**:
    *   Bronze layer assets should typically output raw data as `list[dict]` or `dict` structures, stored as JSON files (using `io_manager_json_txt`). Polars DataFrame creation and filtering should generally occur in subsequent silver-layer assets.
*   **Resource Configuration**:
    *   Configure resource-specific parameters (e.g., API keys, usernames) directly within the `ConfigurableResource` definition in `src/main.py` using `dg.EnvVar`. This simplifies asset signatures and centralizes resource configuration.
*   **Partitioning**:
    *   Define `partitions_def` (e.g., `dg.DailyPartitionsDefinition`) and `automation_condition` (e.g., `dg.AutomationCondition.on_cron`) for assets that process time-partitioned data.
    *   When fetching data for a partition, filter API calls by the `context.partition_key` if the API supports it. If not, fetch a broader range and filter the resulting Polars DataFrame by the `partition_key` date range.

### Helper Functions

Break down complex pipelines into smaller, focused helper functions. Each helper function should represent a single logical step in the data transformation process. This makes the pipeline easier to understand, debug, and unit test. These helpers are typically specific to one pipeline and should be co-located with the asset that uses them or defined within the same module.

### Utility Functions

If a function is general-purpose and can be reused across multiple pipelines (e.g., a custom date parsing function, a data cleaning utility), it should be placed in the `src/utils/` directory. This promotes code reuse and keeps our core data transformation logic separate from common, cross-pipeline utilities. Ensure these utility functions are well-documented and rigorously tested.

**Example (referencing `src/assets/entertainment/spotify_play_history.py`):**

Instead of performing all transformations within the main asset function, you would create helper functions like:

```python
import polars as pl
import datetime
import hashlib

# ... (other imports)

def _extract_and_hash_play_history_id(
    played_at: str, song_id: str
) -> str:
    """
    Generates a unique play history ID by hashing played_at and song_id.

    Parameters
    ----------
    played_at : str
        Timestamp when the song was played.
    song_id : str
        Spotify ID of the song.

    Returns
    -------
    str
        MD5 hash representing the play history ID.
    """
    hash_input = f"{played_at}_{song_id}"
    return hashlib.md5(hash_input.encode()).hexdigest()  # noqa: S324

def _parse_raw_spotify_item(item: dict) -> dict:
    """
    Parses a single raw Spotify play history item into a processed dictionary.

    Parameters
    ----------
    item : dict
        A dictionary representing a single item from the Spotify API response.

    Returns
    -------
    dict
        A processed dictionary containing relevant play history fields.
    """
    track = item["track"]
    album = track["album"]
    played_at = item["played_at"]
    song_id = track["id"]

    play_history_id = _extract_and_hash_play_history_id(played_at, song_id)
    duration_seconds = track["duration_ms"] / 1000
    played_at_dt = datetime.datetime.fromisoapattern(played_at)
    played_date = played_at_dt.date()

    return {
        "play_history_id": play_history_id,
        "played_at": played_at,
        "played_date": played_date,
        "duration_seconds": duration_seconds,
        "duration_ms": track["duration_ms"],
        "artist_names": [artist["name"] for artist in track["artists"]],
        "song_id": song_id,
        "song_name": track["name"],
        "album_name": album["name"],
        "popularity_points_by_spotify": track["popularity"],
        "is_explicit": track["explicit"],
        "song_release_date": album["release_date"],
        "no_of_available_markets": len(album["available_markets"]),
        "album_type": album["album_type"],
        "total_tracks": album["total_tracks"],
    }

# Then, your main asset function would utilize these:
# @dg.asset(...)
# def spotify_play_history_silver(...):
#     processed_history_data = [_parse_raw_spotify_item(item) for json_dict in spotify_play_history_bronze for item in json_dict.get("items", [])]
#     # ... create DataFrame and deduplicate
#     # ...
#     return deduplicated_df.with_columns(
#         pl.col("played_at").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.fZ", time_unit="ms")
#     )
```

### Aggregations and Column Creation

When performing aggregations or creating new columns with Polars, always use the "named" approach (e.g., `with_columns({new_col_name}=pl.expr(...))`). This improves readability and makes the intent of the transformation explicit.

Example:

```python
import polars as pl

df = pl.DataFrame({
    "value": [1, 2, 3],
    "category": ["A", "A", "B"]
})

# Bad (unclear intent for new column name)
# df.with_columns(pl.col("value").mean().alias("avg_value"))

# Good (explicitly names the new column)
df_transformed = df.group_by("category").agg(
    total_value=pl.col("value").sum(),
    avg_value=pl.col("value").mean(), # Using named aggregation
).with_columns(
    squared_value=(pl.col("total_value") ** 2) # Using named column creation
)

# Or, for simple column creation:
df_with_new_col = df.with_columns(
    double_value=pl.col("value") * 2
)
```

## 6. Unit Testing

Every pipeline helper function and utility function should have corresponding unit tests. Tests should cover typical scenarios, edge cases, and error conditions. Use a testing framework like `pytest`.

Tests should be placed in a `tests/` directory mirroring the structure of `src/`. For example, tests for `src/utils/date.py` would be in `tests/utils/test_date.py`.

---
This document will evolve as the project grows. Thank you for your contributions!
