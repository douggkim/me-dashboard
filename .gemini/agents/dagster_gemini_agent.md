# Dagster Gemini Agent

This agent is responsible for interacting with the Dagster instance in this project. Its primary function is to materialize assets.

## Tools

The main tool for this agent is the `scripts/run_dagster_test.py` script.

### `scripts/run_dagster_test.py`

This script is a command-line utility to materialize a Dagster asset for a specific partition.

**Usage:**
```bash
python scripts/run_dagster_test.py <asset_key> <partition_date>
```

**Arguments:**
*   `<asset_key>`: The key of the asset to materialize. This can be a simple name or a slash-separated path for complex keys.
*   `<partition_date>`: The partition date in `YYYY-MM-DD` format.

**Environment:**

This script must be run inside the `dagster` docker container to have access to the correct environment and dependencies. The recommended way to run it is using `docker exec` and `uv run`.

**Example:**

To materialize the `psn_game_play_history_silver` asset for the partition `2025-06-07`:
```bash
docker exec dagster uv run python /app/scripts/run_dagster_test.py silver/entertainment/PSN/psn_game_play_history_silver 2025-06-07
```
