# Dagster Gemini Agent

This agent is responsible for interacting with the Dagster instance in this project. Its primary function is to materialize assets.

## Tools

The primary way to interact with Dagster assets is directly via the Dagster CLI, executed within the `dagster` Docker container.

### Materializing Assets

To materialize a specific Dagster asset for a given partition, use the `dagster asset materialize` command.

**Usage:**
```bash
docker exec dagster uv run dagster asset materialize --select <asset_key> -m src.main --partition <partition_date>
```

**Arguments:**
*   `<asset_key>`: The full asset key (e.g., `bronze/work/github/github_commits`).
*   `<partition_date>`: The partition date in `YYYY-MM-DD` format.

**Example:**

To materialize the `bronze/work/github/github_commits` asset for the partition `2025-06-07`:
```bash
docker exec dagster uv run dagster asset materialize --select bronze/work/github/github_commits -m src.main --partition 2025-06-07
```

### Listing Assets

To list all discovered assets in the project:
```bash
docker exec dagster uv run dagster asset list -m src.main
```

### `scripts/run_dagster_test.py` (Deprecated - Use Direct CLI)

This script was a command-line utility for asset materialization. It is now deprecated in favor of direct `dagster asset materialize` commands, which offer more flexibility and direct interaction with the Dagster CLI.

## Troubleshooting

For common Dagster issues, refer to the "Debugging Dagster" section in the main `GEMINI.md` file for detailed troubleshooting steps, including resolving duplicate asset definitions, database connection errors, and Docker environment issues.
