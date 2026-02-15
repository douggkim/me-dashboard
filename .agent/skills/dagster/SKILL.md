---
name: dagster
description: Manage and interact with the Dagster instance to list and materialize assets.
---

# Dagster Skill

This skill allows you to interact with the Dagster instance in the `doug-dashboard` project. Its primary function is to list and materialize assets.

## Usage

You will primarily use the `run_command` tool to execute `dagster` CLI commands within the `dagster` Docker container.

### Materializing Assets

To materialize a specific Dagster asset for a given partition, use the `dagster asset materialize` command inside the docker container.

**Command Structure:**

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

## Debugging & Troubleshooting

Encountering issues with Dagster asset discovery or execution is common. Utilize the following guide to resolve common problems.

### Common Errors

*   **`DagsterInvalidDefinitionError: Asset key ... is defined multiple times.`**:
    *   **Cause**: Asset defined twice, often due to incorrect `__init__.py` or duplicate imports.
    *   **Fix**: Check `src/main.py` asset loading and ensure `__init__.py` files don't re-export assets redundantly.

*   **Database Connection Errors (`psycopg2.OperationalError`)**:
    *   **Cause**: `dagster` container cannot reach `dagsterdb`.
    *   **Fix**: Check Docker networking and ensure `dagsterdb` is healthy.

*   **`DagsterInvariantViolationError: $DAGSTER_HOME ... is not a directory`**:
    *   **Cause**: Missing metadata directory after Docker cleanup.
    *   **Fix**: Run `mkdir -p data/dagster` on host.

*   **`Exit Code 137`**:
    *   **Cause**: OOM (Out of Memory) kill.
    *   **Fix**: Check Docker stats; potentially increase resource limits.

### Troubleshooting Steps

1.  **Check Logs**: `docker logs dagster`
2.  **Verify Discovery**: Run `dagster asset list` (see Usage above) to check what Dagster sees.
3.  **Clean Environment**:
    ```bash
    docker compose down
    docker compose down --volumes --rmi all
    mkdir -p data/dagster
    docker compose up --build -d
    ```
4.  **Isolate Component**: Temporarily remove the failing asset from `src/main.py` to confirm isolation.
