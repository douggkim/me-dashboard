# Gemini CLI Project Reference

This document serves as an internal reference for the Gemini CLI agent, capturing key information and understanding of the `doug-dashboard` project.

## Project Overview
This repository appears to be a data dashboard project, likely focused on collecting, processing, and displaying personal data from various sources (e.g., Spotify, PlayStation, location data). It utilizes Python for scripting and data processing, Docker for containerization, and Dagster for orchestrating data pipelines. AWS Lambda functions are also present, suggesting serverless components for data collection. For ad-hoc data analysis and exploration, the project is configured to use DuckDB to directly query data files and object storage.

## Key Technologies/Frameworks
*   **Python**: Primary language for scripting, data processing, and Dagster assets/jobs.
*   **Docker/Docker Compose**: Used for containerization of services (e.g., Postgres, Minio, Dagster).
*   **Dagster**: Data orchestration platform, indicated by `jobs/`, `resources/`, `assets/` directories, and `data/dagster/`.
*   **AWS Lambda**: Serverless functions for data collection (`lambda_code/health-data-collection.py`, `loc-data-collection.py`).
*   **PostgreSQL**: Database, likely for storing processed data (`data/postgres/`).
*   **Minio**: S3-compatible object storage, possibly for raw data or intermediate files (`data/minio/`).
*   **DuckDB**: An in-process analytical database used for interactively and efficiently querying data directly from files, including from Minio (S3) object storage.
*   **Ruff**: Python linter/formatter (`ruff.toml`).
*   **uv**: Python package installer/resolver (`uv.lock`).

## Directory Structure and Purpose

*   `.claude/`: Appears to be configuration/data for another AI agent (Claude).
*   `.github/workflows/`: GitHub Actions for CI/CD, specifically for deploying Lambda functions.
*   `.venv/`: Python virtual environment.
*   `data/`: Contains local data volumes for various services (azureite, dagster, minio, postgres, testing).
*   `docs/`: Documentation, including `data_dictionary.md`.
*   `lambda_code/`: Python code for AWS Lambda functions.
*   `scripts/`: General utility scripts.
    *   `run_dagster_test.py`: A script to materialize Dagster assets.
*   `src/`: Main application source code.
    *   `src/main.py`: Likely the entry point or main application logic.
    *   `src/assets/`: Dagster assets (data definitions and computation).
        *   `src/assets/entertainment/`: Entertainment-related data assets (PlayStation, Spotify).
        *   `src/assets/location/`: Location-related data assets.
    *   `src/configs/`: Configuration files (e.g., `file_paths.yml`).
    *   `src/jobs/`: Dagster jobs (collections of assets/ops).
    *   `src/resources/`: Dagster resources (external services, connections).
        *   `src/resources/psn_resource.py`: PlayStation Network resource.
        *   `src/resources/spotify_resource.py`: Spotify resource.
    *   `src/schedules/`: Dagster schedules for recurring job execution.
    *   `src/utils/`: Utility functions (AWS, Azure, date, data loaders, encoder, global helpers).
    *   `src/validation/`: Data validation logic.
        *   `src/validation/schemas/`: Data schemas.

## Development Lifecycle

1.  **Make code changes:** Modify the source code as required for the task.
2.  **Run Dagster pipelines:** After making changes to data pipelines, materialize the assets to test the changes.
    *   **Running Dagster CLI commands:** All Dagster CLI commands (e.g., `dagster asset list`, `dagster asset materialize`) must be run inside the `dagster` Docker container. The recommended way to execute these is using `docker exec` and `uv run`.
    *   **Example (List Assets):** `docker exec dagster uv run dagster asset list -m src.main`
    *   **Example (Materialize a specific asset):** `docker exec dagster uv run dagster asset materialize --select <asset_key> -m src.main`
    *   **Note:** The `scripts/run_dagster_test.py` script can also be used for specific asset materialization, but direct `dagster` CLI commands offer more flexibility.
3.  **Query the results:** Use the `duckdb_gemini_agent` to query the materialized data and verify the changes.

## Debugging Dagster

Encountering issues with Dagster asset discovery or execution is common. Here's a guide to common problems and troubleshooting steps:

### Common Errors:

*   **`DagsterInvalidDefinitionError: Asset key ... is defined multiple times.`**: This indicates that Dagster's asset discovery mechanism is finding the same asset definition more than once. This can be caused by:
    *   Incorrect `__init__.py` files (e.g., re-exporting assets multiple times or in conflicting ways).
    *   Modules being inadvertently imported multiple times.
    *   Caching issues within the Dagster instance.
*   **Database Connection Errors (`psycopg2.OperationalError: could not translate host name "dagsterdb" to address: Name or service not known`)**: The Dagster container cannot connect to the PostgreSQL database (`dagsterdb` service). This usually points to Docker networking issues or the database service not being fully ready.
*   **`DagsterInvariantViolationError: $DAGSTER_HOME "/app/data/dagster" is not a directory or does not exist.`**: The `DAGSTER_HOME` directory, where Dagster stores its metadata, is missing. This often happens after aggressive Docker cleanup.
*   **`Exit Code 137` on `docker exec`**: The process inside the Docker container was killed, often due to running out of memory. This can also indicate a deeper underlying crash without a Python traceback.

### Troubleshooting Steps:

1.  **Check Container Logs**: Always start by checking the logs of the relevant Docker container (e.g., `docker logs dagster`). This often provides detailed Python tracebacks or error messages.
2.  **Verify Asset Discovery**: Use `docker exec dagster uv run dagster asset list -m src.main` to see if Dagster recognizes your assets and if any duplicates appear.
3.  **Inspect `__init__.py` Files**: Ensure your `__init__.py` files are structured correctly for asset discovery. For most cases, leaving intermediate `__init__.py` files empty and explicitly listing asset-containing modules in `src/main.py`'s `dg.load_assets_from_modules` is the most robust approach.
4.  **Clean Docker Environment**: For persistent issues or caching problems, a full Docker cleanup is often necessary:
    *   Stop and remove all containers: `docker compose down`
    *   Remove all images and volumes (use with caution): `docker compose down --volumes --rmi all`
    *   (If network removal fails): Try `docker network prune`.
    *   Ensure `DAGSTER_HOME` exists: `mkdir -p data/dagster` on the host machine.
    *   Bring up services with a rebuild: `docker compose up --build -d`
5.  **Explicit Module Loading**: If `load_assets_from_package_module` causes issues, switch to `load_assets_from_modules` in `src/main.py` and explicitly list each Python module that contains assets (e.g., `dg.load_assets_from_modules([playstation_module, spotify_module, github_module])`).
6.  **Dependency Order**: Ensure `dagster` service in `docker-compose.yml` waits for `dagsterdb` to be fully started using `depends_on` with `condition: service_started`.
7.  **Isolate Problem**: If modifying a new asset or resource causes an error, temporarily remove it from `src/main.py`'s asset list to confirm that the problem is indeed isolated to that specific component.

## Initial Understanding / Next Steps
The project seems well-structured for data engineering workflows. My immediate goal is to understand how these components interact and to be able to make changes or add features as requested. I will refer to this document to quickly recall project context.

