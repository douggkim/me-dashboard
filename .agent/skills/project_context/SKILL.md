---
name: project_context
description: Provides a high-level overview of the `doug-dashboard` project, including key technologies, directory structure, and development lifecycle.
---

# Project Context

This skill provides essential context about the `doug-dashboard` project. activate this skill when you need to understand the project's purpose, architecture, or locate specific components.

## Project Overview

`doug-dashboard` is a data dashboard project focused on collecting, processing, and displaying personal data from various sources (e.g., Spotify, PlayStation, location data).

## Key Technologies

*   **Python**: Primary language for scripting, data processing, and Dagster assets/jobs.
*   **Dagster**: Data orchestration platform (`jobs/`, `resources/`, `assets/`).
*   **Docker/Docker Compose**: Containerization for Postgres, Minio, Dagster.
*   **AWS Lambda**: Serverless data collection functions.
*   **DuckDB**: Analytical database for direct file querying.
*   **Minio**: S3-compatible object storage.
*   **PostgreSQL**: Database for processed data.

## Directory Structure

*   `src/`: Main application source code.
    *   `src/assets/`: Dagster assets.
    *   `src/jobs/`: Dagster jobs.
    *   `src/resources/`: Dagster resources.
*   `data/`: Local data volumes.
*   `lambda_code/`: AWS Lambda functions.
*   `scripts/`: Utility scripts.

## Development Lifecycle

1.  **Make code changes**: Modify source code in `src/`.
2.  **Run Dagster pipelines**: Materialize assets using the **Dagster Skill**.
3.  **Query results**: Verify data using the **Data Analysis Skill**.
