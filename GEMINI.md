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

## Initial Understanding / Next Steps
The project seems well-structured for data engineering workflows. My immediate goal is to understand how these components interact and to be able to make changes or add features as requested. I will refer to this document to quickly recall project context.
