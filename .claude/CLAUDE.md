# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Dagster-based data pipeline project called "doug-dashboard" that processes personal data from various sources including Spotify play history and PlayStation activity. The project uses a medallion architecture (bronze/silver/gold) with Delta Lake for data storage and can run locally with Docker or deploy to cloud infrastructure.

## Development Commands

### Docker Environment
- `make dev`: Start development environment with all services (includes MinIO for local S3 simulation)
- `make up`: Start production environment 
- `make down`: Stop all services
- `make dev-down`: Stop development services only
- `make clean`: Clean Docker resources for this project only

### Code Quality
- `ruff check .`: Run linting (configured in ruff.toml with comprehensive rules)
- `ruff format .`: Format code
- Python 3.13+ required

### Dagster Operations
- Dagster UI runs on port 3000 when containers are running
- PostgreSQL runs on port 5432 for Dagster metadata storage
- MinIO (S3-compatible storage) runs on ports 9000 (API) and 9001 (console) in dev mode

## Architecture

### Core Components
- **Assets**: Located in `src/assets/` - Data processing pipelines organized by domain (entertainment, etc.)
- **Resources**: Located in `src/resources/` - External service connectors (Spotify, PlayStation, AWS, etc.)
- **IO Managers**: Custom Delta Lake-based storage managers for different data types (Pandas, Polars, JSON)
- **Jobs**: Maintenance operations like Delta table optimization in `src/jobs/`
- **Utils**: Shared utilities for AWS, data loading, encoding, and date operations

### Data Flow
1. **Bronze Layer**: Raw data ingestion from APIs (Spotify, PlayStation) stored as JSON or Delta tables
2. **Silver Layer**: Cleaned and processed data  
3. **Gold Layer**: Business-ready aggregated data
4. Assets use partitioned definitions (daily partitions) with automated scheduling

### Key Resources
- `SpotifyResource`: Handles Spotify API authentication and data retrieval
- `PSNResource`: PlayStation Network data collection
- `DataLoaderResource`: Configurable data loading with file path management
- Custom IO managers for Delta Lake storage with AWS S3 backends

### Environment Configuration
- Uses `.env` file for secrets (not committed)
- File paths configured in `src/configs/file_paths.yml`
- AWS credentials and storage options handled via `src/utils/aws.py`

## Lambda Deployment

The project includes AWS Lambda deployment via GitHub Actions:
- Lambda code in `lambda_code/` directory
- Automated deployment on pushes to main branch affecting lambda code
- Deploys to `loc-data-collection` Lambda function in us-west-1 region

## Development Notes

- All assets should follow the established naming conventions with domain prefixes
- Use the existing medallion architecture pattern for new data sources
- Follow the established resource pattern for new external service integrations
- Delta table maintenance is automated via scheduled jobs

### Import Guidelines
- **ALWAYS use absolute imports starting with `src.`** - Never use relative imports or sys.path manipulation
- Example: `from src.utils.health_data import parse_health_csv_content` 
- Example: `from src.resources.spotify_resource import SpotifyResource`
- This ensures imports work correctly from any execution context (tests, Dagster UI, CLI, etc.)