# Dataset Refresh Cadences

This document outlines the refresh cadences for all datasets managed in the `doug-dashboard` project. 

The project uses a combination of event-driven AWS Lambda functions for raw data ingestion from client devices, and scheduled Dagster pipelines to process data from Bronze to Silver layers.

## 1. Dagster Pipeline Pipelines (Processing & API Pulls)

Dagster acts as both an orchestrator for processing data already in S3, and as an active data collector for external APIs. It utilizes `AutomationCondition.on_cron` to schedule Bronze runs, and `AutomationCondition.eager()` to sequentially process downstream Silver dependencies as soon as Bronze completes.

| Dataset Domain | Asset Group | Bronze Materialization Cadence | Silver Materialization Cadence | Time Location | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **GitHub** | `work` | Daily at Midnight (`0 0 * * *`) | Eager (Immediately after Bronze) | UTC | Pulls latest commits and PRs using the GitHub API. Partitions are offset to fetch the previous day's data securely. |
| **PlayStation** | `entertainment` | Daily at Midnight (`0 0 * * *`) | Eager (Immediately after Bronze) | UTC | Pulls played games and trophies via PSN API. |
| **Spotify** | `entertainment` | Every 3 hours (`0 */3 * * *`) | Eager (Immediately after Bronze) | UTC | Pulls recent play history via Spotify API. Both layers update simultaneously. |
| **Location** | `location` | N/A (Uploaded continuously via Lambda) | Daily at 6:00 AM (`0 6 * * *`) | UTC | Processes raw GPS data uploaded by the Overland app. Since there is no Bronze asset step, the Silver layer initiates execution daily. |
| **Screen Time** | `screen_time` | N/A (Uploaded asynchronously via Lambda) | Daily at 6:00 AM (`0 6 * * *`) | UTC | Processes app usage data uploaded by Apple Shortcuts/Mac scrapers. Just like `Location`, the Silver layer runs independently since it accesses S3 files directly. |

## 2. Raw Data Ingestion (AWS Lambdas)

For some datasets, raw data collection is "push-based". AWS Lambda endpoints receive HTTP POST payloads from client apps. The "cadence" depends on the client's configuration rather than a server-side schedule:

| Dataset Domain | Ingestion Source | Ingestion Cadence |
| :--- | :--- | :--- |
| **Location Data** | Overland App (`loc-data-collection.py`) | Continuous/Event-driven (whenever the app detects significant location changes and pushes to API Gateway). |
| **Screen Time** | Apple Shortcuts/Mac Script (`screen-time-collection.py`) | Dependent on user's device configuration (e.g., Mac cron tab or iOS daily shortcut triggers). |
| **Health Data** | iOS Health App/Shortcuts (`health-data-collection.py`) | Dependent on iOS shortcut trigger (likely pushed daily or after workouts). |
