"""Dagster Ops/Job to automatically compact and optimize delta tables."""

import os
from typing import Any

import dagster as dg
from deltalake import DeltaTable
from furl import furl
from loguru import logger
from pydantic import Field

from src.utils.aws import AWSCredentialFormat, get_aws_storage_options


class DeltaMaintenanceConfig(dg.Config):
    """Configuration for Delta table maintenance operations.

    Parameters
    ----------
    table_paths : list[str] | None, optional
        List of S3 paths to Delta tables to maintain. If None, tables will be
        discovered automatically based on configured prefixes.
    vacuum_retention_hours : int, optional
        Number of hours to retain old file versions before vacuuming.
        Defaults to 168 hours (7 days) from environment variable
        DELTA_LOG_RETENTION_HRS or falls back to 168.
    storage_options : dict[str, Any] | None, optional
        Storage options for accessing Delta tables, typically containing
        AWS credentials and configuration.
    """

    table_paths: list[str] | None = Field(
        default=None,
        description="List of S3 paths to Delta tables to maintain. If None, tables will be discovered automatically",
    )

    vacuum_retention_hours: int = Field(
        default_factory=lambda: int(os.getenv("DELTA_LOG_RETENTION_HRS", "168")),
        description="Hours to retain old file versions before vacuuming",
        ge=1,  # Greater than or equal to 1
        le=8760,  # Less than or equal to 1 year
    )

    storage_options: dict[str, Any] | None = Field(
        default=None, description="Storage options for accessing Delta tables (AWS credentials, etc.)"
    )


@dg.op(out=dg.Out(io_manager_key="mem_io_manager"))
def build_maintenance_config(target_table_paths: list[str]) -> DeltaMaintenanceConfig:
    """Build Delta maintenance configuration with discovered table paths.

    This Dagster op creates a DeltaMaintenanceConfig object by combining the
    discovered table paths with AWS storage credentials and default retention
    settings. The configuration object is used by downstream maintenance
    operations (optimize and vacuum) to perform Delta table maintenance.

    Parameters
    ----------
    context : OpExecutionContext
        Dagster execution context providing access to logging, resources,
        and other runtime information.
    target_table_paths : list[str]
        List of S3 URIs for Delta tables that should undergo maintenance.
        These paths are typically discovered by the get_maintenance_target_tables op.

    Returns
    -------
    DeltaMaintenanceConfig
        Configuration object containing:
        - table_paths : The provided list of table paths
        - storage_options : AWS credentials formatted as strings
        - vacuum_retention_hours : Default retention period from environment

    Notes
    -----
    - Uses AWS credential strings format for compatibility with Delta Lake operations
    - Vacuum retention hours are set from DELTA_LOG_RETENTION_HRS environment variable
    - The configuration object is serialized using the mem_io_manager for passing
      between ops in the same execution process
    - This op acts as a br
    """
    return DeltaMaintenanceConfig(
        table_paths=target_table_paths,
        storage_options=get_aws_storage_options(return_credential_type=AWSCredentialFormat.CREDENTIAL_STRINGS),
    )


def get_delta_tables_by_prefix(instance: dg.DagsterInstance, prefixes: list[str]) -> list[str]:
    """Get Delta table S3 paths that match given asset key prefixes.

    This function queries the Dagster instance for all asset entries and filters
    them based on the provided prefixes. It then converts the matching asset keys
    to S3 paths following a standard naming convention.

    Parameters
    ----------
    instance : DagsterInstance
        The Dagster instance to query for asset entries.
    prefixes : list[str]
        List of prefix strings to match against asset keys. Asset keys that
        start with any of these prefixes will be included in the results.

    Returns
    -------
    list[str]
        List of S3 paths for Delta tables that match the given prefixes.
        Paths follow the format: 's3://bucket-name/path/from/asset/key/'

    Examples
    --------
    >>> from dagster import DagsterInstance
    >>> instance = DagsterInstance.get()
    >>> prefixes = ['bronze', 'silver']
    >>> table_paths = get_delta_tables_by_prefix(instance, prefixes)
    >>> print(table_paths)
    ['s3://my-bucket/bronze/sales_data/', 's3://my-bucket/silver/customer_data/']

    >>> # Single prefix
    >>> bronze_tables = get_delta_tables_by_prefix(instance, ['bronze'])
    >>> print(bronze_tables)
    ['s3://my-bucket/bronze/sales_data/', 's3://my-bucket/bronze/inventory/']

    Notes
    -----
    - The bucket name is currently hardcoded as "your-bucket-name" and should be
      configured based on your environment
    - Asset keys are converted to S3 paths by joining all path segments with '/'
    - A trailing '/' is added to indicate a directory path
    - If an error occurs during processing, it will be caught and printed, but
      the function will continue processing other entries

    See Also
    --------
    AssetKey.has_prefix : Method used to check if asset key matches prefix
    DagsterInstance.all_asset_entries : Method to get all asset entries
    """
    table_paths = []
    asset_keys_to_process = []

    asset_keys = instance.get_asset_keys()

    for asset_key in asset_keys:
        for prefix in prefixes:  # Check if the asset key contains any of the prefixes
            asset_identifiers = asset_key.path  # to a string list
            if prefix in asset_identifiers:
                # Create the asset keys as path format and append
                joined_asset_key = "/".join(asset_identifiers)
                asset_keys_to_process.append(joined_asset_key)

    # Dedupe duplicate entries
    asset_keys_to_process = set(asset_keys_to_process)

    logger.info(f"Asset keys containing prefix - {prefixes}: {asset_keys_to_process}")

    # Create a list of table paths (e.g. 's3://me-dashboard/silver/entertainment/spotify/spotify_play_history_silver')
    for asset_key in asset_keys_to_process:
        output_base_path = furl(os.getenv("OUTPUT_BASE_PATH"))
        output_base_path.path.add(asset_key)

        table_paths.append(str(output_base_path))

    return table_paths


@dg.op(name="get_maintenance_target_tables", out=dg.Out(io_manager_key="mem_io_manager"))
def get_maintenance_taget_tables(context: dg.OpExecutionContext) -> list[str]:
    """Get target table paths for Delta table maintenance operations.

    This Dagster op queries the current Dagster instance to find all Delta tables
    that match the configured prefixes ("silver" and "gold") and returns their
    S3 paths for maintenance operations.

    Parameters
    ----------
    context : OpExecutionContext
        Dagster execution context providing access to the instance, logging,
        and other runtime information.

    Returns
    -------
    list[str]
        List of S3 URIs for Delta tables that should undergo maintenance.
        Each path represents a Delta table location in S3.

    Notes
    -----
    - Currently configured to target "silver" and "gold" tier tables
    - Uses the Dagster instance from the execution context
    - Logs the number of target tables found for monitoring
    """
    # Get instance from the op context
    instance = context.instance

    # Get tables by prefix
    prefixes = ["silver", "gold"]
    target_table_paths = get_delta_tables_by_prefix(instance, prefixes)

    context.log.info(f"Target tables for maintenance ({len(target_table_paths)} tables): {target_table_paths}")

    return target_table_paths


@dg.op(out=dg.Out(io_manager_key="mem_io_manager"))
def optimize_delta_tables(context: dg.OpExecutionContext, maintenance_config: DeltaMaintenanceConfig) -> None:
    """Optimize Delta tables by compacting small files.

    This operation performs file compaction on Delta tables to improve query
    performance and reduce the number of small files. Compaction combines
    multiple small Parquet files into larger, more efficient files without
    changing the data content.

    Parameters
    ----------
    context : OpExecutionContext
        Dagster execution context for logging and monitoring.
    config : DeltaMaintenanceConfig
        Configuration containing table paths, storage options, and other
        maintenance parameters.


    Notes
    -----
    - Uses Delta Lake's optimize.compact() method for file compaction
    - Does not delete the old files - use vacuum_delta_tables() to clean up
    - Improves query performance by reducing file overhead
    - Safe to run on active tables as it creates new versions atomically
    """
    metadata_log = {}

    for table_path in maintenance_config.table_paths:
        try:
            context.log.info(f"🔧 Optimizing {table_path}")

            dt = DeltaTable(table_path, storage_options=maintenance_config.storage_options)
            result = dt.optimize.compact()

            context.log.info(
                f"✅ Optimized {table_path}: {result['numFilesRemoved']} → {result['numFilesAdded']} files"
            )
            metadata_log[table_path] = {
                "table": table_path,
                "status": "success",
                "files_removed": result["numFilesRemoved"],
                "files_added": result["numFilesAdded"],
            }

        except Exception as e:
            context.log.exception(f"❌ Failed to optimize {table_path}")
            metadata_log[table_path] = {"table": table_path, "status": "error", "error": str(e)}

    context.add_output_metadata(metadata_log)


@dg.op(out=dg.Out(io_manager_key="mem_io_manager"))
def vacuum_delta_tables(context: dg.OpExecutionContext, maintenance_config: DeltaMaintenanceConfig) -> None:
    """Remove old data files from Delta tables to reclaim storage space.

    This operation permanently deletes data files that are no longer referenced
    by any table version within the retention period. Vacuuming helps reduce
    storage costs and clean up files left behind by previous operations.

    Parameters
    ----------
    context : OpExecutionContext
        Dagster execution context for logging and monitoring.
    config : DeltaMaintenanceConfig
        Configuration containing table paths, retention period, and storage options.


    Warnings
    --------
    - This operation permanently deletes files and cannot be undone
    - Files deleted may contain historical data needed for time travel queries
    - Ensure retention period is appropriate for your use case
    - Do not run with very short retention periods on active tables

    Notes
    -----
    - Uses Delta Lake's vacuum() method with configurable retention
    - Default retention is 7 days (168 hours) as configured
    - Should be run after optimize operations to clean up old files
    - Helps reduce storage costs and improve table performance

    See Also
    --------
    optimize_delta_tables : Companion operation that creates files to be vacuumed
    DeltaTable.vacuum : Underlying Delta Lake operation
    """
    metadata_log = {}
    for table_path in maintenance_config.table_paths:
        try:
            context.log.info(f"🧹 Vacuuming {table_path}")

            dt = DeltaTable(table_path, storage_options=maintenance_config.storage_options)

            deleted_files = dt.vacuum(retention_hours=maintenance_config.vacuum_retention_hours, dry_run=False)
            vacuum_result = {"table": table_path, "status": "success", "files_deleted": len(deleted_files)}

            context.log.info(f"✅ Vacuumed {table_path}: {vacuum_result}")

            metadata_log[table_path] = vacuum_result

        except Exception as e:
            context.log.exception(f"❌ Failed to vacuum {table_path}")
            metadata_log[table_path] = {"table": table_path, "status": "error", "error": str(e)}

    context.add_output_metadata(metadata_log)


@dg.job(
    name="delta_table_maintenance_job",
    executor_def=dg.in_process_executor,  # Using Single process executor as we are using in-memory io manager
    resource_defs={"mem_io_manager": dg.mem_io_manager},
)
def delta_maintenance() -> None:
    """Dagster job for automated Delta table maintenance.

    This job orchestrates the complete maintenance workflow for Delta tables:
    1. Discovers target tables based on configured prefixes
    2. Optimizes tables by compacting small files
    3. Vacuums tables to remove old, unreferenced files

    The job targets tables with "silver" and "gold" prefixes and uses AWS
    credentials from environment variables for S3 access.

    Notes
    -----
    - Runs optimize before vacuum to ensure proper file cleanup
    - Uses environment-based AWS credential configuration
    - Both operations are performed on the same set of discovered tables
    - Results from both operations are returned for monitoring

    See Also
    --------
    get_maintenance_target_tables : Discovers tables for maintenance
    optimize_delta_tables : Compacts small files for better performance
    vacuum_delta_tables : Removes old files to reclaim storage
    """
    target_table_paths = get_maintenance_taget_tables()

    maintenance_config = build_maintenance_config(target_table_paths)

    optimize_delta_tables(maintenance_config=maintenance_config)
    vacuum_delta_tables(maintenance_config=maintenance_config)
