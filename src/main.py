"""Main entrypoint for Dagster deployment."""

import dagster as dg
from dagster_deltalake import LocalConfig
from dagster_deltalake_pandas import DeltaLakePandasIOManager
from dagster_deltalake_polars import DeltaLakePolarsIOManager

import src.assets as assets_module
import src.validation.asset_checks as asset_checks_module

defs = dg.Definitions(
    assets=dg.with_source_code_references(dg.load_assets_from_package_module(assets_module)),
    resources={
        "io_manager_pl": DeltaLakePolarsIOManager(
            root_uri="./data/testing/",
            overwrite_schema=True,
            storage_options=LocalConfig(),  # change to Cloud storage configs if needed (e.g. `S3Config`, `AzureConfig`)
            # could be better to have a wrapper function for setting up based on env variable
        ),
        "io_manager": DeltaLakePandasIOManager(
            root_uri="./data/testing",
            overwrite_schema=True,
            storage_options=LocalConfig(),
        ),
    },
    asset_checks=dg.load_asset_checks_from_package_module(asset_checks_module),
    jobs=[],
)
