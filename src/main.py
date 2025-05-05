"""Main entrypoint for Dagster deployment."""
import os

import dagster as dg
from dagster_deltalake import LocalConfig

import src.assets as assets_module
import src.validation.asset_checks as asset_checks_module
from src.resources.io_managers import (PandasDeltaIOManager,
                                       PolarsDeltaIOManager)
from src.utils.azure import AzureCredentialFormat, get_azure_storage_options

defs = dg.Definitions(
    assets=dg.with_source_code_references(dg.load_assets_from_package_module(assets_module)),
    resources={
        "io_manager_pl": PolarsDeltaIOManager(
            output_base_path=os.getenv("OUTPUT_BASE_PATH"), 
            storage_options=get_azure_storage_options(return_credential_type=AzureCredentialFormat.CREDENTIAL_STRINGS)
        ),
        "io_manager": PandasDeltaIOManager(
            output_base_path=os.getenv("OUTPUT_BASE_PATH"), 
            storage_options=get_azure_storage_options(return_credential_type=AzureCredentialFormat.CREDENTIAL_STRINGS)
        ),
    },
    asset_checks=dg.load_asset_checks_from_package_module(asset_checks_module),
    jobs=[],
)
