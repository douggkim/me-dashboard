"""Main entrypoint for Dagster deployment."""

import os

import dagster as dg

import src.assets as assets_module
import src.validation.asset_checks as asset_checks_module
from src.jobs.delta_optimization import delta_maintenance
from src.resources.data_loader import DataLoaderResource
from src.resources.io_managers import JSONTextIOManager, PandasDeltaIOManager, PolarsDeltaIOManager
from src.resources.psn_resource import PSNResource
from src.resources.spotify_resource import SpotifyResource
from src.utils.aws import AWSCredentialFormat, get_aws_storage_options

defs = dg.Definitions(
    assets=dg.with_source_code_references(dg.load_assets_from_package_module(assets_module)),
    resources={
        "io_manager_pl": PolarsDeltaIOManager(
            output_base_path=os.getenv("OUTPUT_BASE_PATH"),
            storage_options=get_aws_storage_options(return_credential_type=AWSCredentialFormat.CREDENTIAL_STRINGS),
        ),
        "io_manager": PandasDeltaIOManager(
            output_base_path=os.getenv("OUTPUT_BASE_PATH"),
            storage_options=get_aws_storage_options(return_credential_type=AWSCredentialFormat.CREDENTIAL_STRINGS),
        ),
        "io_manager_json_txt": JSONTextIOManager(
            output_base_path=os.getenv("OUTPUT_BASE_PATH"),
            storage_options=get_aws_storage_options(return_credential_type=AWSCredentialFormat.UTILIZE_ENV_VARS),
        ),
        "data_loader": DataLoaderResource(config_path=os.getenv("FILE_PATH_CONFIG_PATH")),
        "spotify_resource": SpotifyResource(
            client_id=dg.EnvVar("SPOTIFY_CLIENT_ID"),
            client_secret=dg.EnvVar("SPOTIFY_CLIENT_SECRET"),
            refresh_token=dg.EnvVar("SPOTIFY_REFRESH_TOKEN"),
        ),
        "psn_resource": PSNResource(refresh_token=dg.EnvVar("PSN_REFRESH_TOKEN")),
    },
    jobs=[delta_maintenance],
    asset_checks=dg.load_asset_checks_from_package_module(asset_checks_module),
)
