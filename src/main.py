"""Main entrypoint for Dagster deployment."""

import os

import dagster as dg

import src.validation.asset_checks as asset_checks_module
from src.assets.work.github import github
from src.assets.entertainment import playstation, spotify_play_history
from src.jobs.delta_optimization import delta_maintenance, weekly_delta_maintenance_schedule
from src.resources.data_loader import DataLoaderResource
from src.resources.geo_encoder import GeoEncoderResource
from src.resources.github_resource import GithubResource
from src.resources.io_managers import JSONTextIOManager, PandasDeltaIOManager, PolarsDeltaIOManager
from src.resources.psn_resource import PSNResource
from src.resources.spotify_resource import SpotifyResource
from src.utils.aws import AWSCredentialFormat, get_aws_storage_options

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([playstation, spotify_play_history, github]),
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
        "github_resource": GithubResource(
            github_token=dg.EnvVar("GITHUB_TOKEN"), github_username=dg.EnvVar("GITHUB_USERNAME")
        ),
        "geo_encoder": GeoEncoderResource(
            google_maps_api_key=dg.EnvVar("GOOGLE_MAPS_API_KEY"),
            s3_bucket=dg.EnvVar("AWS_S3_BUCKET_NAME"),
            s3_cache_location="gps_cache/geocoding_cache.json",
            cache_precision=6,
            cache_ttl_days=30,
            queries_per_second=40,
        ),
    },
    jobs=[delta_maintenance],
    schedules=[weekly_delta_maintenance_schedule],
    asset_checks=dg.load_asset_checks_from_package_module(asset_checks_module),
)
