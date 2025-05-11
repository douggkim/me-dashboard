"""AWS S3 authentication and configuration utilities."""

import os
from enum import Enum, auto
from typing import Any
from urllib.parse import urlparse
import dagster as dg 

import boto3
from loguru import logger


class AWSCredentialFormat(Enum):
    """Authentication credential types for AWS S3 storage."""

    UTILIZE_ENV_VARS = auto()  # Uses boto3 Session/credentials directly
    CREDENTIAL_STRINGS = auto()  # Uses access_key, secret_key strings
    ALL_OPTIONS = auto()


def get_aws_storage_options(return_credential_type: AWSCredentialFormat) -> dict[str, Any]:
    """
    Retrieve AWS S3 storage options for authentication with storage services.

    Creates a dictionary with authentication parameters based on the specified
    credential format, allowing underlying libraries to automatically handle
    authentication for S3 or MinIO.

    Parameters
    ----------
    return_credential_type : AWSCredentialFormat
        The credential format to use:
        - CREDENTIAL_OBJECT: Returns client_kwargs dict with Session object
          (for libraries like fsspec that accept credential objects)
        - CREDENTIAL_STRINGS: Provides key/secret directly
          (for libraries like DeltaTable that handle credential creation internally)
        - ALL_OPTIONS: Returns all available authentication options and parameters

    file_path : str, optional
        Path to the file, used for extracting the bucket name if needed.

    Returns
    -------
    dict
        Dictionary containing AWS authentication parameters appropriate for the
        specified credential format.

    Raises
    ------
    ValueError
        - if missing critical credential info
        - if provided with wrong authentication type
    """
    # Get credentials from environment variables
    access_key = dg.EnvVar("AWS_ACCESS_KEY_ID").get_value()
    secret_key = dg.EnvVar("AWS_SECRET_ACCESS_KEY").get_value()
    region = dg.EnvVar("AWS_REGION").get_value()

    endpoint_url = dg.EnvVar("AWS_S3_ENDPOINT").get_value()

    storage_options = {}
    # Get S3 endpoint - defaults differ based on emulator mode
    if os.getenv("AWS_S3_USE_EMULATOR", "0") == "1":
        logger.info(f"Setting up S3 storage options for local MinIO emulator at {endpoint_url}")
    else:
        logger.info("Setting up S3 storage options for production AWS S3")

    # Validate required credentials
    if not access_key or not secret_key:
        missing_vars = []
        if not access_key:
            missing_vars.append("AWS_ACCESS_KEY_ID")
        if not secret_key:
            missing_vars.append("AWS_SECRET_ACCESS_KEY")
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # Return format-specific configurations
    if return_credential_type == AWSCredentialFormat.UTILIZE_ENV_VARS:
        # Add client_kwargs with session
        storage_options["client_kwargs"] = {"endpoint_url": endpoint_url}

        return storage_options

    if return_credential_type == AWSCredentialFormat.CREDENTIAL_STRINGS:
        storage_options["aws_access_key_id"] = access_key
        storage_options["secret_access_key"] = secret_key
        if os.getenv("ENVIRONMENT") != "prod":
            storage_options["endpoint_url"] = endpoint_url
        storage_options["region"] = region
        storage_options["AWS_ALLOW_HTTP"] = "true"
        return storage_options

    if return_credential_type == AWSCredentialFormat.ALL_OPTIONS:
        # Add boto3 session
        session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)

        # Add session AND client_kwargs (for maximum flexibility)
        storage_options["session"] = session
        storage_options["client_kwargs"]["session"] = session

        return storage_options

    valid_types = ", ".join([f"{auth_type.name}" for auth_type in AWSCredentialFormat])
    raise ValueError(f"Invalid authentication type provided. Please use one of: {valid_types}")


def extract_bucket_name(file_path: str) -> str:
    """
    Extract the S3 bucket name from the given file path URL.

    Parameters
    ----------
    file_path : str
        The file path URL (e.g. "s3://my-bucket/path/to/file").

    Returns
    -------
    str
        The extracted bucket name, or empty string if no bucket can be determined.
    """
    # For a standard S3 URL
    parsed = urlparse(file_path)

    # Handle s3:// URLs
    if parsed.scheme == "s3":
        return parsed.netloc

    # Return environment bucket if no bucket in path
    return dg.EnvVar("AWS_S3_BUCKET_NAME").get_value()
