"""Utility module for Azure functions that support Azure configuration."""

import os
from enum import Enum, auto
from urllib.parse import urlparse

from azure.identity import ClientSecretCredential
from loguru import logger


class AzureCredentialFormat(Enum):
    """Authentication credential types for Azure storage."""

    CREDENTIAL_OBJECT = auto()  # Uses DefaultAzureCredential directly
    CREDENTIAL_STRINGS = auto()  # Uses client_id, client_secret, tenant_id
    ALL_OPTIONS = auto()


def get_azure_storage_options(
    return_credential_type: AzureCredentialFormat, file_path: str | None = None
) -> dict[str, str | ClientSecretCredential]:
    """
    Retrieve Azure storage options for authentication with storage services.

    Creates a dictionary with authentication parameters based on the specified
    credential format, allowing underlying libraries to automatically handle
    token rotation for long-running processes.

    Parameters
    ----------
    return_credential_type : AzureCredentialFormat
        The credential format to use:
        - CREDENTIAL_OBJECT: Uses ClientSecretCredential object directly
          (for libraries like fsspec and pandas that accept credential objects)
        - CREDENTIAL_STRINGS: Provides tenant_id, azure_client_id, azure_client_secret
          (for libraries like DeltaTable that handle credential creation internally)
        - ALL_OPTIONS: Returns all available authentication options and parameters
          (for use cases where user wants to use the options in a custom way)

    file_path : str, optional
        Path to the table directory used for extracting the storage_account name.
        If not provided, defaults to the value from environment variables.

    Returns
    -------
    dict
        Dictionary containing Azure authentication parameters appropriate for the
        specified credential format. Common keys include:
        - account_name: The Azure storage account name
        - credential: ClientSecretCredential object (for CREDENTIAL_OBJECT)
        - tenant_id, azure_client_id, azure_client_secret (for CREDENTIAL_STRINGS)

        When ALL_OPTIONS is specified, returns a comprehensive dictionary with
        all possible authentication parameters.

        Returns minimal dict with just azure_storage_use_emulator if the environment is 'local'.

    Raises
    ------
    ValueError
        If an invalid credential format is provided.
    ValueError
        If azure credentials are not provided.
    """
    azure_storage_use_emulator = os.getenv("AZURE_STORAGE_USE_EMULATOR", "0")

    if azure_storage_use_emulator == "1":
        logger.info("Setting up azure storage options for local Azurite")
        return {
            "azure_storage_use_emulator": azure_storage_use_emulator,
        }

    # initiate all credentials
    storage_account = extract_storage_account(file_path) if file_path else os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    if not tenant_id or not client_id or not client_secret:
        missing_vars = []
        if not tenant_id:
            missing_vars.append("AZURE_TENANT_ID")
        if not client_id:
            missing_vars.append("AZURE_CLIENT_ID")
        if not client_secret:
            missing_vars.append("AZURE_CLIENT_SECRET")

        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    token = credential.get_token("https://storage.azure.com/.default").token

    # Base options common to all auth types
    storage_options = {
        "account_name": storage_account,
        "azure_storage_use_emulator": azure_storage_use_emulator,
    }

    if return_credential_type == AzureCredentialFormat.CREDENTIAL_OBJECT:
        storage_options["credential"] = credential

    elif return_credential_type == AzureCredentialFormat.CREDENTIAL_STRINGS:
        storage_options.update({
            "tenant_id": tenant_id,
            "azure_client_id": client_id,
            "azure_client_secret": client_secret,
        })

    elif return_credential_type == AzureCredentialFormat.ALL_OPTIONS:
        # Include all possible authentication options
        storage_options.update({
            "credential": credential,
            "token": token,
            "tenant_id": tenant_id,
            "azure_client_id": client_id,
            "azure_client_secret": client_secret,
        })

    else:
        valid_types = ", ".join([f"{auth_type.name}" for auth_type in AzureCredentialFormat])
        raise ValueError(f"Invalid authentication type provided. Please use one of: {valid_types}")

    return storage_options


def extract_storage_account(file_path: str) -> str:
    """
    Extract the storage account name from the given file path URL.

    The function parses the URL and extracts the storage account name by looking
    at the part after the "@" symbol in the network location. For example, if the
    URL is of the form "abfss://refined@storagemdp01us2prd.dfs.core.windows.net/...",
    it returns "storagemdp01us2prd".

    Parameters
    ----------
    file_path : str
        The file path URL (e.g. "abfss://refined@storagemdp01us2prd.dfs.core.windows.net/...").

    Returns
    -------
    str
        The extracted storage account name.

    Examples
    --------
    >>> extract_storage_account("abfss://refined@storagemdp01us2prd.dfs.core.windows.net/fuelight360/cga/cga_summary/tbl_final/")
    'storagemdp01us2prd'
    >>> extract_storage_account("abfss://refined@storagemdp01us2dev.dfs.core.windows.net/fuelight360/mpm/data.parquet")
    'storagemdp01us2dev'
    >>> extract_storage_account("file:///local/path/to/file")
    ''
    """
    parsed = urlparse(file_path)
    if "@" in parsed.netloc:
        _, after_at = parsed.netloc.split("@", 1)
        storage_account = after_at.split(".")[0]
        logger.info(f"Loading from {storage_account}")
        return storage_account
    return ""
