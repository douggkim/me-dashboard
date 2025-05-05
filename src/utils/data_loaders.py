"""Data Loader supporting multiple file formats (csv, parquet, DeltaTable) and outputs DataFrames/ pyarrow datasets."""

import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any
from urllib.parse import urlparse

import fsspec
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
import yaml
from deltalake import DeltaTable
from loguru import logger

from src.utils.azure import AzureCredentialFormat, get_azure_storage_options
from src.utils.global_helpers import filter_kwargs


class FileFormat(Enum):
    """Enum representing supported file formats."""

    DELTA = "delta"
    CSV = "csv"
    PARQUET = "parquet"
    UNKNOWN = "unknown"


class BaseLoader(ABC):
    """Abstract base class for loaders."""

    @abstractmethod
    def load(self, **kwargs: Any) -> Any:
        """
        Load the data from the file.

        Parameters
        ----------
        **kwargs : dict[str, Any]
            Additional keyword arguments for the underlying reader.

        Returns
        -------
        Any
            The loaded data.

        Raises
        ------
        NotImplementedError
            If not implemented in a subclass.
        """

    def _infer_data_format(self, file_path: str) -> str:
        """
        Infer the data format from the file extension or using the Delta library.

        Parameters
        ----------
        file_path : str
            The path to the table directory.

        Returns
        -------
        str
            The inferred data format (e.g. 'delta', 'csv', 'parquet', or 'unknown').

        Raises
        ------
        None
        """
        lowercase_file_path = file_path.lower()
        storage_options = get_azure_storage_options(
            file_path=file_path, return_credential_type=AzureCredentialFormat.CREDENTIAL_STRINGS
        )

        # For validating whether the path is DeltaTable
        if DeltaTable.is_deltatable(table_uri=file_path, storage_options=storage_options):
            input_data_format = FileFormat.DELTA

        elif lowercase_file_path.endswith(".csv"):
            input_data_format = FileFormat.CSV

        elif lowercase_file_path.endswith(".parquet"):
            input_data_format = FileFormat.PARQUET

        else:
            input_data_format = FileFormat.UNKNOWN

        logger.info(f"Inferring file format of {file_path}. Determined to be of type {input_data_format}.")

        return input_data_format


class PandasDataLoader(BaseLoader):
    """Loader for producing a pandas DataFrame."""

    def load(self, file_path: str, **kwargs: Any) -> pd.DataFrame:
        """
        Load data and return it as a pandas DataFrame.

        Parameters
        ----------
        file_path : str
            The path to the table directory.
        **kwargs : dict[str, Any]
            Additional keyword arguments to pass to the pandas reader functions.
            For example, 'nrows', 'compression', etc.

        Returns
        -------
        pd.DataFrame
            The loaded data as a pandas DataFrame.

        Raises
        ------
        ValueError
            If the file format is unsupported by PandasDataLoader.
        """
        input_format = self._infer_data_format(file_path=file_path)

        if input_format == FileFormat.CSV:
            valid_kwargs = filter_kwargs(pd.read_csv, kwargs)
            return pd.read_csv(
                filepath_or_buffer=file_path,
                storage_options=get_azure_storage_options(
                    file_path=file_path, return_credential_type=AzureCredentialFormat.CREDENTIAL_OBJECT
                ),
                **valid_kwargs,
            )

        if input_format == FileFormat.PARQUET:
            valid_kwargs = filter_kwargs(pd.read_parquet, kwargs)
            return pd.read_parquet(
                path=file_path,
                storage_options=get_azure_storage_options(
                    file_path=file_path, return_credential_type=AzureCredentialFormat.CREDENTIAL_OBJECT
                ),
                **valid_kwargs,
            )

        if input_format == FileFormat.DELTA:
            storage_options = get_azure_storage_options(
                file_path=file_path, return_credential_type=AzureCredentialFormat.CREDENTIAL_STRINGS
            )
            dt = DeltaTable(
                table_uri=file_path,
                storage_options=storage_options,
            )
            valid_kwargs = filter_kwargs(dt.to_pandas, kwargs)
            return dt.to_pandas(**valid_kwargs)

        raise ValueError(f"Unsupported format for PandasDataLoader: {input_format}")


class PolarsDataLoader(BaseLoader):
    """
    Loader for producing a Polars DataFrame or LazyFrame.

    This loader leverages Polars' scan_* functions to efficiently load data from various
    file formats. When using the scan functions, data is loaded lazily (as LazyFrames)
    until materialized. The loader can return either a lazy or materialized DataFrame
    based on the provided parameters.

    For Parquet and Delta formats, this loader automatically sets appropriate PyArrow
    options (such as timestamp handling) if they are not explicitly provided.

    See Also
    --------
    PandasDataLoader : For loading data as pandas DataFrames
    ArrowDatasetLoader : For loading data as Arrow datasets
    """

    def load(self, file_path: str, **kwargs: Any) -> pl.DataFrame:
        """
        Load data and return it as a polars DataFrame or LazyFrame.

        Parameters
        ----------
        file_path : str
            The path to the table directory.
        **kwargs : dict[str, Any]
            Additional keyword arguments to pass to the polars reader functions.
            For example:
                - lazy (bool): If True, use the scan API to load data lazily.
                - use_pyarrow (bool): Whether to force the PyArrow backend for reading Parquet.
                - pyarrow_options (dict): Additional options for the PyArrow backend.

        Returns
        -------
        pl.DataFrame or pl.LazyFrame
            The loaded data as a polars DataFrame, or as a LazyFrame if lazy loading is requested.

        Raises
        ------
        ValueError
            If the file format is unsupported by PolarsDataLoader.
        """
        input_format = self._infer_data_format(file_path=file_path)

        storage_options = get_azure_storage_options(
            file_path=file_path, return_credential_type=AzureCredentialFormat.CREDENTIAL_STRINGS
        )

        # For parquet and delta, set pyarrow_options if not provided.
        if input_format in {FileFormat.PARQUET, FileFormat.DELTA}:
            if "pyarrow_options" not in kwargs:
                kwargs["pyarrow_options"] = {}
            if "parquet_read_options" not in kwargs["pyarrow_options"]:
                kwargs["pyarrow_options"]["parquet_read_options"] = {}
            kwargs["pyarrow_options"]["parquet_read_options"].setdefault("coerce_int96_timestamp_unit", "us")
            if "use_pyarrow" not in kwargs:
                kwargs["use_pyarrow"] = True

        if input_format == FileFormat.CSV:
            valid_kwargs = filter_kwargs(pl.scan_csv, kwargs)
            return pl.scan_csv(source=file_path, storage_options=storage_options, **valid_kwargs)

        if input_format == FileFormat.PARQUET:
            valid_kwargs = filter_kwargs(pl.scan_parquet, kwargs)
            return pl.scan_parquet(source=file_path, storage_options=storage_options, **valid_kwargs)

        if input_format == FileFormat.DELTA:
            valid_kwargs = filter_kwargs(pl.scan_delta, kwargs)
            return pl.scan_delta(source=file_path, storage_options=storage_options, **valid_kwargs)

        raise ValueError(f"Unsupported format for PolarsDataLoader: {input_format}")


class ArrowDatasetLoader(BaseLoader):
    """Loader for producing a pyarrow Dataset (streaming mode)."""

    def load(self, file_path: str, **kwargs: Any) -> ds.Dataset:
        """
        Load data and return it as a pyarrow Dataset.

        This method creates a pyarrow Dataset, which provides a streaming interface
        for efficient data processing. It automatically handles local vs. remote
        filesystem access based on the environment.


        Parameters
        ----------
        file_path : str
            The path to the table directory.
        **kwargs : dict[str, Any]
            Additional keyword arguments to pass to the pyarrow dataset constructor.
            For example, 'schema', 'partitioning', 'batch_size', etc.

        Returns
        -------
        ds.Dataset
            The loaded data as a pyarrow Dataset.

        Raises
        ------
        ValueError
            If the file format is unsupported for streaming mode (only 'parquet' or 'delta' are supported).
        """
        input_format = self._infer_data_format(file_path=file_path)

        if input_format == FileFormat.PARQUET:
            parsed = urlparse(file_path)
            storage_options = get_azure_storage_options(
                file_path=file_path, return_credential_type=AzureCredentialFormat.CREDENTIAL_OBJECT
            )
            fs = (
                fsspec.filesystem("file")
                if parsed.scheme in {"", "file"}
                else fsspec.filesystem(parsed.scheme, **storage_options)
            )
            valid_kwargs = filter_kwargs(ds.dataset, kwargs)
            return ds.dataset(source=file_path, filesystem=fs, format="parquet", **valid_kwargs)

        if input_format == FileFormat.DELTA:
            storage_options = get_azure_storage_options(
                file_path=file_path, return_credential_type=AzureCredentialFormat.CREDENTIAL_STRINGS
            )
            dt = DeltaTable(table_uri=file_path, storage_options=storage_options)
            valid_kwargs = filter_kwargs(dt.to_pyarrow_dataset, kwargs)
            return dt.to_pyarrow_dataset(**valid_kwargs)

        raise ValueError("Streaming mode (ds.Dataset) is only supported for Parquet or Delta formats.")


def get_storage_path(dataset_name: str, table_name: str, config_path: str = os.getenv("FILE_PATH_CONFIG_PATH")) -> str:
    """
    Retrieve the storage path from the configuration using a two-layer design.

    Parameters
    ----------
    dataset_name : str
        The top-level dataset name (e.g., 'cga', 'mpm').
    table_name : str
        The specific table name (e.g., 'cga_summary').
    config_path : str, optional
        The path to the configuration file. Default is the value of the
        environment variable 'FILE_PATH_CONFIG_PATH'.

    Returns
    -------
    str
        The resolved file path.

    Raises
    ------
    KeyError
        If the configuration file is malformed or if the specified dataset, table,
        or environment is not found in the configuration.
    """
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    env = os.getenv("ENVIRONMENT")

    if "paths" not in config:
        raise KeyError("Malformed configuration file. Expecting `paths` top-level key.")

    if dataset_name not in config["paths"]:
        raise KeyError(f"Cannot find entries for dataset {dataset_name} in paths configuration.")

    if table_name not in config["paths"][dataset_name]:
        raise KeyError(f"Cannot find entry for table {table_name} within dataset {dataset_name}")

    if env not in config["paths"][dataset_name][table_name]:
        raise KeyError(f"Cannot find entry for environment {env} within table {dataset_name}.{table_name}")

    return config["paths"][dataset_name][table_name][env]
