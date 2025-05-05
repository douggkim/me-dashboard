"""Data Loader supporting multiple file formats (csv, parquet, DeltaTable) and outputs DataFrames/ pyarrow datasets."""

import os
from typing import Any

import dagster as dg
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
import yaml
from pydantic import PrivateAttr

from src.utils.data_loaders import (ArrowDatasetLoader, PandasDataLoader,
                                    PolarsDataLoader)


class DataLoaderResource(dg.ConfigurableResource):
    """
    Combined DataLoader that loads configuration, performs authentication, and loads using output-specific strategies.

    Parameters
    ----------
        config_path : str
            The path to the YAML configuration file.
    """

    config_path: str
    _env: str = PrivateAttr()
    _config: dict[str, Any] = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:  # noqa: ARG002
        """Set up the class attributes when calling the Dagster resource.

        Args:
            context (dg.InitResourceContext): _description_
        """
        self._env = os.getenv("FUELIGHT_ENVIRONMENT", "local")
        self._config = self._load_config(self.config_path)

    def _load_config(self, config_path: str) -> dict[str, Any]:
        """
        Load the YAML configuration file.

        Parameters
        ----------
        config_path : str
            The configuration file path.

        Returns
        -------
        dict[str, Any]
            The loaded configuration.
        """
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_storage_path(self, dataset_name: str, table_name: str) -> str:
        """
        Retrieve the storage path from the configuration using a two-layer design.

        Parameters
        ----------
        dataset_name : str
            The top-level dataset name (e.g., 'cga', 'mpm').
        table_name : str
            The specific table name (e.g., 'cga_summary').

        Returns
        -------
        str
            The resolved file path.

        Raises
        ------
        KeyError
            If the specified dataset/table is not found for the current environment.
        """
        if "paths" not in self._config:
            raise KeyError("Malformed configuration file. Expecting `paths` top-level key.")

        if dataset_name not in self._config["paths"]:
            raise KeyError(f"Cannot find entries for dataset {dataset_name} in paths configuration.")

        if table_name not in self._config["paths"][dataset_name]:
            raise KeyError(f"Cannot find entry for table {table_name} within dataset {dataset_name}")

        if self._env not in self._config["paths"][dataset_name][table_name]:
            raise KeyError(f"Cannot find entry for environment {self._env} within table {dataset_name}.{table_name}")

        return self._config["paths"][dataset_name][table_name][self._env]

    def load_data(self, dataset_name: str, table_name: str, out_format: type[Any], **kwargs: Any) -> Any:
        """
        Load data for a specific dataset and table, returning the data in the desired output format.

        This was created to bypass the boilerplate code for Azure authentication and data type coercion issues.
        (e.g.int96_timestamp)

        Supported output types:
            - pd.DataFrame (using PandasDataLoader)
            - pl.DataFrame or pl.LazyFrame (using PolarsDataLoader)
            - ds.Dataset (using ArrowDatasetLoader)

        Parameters
        ----------
        dataset_name : str
            The top-level dataset name.
        table_name : str
            The specific table name.
        out_format : type[Any]
            The desired output format class. Supported values are:
            - pd.DataFrame : Returns a pandas DataFrame
            - pl.DataFrame : Returns a Polars DataFrame (materialized)
            - pl.LazyFrame : Returns a Polars LazyFrame (not materialized)
            - ds.Dataset : Returns a PyArrow Dataset (for streaming access)
        **kwargs : dict[str, Any]
            Additional keyword arguments passed to the underlying loader.
            Common options include:
            - For pandas: nrows, compression, etc.
            - For Polars: lazy=True/False, use_pyarrow=True/False, etc.
            - For PyArrow Dataset: batch_size, partitioning, etc.

        Returns
        -------
        Any
            The loaded data in the requested output format (pandas DataFrame,
            Polars DataFrame, Polars LazyFrame, or PyArrow Dataset).


        Raises
        ------
        ValueError
            If the output type is unsupported or if the file format is unsupported.

        Examples
        --------
        >>> loader = DataLoader("config.yaml")
        >>> # Load as pandas DataFrame
        >>> df_pandas = loader.load_data("my_dataset", "table1", pd.DataFrame)

        >>> # Load as Polars LazyFrame
        >>> df_lazy = loader.load_data("my_dataset", "table1", pl.LazyFrame)

        >>> # Load as Polars DataFrame (materialized)
        >>> df_polars = loader.load_data("my_dataset", "table1", pl.DataFrame)

        >>> # Load as PyArrow Dataset
        >>> ds_arrow = loader.load_data("my_dataset", "table1", ds.Dataset)

        >>> # Load Polars with partition filter (correct predicate pushdown)
        >>> df_filtered = loader.load_data(
        ...     "my_dataset",
        ...     "table1",
        ...     pl.DataFrame,
        ...     pyarrow_options={
        ...         "partitions": [
        ...             ("is_current", "=", "true")
        ...         ]
        ...     }
        ... )

        >>> # Multiple partition filters for Polars
        >>> df_multi_filter = loader.load_data(
        ...     "my_dataset",
        ...     "partitioned_table",
        ...     pl.DataFrame,
        ...     pyarrow_options={
        ...         "partitions": [
        ...             ("year", "=", "2023"),
        ...             ("month", "in", ["01", "02", "03"])
        ...         ]
        ...     }
        ... )
        """
        file_path = self.get_storage_path(dataset_name, table_name)
        if out_format is pd.DataFrame:
            loader = PandasDataLoader()

        elif out_format in {pl.DataFrame, pl.LazyFrame}:
            loader = PolarsDataLoader()

        elif out_format == ds.Dataset:
            loader = ArrowDatasetLoader()

        else:
            raise ValueError(f"Unsupported output type: {out_format}")

        data = loader.load(file_path, **kwargs)

        if out_format is pl.DataFrame:  # loads onto memory
            data = data.collect()

        return data