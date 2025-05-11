"""Defines custom IOManagers for use with Dagster."""

from abc import ABC, abstractmethod
from typing import Any

import deltalake
import pandas as pd
import polars as pl
from dagster import ConfigurableIOManager, InputContext, MetadataValue, OutputContext
from furl import furl
from loguru import logger


class GenericInputDeltaIOManager(ConfigurableIOManager, ABC):
    """A generic input IOManager that provides a standard implementation for writing data as a Delta Table.

    The input method is abstract and must be implemented by subclasses since different assets may require different
    DataFrame types.

    Attributes
    ----------
    storage_options : dict
        A dictionary of connection options used to connect to AWS S3 Storage.
    """

    storage_options: dict = dict()  # noqa: RUF012
    output_base_path: str

    @abstractmethod
    def load_input(self, context: InputContext) -> Any:
        """Load input data from the specified path."""

    def _get_metadata_values(self, context: InputContext | OutputContext, metadata_key: str) -> list[str]:
        """
        Extract certain metadata values from asset metadata.

        Parameters
        ----------
        context : InputContext or OutputContext
            The Dagster context object containing asset metadata

        Returns
        -------
        list[str]
            A list of the metadata values

        Examples
        --------
        >>> # With metadata containing provided metadata_key in metadata
        >>> _get_metadata_values(context_with_metadata)
        ["country", "brand_id", "date"]
        """
        asset_metadata = context.definition_metadata
        return asset_metadata.get(metadata_key, [])

    def _get_merge_predicates(self, primary_keys: list[str]) -> str:
        """
        Generate merge predicates for Delta table merge operations based on specified columns.

        Parameters
        ----------
        primary_keys : list[str]
            A list of column names to use as merge keys

        Returns
        -------
        str
            A string containing the merge predicate expression with source and target aliases

        Raises
        ------
        ValueError
            If primary_keys is empty or None

        Examples
        --------
        >>> _get_merge_predicates(["id", "region"])
        "s.id = t.id AND s.region = t.region"
        """
        if not primary_keys:
            raise ValueError("At least one column must be specified for merge operations")

        predicates = [f"s.{col} = t.{col}" for col in primary_keys]
        return " AND ".join(predicates)

    def _get_storage_path(self, context: InputContext | OutputContext) -> str:
        """
        Get the path for the Delta Table based on the asset identifier.

        Parameters
        ----------
        context : InputContext or OutputContext
            The Dagster context for the input or output operation.

        Returns
        -------
        str
            The path to the Delta Table
        """
        asset_identifiers = context.asset_key.parts
        # Concatenate asset parts to form the dataset name
        output_base_path = furl(self.output_base_path)
        for segment in asset_identifiers:
            output_base_path.path.add(segment)

        return str(output_base_path)

    def handle_output(self, context: OutputContext, obj: pl.LazyFrame | pl.DataFrame | pd.DataFrame) -> None:
        """Write a DataFrame to a specified path in AWS S3 storage as a Delta Table.

        Parameters
        ----------
        context : OutputContext
            The Dagster context for the output operation.
        obj : pl.LazyFrame | pl.DataFrame | pd.DataFrame
            The DataFrame to be written. It can be a polars LazyFrame, polars DataFrame, or pandas DataFrame.

        Raises
        ------
        TypeError
            Raised when the provided data is of an unsuported type.
        """
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()
        elif isinstance(obj, pd.DataFrame):
            obj = pl.from_pandas(obj)
        elif not isinstance(obj, pl.DataFrame):
            raise TypeError("Unsupported object type. Must be a polars LazyFrame, DataFrame, or pandas DataFrame.")

        # Save metadata
        with pl.Config(tbl_formatting="MARKDOWN", tbl_hide_column_data_types=True, tbl_hide_dataframe_shape=True):
            metadata = {
                "df": MetadataValue.md(repr(obj.head())),
                "dagster/row_count": obj.shape[0],
                "n_cols": obj.shape[1],
            }
            context.add_output_metadata(metadata)

        # Save the DataFrame to the specified path
        path = self._get_storage_path(context)

        context.log.debug(
            f"Dumping to AWS S3 storage using the following storage options: {self.storage_options} and file path: {path}"
        )

        is_delta = deltalake.DeltaTable.is_deltatable(table_uri=f"s3://{path}", storage_options=self.storage_options)
        primary_keys = self._get_metadata_values(context=context, metadata_key="primary_keys")
        partition_cols = self._get_metadata_values(context=context, metadata_key="partition_cols")

        if is_delta and primary_keys:
            merge_predicate = self._get_merge_predicates(primary_keys=primary_keys)

            context.log.info(f"Table at s3://{path} exists, performing merge operation using columns:{primary_keys}")
            logger.info(f"Generated merge predicate for TABLE MERGE: {merge_predicate}")

            (
                obj.write_delta(
                    f"s3://{path}",
                    mode="merge",
                    delta_merge_options={
                        "predicate": merge_predicate,
                        "source_alias": "s",
                        "target_alias": "t",
                    },
                    delta_write_options={"schema_mode": "overwrite"},
                    storage_options=self.storage_options,
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        elif partition_cols:
            context.log.info(f"Writing to {path} - the table will be partitioned using columns: {partition_cols}")

            obj.write_delta(
                target=f"s3://{path}",
                mode="overwrite",
                storage_options=self.storage_options,
                delta_write_options={"partition_by": partition_cols, "schema_mode": "overwrite"},
            )
        else:
            context.log.warning(
                f"Writing to {path} - No partition keys have been set. The table will be fully overwritten"
            )
            obj.write_delta(
                target=f"s3://{path}",
                mode="overwrite",
                storage_options=self.storage_options,
                delta_write_options={"schema_mode": "overwrite"},
            )


class PandasDeltaIOManager(GenericInputDeltaIOManager):
    """An IOManager for loading and saving pandas DataFrames."""

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load input data as a Delta Table from the specified path.

        Parameters
        ----------
        context : InputContext
            The Dagster context for the input operation.

        Returns
        -------
        pd.DataFrame
            The loaded DataFrame.
        """
        path = self._get_storage_path(context)

        return (
            pl.scan_delta(
                f"s3://{path}",
                storage_options=self.storage_options,
            )
            .collect()
            .to_pandas()
        )


class PolarsLazyDeltaIOManager(GenericInputDeltaIOManager):
    """An IOManager for loading and saving polars LazyFrames."""

    def load_input(self, context: InputContext) -> pl.LazyFrame:
        """Load input data as a Delta Table from the specified path.

        Parameters
        ----------
        context : InputContext
            The Dagster context for the input operation.

        Returns
        -------
        pl.LazyFrame
            The loaded LazyFrame.
        """
        path = self._get_storage_path(context)
        return pl.scan_delta(
            f"s3://{path}",
            storage_options=self.storage_options,
        )


class PolarsDeltaIOManager(GenericInputDeltaIOManager):
    """An IOManager for loading and saving polars DataFrames."""

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """Load input data as a Delta Table from the specified path.

        Parameters
        ----------
        context : InputContext
            The Dagster context for the input operation.

        Returns
        -------
        pl.DataFrame
            The loaded DataFrame.
        """
        path = self._get_storage_path(context)
        return pl.scan_delta(
            f"s3://{path}",
            storage_options=self.storage_options,
        ).collect()
