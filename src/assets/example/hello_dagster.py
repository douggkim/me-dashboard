"""Example assets containing boilerplate code."""

import dagster as dg
import pandas as pd
from dagster_pandera import pandera_schema_to_dagster_type

from src.validation.schemas.example.hello_dagster import HelloDagsterSchema


@dg.asset(
    name="hello_dagster",
    key_prefix=["example"],
    description="Some boilerplate example",
    group_name="example",
    kinds={"pandas"},
    tags={"domain": "example", "source": "me"},
    owners=["doug@randomplace.com"],
    dagster_type=pandera_schema_to_dagster_type(HelloDagsterSchema),
)
def hello_dagster() -> pd.DataFrame:
    """Return an example asset showcasing how dagster asset should be used.

    Returns
    -------
        pd.DataFrame: some example dataframe
    """
    return pd.DataFrame([dict(hello="hi", who="dagster"), dict(hello="hi", who="anyone out there?")])
