"""Example schema check for hello_dagster example."""

import pandera as pa
from pandera.typing import Series


class HelloDagsterSchema(pa.DataFrameModel):
    """Pandera Schema for the example/hello_dagster asset."""

    hello: Series[str] = pa.Field(isin=["hello", "hi"], coerce=True, description="Greeting message")
    who: Series[str] = pa.Field(coerce=True, description="Target of the greeting")
