import pandera.polars as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.engines.polars_engine import DateTime


class ScreenTimeIphoneSilverSchema(pa.DataFrameModel):
    """Schema for iPhone Screen Time Silver Layer."""

    id: str = pa.Field(description="Unique hash of device_id and usage_date")
    device_id: str = pa.Field()
    usage_date: str = pa.Field(description="Date of usage in YYYY-MM-DD format")
    total_usage_seconds: int = pa.Field(ge=0)
    updated_at: DateTime = pa.Field()
    device_name: str = pa.Field(nullable=True)
    device_type: str = pa.Field()

    class Config:
        """Config for ScreenTimeIphoneSilverSchema."""

        coerce = True
        strict = True


class ScreenTimeMacSilverSchema(pa.DataFrameModel):
    """Schema for Mac Screen Time Silver Layer."""

    id: str = pa.Field(description="Unique hash of device_id, usage_date, and bundle_id")
    device_id: str = pa.Field()
    usage_date: str = pa.Field(description="Date of usage in YYYY-MM-DD format")
    bundle_id: str = pa.Field()
    app_name: str = pa.Field(nullable=True)
    total_usage_seconds: int = pa.Field(ge=0)
    updated_at: DateTime = pa.Field()
    device_name: str = pa.Field(nullable=True)
    device_type: str = pa.Field()

    class Config:
        """Config for ScreenTimeMacSilverSchema."""

        coerce = True
        strict = True


ScreenTimeIphoneSilverDagsterType = pandera_schema_to_dagster_type(ScreenTimeIphoneSilverSchema)
ScreenTimeMacSilverDagsterType = pandera_schema_to_dagster_type(ScreenTimeMacSilverSchema)
