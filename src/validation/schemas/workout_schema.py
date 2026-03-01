"""Pandera schemas for Workout data validation."""

import pandera.polars as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.engines.polars_engine import DateTime


class WorkoutSilverSchema(pa.DataFrameModel):
    """Schema for Workout Silver Layer."""

    workout_activity_id: str = pa.Field(description="Unique hash of start_pst")
    type: str = pa.Field(description="Type of workout")
    start_pst: DateTime = pa.Field(description="Start time of workout")
    end_pst: DateTime = pa.Field(description="End time of workout")
    activity_date: str = pa.Field(description="Date of activity in YYYY-MM-DD format (PST)")
    duration: str = pa.Field(nullable=True)

    total_energy_kcal: float = pa.Field(nullable=True)
    active_energy_kcal: float = pa.Field(nullable=True)
    max_heart_rate_bpm: float = pa.Field(nullable=True)
    avg_heart_rate_bpm: float = pa.Field(nullable=True)
    distance_km: float = pa.Field(nullable=True)
    avg_speed_km_hr: float = pa.Field(nullable=True)
    step_count_count: float = pa.Field(nullable=True)
    step_cadence_spm: float = pa.Field(nullable=True)
    swimming_stroke_count_count: float = pa.Field(nullable=True)
    swim_stoke_cadence_spm: float = pa.Field(nullable=True)
    flights_climbed_count: float = pa.Field(nullable=True)
    elevation_ascended_m: float = pa.Field(nullable=True)
    elevation_descended_m: float = pa.Field(nullable=True)

    class Config:
        """Config for WorkoutSilverSchema."""

        coerce = True
        strict = False


WorkoutSilverDagsterType = pandera_schema_to_dagster_type(WorkoutSilverSchema)
