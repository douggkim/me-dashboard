"""Pandera schemas for Health data validation."""

import pandera.polars as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.engines.polars_engine import DateTime


class HealthSilverSchema(pa.DataFrameModel):
    """Schema for Health Silver Layer."""

    health_activity_id: str = pa.Field(description="Unique hash of date_time_pst")
    date_time_pst: DateTime = pa.Field(description="Date and time of health metric")
    activity_date: str = pa.Field(description="Date of activity in YYYY-MM-DD format (PST)")

    active_energy_kcal: float = pa.Field(nullable=True)
    apple_exercise_time_min: float = pa.Field(nullable=True)
    apple_move_time_min: float = pa.Field(nullable=True)
    apple_stand_hour_hours: float = pa.Field(nullable=True)
    apple_stand_time_min: float = pa.Field(nullable=True)
    body_temperature_f: float = pa.Field(nullable=True)
    breathing_disturbances_count: float = pa.Field(nullable=True)
    environmental_audio_exposure_dbaspl: float = pa.Field(nullable=True)
    flights_climbed_count: float = pa.Field(nullable=True)
    headphone_audio_exposure_dbaspl: float = pa.Field(nullable=True)
    heart_rate_min_bpm: float = pa.Field(nullable=True)
    heart_rate_max_bpm: float = pa.Field(nullable=True)
    heart_rate_avg_bpm: float = pa.Field(nullable=True)
    heart_rate_variability_ms: float = pa.Field(nullable=True)
    number_of_times_fallen_falls: float = pa.Field(nullable=True)
    physical_effort_met: float = pa.Field(nullable=True)
    respiratory_rate_count_min: float = pa.Field(nullable=True)
    resting_energy_kcal: float = pa.Field(nullable=True)
    resting_heart_rate_bpm: float = pa.Field(nullable=True)
    running_power_watts: float = pa.Field(nullable=True)
    running_speed_mi_hr: float = pa.Field(nullable=True)
    step_count_steps: float = pa.Field(nullable=True)
    walking_speed_mi_hr: float = pa.Field(nullable=True)
    walking_step_length_in: float = pa.Field(nullable=True)

    class Config:
        """Config for HealthSilverSchema."""

        coerce = True
        strict = False


HealthSilverDagsterType = pandera_schema_to_dagster_type(HealthSilverSchema)
