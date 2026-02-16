"""Schema definitions for Spotify data validation."""

import dagster_pandera
import pandera.polars as pa
import polars as pl


class SpotifySilverSchema(pa.DataFrameModel):
    """Schema for spotify_play_history_silver asset."""

    play_history_id: str = pa.Field(nullable=False, description="Unique identifier for the play history record.")
    played_at: pl.Datetime = pa.Field(nullable=False, description="Timestamp when the song was played.")
    played_date: pl.Date = pa.Field(nullable=False, description="Date when the song was played.")
    duration_seconds: float = pa.Field(nullable=False, description="Duration of the song in seconds.")
    duration_ms: int = pa.Field(nullable=False, description="Duration of the song in milliseconds.")
    artist_names: pl.List = pa.Field(description="List of artist names.")
    song_id: str = pa.Field(nullable=False, description="Spotify ID of the song.")
    song_name: str = pa.Field(nullable=False, description="Name of the song.")
    album_name: str = pa.Field(nullable=False, description="Name of the album.")
    popularity_points_by_spotify: int = pa.Field(
        nullable=False, ge=0, le=100, description="Popularity points assigned by Spotify."
    )
    is_explicit: bool = pa.Field(nullable=False, description="Whether the song contains explicit content.")
    song_release_date: str = pa.Field(nullable=False, description="Release date of the song.")
    no_of_available_markets: int = pa.Field(
        nullable=False, description="Number of markets where the song is available."
    )
    album_type: str = pa.Field(nullable=False, description="Type of the album (e.g., album, single, compilation).")
    total_tracks: int = pa.Field(nullable=False, ge=1, description="Total number of tracks in the album.")

    class Config:
        """Pandera configuration."""

        strict = True  # Reject unknown columns
        description = "Schema for processed Spotify play history data."


spotify_silver_dagster_type = dagster_pandera.pandera_schema_to_dagster_type(SpotifySilverSchema)
