"""Unit tests for Spotify artist assets."""

import datetime
from unittest.mock import MagicMock, patch

import dagster as dg
import polars as pl
import pytest

from src.assets.entertainment.spotify_artist_genre_mappings import (
    _apply_scd2_logic,  # noqa: PLC2701
    _extract_unique_artist_ids,  # noqa: PLC2701
    _fetch_artist_details,  # noqa: PLC2701
    _parse_artist_data,  # noqa: PLC2701
    spotify_artist_genre_mapping_bronze,
    spotify_artist_genre_mapping_silver,
)


@pytest.fixture
def mock_spotify_play_history() -> list[dict]:
    """Provide a mock for play history bronze data.

    Returns
    -------
    list[dict]
        A mock block of bronze play history data.
    """
    return [
        {
            "items": [
                {
                    "track": {
                        "artists": [
                            {"id": "artist_1"},
                            {"id": "artist_2"},
                        ]
                    }
                },
                {
                    "track": {
                        "artists": [
                            {"id": "artist_1"},
                            {"id": "artist_3"},
                        ]
                    }
                },
            ]
        }
    ]


@pytest.fixture
def mock_spotify_raw_artists() -> list[dict]:
    """Provide a mock for raw artist data from the API.

    Returns
    -------
    list[dict]
        A mock block of raw artist details.
    """
    return [
        {"id": "artist_1", "name": "Artist One", "genres": ["pop", "rock"]},
        {"id": "artist_2", "name": "Artist Two", "genres": ["jazz"]},
        {"id": "artist_3", "name": "Artist Three", "genres": []},
    ]


def test_extract_unique_artist_ids(mock_spotify_play_history: list[dict]) -> None:
    """Test extracting unique artist IDs from play history."""
    result = _extract_unique_artist_ids(mock_spotify_play_history)
    assert set(result) == {"artist_1", "artist_2", "artist_3"}


def test_fetch_artist_details() -> None:
    """Test fetching artist details from the API one by one."""
    mock_resource = MagicMock()
    mock_resource.call_api.side_effect = [
        {"id": "artist_1", "name": "A1"},
        {"id": "artist_2", "name": "A2"},
    ]
    context = dg.build_asset_context()

    result = _fetch_artist_details(["artist_1", "artist_2"], mock_resource, context)

    assert len(result) == 2
    assert mock_resource.call_api.call_count == 2
    mock_resource.call_api.assert_any_call(endpoint="artists/artist_1")
    mock_resource.call_api.assert_any_call(endpoint="artists/artist_2")


def test_parse_artist_data(mock_spotify_raw_artists: list[dict]) -> None:
    """Test parsing raw artist data into a DataFrame."""
    result_df = _parse_artist_data(mock_spotify_raw_artists)

    assert result_df.height == 3
    assert "artist_id" in result_df.columns
    assert "name" in result_df.columns
    assert "genres" in result_df.columns


def test_apply_scd2_logic_initial_load(mock_spotify_raw_artists: list[dict]) -> None:
    """Test SCD2 logic when no existing data is present."""
    incoming_df = _parse_artist_data(mock_spotify_raw_artists)
    existing_df = pl.DataFrame(
        schema={
            "artist_id": pl.Utf8,
            "name": pl.Utf8,
            "genres": pl.List(pl.Utf8),
            "start_date_scd": pl.Date,
            "end_date_scd": pl.Date,
        }
    )

    current_date = datetime.date(2026, 3, 1)
    result_df = _apply_scd2_logic(incoming_df, existing_df, current_date)

    assert result_df.height == 3
    for row in result_df.to_dicts():
        assert row["start_date_scd"] == datetime.date(1990, 1, 1)
        assert row["end_date_scd"] == datetime.date(9999, 12, 31)


def test_apply_scd2_logic_updates() -> None:
    """Test SCD2 logic with unchanged, new, and updated records."""
    schema = {
        "artist_id": pl.Utf8,
        "name": pl.Utf8,
        "genres": pl.List(pl.Utf8),
        "start_date_scd": pl.Date,
        "end_date_scd": pl.Date,
    }
    existing_df = pl.DataFrame(
        [
            ("artist_1", "Artist One", ["pop", "rock"], datetime.date(1990, 1, 1), datetime.date(9999, 12, 31)),
            ("artist_2", "Old Artist Two", ["blues"], datetime.date(1990, 1, 1), datetime.date(9999, 12, 31)),
            ("artist_2", "Very Old Artist Two", ["nothing"], datetime.date(1980, 1, 1), datetime.date(1990, 1, 1)),
        ],
        schema=schema,
        orient="row",
    )

    incoming_df = pl.DataFrame(
        [
            ("artist_1", "Artist One", ["pop", "rock"]),
            ("artist_2", "Artist Two", ["jazz"]),
            ("artist_3", "Artist Three", []),
        ],
        schema={"artist_id": pl.Utf8, "name": pl.Utf8, "genres": pl.List(pl.Utf8)},
        orient="row",
    )

    current_date = datetime.date(2026, 3, 1)
    result_df = _apply_scd2_logic(incoming_df, existing_df, current_date)

    assert result_df.height == 3
    result_dicts = result_df.to_dicts()

    artist_2_updates = [r for r in result_dicts if r["artist_id"] == "artist_2"]
    assert len(artist_2_updates) == 2

    closed_record = next(r for r in artist_2_updates if r["name"] == "Old Artist Two")
    assert closed_record["end_date_scd"] == current_date

    new_version_record = next(r for r in artist_2_updates if r["name"] == "Artist Two")
    assert new_version_record["start_date_scd"] == current_date
    assert new_version_record["end_date_scd"] == datetime.date(9999, 12, 31)

    artist_3_update = next(r for r in result_dicts if r["artist_id"] == "artist_3")
    assert artist_3_update["start_date_scd"] == datetime.date(1990, 1, 1)
    assert artist_3_update["end_date_scd"] == datetime.date(9999, 12, 31)


@patch("src.assets.entertainment.spotify_artist_genre_mappings._fetch_artist_details")
@patch("src.assets.entertainment.spotify_artist_genre_mappings._extract_unique_artist_ids")
def test_spotify_artist_genre_mapping_bronze(mock_extract: MagicMock, mock_fetch: MagicMock) -> None:
    """Test full bronze asset execution."""
    mock_extract.return_value = ["artist_1"]
    mock_fetch.return_value = [{"id": "artist_1", "name": "A1"}]

    context = dg.build_asset_context()
    mock_resource = MagicMock()

    result = spotify_artist_genre_mapping_bronze(context, [], mock_resource)
    assert result == [{"id": "artist_1", "name": "A1"}]


def test_spotify_artist_genre_mapping_silver_execution(mock_spotify_raw_artists: list[dict]) -> None:
    """Test full silver asset execution."""
    context = dg.build_asset_context()

    # Mock the data loader resource
    mock_data_loader = MagicMock()
    mock_data_loader.load_data.side_effect = Exception("Table not found")

    result_df = spotify_artist_genre_mapping_silver(context, mock_spotify_raw_artists, mock_data_loader)
    assert result_df.height == 3
