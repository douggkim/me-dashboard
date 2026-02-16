"""Unit tests for Spotify play history pipeline helper functions."""

import datetime

from src.assets.entertainment.spotify_play_history import (
    _extract_and_hash_play_history_id,  # noqa: PLC2701
    _parse_raw_spotify_item,  # noqa: PLC2701
)


def test_extract_and_hash_play_history_id() -> None:
    """Test generating a unique play history ID."""
    played_at = "2024-05-20T12:00:00Z"
    song_id = "test_song_id"
    expected_hash = "66e54dad71cafbf495827f0e83106529"

    result = _extract_and_hash_play_history_id(played_at, song_id)
    assert result == expected_hash
    assert isinstance(result, str)


def test_parse_raw_spotify_item() -> None:
    """Test parsing a raw Spotify play history item."""
    played_at = "2024-05-20T12:00:00.000Z"
    song_id = "test_song_id"
    mock_item = {
        "played_at": played_at,
        "track": {
            "id": song_id,
            "name": "Test Song",
            "duration_ms": 180000,
            "popularity": 85,
            "explicit": False,
            "artists": [{"name": "Test Artist"}],
            "album": {
                "name": "Test Album",
                "release_date": "2024-01-01",
                "available_markets": ["US", "GB"],
                "album_type": "album",
                "total_tracks": 10,
            },
        },
    }

    result = _parse_raw_spotify_item(mock_item)

    assert result["play_history_id"] == _extract_and_hash_play_history_id(played_at, song_id)
    assert result["played_at"] == played_at
    assert result["played_date"] == datetime.date(2024, 5, 20)
    assert result["duration_seconds"] == 180.0
    assert result["duration_ms"] == 180000
    assert result["artist_names"] == ["Test Artist"]
    assert result["song_id"] == song_id
    assert result["song_name"] == "Test Song"
    assert result["album_name"] == "Test Album"
    assert result["popularity_points_by_spotify"] == 85
    assert result["is_explicit"] is False
    assert result["song_release_date"] == "2024-01-01"
    assert result["no_of_available_markets"] == 2
    assert result["album_type"] == "album"
    assert result["total_tracks"] == 10
