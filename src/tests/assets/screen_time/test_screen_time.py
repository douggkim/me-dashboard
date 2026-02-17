"""Unit tests for Screen Time assets."""

from datetime import UTC
from unittest.mock import MagicMock, patch

import dagster as dg
import polars as pl
import pytest

from src.assets.screen_time.screen_time_assets import (
    _process_iphone_records,  # noqa: PLC2701
    _process_mac_records,  # noqa: PLC2701
    parse_updated_at,
    screen_time_iphone,
    screen_time_mac,
)


@pytest.fixture
def mock_get_storage_path() -> MagicMock:
    """
    Mock get_storage_path utility.

    Yields
    ------
    MagicMock
        The mocked get_storage_path function.
    """
    with patch("src.assets.screen_time.screen_time_assets.get_storage_path") as mock:
        yield mock


@pytest.fixture
def mock_load_json() -> MagicMock:
    """
    Mock load_bronze_json_files utility.

    Yields
    ------
    MagicMock
        The mocked load_bronze_json_files function.
    """
    with patch("src.assets.screen_time.screen_time_assets.load_bronze_json_files") as mock:
        yield mock


def test_parse_updated_at() -> None:
    """Test updated_at string parsing logic."""
    # Test typical Mac format
    ts = "Feb 16, 2026 at 6:57 PM"
    dt_val = parse_updated_at(ts)
    assert dt_val.year == 2026
    assert dt_val.month == 2
    assert dt_val.day == 16
    assert dt_val.hour == 18
    assert dt_val.minute == 57
    assert dt_val.tzinfo == UTC

    # Test ISO format
    ts_iso = "2026-02-16T18:57:00+00:00"
    dt_iso = parse_updated_at(ts_iso)
    assert dt_iso.year == 2026
    assert dt_iso.month == 2


def test_process_iphone_records() -> None:
    """Test _process_iphone_records helper function logic."""
    context = MagicMock()
    raw_data = [
        {
            "device_id": "iphone1",
            "updated_at": "Feb 16, 2026 at 6:00 PM",
            "usage_seconds": "3600",
            "device_name": "My iPhone",
        },
        # Missing usage_date, should be derived from updated_at
    ]

    results = _process_iphone_records(context, raw_data)

    assert len(results) == 1
    assert results[0]["usage_date"] == "2026-02-16"
    assert results[0]["total_usage_seconds"] == 3600
    assert results[0]["device_type"] == "iphone"
    assert results[0]["id"] is not None


def test_process_mac_records() -> None:
    """Test _process_mac_records helper function logic."""
    context = MagicMock()
    raw_data = [
        {
            "device_id": "mac1",
            "usage_date": "2026-02-16",
            "updated_at": "Feb 16, 2026 at 8:00 PM",
            "device_name": "My Mac",
            "data": [{"bundle_id": "com.apple.Safari", "total_usage_seconds": 120}],
        },
        # Duplicate record with OLDER timestamp (should be ignored)
        {
            "device_id": "mac1",
            "usage_date": "2026-02-16",
            "updated_at": "Feb 16, 2026 at 7:00 PM",
            "device_name": "My Mac",
            "data": [{"bundle_id": "com.apple.Safari", "total_usage_seconds": 999}],
        },
    ]

    results = _process_mac_records(context, raw_data)

    assert len(results) == 1
    assert results[0]["total_usage_seconds"] == 120  # Should keep the one from 8:00 PM
    assert results[0]["device_type"] == "mac"


def test_screen_time_iphone(mock_get_storage_path: MagicMock, mock_load_json: MagicMock) -> None:
    """Test iPhone screen time asset processing and deduplication."""
    mock_get_storage_path.return_value = "dummy/path"

    # Mock data: 2 records for same device/date, one newer
    mock_load_json.return_value = [
        {
            "device_id": "iphone1",
            "updated_at": "Feb 16, 2026 at 6:00 PM",
            "usage_seconds": "3600",
            "device_name": "My iPhone",
        },
        {
            "device_id": "iphone1",
            "updated_at": "Feb 16, 2026 at 8:00 PM",  # Newer
            "usage_seconds": "7200",
            "device_name": "My iPhone",
        },
    ]

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        iphone_df = screen_time_iphone(context=context)

        # Check path construction
        # Expected path: dummy/path/2026_02_16
        mock_load_json.assert_called_with(context, "dummy/path/2026_02_16")

    # Should only have 1 row (deduplicated)
    assert iphone_df.height == 1
    row = iphone_df.to_dicts()[0]
    assert row["total_usage_seconds"] == 7200
    assert row["device_type"] == "iphone"
    assert row["usage_date"] == "2026-02-16"  # derived from updated_at


def test_screen_time_mac(mock_get_storage_path: MagicMock, mock_load_json: MagicMock) -> None:
    """Test Mac screen time asset processing and deduplication."""
    mock_get_storage_path.return_value = "dummy/path"

    # Mock data: 1 report with multiple apps
    mock_load_json.return_value = [
        {
            "device_id": "mac1",
            "usage_date": "2026-02-16",
            "updated_at": "Feb 16, 2026 at 8:00 PM",
            "device_name": "My Mac",
            "data": [
                {"bundle_id": "com.apple.Safari", "total_usage_seconds": 120},
                {"bundle_id": "com.google.Chrome", "total_usage_seconds": 300},
            ],
        }
    ]

    with dg.build_asset_context(partition_key="2026-02-16") as context:
        mac_df = screen_time_mac(context=context)

        # Check path construction
        mock_load_json.assert_called_with(context, "dummy/path/2026_02_16")

    assert mac_df.height == 2

    safari = mac_df.filter(pl.col("bundle_id") == "com.apple.Safari").to_dicts()[0]
    assert safari["total_usage_seconds"] == 120
    assert safari["app_name"] == "Safari"

    chrome = mac_df.filter(pl.col("bundle_id") == "com.google.Chrome").to_dicts()[0]
    assert chrome["total_usage_seconds"] == 300
    assert chrome["app_name"] == "Chrome"
