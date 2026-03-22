"""Test smart Vision fallback logic."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, date
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock, patch
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import HistoryRequest
from vnpy_crypto_binance_datafeed.datafeed import BinanceDatafeed


def test_download_from_vision_return_type():
    """Test that _download_from_vision returns correct tuple type."""
    df = BinanceDatafeed()

    # Create a mock request
    req = HistoryRequest(
        symbol="BTCUSDT_SPOT_BINANCE",
        exchange=Exchange.GLOBAL,
        interval=Interval.MINUTE,
        start=datetime(2026, 1, 1),
        end=datetime(2026, 1, 31),
    )

    # Mock the vision client to return None (simulating 404)
    with patch.object(df, "vision_client") as mock_client:
        mock_client.download_klines.return_value = None

        result = df._download_from_vision(
            req=req,
            binance_interval="1m",
            start_time=datetime(2026, 1, 1),
            end_time=datetime(2026, 1, 31),
            interval=Interval.MINUTE,
        )

        # Verify return type is tuple
        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        assert len(result) == 2, f"Expected tuple of length 2, got {len(result)}"

        bars, missing_months = result

        # Verify types
        assert isinstance(bars, list), f"Expected list for bars, got {type(bars)}"
        assert isinstance(missing_months, list), (
            f"Expected list for missing_months, got {type(missing_months)}"
        )

        # Verify missing_months contains tuples of dates
        for item in missing_months:
            assert isinstance(item, tuple), (
                f"Expected tuple in missing_months, got {type(item)}"
            )
            assert len(item) == 2, f"Expected tuple of length 2, got {len(item)}"
            assert isinstance(item[0], date), (
                f"Expected date for start, got {type(item[0])}"
            )
            assert isinstance(item[1], date), (
                f"Expected date for end, got {type(item[1])}"
            )

        print("[PASS] test_download_from_vision_return_type")


def test_get_last_day_of_month():
    """Test _get_last_day_of_month helper method."""
    df = BinanceDatafeed()

    # Test regular month
    result = df._get_last_day_of_month(date(2026, 1, 15))
    assert result == date(2026, 1, 31), f"Expected 2026-01-31, got {result}"

    # Test February (non-leap year)
    result = df._get_last_day_of_month(date(2026, 2, 1))
    assert result == date(2026, 2, 28), f"Expected 2026-02-28, got {result}"

    # Test December (year boundary)
    result = df._get_last_day_of_month(date(2026, 12, 1))
    assert result == date(2026, 12, 31), f"Expected 2026-12-31, got {result}"

    print("[PASS] test_get_last_day_of_month")


def test_timezone_offset_in_date_extraction():
    """
    Test that timezone offset is correctly handled when extracting year/month for Vision download.

    Beijing time 2024-01-01 00:00:00 (Asia/Shanghai, UTC+8) = UTC 2023-12-31 16:00:00
    So the code should download month 2023-12 (December), NOT 2024-01 (January).

    This test verifies the bug: current code uses .date() directly without UTC conversion,
    causing it to download the wrong month when timezone offset crosses month boundary.
    """
    df = BinanceDatafeed()

    # Create timezone-aware datetime in Beijing time (Asia/Shanghai, UTC+8)
    # Beijing 2024-01-01 00:00:00 = UTC 2023-12-31 16:00:00
    beijing_time = datetime(2024, 1, 1, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    # Same date for end time (single month download)
    end_time = datetime(2024, 1, 1, 23, 59, tzinfo=ZoneInfo("Asia/Shanghai"))

    req = HistoryRequest(
        symbol="BTCUSDT_SPOT_BINANCE",
        exchange=Exchange.GLOBAL,
        interval=Interval.MINUTE,
        start=beijing_time,
        end=end_time,
    )

    with patch.object(df, "vision_client") as mock_client:
        # Mock download_klines to return None (simulating no data, but captures call)
        mock_client.download_klines.return_value = None

        # Also mock the checksum methods to avoid errors
        mock_client.get_checksum.return_value = None
        mock_client.verify_checksum.return_value = True

        df._download_from_vision(
            req=req,
            binance_interval="1m",
            start_time=beijing_time,
            end_time=end_time,
            interval=Interval.MINUTE,
        )

        # Verify that download_klines was called with UTC-correct year and month
        # Beijing 2024-01-01 00:00:00 should map to UTC 2023-12-31 16:00:00
        # Therefore the month to download is 2023-12, NOT 2024-01
        mock_client.download_klines.assert_called()

        # Get the actual call arguments (first call should be the UTC month)
        call_args = mock_client.download_klines.call_args_list[0]
        called_year = call_args[0][2]  # 3rd positional arg is year
        called_month = call_args[0][3]  # 4th positional arg is month

        # The bug: current code uses .date() directly so it would pass year=2024, month=1
        # The fix: should convert to UTC first so it passes year=2023, month=12
        assert called_year == 2023, (
            f"Expected year=2023 (UTC month for Beijing 2024-01-01 00:00:00), "
            f"but got year={called_year}. "
            f"This indicates the timezone offset bug - code uses .date() without UTC conversion."
        )
        assert called_month == 12, (
            f"Expected month=12 (December, UTC month for Beijing 2024-01-01 00:00:00), "
            f"but got month={called_month}. "
            f"This indicates the timezone offset bug - code uses .date() without UTC conversion."
        )

        print("[PASS] test_timezone_offset_in_date_extraction")


if __name__ == "__main__":
    print("Running Vision Fallback Tests...")
    print("-" * 50)

    test_download_from_vision_return_type()
    test_get_last_day_of_month()
    test_timezone_offset_in_date_extraction()

    print("-" * 50)
    print("All tests passed!")
