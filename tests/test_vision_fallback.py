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
        So the code should download month 2023-12 ( NOT 2024-01.

        This test verifies the bug: current code uses .date() directly without UTC conversion,
        causing it to download the wrong month when timezone offset crosses month boundary.
    """
    df = BinanceDatafeed()

    # Create timezone-aware datetime in Beijing time (Asia/Shanghai, UTC+8)
    # Beijing 2024-01-01 00:00:00 = UTC 2023-12-31 16:00:00
    beijing_time = datetime(2024, 1, 1, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    end_time = datetime(2024, 1, 1, 23, 59, tzinfo=ZoneInfo("Asia/Shanghai"))

    # Same date for end time (single month download)
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
        df._download_from_vision(
            req=req,
            binance_interval="1m",
            start_time=beijing_time,
            end_time=end_time,
            interval=Interval.MINUTE,
        )

        # Verify that download_klines was called with UTC-correct year and month
        # Beijing 2024-01-01 00:00:00 should map to UTC 2023-12-31 16:00:00
        # Therefore the month to download is 2023-12, NOT 2024-01.
        call_args = mock_client.download_klines.call_args_list[0]
        called_year = call_args[1].get("year") or call_args[0][2]
        called_month = call_args[1].get("month") or call_args[0][3]
        assert called_year == 2023, (
            f"Expected year=2023 (UTC month for Beijing 2024-01-01 00:00:00), got year={called_year}. "
        )
        assert called_month == 12, (
            f"Expected month=12 (December, UTC month for Beijing 2024-01-01 00:00:00). got month={called_month}. "
        )
    print("[PASS] test_timezone_offset_in_date_extraction")


def test_both_source_uses_smart_fallback():
    """
    Test that when source == "both", REST API only downloads missing months,
    not re-downloading months already downloaded by Vision.

    Scenario:
    - Request data from 2026-01-01 to 2026-03-31
    - _determine_data_source returns "both" (crosses history/recent boundary)
    - Vision returns:
      - bars_vision: data for 2026-01 and 2026-02
      - missing_months: [(2026-03-01, 2026-03-31)]  # Only March is missing
    - Expected behavior:
      - _download_from_rest is called ONCE to download March data
      - _download_from_rest is NOT called for Jan/Feb data
    """
    df = BinanceDatafeed()

    # Create request spanning 3 months
    req = HistoryRequest(
        symbol="BTCUSDT_SPOT_BINANCE",
        exchange=Exchange.GLOBAL,
        interval=Interval.MINUTE,
        start=datetime(2026, 1, 1),
        end=datetime(2026, 3, 31, 23, 59),
    )

    # Mock _determine_data_source to return "both"
    with patch.object(df, "_determine_data_source", return_value="both"):
        # Mock _download_from_vision to return partial data
        # Returns: (bars_vision, missing_months)
        mock_vision_bars = [
            MagicMock(datetime=datetime(2026, 1, 1, 0, 0)),
            MagicMock(datetime=datetime(2026, 1, 1, 0, 1)),
            MagicMock(datetime=datetime(2026, 2, 1, 0, 0)),
            MagicMock(datetime=datetime(2026, 2, 1, 0, 1)),
        ]
        # Only March is missing
        missing_months = [(date(2026, 3, 1), date(2026, 3, 31))]

        with patch.object(
            df, "_download_from_vision", return_value=(mock_vision_bars, missing_months)
        ):
            # mock _download_from_rest to track calls
            with patch.object(df, "_download_from_rest") as mock_rest:
                mock_rest.return_value = [
                    MagicMock(datetime=datetime(2026, 3, 1, 0, 0)),
                    MagicMock(datetime=datetime(2026, 3, 1, 0, 1)),
                ]

                # mock database to return empty (no existing data)
                with patch.object(df, "database") as mock_db:
                    mock_db.load_bar_data.return_value = []

                    # Execute query_bar_history
                    result = df.query_bar_history(req, output=print)

                    # Verify _download_from_vision was called once
                    # Note: The patch replaces df._download_from_vision, so we check via the mock context
                    # The mock is still active here

                    # Verify _download_from_rest was called ONCE (only for missing March)
                    assert mock_rest.call_count == 1, (
                        f"Expected REST to be called once, but was called {mock_rest.call_count} times"
                    )

                    # Verify REST was called with March dates only
                    call_args = mock_rest.call_args
                    # call_args is (args, kwargs)
                    # args[0] is位置参数
                    actual_start = call_args[0][2]  # start_time parameter
                    actual_end = call_args[0][3]  # end_time parameter
                    assert actual_start is not None, (
                        f"start_time is None, got {actual_start}"
                    )
                    assert actual_end is not None, f"end_time is None, got {actual_end}"
                    # Verify month correct (3月)
                    assert actual_start.month == 3, (
                        f"Expected REST to start in March, got month {actual_start.month}"
                    )
                    assert actual_end.month == 3, (
                        f"Expected REST to end in March, got month {actual_end.month}"
                    )

                    # Verify result contains both vision and rest bars
                    assert len(result) >= 4, (
                        f"Expected at least 4 bars (2 vision + 2 rest), got {len(result)}"
                    )

    print("[PASS] test_both_source_uses_smart_fallback")


def test_both_source_no_duplicate_vision_months():
    """
    Test that REST API is never called for months that Vision already downloaded.

    This specifically tests that the smart fallback logic prevents redundant downloads
    when Vision successfully provides data for certain months.
    """
    df = BinanceDatafeed()

    # Request data for Jan-Feb 2026 (both historical, so source="vision" or "both")
    req = HistoryRequest(
        symbol="BTCUSDT_SPOT_BINANCE",
        exchange=Exchange.GLOBAL,
        interval=Interval.MINUTE,
        start=datetime(2026, 1, 1),
        end=datetime(2026, 2, 28),
    )

    with patch.object(df, "_determine_data_source", return_value="both"):
        # Vision returns all data (no missing months)
        mock_vision_bars = [
            MagicMock(datetime=datetime(2026, 1, 15, 12, 0)),
            MagicMock(datetime=datetime(2026, 2, 15, 12, 0)),
        ]
        missing_months = []  # No missing months!

        with patch.object(
            df, "_download_from_vision", return_value=(mock_vision_bars, missing_months)
        ):
            with patch.object(df, "_download_from_rest") as mock_rest:
                mock_rest.return_value = []

                with patch.object(df, "database") as mock_db:
                    mock_db.load_bar_data.return_value = []

                    result = df.query_bar_history(req, output=print)

                    # REST should NOT be called at all since no missing months
                    assert mock_rest.call_count == 0, (
                        f"Expected REST to NOT be called, but was called {mock_rest.call_count} times"
                    )

                    # Result should only contain vision bars
                    assert len(result) == 2, (
                        f"Expected 2 vision bars, got {len(result)}"
                    )

    print("[PASS] test_both_source_no_duplicate_vision_months")


if __name__ == "__main__":
    print("Running Vision Fallback Tests...")
    print("-" * 50)

    test_download_from_vision_return_type()
    test_get_last_day_of_month()
    test_timezone_offset_in_date_extraction()
    test_both_source_uses_smart_fallback()
    test_both_source_no_duplicate_vision_months()

    print("-" * 50)
    print("All tests passed!")
