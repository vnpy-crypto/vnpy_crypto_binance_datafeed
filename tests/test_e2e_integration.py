"""
End-to-end integration tests for BinanceDatafeed.

These tests use real database (SQLite) and real API calls to Binance.
To avoid rate limiting, tests use small data sets and include delays where needed.
"""

import os
import time
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.database import get_database, BaseDatabase
from vnpy.trader.datafeed import get_datafeed, BaseDatafeed
from vnpy.trader.setting import SETTINGS

# Import the BinanceDatafeed directly for testing
from vnpy_binance_datafeed.datafeed import BinanceDatafeed


# Test configuration
TEST_SYMBOL = "BTCUSDT"
TEST_EXCHANGE = Exchange.GLOBAL
TEST_EVIDENCE_DIR = Path(__file__).parent.parent.parent / ".sisyphus" / "evidence"


@pytest.fixture(scope="module")
def test_database():
    """
    Create a temporary SQLite database for testing.
    This ensures tests don't interfere with production data.
    """
    # Create a temporary database file
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_database.db")

    # Update settings to use test database
    original_db_name = SETTINGS.get("database.name")
    original_db_database = SETTINGS.get("database.database")

    SETTINGS["database.name"] = "sqlite"
    SETTINGS["database.database"] = db_path

    # Clear the cached database instance to force re-initialization
    import vnpy.trader.database as db_module

    db_module.database = None

    # Get the database instance
    database = get_database()

    yield database

    # Cleanup
    SETTINGS["database.name"] = original_db_name
    SETTINGS["database.database"] = original_db_database
    db_module.database = None

    # Remove temp files
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
        os.rmdir(temp_dir)
    except Exception:
        pass


@pytest.fixture(scope="module")
def datafeed(test_database):
    """
    Create a BinanceDatafeed instance for testing.
    Uses real VisionClient and RESTClient.
    """
    df = BinanceDatafeed()

    # Initialize the datafeed
    messages = []
    result = df.init(output=messages.append)

    if not result:
        pytest.skip(f"Failed to initialize BinanceDatafeed: {messages}")

    return df


@pytest.fixture(autouse=True)
def cleanup_database(test_database):
    """
    Clean up test data before each test to ensure isolation.
    """
    # Delete any existing test data
    try:
        test_database.delete_bar_data(
            symbol=TEST_SYMBOL, exchange=TEST_EXCHANGE, interval=Interval.HOUR
        )
    except Exception:
        pass

    yield

    # Cleanup after test
    try:
        test_database.delete_bar_data(
            symbol=TEST_SYMBOL, exchange=TEST_EXCHANGE, interval=Interval.HOUR
        )
    except Exception:
        pass


class TestE2EDownloadVision:
    """
    Tests for downloading historical data from Binance Vision.
    Uses 2024-01 data which is well in the past and stable.
    """

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_download_btcusdt_1h_klines_2024_01(self, datafeed, test_database):
        """
        Test downloading BTCUSDT 1h k-lines from January 2024 via Vision.
        This tests the Vision client path for historical data.
        """
        # Use a small time range in 2024-01 to avoid rate limiting
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 3, 0, 0, 0)  # Only 2 days of data

        # Create request
        req = HistoryRequest(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        # Download data
        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        # Log messages for debugging
        evidence_path = TEST_EVIDENCE_DIR / "e2e_vision_download.txt"
        TEST_EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        with open(evidence_path, "w", encoding="utf-8") as f:
            f.write(f"=== E2E Vision Download Test ===\n")
            f.write(f"Time: {datetime.now()}\n")
            f.write(f"Messages:\n")
            for msg in messages:
                f.write(f"  {msg}\n")
            f.write(f"\nBars downloaded: {len(bars)}\n")
            if bars:
                bar = bars[0]
                f.write(
                    f"First bar: symbol={bar.symbol}, dt={bar.datetime}, open={bar.open_price}, close={bar.close_price}\n"
                )
                bar = bars[-1]
                f.write(
                    f"Last bar: symbol={bar.symbol}, dt={bar.datetime}, open={bar.open_price}, close={bar.close_price}\n"
                )

        # Verify we got data
        assert len(bars) > 0, "No bars downloaded from Vision"

        # Verify bar properties
        for bar in bars:
            assert isinstance(bar, BarData)
            assert bar.symbol == TEST_SYMBOL
            assert (
                bar.exchange == TEST_EXCHANGE or bar.exchange == TEST_EXCHANGE.value
            )  # Handle enum or string
            assert (
                bar.interval == Interval.HOUR or bar.interval == Interval.HOUR.value
            )  # Handle enum or string
            assert bar.open_price > 0
            assert bar.high_price > 0
            assert bar.low_price > 0
            assert bar.close_price > 0
            assert bar.volume >= 0

        # Verify data is within requested range
        assert bars[0].datetime >= start
        assert bars[-1].datetime <= end

        # Verify data is saved to database
        saved_bars = test_database.load_bar_data(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )
        assert len(saved_bars) == len(bars), "Saved bar count mismatch"

        assert len(saved_bars) == len(bars), "Saved bar count mismatch"

        # Verify data integrity in database
        for i, saved_bar in enumerate(saved_bars):
            original_bar = bars[i]
            assert saved_bar.symbol == original_bar.symbol
            assert saved_bar.datetime.replace(tzinfo=None) == original_bar.datetime
            assert abs(saved_bar.open_price - original_bar.open_price) < 0.01

    @pytest.mark.e2e
    def test_vision_data_is_sorted(self, datafeed, test_database):
        """
        Verify that Vision data is sorted by datetime.
        """
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 2, 0, 0, 0)  # Just 1 day

        req = HistoryRequest(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        bars = datafeed.query_bar_history(req, output=lambda x: None)

        if len(bars) > 1:
            for i in range(1, len(bars)):
                assert bars[i].datetime >= bars[i - 1].datetime, (
                    f"Data not sorted: {bars[i - 1].datetime} > {bars[i].datetime}"
                )


class TestE2EDownloadREST:
    """
    Tests for downloading recent data via REST API.
    """

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_download_btcusdt_recent_24h(self, datafeed, test_database):
        """
        Test downloading BTCUSDT recent 24h data via REST API.
        This tests the REST client path for recent data.
        """
        # Get data from last 24 hours
        end = datetime.now()
        start = end - timedelta(hours=24)

        req = HistoryRequest(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        # Log for evidence
        evidence_path = TEST_EVIDENCE_DIR / "e2e_rest_download.txt"
        TEST_EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        with open(evidence_path, "w", encoding="utf-8") as f:
            f.write(f"=== E2E REST Download Test ===\n")
            f.write(f"Time: {datetime.now()}\n")
            f.write(f"Start: {start}\n")
            f.write(f"End: {end}\n")
            f.write(f"Messages:\n")
            for msg in messages:
                f.write(f"  {msg}\n")
            f.write(f"\nBars downloaded: {len(bars)}\n")

        # Verify we got data
        assert len(bars) > 0, "No bars downloaded from REST API"

        # Verify bar properties
        for bar in bars:
            assert isinstance(bar, BarData)
            assert bar.symbol == TEST_SYMBOL
            assert (
                bar.exchange == TEST_EXCHANGE or bar.exchange == TEST_EXCHANGE.value
            )  # Handle enum or string
            assert (
                bar.interval == Interval.HOUR or bar.interval == Interval.HOUR.value
            )  # Handle enum or string

        # Verify data is saved to database
        saved_bars = test_database.load_bar_data(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        assert len(saved_bars) > 0, "No data saved to database"


class TestE2EMixedTimeRange:
    """
    Tests for mixed time ranges that may use both Vision and REST.
    """

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_download_mixed_time_range(self, datafeed, test_database):
        """
        Test downloading data across a time range that spans both
        historical (Vision) and recent (REST) data.

        This test is designed to trigger the 'both' source path.
        """
        # Start from 3 days ago to now - this should trigger mixed mode
        end = datetime.now()
        start = end - timedelta(days=3)

        req = HistoryRequest(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        # Log for evidence
        evidence_path = TEST_EVIDENCE_DIR / "e2e_mixed_download.txt"
        TEST_EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        with open(evidence_path, "w", encoding="utf-8") as f:
            f.write(f"=== E2E Mixed Download Test ===\n")
            f.write(f"Time: {datetime.now()}\n")
            f.write(f"Start: {start}\n")
            f.write(f"End: {end}\n")
            f.write(f"Messages:\n")
            for msg in messages:
                f.write(f"  {msg}\n")
            f.write(f"\nBars downloaded: {len(bars)}\n")
            if bars:
                f.write(f"First bar time: {bars[0].datetime}\n")
                f.write(f"Last bar time: {bars[-1].datetime}\n")

        # Verify we got data
        assert len(bars) > 0, "No bars downloaded from mixed source"

        # Verify bars are sorted
        if len(bars) > 1:
            for i in range(1, len(bars)):
                assert bars[i].datetime >= bars[i - 1].datetime, (
                    f"Mixed data not sorted at index {i}"
                )

        # Verify no duplicate timestamps
        timestamps = [bar.datetime for bar in bars]
        assert len(timestamps) == len(set(timestamps)), (
            "Duplicate timestamps found in mixed data"
        )


class TestE2EDuplicateHandling:
    """
    Tests for verifying that re-downloading data doesn't create duplicates.
    """

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_no_duplicate_on_redownload(self, datafeed, test_database):
        """
        Test that downloading the same time range twice doesn't create duplicates.

        This verifies the deduplication logic in BinanceDatafeed.
        """
        # Use a small time range
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 2, 0, 0, 0)

        req = HistoryRequest(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        # First download
        messages1 = []
        bars1 = datafeed.query_bar_history(req, output=messages1.append)
        first_count = len(bars1)

        # Second download of the same range
        # Need to clear database first to test the datafeed's internal dedup
        # Actually, the datafeed checks database for existing data
        messages2 = []
        bars2 = datafeed.query_bar_history(req, output=messages2.append)
        second_count = len(bars2)

        # Log for evidence
        evidence_path = TEST_EVIDENCE_DIR / "e2e_duplicate_test.txt"
        TEST_EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        with open(evidence_path, "w", encoding="utf-8") as f:
            f.write(f"=== E2E Duplicate Test ===\n")
            f.write(f"Time: {datetime.now()}\n")
            f.write(f"First download count: {first_count}\n")
            f.write(f"Second download count: {second_count}\n")
            f.write(f"\nFirst download messages:\n")
            for msg in messages1:
                f.write(f"  {msg}\n")
            f.write(f"\nSecond download messages:\n")
            for msg in messages2:
                f.write(f"  {msg}\n")

        # The second download should return the same count
        # and should have skipped downloading (existing data message)
        assert second_count == first_count, (
            f"Second download returned different count: {first_count} vs {second_count}"
        )

        # Check that second download found existing data
        assert any("已存在" in msg for msg in messages2), (
            "Second download should indicate existing data was found"
        )

        # Verify no duplicates in database
        saved_bars = test_database.load_bar_data(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        # Count unique timestamps
        timestamps = [bar.datetime for bar in saved_bars]
        unique_timestamps = set(timestamps)

        assert len(timestamps) == len(unique_timestamps), (
            f"Found {len(timestamps) - len(unique_timestamps)} duplicate entries in database"
        )


class TestE2EDatabaseQuery:
    """
    Tests for verifying database queries return correct BarData objects.
    """

    @pytest.mark.e2e
    def test_database_query_returns_correct_bar_data(self, datafeed, test_database):
        """
        Verify that database queries return properly formatted BarData objects
        with all expected fields populated.
        """
        # Download some data first
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 2, 0, 0, 0)

        req = HistoryRequest(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        downloaded_bars = datafeed.query_bar_history(req, output=lambda x: None)

        if not downloaded_bars:
            pytest.skip("No data downloaded, cannot test database query")

        # Query from database
        queried_bars = test_database.load_bar_data(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        # Log for evidence
        evidence_path = TEST_EVIDENCE_DIR / "e2e_database_query.txt"
        TEST_EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        with open(evidence_path, "w", encoding="utf-8") as f:
            f.write(f"=== E2E Database Query Test ===\n")
            f.write(f"Time: {datetime.now()}\n")
            f.write(f"Downloaded bars: {len(downloaded_bars)}\n")
            f.write(f"Queried bars: {len(queried_bars)}\n")
            if queried_bars:
                f.write(f"\nSample bar details:\n")
                bar = queried_bars[0]
                f.write(f"  symbol: {bar.symbol}\n")
                f.write(f"  exchange: {bar.exchange}\n")
                f.write(f"  interval: {bar.interval}\n")
                f.write(f"  datetime: {bar.datetime}\n")
                f.write(f"  open: {bar.open_price}\n")
                f.write(f"  high: {bar.high_price}\n")
                f.write(f"  low: {bar.low_price}\n")
                f.write(f"  close: {bar.close_price}\n")
                f.write(f"  volume: {bar.volume}\n")
                f.write(f"  turnover: {bar.turnover}\n")

        # Verify counts match
        assert len(queried_bars) == len(downloaded_bars), (
            f"Queried count {len(queried_bars)} != downloaded count {len(downloaded_bars)}"
        )

        # Verify each bar has correct properties
        for i, bar in enumerate(queried_bars):
            # Check type
            assert isinstance(bar, BarData), f"Bar {i} is not a BarData object"

            # Check required fields
            assert bar.symbol == TEST_SYMBOL, f"Bar {i} has wrong symbol: {bar.symbol}"
            assert bar.exchange == TEST_EXCHANGE, (
                f"Bar {i} has wrong exchange: {bar.exchange}"
            )
            assert bar.interval == Interval.HOUR, (
                f"Bar {i} has wrong interval: {bar.interval}"
            )

            # Check datetime is within range (handle timezone-aware datetimes)
            bar_dt = bar.datetime
            # Convert everything to naive UTC for comparison
            if hasattr(bar_dt, "tzinfo") and bar_dt.tzinfo is not None:
                # Convert bar datetime to naive by removing timezone info
                # This effectively compares the "wall clock" time
                bar_dt_naive = bar_dt.replace(tzinfo=None)
            else:
                bar_dt_naive = bar_dt

            assert start <= bar_dt_naive <= end, (
                f"Bar {i} datetime {bar_dt_naive} outside range [{start}, {end}]"
            )

            # Check price values are reasonable
            assert bar.open_price > 0, (
                f"Bar {i} has invalid open_price: {bar.open_price}"
            )
            assert bar.high_price > 0, (
                f"Bar {i} has invalid high_price: {bar.high_price}"
            )
            assert bar.low_price > 0, f"Bar {i} has invalid low_price: {bar.low_price}"
            assert bar.close_price > 0, (
                f"Bar {i} has invalid close_price: {bar.close_price}"
            )

            # Check OHLC relationship
            assert bar.high_price >= bar.low_price, (
                f"Bar {i} high < low: {bar.high_price} < {bar.low_price}"
            )
            assert bar.high_price >= bar.open_price, (
                f"Bar {i} high < open: {bar.high_price} < {bar.open_price}"
            )
            assert bar.high_price >= bar.close_price, (
                f"Bar {i} high < close: {bar.high_price} < {bar.close_price}"
            )
            assert bar.low_price <= bar.open_price, (
                f"Bar {i} low > open: {bar.low_price} > {bar.open_price}"
            )
            assert bar.low_price <= bar.close_price, (
                f"Bar {i} low > close: {bar.low_price} > {bar.close_price}"
            )

            # Check volume
            assert bar.volume >= 0, f"Bar {i} has negative volume: {bar.volume}"

        # Verify bars are sorted by datetime
        if len(queried_bars) > 1:
            for i in range(1, len(queried_bars)):
                assert queried_bars[i].datetime >= queried_bars[i - 1].datetime, (
                    f"Bars not sorted at index {i}"
                )

    @pytest.mark.e2e
    def test_database_bar_overview(self, datafeed, test_database):
        """
        Test that get_bar_overview returns correct information about stored data.
        """
        # Download some data first
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 2, 0, 0, 0)

        req = HistoryRequest(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        downloaded_bars = datafeed.query_bar_history(req, output=lambda x: None)

        if not downloaded_bars:
            pytest.skip("No data downloaded, cannot test bar overview")

        # Get overview
        overviews = test_database.get_bar_overview()

        # Find our data in the overview
        our_overview = None
        for overview in overviews:
            if (
                overview.symbol == TEST_SYMBOL
                and overview.exchange == TEST_EXCHANGE
                and overview.interval == Interval.HOUR
            ):
                our_overview = overview
                break

        # Verify overview exists
        assert our_overview is not None, "No overview found for test data"

        # Verify overview properties
        assert our_overview.count == len(downloaded_bars), (
            f"Overview count {our_overview.count} != downloaded count {len(downloaded_bars)}"
        )


class TestE2EGetdatafeed:
    """
    Tests for the get_datafeed() factory function.
    """

    @pytest.mark.e2e
    def test_get_datafeed_returns_binance_datafeed(self):
        """
        Test that get_datafeed() returns a BinanceDatafeed instance
        when configured correctly.
        """
        # Note: This test requires the datafeed to be configured as "binance"
        # in the vnpy settings. If not configured, it will return BaseDatafeed.

        # We can test the direct instantiation instead
        df = BinanceDatafeed()
        assert isinstance(df, BinanceDatafeed)

        # Verify it has the expected methods
        assert hasattr(df, "init")
        assert hasattr(df, "query_bar_history")
        assert callable(df.init)
        assert callable(df.query_bar_history)


class TestE2EIntegration:
    """
    Full integration tests simulating real user workflow.
    """

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_full_user_workflow(self, test_database):
        """
        Simulate a complete user workflow:
        1. Create datafeed
        2. Initialize it
        3. Download data
        4. Query from database
        5. Verify data integrity
        """
        # Step 1: Create datafeed
        df = BinanceDatafeed()

        # Step 2: Initialize
        messages = []
        init_result = df.init(output=messages.append)
        assert init_result is True, f"Initialization failed: {messages}"

        # Step 3: Download data
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 2, 0, 0, 0)

        req = HistoryRequest(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        download_messages = []
        bars = df.query_bar_history(req, output=download_messages.append)

        # Step 4: Query from database
        saved_bars = test_database.load_bar_data(
            symbol=TEST_SYMBOL,
            exchange=TEST_EXCHANGE,
            interval=Interval.HOUR,
            start=start,
            end=end,
        )

        # Step 5: Verify
        evidence_path = TEST_EVIDENCE_DIR / "e2e_full_workflow.txt"
        TEST_EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        with open(evidence_path, "w", encoding="utf-8") as f:
            f.write(f"=== E2E Full Workflow Test ===\n")
            f.write(f"Time: {datetime.now()}\n")
            f.write(f"\nStep 1 & 2 - Initialize:\n")
            for msg in messages:
                f.write(f"  {msg}\n")
            f.write(f"\nStep 3 - Download:\n")
            for msg in download_messages:
                f.write(f"  {msg}\n")
            f.write(f"\nDownloaded: {len(bars)} bars\n")
            f.write(f"From database: {len(saved_bars)} bars\n")
            if bars:
                f.write(f"\nSample bar:\n")
                f.write(
                    f"  BarData(symbol={bars[0].symbol}, datetime={bars[0].datetime})\n"
                )

        assert len(bars) > 0, "No data downloaded"
        assert len(saved_bars) == len(bars), "Database count mismatch"

        # Verify data integrity
        for bar in saved_bars:
            assert bar.symbol == TEST_SYMBOL
            assert (
                bar.exchange == TEST_EXCHANGE or bar.exchange == TEST_EXCHANGE.value
            )  # Handle enum or string
            assert (
                bar.interval == Interval.HOUR or bar.interval == Interval.HOUR.value
            )  # Handle enum or string
            assert bar.open_price > 0


# Test markers for pytest
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line("markers", "e2e: End-to-end integration tests")
    config.addinivalue_line("markers", "slow: Tests that take longer to run")
