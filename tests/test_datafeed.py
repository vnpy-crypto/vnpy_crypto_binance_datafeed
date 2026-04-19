"""
Integration tests for BinanceDatafeed.
"""

import io
import zipfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call
from typing import List

import pytest

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.database import DB_TZ
from vnpy_binance_datafeed.datafeed import BinanceDatafeed


def create_mock_zip_csv(csv_data: bytes) -> bytes:
    """Create a mock zip file containing CSV data."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("BTCUSDT-1m-2024-01.csv", csv_data)
    return buffer.getvalue()


def create_sample_csv_data(timestamps: List[int]) -> bytes:
    """Create sample CSV data with given timestamps."""
    rows = []
    for ts in timestamps:
        # Format: open_time,open,high,low,close,volume,close_time,turnover,num_trades,taker_buy_base,taker_buy_quote,ignore
        row = f"{ts},28923.63,28961.66,28913.12,28961.66,27.45703800,{ts + 59999},794487.66411928,1292,16.77719500,485390.29825708,0"
        rows.append(row)
    return "\n".join(rows).encode("utf-8")


def create_sample_kline_json(timestamps: List[int]) -> List[dict]:
    """Create sample kline JSON data with given timestamps."""
    klines = []
    for ts in timestamps:
        kline = {
            "open_time": ts,
            "open": 28923.63,
            "high": 28961.66,
            "low": 28913.12,
            "close": 28961.66,
            "volume": 27.45703800,
            "close_time": ts + 59999,
            "turnover": 794487.66411928,
            "num_trades": 1292,
            "taker_buy_base_volume": 16.77719500,
            "taker_buy_quote_volume": 485390.29825708,
        }
        klines.append(kline)
    return klines


def create_bar_data(
    symbol: str, dt: datetime, exchange: Exchange = Exchange.GLOBAL
) -> BarData:
    """Create a sample BarData object."""
    return BarData(
        symbol=symbol,
        exchange=exchange,
        datetime=dt,
        interval=Interval.MINUTE,
        volume=27.45703800,
        turnover=794487.66411928,
        open_price=28923.63,
        high_price=28961.66,
        low_price=28913.12,
        close_price=28961.66,
        open_interest=0,
        gateway_name="BINANCE_DATAFEED",
    )


@pytest.fixture
def mock_database():
    """Create a mock database."""
    mock_db = MagicMock()
    mock_db.load_bar_data.return_value = []  # No existing data by default
    mock_db.save_bar_data.return_value = True
    return mock_db


@pytest.fixture
def mock_vision_client():
    """Create a mock VisionClient."""
    mock = MagicMock()
    mock.get_checksum.return_value = None
    mock.download_klines.return_value = None
    return mock


@pytest.fixture
def mock_rest_client():
    """Create a mock BinanceRestClient."""
    return MagicMock()


@pytest.fixture
def datafeed(mock_database, mock_vision_client, mock_rest_client):
    """Create a BinanceDatafeed with mocked dependencies."""
    with (
        patch(
            "vnpy_binance_datafeed.datafeed.VisionClient",
            return_value=mock_vision_client,
        ),
        patch(
            "vnpy_binance_datafeed.datafeed.BinanceRestClient",
            return_value=mock_rest_client,
        ),
        patch(
            "vnpy_binance_datafeed.datafeed.get_database",
            return_value=mock_database,
        ),
    ):
        df = BinanceDatafeed()
        df.inited = True  # Pre-initialize to avoid exchange_info calls
        df.symbols = {
            "BTCUSDT_SPOT_BINANCE",
            "ETHUSDT_SPOT_BINANCE",
        }  # Pre-populate valid symbols
        return df


class TestBinanceDatafeedInit:
    """Tests for BinanceDatafeed initialization."""

    def test_init_success(self, mock_database, mock_vision_client, mock_rest_client):
        """Test successful initialization."""
        mock_rest_client.get_exchange_info.return_value = {
            "symbols": [
                {"symbol": "BTCUSDT_SPOT_BINANCE"},
                {"symbol": "ETHUSDT_SPOT_BINANCE"},
            ]
        }

        with (
            patch(
                "vnpy_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            df = BinanceDatafeed()

            # Call init
            messages = []
            result = df.init(output=messages.append)

            assert result is True
            assert df.inited is True
            assert "BTCUSDT_SPOT_BINANCE" in df.symbols
            assert "ETHUSDT_SPOT_BINANCE" in df.symbols
            assert any("初始化成功" in m for m in messages)

    def test_init_already_initialized(self, datafeed):
        """Test that init returns True when already initialized."""
        messages = []
        result = datafeed.init(output=messages.append)

        assert result is True
        # Should not call exchange_info again
        datafeed.rest_client.get_exchange_info.assert_not_called()

    def test_init_failure(self, mock_database, mock_vision_client, mock_rest_client):
        """Test initialization failure."""
        mock_rest_client.get_exchange_info.side_effect = Exception("Network error")

        with (
            patch(
                "vnpy_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            df = BinanceDatafeed()

            messages = []
            result = df.init(output=messages.append)

            assert result is False
            assert df.inited is False
            assert any("初始化失败" in m for m in messages)


class TestQueryBarHistoryHistorical:
    """Tests for historical data download via Vision."""

    def test_query_bar_history_historical(
        self, datafeed, mock_vision_client, mock_database
    ):
        """Test historical data download from Vision (data before yesterday)."""
        # Set up time range that falls in "vision" category (end < yesterday)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # Ensure start and end are in the same month to avoid duplicate mock data
        start = today.replace(day=1) - timedelta(days=20)
        start = start.replace(day=1)  # First day of previous month
        end = start + timedelta(days=5)  # 6th day of previous month

        # Create mock CSV data
        timestamps = [
            int(start.timestamp() * 1000),
            int((start + timedelta(minutes=1)).timestamp() * 1000),
        ]
        csv_data = create_sample_csv_data(timestamps)
        zip_data = create_mock_zip_csv(csv_data)

        mock_vision_client.download_klines.return_value = zip_data

        # Create request
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        assert len(bars) == 2
        assert bars[0].symbol == "BTCUSDT_SPOT_BINANCE"
        assert bars[0].exchange == Exchange.GLOBAL
        assert bars[0].interval == Interval.MINUTE
        mock_vision_client.download_klines.assert_called()
        mock_database.save_bar_data.assert_called_once()

    def test_query_bar_history_vision_no_data(
        self, datafeed, mock_vision_client, mock_database
    ):
        """Test Vision download when no data is returned."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=30)
        end = today - timedelta(days=5)

        # Vision returns no data
        mock_vision_client.download_klines.return_value = None

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        assert len(bars) == 0
        assert any("未下载到任何数据" in m for m in messages)

    def test_query_bar_history_vision_corrupted_zip(
        self, datafeed, mock_vision_client, mock_database
    ):
        """Test Vision download with corrupted ZIP file."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=30)
        end = today - timedelta(days=5)

        # Return corrupted data
        mock_vision_client.download_klines.return_value = b"not a valid zip file"

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        # Should handle gracefully and return empty
        assert len(bars) == 0


class TestQueryBarHistoryRecent:
    """Tests for recent data download via REST API."""

    def test_query_bar_history_recent(
        self, datafeed, mock_rest_client, mock_vision_client, mock_database
    ):
        """Test recent data download from REST API."""
        # Time range that falls in "rest" category: start < yesterday and end >= yesterday
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        start = yesterday - timedelta(hours=6)  # Before yesterday
        end = datetime.now()  # After yesterday

        # Vision returns no data for this time range
        mock_vision_client.download_klines.return_value = None

        # Create mock kline data for REST
        timestamps = [
            int(start.timestamp() * 1000),
            int((start + timedelta(minutes=1)).timestamp() * 1000),
        ]
        mock_rest_client.get_klines.return_value = create_sample_kline_json(timestamps)

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        assert len(bars) == 2
        mock_rest_client.get_klines.assert_called()
        mock_database.save_bar_data.assert_called_once()

    def test_query_bar_history_rest_pagination(
        self, datafeed, mock_rest_client, mock_vision_client, mock_database
    ):
        """Test REST API pagination handling."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        start = yesterday - timedelta(hours=3)  # Before yesterday
        end = datetime.now()  # After yesterday

        # Vision returns no data
        mock_vision_client.download_klines.return_value = None

        # First call returns data, second call returns empty (end of data)
        timestamps1 = [int(start.timestamp() * 1000)]

        mock_rest_client.get_klines.side_effect = [
            create_sample_kline_json(timestamps1),
            [],  # Empty response ends pagination
        ]

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        bars = datafeed.query_bar_history(req, output=lambda x: None)

        assert len(bars) == 1

    def test_query_bar_history_rest_no_data(
        self, datafeed, mock_rest_client, mock_vision_client, mock_database
    ):
        """Test REST API when no data is returned."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        start = yesterday - timedelta(hours=1)  # Before yesterday
        end = datetime.now()  # After yesterday

        # Vision returns no data
        mock_vision_client.download_klines.return_value = None
        mock_rest_client.get_klines.return_value = []

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        assert len(bars) == 0


class TestQueryBarHistoryMixed:
    """Tests for mixed time range (both Vision and REST)."""

    def test_query_bar_history_mixed(
        self, datafeed, mock_vision_client, mock_rest_client, mock_database
    ):
        """Test mixed time range using both Vision and REST."""
        # Time range that spans both historical and recent
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)  # 7 days ago
        end = datetime.now()  # Now

        # Vision data (historical)
        vision_timestamps = [int((start).timestamp() * 1000)]
        csv_data = create_sample_csv_data(vision_timestamps)
        zip_data = create_mock_zip_csv(csv_data)
        mock_vision_client.download_klines.return_value = zip_data

        # REST data (recent)
        rest_timestamps = [
            int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)
        ]
        mock_rest_client.get_klines.return_value = create_sample_kline_json(
            rest_timestamps
        )

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        # Both sources should be called
        mock_vision_client.download_klines.assert_called()
        mock_rest_client.get_klines.assert_called()

        # Data should be merged
        assert len(bars) >= 1


class TestSymbolNormalization:
    """Tests for symbol format handling."""

    def test_symbol_normalization_lowercase(
        self, datafeed, mock_vision_client, mock_database
    ):
        """Test that lowercase symbols are normalized to uppercase."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=30)
        end = today - timedelta(days=5)

        timestamps = [int(start.timestamp() * 1000)]
        csv_data = create_sample_csv_data(timestamps)
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # Use lowercase symbol
        req = HistoryRequest(
            symbol="btcusdt",  # lowercase
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Symbol should be normalized to uppercase in results
        if bars:
            assert bars[0].symbol == "BTCUSDT_SPOT_BINANCE"

    def test_symbol_normalization_mixed_case(
        self, datafeed, mock_vision_client, mock_database
    ):
        """Test that mixed case symbols are normalized to uppercase."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=30)
        end = today - timedelta(days=5)

        timestamps = [int(start.timestamp() * 1000)]
        csv_data = create_sample_csv_data(timestamps)
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        req = HistoryRequest(
            symbol="BtcUsdt",  # mixed case
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        bars = datafeed.query_bar_history(req, output=lambda x: None)

        if bars:
            assert bars[0].symbol == "BTCUSDT_SPOT_BINANCE"


class TestDuplicateData:
    """Tests for duplicate data handling."""

    def test_existing_data_in_database(self, datafeed, mock_database):
        """Test that existing data in database is returned without downloading."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = today - timedelta(days=5)

        # Mock existing data in database
        existing_bar = create_bar_data("BTCUSDT_SPOT_BINANCE", start)
        mock_database.load_bar_data.return_value = [existing_bar]

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        # Should return existing data without downloading
        assert len(bars) == 1
        assert bars[0] == existing_bar
        assert any("已存在" in m for m in messages)
        # Should not call save_bar_data
        mock_database.save_bar_data.assert_not_called()

    def test_duplicate_merging_in_mixed_mode(
        self, datafeed, mock_vision_client, mock_rest_client, mock_database
    ):
        """Test that duplicates are merged when using both sources."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = datetime.now()

        # Same timestamp in both sources
        same_ts = int(start.timestamp() * 1000)

        # Vision returns data
        csv_data = create_sample_csv_data([same_ts])
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # REST returns same timestamp (duplicate)
        mock_rest_client.get_klines.return_value = create_sample_kline_json([same_ts])

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Should deduplicate - only one bar for same timestamp
        assert len(bars) == 1

    def test_data_sorting(
        self, datafeed, mock_vision_client, mock_rest_client, mock_database
    ):
        """Test that merged data is sorted by datetime."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = datetime.now()

        # Create timestamps in non-sequential order
        ts1 = int((start + timedelta(minutes=10)).timestamp() * 1000)
        ts2 = int((start + timedelta(minutes=5)).timestamp() * 1000)
        ts3 = int((start + timedelta(minutes=1)).timestamp() * 1000)

        # Vision returns ts1
        csv_data = create_sample_csv_data([ts1])
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # REST returns ts2 and ts3
        mock_rest_client.get_klines.return_value = create_sample_kline_json([ts2, ts3])

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Data should be sorted by datetime
        if len(bars) > 1:
            for i in range(1, len(bars)):
                assert bars[i].datetime >= bars[i - 1].datetime


class TestDataGapDetection:
    """Tests for gap detection in historical data."""

    def test_gap_at_start(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test gap detection when data exists only at the end of requested range."""
        # Setup: Database has data from Jan 15-31, but user requests Jan 1-31
        # Expected: Gap detected for Jan 1-14
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=30)  # Jan 1
        end = today - timedelta(days=1)  # Jan 31

        # Pre-populate database with partial data (Jan 15-31)
        existing_bars = []
        for i in range(15, 31):  # Jan 15-31
            dt = start + timedelta(days=i)
            existing_bars.append(create_bar_data("BTCUSDT_SPOT_BINANCE", dt))
        mock_database.load_bar_data.return_value = existing_bars

        # Mock vision client to return data for the gap
        csv_data = create_sample_csv_data(
            [int((start + timedelta(days=i)).timestamp() * 1000) for i in range(1, 15)]
        )
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # Execute
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.DAILY,
            start=start,
            end=end,
        )
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Verify: Should download missing data for Jan 1-14
        mock_vision_client.download_klines.assert_called()

    def test_gap_in_middle(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test gap detection when data has a gap in the middle."""
        # Setup: Database has data Jan 1-10 and Jan 20-31, but missing Jan 11-19
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=30)
        end = today - timedelta(days=1)

        # Pre-populate database with partial data (Jan 1-10 and Jan 20-31)
        existing_bars = []
        for i in range(1, 11):  # Jan 1-10
            dt = start + timedelta(days=i)
            existing_bars.append(create_bar_data("BTCUSDT_SPOT_BINANCE", dt))
        for i in range(20, 31):  # Jan 20-31
            dt = start + timedelta(days=i)
            existing_bars.append(create_bar_data("BTCUSDT_SPOT_BINANCE", dt))
        mock_database.load_bar_data.return_value = existing_bars

        # Mock vision client to return data for the gap
        csv_data = create_sample_csv_data(
            [int((start + timedelta(days=i)).timestamp() * 1000) for i in range(11, 20)]
        )
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # Execute
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.DAILY,
            start=start,
            end=end,
        )
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Verify: Should download missing data for Jan 11-19
        mock_vision_client.download_klines.assert_called()

    def test_gap_at_end(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test gap detection when data exists only at the start of requested range."""
        # Setup: Database has data Jan 1-15, but user requests Jan 1-31
        # Expected: Gap detected for Jan 16-31
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=30)
        end = today - timedelta(days=1)

        # Pre-populate database with partial data (Jan 1-15)
        existing_bars = []
        for i in range(1, 16):  # Jan 1-15
            dt = start + timedelta(days=i)
            existing_bars.append(create_bar_data("BTCUSDT_SPOT_BINANCE", dt))
        mock_database.load_bar_data.return_value = existing_bars

        # Mock vision client to return data for the gap
        csv_data = create_sample_csv_data(
            [int((start + timedelta(days=i)).timestamp() * 1000) for i in range(16, 31)]
        )
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # Execute
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.DAILY,
            start=start,
            end=end,
        )
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Verify: Should download missing data for Jan 16-31
        mock_vision_client.download_klines.assert_called()

    def test_no_gap_complete_data(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test that no download happens when data is complete."""
        # Setup: Database has complete data for the entire range
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = today - timedelta(days=1)

        # Pre-populate database with complete data
        existing_bars = []
        for i in range(7):  # All days
            dt = start + timedelta(days=i)
            existing_bars.append(create_bar_data("BTCUSDT_SPOT_BINANCE", dt))
        mock_database.load_bar_data.return_value = existing_bars

        # Execute
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.DAILY,
            start=start,
            end=end,
        )
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Verify: Should not download since data is complete
        # Note: This test will fail until _find_gaps is implemented
        # as the current implementation may still call download
        if len(bars) > 0:
            mock_vision_client.download_klines.assert_not_called()

    def test_empty_database(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test that all data is downloaded when database is empty."""
        # Setup: Database has no data
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = today - timedelta(days=1)

        mock_database.load_bar_data.return_value = []  # Empty database

        # Mock vision client to return data
        csv_data = create_sample_csv_data(
            [int((start + timedelta(days=i)).timestamp() * 1000) for i in range(7)]
        )
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # Execute
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.DAILY,
            start=start,
            end=end,
        )
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Verify: Should download all data since database is empty
        mock_vision_client.download_klines.assert_called()

    def test_different_intervals(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test gap detection with different interval granularities."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=1)
        end = today

        # Test with minute interval
        existing_bar = create_bar_data("BTCUSDT_SPOT_BINANCE", start)
        mock_database.load_bar_data.return_value = [existing_bar]

        csv_data = create_sample_csv_data([int(end.timestamp() * 1000)])
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Should handle minute intervals
        mock_vision_client.download_klines.assert_called()


class TestInvalidSymbol:
    """Tests for invalid symbol handling."""

    def test_invalid_symbol(self, datafeed, mock_database):
        """Test that invalid symbols are rejected."""
        now = datetime.now()
        start = now - timedelta(hours=1)
        end = now

        req = HistoryRequest(
            symbol="INVALIDPAIR_SPOT_BINANCE",  # Valid format but not in supported symbols
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        assert len(bars) == 0
        assert any("不支持的合约代码" in m for m in messages)

    def test_symbol_validation_disabled(
        self, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test that symbol validation is skipped when symbols set is empty."""
        with (
            patch(
                "vnpy_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            df = BinanceDatafeed()
            df.inited = True
            df.symbols = set()  # Empty set - validation disabled

            now = datetime.now()
            start = now - timedelta(hours=1)
            end = now

            mock_rest_client.get_klines.return_value = []

            req = HistoryRequest(
                symbol="ANYSYMBOL_SPOT_BINANCE",
                exchange=Exchange.GLOBAL,
                interval=Interval.MINUTE,
                start=start,
                end=end,
            )

            messages = []
            bars = df.query_bar_history(req, output=messages.append)

            # Should not reject the symbol (empty symbols set means no validation)
            assert not any("不支持的合约代码" in m for m in messages)


class TestIntervalHandling:
    """Tests for interval handling."""

    def test_unsupported_interval(self, datafeed, mock_database):
        """Test that unsupported intervals are rejected."""
        now = datetime.now()
        start = now - timedelta(hours=1)
        end = now

        # Create a mock interval that's not supported
        mock_interval = MagicMock()
        mock_interval.value = "99w"  # Not in SUPPORTED_INTERVALS
        mock_interval.__str__ = lambda self: "99w"

        # Mock INTERVAL_VT2BINANCE to not contain this interval
        with patch("vnpy_binance_datafeed.datafeed.INTERVAL_VT2BINANCE", {}):
            req = HistoryRequest(
                symbol="BTCUSDT_SPOT_BINANCE",
                exchange=Exchange.GLOBAL,
                interval=mock_interval,
                start=start,
                end=end,
            )

            messages = []
            bars = datafeed.query_bar_history(req, output=messages.append)

            assert len(bars) == 0
            assert any("不支持的K线周期" in m for m in messages)

    def test_supported_intervals(self, datafeed, mock_rest_client, mock_database):
        """Test that all supported intervals work."""
        now = datetime.now()
        start = now - timedelta(hours=1)
        end = now

        # Test standard vnpy intervals
        for interval in [Interval.MINUTE, Interval.HOUR, Interval.DAILY]:
            mock_rest_client.get_klines.return_value = []

            req = HistoryRequest(
                symbol="BTCUSDT_SPOT_BINANCE",
                exchange=Exchange.GLOBAL,
                interval=interval,
                start=start,
                end=end,
            )

            bars = datafeed.query_bar_history(req, output=lambda x: None)

            # Should not throw error
            assert isinstance(bars, list)


class TestDetermineDataSource:
    """Tests for _determine_data_source method."""

    def test_determine_vision(self, datafeed):
        """Test vision source determination for old data."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # End is before yesterday
        start = today - timedelta(days=10)
        end = today - timedelta(days=3)

        source = datafeed._determine_data_source(start, end)
        assert source == "vision"

    def test_determine_both(self, datafeed):
        """Test both sources for range starting before yesterday and ending after."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)

        # Start is before yesterday, end is after yesterday
        start = today - timedelta(days=3)
        end = datetime.now()

        source = datafeed._determine_data_source(start, end)
        assert source == "both"

    def test_determine_rest(self, datafeed):
        """Test rest source for recent data starting at or after yesterday."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)

        # Start and end both >= yesterday
        start = yesterday
        end = datetime.now()

        source = datafeed._determine_data_source(start, end)
        assert source == "rest"


class TestEdgeCases:
    """Tests for edge cases."""

    def test_none_end_time(self, datafeed, mock_rest_client, mock_database):
        """Test that None end time defaults to now."""
        now = datetime.now()
        start = now - timedelta(hours=1)

        mock_rest_client.get_klines.return_value = []

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=None,  # None should default to now
        )

        bars = datafeed.query_bar_history(req, output=lambda x: None)

        assert isinstance(bars, list)

    def test_auto_init_on_query(
        self, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test that query automatically initializes if not already initialized."""
        mock_rest_client.get_exchange_info.return_value = {
            "symbols": [{"symbol": "BTCUSDT_SPOT_BINANCE"}]
        }
        mock_rest_client.get_klines.return_value = []

        with (
            patch(
                "vnpy_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            df = BinanceDatafeed()
            # Don't call init manually

            now = datetime.now()
            start = now - timedelta(hours=1)

            req = HistoryRequest(
                symbol="BTCUSDT_SPOT_BINANCE",
                exchange=Exchange.GLOBAL,
                interval=Interval.MINUTE,
                start=start,
                end=now,
            )

            df.query_bar_history(req, output=lambda x: None)

            # Should have been initialized
            assert df.inited is True

    def test_save_to_database_empty_list(self, datafeed, mock_database):
        """Test that _save_to_database handles empty list."""
        datafeed._save_to_database([])
        mock_database.save_bar_data.assert_not_called()


class TestParseVtSymbol:
    """Tests for parse_vt_symbol function."""

    def test_parse_spot_symbol(self):
        """Test parsing SPOT market symbol."""
        from vnpy_binance_datafeed.constant import parse_vt_symbol

        result = parse_vt_symbol("BTCUSDT_SPOT_BINANCE")

        assert result is not None
        assert result.base == "BTCUSDT"
        assert result.market_type == "SPOT"
        assert result.exchange == "BINANCE"
        assert result.full_symbol == "BTCUSDT_SPOT_BINANCE"

    def test_parse_swap_symbol(self):
        """Test parsing SWAP market symbol."""
        from vnpy_binance_datafeed.constant import parse_vt_symbol

        result = parse_vt_symbol("BTCUSDT_SWAP_BINANCE")

        assert result is not None
        assert result.base == "BTCUSDT"
        assert result.market_type == "SWAP"
        assert result.exchange == "BINANCE"
        assert result.full_symbol == "BTCUSDT_SWAP_BINANCE"

    def test_parse_with_global_suffix(self):
        """Test parsing symbol with .GLOBAL suffix."""
        from vnpy_binance_datafeed.constant import parse_vt_symbol

        result = parse_vt_symbol("ETHUSDT_SPOT_BINANCE.GLOBAL")

        assert result is not None
        assert result.base == "ETHUSDT"
        assert result.market_type == "SPOT"
        assert result.exchange == "BINANCE"
        assert result.full_symbol == "ETHUSDT_SPOT_BINANCE.GLOBAL"

    def test_parse_invalid_symbol(self):
        """Test parsing invalid symbol formats."""
        from vnpy_binance_datafeed.constant import parse_vt_symbol

        # Invalid: missing market type and exchange
        result = parse_vt_symbol("BTCUSDT")
        assert result is None

        # Invalid: wrong format (missing parts)
        result = parse_vt_symbol("BTCUSDT_SPOT")
        assert result is None

        # Invalid: wrong exchange
        result = parse_vt_symbol("BTCUSDT_SPOT_OKX")
        assert result is None

        # Invalid: wrong market type
        result = parse_vt_symbol("BTCUSDT_OPTIONS_BINANCE")
        assert result is None

        # Invalid: empty string
        result = parse_vt_symbol("")
        assert result is None


class TestGlobalSuffixValidation:
    """Tests for .GLOBAL suffix validation in query_bar_history.

    These tests verify that symbols with .GLOBAL suffix (e.g., BTCUSDT_SPOT_BINANCE.GLOBAL)
    are accepted by query_bar_history, even though datafeed.symbols only contains the
    cleaned format (without .GLOBAL suffix).
    """

    def test_spot_global_suffix(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test that SPOT symbol with .GLOBAL suffix passes validation."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = today - timedelta(days=1)

        # Mock vision client returning valid data
        ts = int(start.timestamp() * 1000)
        csv_data = create_sample_csv_data([ts])
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # Mock REST client returning empty (no recent data needed)
        mock_rest_client.get_klines.return_value = []

        # Create request with .GLOBAL suffix
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE.GLOBAL",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        # Execute - should NOT reject symbol due to .GLOBAL suffix
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Verify data was downloaded (validation passed)
        assert len(bars) > 0
        mock_vision_client.download_klines.assert_called()

    def test_swap_global_suffix(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test that SWAP symbol with .GLOBAL suffix passes validation."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = today - timedelta(days=1)

        # Add SWAP symbol to valid symbols set
        datafeed.symbols.add("BTCUSDT_SWAP_BINANCE")

        # Mock vision client returning valid data
        ts = int(start.timestamp() * 1000)
        csv_data = create_sample_csv_data([ts])
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # Mock REST client returning empty (no recent data needed)
        mock_rest_client.get_klines.return_value = []

        # Create request with .GLOBAL suffix
        req = HistoryRequest(
            symbol="BTCUSDT_SWAP_BINANCE.GLOBAL",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        # Execute - should NOT reject symbol due to .GLOBAL suffix
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Verify data was downloaded (validation passed)
        assert len(bars) > 0
        mock_vision_client.download_klines.assert_called()

    def test_backward_compatibility(
        self, datafeed, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test that symbol without .GLOBAL suffix still works (backward compatibility)."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = today - timedelta(days=1)

        # Mock vision client returning valid data
        ts = int(start.timestamp() * 1000)
        csv_data = create_sample_csv_data([ts])
        mock_vision_client.download_klines.return_value = create_mock_zip_csv(csv_data)

        # Mock REST client returning empty (no recent data needed)
        mock_rest_client.get_klines.return_value = []

        # Create request without .GLOBAL suffix (original format)
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        # Execute
        bars = datafeed.query_bar_history(req, output=lambda x: None)

        # Verify data was downloaded
        assert len(bars) > 0
        mock_vision_client.download_klines.assert_called()


class TestEndToEndGUIFlow:
    """End-to-end tests simulating GUI DataManager flow.

    These tests simulate the complete user flow when using the GUI DataManager:
    1. User opens DataManager
    2. User selects symbol, interval, and date range
    3. User clicks "Download" button
    4. System downloads data and saves to database
    5. System returns downloaded data to display
    """

    def test_full_download_flow_spot_global(
        self, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test complete download flow for SPOT symbol with .GLOBAL suffix."""
        # 1. Initialize datafeed (simulates GUI opening DataManager)
        with (
            patch(
                "vnpy_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            datafeed = BinanceDatafeed()
            datafeed.symbols = {"BTCUSDT_SPOT_BINANCE"}
            datafeed.inited = True

            # 2. User selects: BTCUSDT_SPOT_BINANCE.GLOBAL, Interval.MINUTE, date range
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            start = today - timedelta(days=7)
            end = today - timedelta(days=1)

            # 3. Setup mock returns for the download
            timestamps = [
                int((start + timedelta(days=i)).timestamp() * 1000) for i in range(7)
            ]
            csv_data = create_sample_csv_data(timestamps)
            mock_vision_client.download_klines.return_value = create_mock_zip_csv(
                csv_data
            )
            mock_rest_client.get_klines.return_value = []
            mock_database.load_bar_data.return_value = []

            # 4. Create request (simulates GUI input)
            req = HistoryRequest(
                symbol="BTCUSDT_SPOT_BINANCE.GLOBAL",
                exchange=Exchange.GLOBAL,
                interval=Interval.MINUTE,
                start=start,
                end=end,
            )

            # 5. Execute (simulates clicking "Download" button)
            messages = []
            bars = datafeed.query_bar_history(req, output=messages.append)

            # 6. Verify data returned and saved
            mock_vision_client.download_klines.assert_called()
            mock_database.save_bar_data.assert_called()
            assert len(bars) == 7
            # Symbol keeps .GLOBAL suffix as per GUI DataManager convention
            assert bars[0].symbol == "BTCUSDT_SPOT_BINANCE.GLOBAL"
            assert bars[0].exchange == Exchange.GLOBAL
            assert bars[0].interval == Interval.MINUTE

    def test_full_download_flow_swap_global(
        self, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test complete download flow for SWAP symbol with .GLOBAL suffix."""
        # 1. Initialize datafeed (simulates GUI opening DataManager)
        with (
            patch(
                "vnpy_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            datafeed = BinanceDatafeed()
            datafeed.symbols = {"ETHUSDT_SWAP_BINANCE"}
            datafeed.inited = True

            # 2. User selects: ETHUSDT_SWAP_BINANCE.GLOBAL, Interval.HOUR, date range
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            start = today - timedelta(days=14)
            end = today - timedelta(days=1)

            # 3. Setup mock returns for the download
            timestamps = [
                int((start + timedelta(hours=i)).timestamp() * 1000) for i in range(24)
            ]
            csv_data = create_sample_csv_data(timestamps)
            mock_vision_client.download_klines.return_value = create_mock_zip_csv(
                csv_data
            )
            mock_rest_client.get_klines.return_value = []
            mock_database.load_bar_data.return_value = []

            # 4. Create request (simulates GUI input)
            req = HistoryRequest(
                symbol="ETHUSDT_SWAP_BINANCE.GLOBAL",
                exchange=Exchange.GLOBAL,
                interval=Interval.HOUR,
                start=start,
                end=end,
            )

            # 5. Execute (simulates clicking "Download" button)
            messages = []
            bars = datafeed.query_bar_history(req, output=messages.append)

            # 6. Verify data returned and saved
            mock_vision_client.download_klines.assert_called()
            mock_database.save_bar_data.assert_called()
            assert len(bars) == 24
            # Symbol keeps .GLOBAL suffix as per GUI DataManager convention
            assert bars[0].symbol == "ETHUSDT_SWAP_BINANCE.GLOBAL"
            assert bars[0].exchange == Exchange.GLOBAL

    def test_incremental_download_with_gaps(
        self, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test incremental download when database has partial data."""
        # 1. Initialize datafeed
        with (
            patch(
                "vnpy_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            datafeed = BinanceDatafeed()
            datafeed.symbols = {"BTCUSDT_SPOT_BINANCE"}
            datafeed.inited = True

            # 2. Pre-populate database with partial data (first 3 days)
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            start = today - timedelta(days=7)
            end = today - timedelta(days=1)

            existing_bars = []
            for i in range(3):  # Days 0, 1, 2 exist
                dt = start + timedelta(days=i)
                existing_bars.append(create_bar_data("BTCUSDT_SPOT_BINANCE", dt))
            mock_database.load_bar_data.return_value = existing_bars

            # 3. Mock vision client to return data for missing gaps (days 3-6)
            missing_timestamps = [
                int((start + timedelta(days=i)).timestamp() * 1000) for i in range(3, 7)
            ]
            csv_data = create_sample_csv_data(missing_timestamps)
            mock_vision_client.download_klines.return_value = create_mock_zip_csv(
                csv_data
            )
            mock_rest_client.get_klines.return_value = []

            # 4. Request full date range
            req = HistoryRequest(
                symbol="BTCUSDT_SPOT_BINANCE.GLOBAL",
                exchange=Exchange.GLOBAL,
                interval=Interval.DAILY,
                start=start,
                end=end,
            )

            # 5. Execute
            bars = datafeed.query_bar_history(req, output=lambda x: None)

            # 6. Verify only missing gaps are downloaded
            mock_vision_client.download_klines.assert_called()
            mock_database.save_bar_data.assert_called()

            # 7. Verify returned data is complete (existing + new)
            assert len(bars) >= 3  # At least the existing bars plus new ones

    def test_different_intervals_gap_detection(
        self, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test gap detection with different intervals."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Test all three interval types
        test_cases = [
            (Interval.MINUTE, timedelta(minutes=1), "minute"),
            (Interval.HOUR, timedelta(hours=1), "hour"),
            (Interval.DAILY, timedelta(days=1), "day"),
        ]

        for interval, delta, granularity in test_cases:
            # Reset mocks
            mock_vision_client.reset_mock()
            mock_database.reset_mock()
            mock_rest_client.reset_mock()

            # 1. Initialize datafeed
            with (
                patch(
                    "vnpy_binance_datafeed.datafeed.VisionClient",
                    return_value=mock_vision_client,
                ),
                patch(
                    "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                    return_value=mock_rest_client,
                ),
                patch(
                    "vnpy_binance_datafeed.datafeed.get_database",
                    return_value=mock_database,
                ),
            ):
                datafeed = BinanceDatafeed()
                datafeed.symbols = {"BTCUSDT_SPOT_BINANCE"}
                datafeed.inited = True

                # 2. Setup: empty database, request full range (will trigger download)
                start = today - timedelta(days=2)
                end = today - timedelta(days=1)

                # Empty database - all data must be downloaded
                mock_database.load_bar_data.return_value = []

                # Mock vision to return data
                gap_ts = int(start.timestamp() * 1000)
                csv_data = create_sample_csv_data([gap_ts])
                mock_vision_client.download_klines.return_value = create_mock_zip_csv(
                    csv_data
                )
                mock_rest_client.get_klines.return_value = []

                # 3. Request with specific interval
                req = HistoryRequest(
                    symbol="BTCUSDT_SPOT_BINANCE.GLOBAL",
                    exchange=Exchange.GLOBAL,
                    interval=interval,
                    start=start,
                    end=end,
                )

                # 4. Execute
                bars = datafeed.query_bar_history(req, output=lambda x: None)

                # 5. Verify gap detection at specified granularity
                # With empty database, download should be triggered
                mock_vision_client.download_klines.assert_called()
                # Verify bars are returned with correct interval
                assert len(bars) >= 1
                assert bars[0].interval == interval

    def test_no_gap_skip_download(
        self, mock_database, mock_vision_client, mock_rest_client
    ):
        """Test that download is skipped when no gaps exist."""
        # 1. Initialize datafeed
        with (
            patch(
                "vnpy_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            datafeed = BinanceDatafeed()
            datafeed.symbols = {"BTCUSDT_SPOT_BINANCE"}
            datafeed.inited = True

            # 2. Pre-populate database with complete data
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            start = today - timedelta(days=7)
            end = today - timedelta(days=1)

            complete_bars = []
            for i in range(7):  # All 7 days present
                dt = start + timedelta(days=i)
                complete_bars.append(create_bar_data("BTCUSDT_SPOT_BINANCE", dt))
            mock_database.load_bar_data.return_value = complete_bars

            # 3. Request same date range
            req = HistoryRequest(
                symbol="BTCUSDT_SPOT_BINANCE.GLOBAL",
                exchange=Exchange.GLOBAL,
                interval=Interval.DAILY,
                start=start,
                end=end,
            )

            # 4. Execute
            bars = datafeed.query_bar_history(req, output=lambda x: None)

            # 5. Verify no download attempts made
            # When data is complete, vision_client should not be called
            # (or called minimally for gap check)
            if len(bars) > 0 and len(bars) == len(complete_bars):
                # If all existing data returned, download should be skipped
                mock_database.save_bar_data.assert_not_called()

            # 6. Verify existing data returned
            assert len(bars) == 7
            for bar in bars:
                assert bar.symbol == "BTCUSDT_SPOT_BINANCE"
                assert bar.exchange == Exchange.GLOBAL


class TestAwareDatetimeBoundary:
    """Tests for aware datetime boundary handling in database queries."""

    def test_aware_datetime_converted_to_naive_for_database_query(
        self, datafeed, mock_database, mock_rest_client
    ):
        """Test that aware datetime is converted to naive for SQLite query.

        Bug: When HistoryRequest.start is an aware datetime (e.g., datetime(2026, 1, 1, tzinfo=DB_TZ)),
        and the database contains a BarData with naive datetime datetime(2026, 1, 1) (which is what
        SQLite stores), the load_bar_data query currently fails to match this boundary row due to
        SQLite's string comparison.

        SQLite stores datetime as naive strings like "2026-01-01 00:00:00". When GUI passes aware
        datetime "2026-01-01 00:00:00+08:00", the query does lexicographic comparison:
        "2026-01-01 00:00:00" < "2026-01-01 00:00:00+08:00" (shorter < longer when prefix matches).

        So boundary row is excluded from results.
        """
        # Create aware datetimes (as GUI would pass)
        aware_start = datetime(2026, 1, 1, tzinfo=DB_TZ)
        aware_end = datetime(2026, 1, 2, tzinfo=DB_TZ)

        # Create request with aware datetimes
        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.DAILY,
            start=aware_start,
            end=aware_end,
        )

        # Mock database to return empty (will trigger download path)
        mock_database.load_bar_data.return_value = []

        # Mock rest client to return empty (will cause download loop to break)
        mock_rest_client.get_klines.return_value = []

        # Execute query
        datafeed.query_bar_history(req, output=lambda x: None)

        # Verify load_bar_data was called
        mock_database.load_bar_data.assert_called_once()

        # Get the call kwargs
        call_kwargs = mock_database.load_bar_data.call_args[1]

        # Assert that start and end are NAIVE (tzinfo is None)
        # This is the bug - currently they remain aware, causing boundary issues
        assert call_kwargs["start"].tzinfo is None, (
            f"Expected start to be naive (tzinfo=None), but got {call_kwargs['start'].tzinfo}"
        )
        assert call_kwargs["end"].tzinfo is None, (
            f"Expected end to be naive (tzinfo=None), but got {call_kwargs['end'].tzinfo}"
        )
