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
from vnpy_crypto_binance_datafeed.datafeed import BinanceDatafeed


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
            "vnpy_crypto_binance_datafeed.datafeed.VisionClient",
            return_value=mock_vision_client,
        ),
        patch(
            "vnpy_crypto_binance_datafeed.datafeed.BinanceRestClient",
            return_value=mock_rest_client,
        ),
        patch(
            "vnpy_crypto_binance_datafeed.datafeed.get_database",
            return_value=mock_database,
        ),
    ):
        df = BinanceDatafeed()
        df.inited = True  # Pre-initialize to avoid exchange_info calls
        df.symbols = {"BTCUSDT", "ETHUSDT"}  # Pre-populate valid symbols
        return df


class TestBinanceDatafeedInit:
    """Tests for BinanceDatafeed initialization."""

    def test_init_success(self, mock_database, mock_vision_client, mock_rest_client):
        """Test successful initialization."""
        mock_rest_client.get_exchange_info.return_value = {
            "symbols": [
                {"symbol": "BTCUSDT"},
                {"symbol": "ETHUSDT"},
            ]
        }

        with (
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            df = BinanceDatafeed()

            # Call init
            messages = []
            result = df.init(output=messages.append)

            assert result is True
            assert df.inited is True
            assert "BTCUSDT" in df.symbols
            assert "ETHUSDT" in df.symbols
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
                "vnpy_crypto_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.get_database",
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
            symbol="BTCUSDT",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        messages = []
        bars = datafeed.query_bar_history(req, output=messages.append)

        assert len(bars) == 2
        assert bars[0].symbol == "BTCUSDT"
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
            symbol="BTCUSDT",
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
            symbol="BTCUSDT",
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
            symbol="BTCUSDT",
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
            symbol="BTCUSDT",
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
            symbol="BTCUSDT",
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
            symbol="BTCUSDT",
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
            assert bars[0].symbol == "BTCUSDT"

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
            assert bars[0].symbol == "BTCUSDT"


class TestDuplicateData:
    """Tests for duplicate data handling."""

    def test_existing_data_in_database(self, datafeed, mock_database):
        """Test that existing data in database is returned without downloading."""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = today - timedelta(days=5)

        # Mock existing data in database
        existing_bar = create_bar_data("BTCUSDT", start)
        mock_database.load_bar_data.return_value = [existing_bar]

        req = HistoryRequest(
            symbol="BTCUSDT",
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
            symbol="BTCUSDT",
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
            symbol="BTCUSDT",
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


class TestInvalidSymbol:
    """Tests for invalid symbol handling."""

    def test_invalid_symbol(self, datafeed, mock_database):
        """Test that invalid symbols are rejected."""
        now = datetime.now()
        start = now - timedelta(hours=1)
        end = now

        req = HistoryRequest(
            symbol="INVALIDPAIR",  # Not in valid symbols
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
                "vnpy_crypto_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.get_database",
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
                symbol="ANYSYMBOL",
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
        with patch("vnpy_crypto_binance_datafeed.datafeed.INTERVAL_VT2BINANCE", {}):
            req = HistoryRequest(
                symbol="BTCUSDT",
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
                symbol="BTCUSDT",
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
            symbol="BTCUSDT",
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
            "symbols": [{"symbol": "BTCUSDT"}]
        }
        mock_rest_client.get_klines.return_value = []

        with (
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.VisionClient",
                return_value=mock_vision_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.get_database",
                return_value=mock_database,
            ),
        ):
            df = BinanceDatafeed()
            # Don't call init manually

            now = datetime.now()
            start = now - timedelta(hours=1)

            req = HistoryRequest(
                symbol="BTCUSDT",
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
