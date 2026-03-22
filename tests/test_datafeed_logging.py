"""
Tests for BinanceDatafeed logging behavior (TDD RED phase).

These tests verify that messages are properly routed to both logger and output callback:
- INFO messages: logged via logger.info() only (not output callback)
- WARNING messages: logged via logger.warning() only (not output callback)
- ERROR messages: logged via BOTH logger.error() AND output callback

These tests will FAIL until _log_info, _log_warning, _log_error methods are implemented.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import HistoryRequest
from vnpy_crypto_binance_datafeed.datafeed import BinanceDatafeed


@pytest.fixture
def mock_database():
    """Create a mock database."""
    mock_db = MagicMock()
    mock_db.load_bar_data.return_value = []
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
def mock_datafeed(mock_database, mock_vision_client, mock_rest_client):
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
        df.inited = True
        df.symbols = {
            "BTCUSDT_SPOT_BINANCE",
            "ETHUSDT_SPOT_BINANCE",
        }
        return df


class TestInfoMessagesLoggedNotOutput:
    """Tests for INFO message logging behavior.

    INFO messages should be logged via logger.info() but NOT sent to output callback.
    """

    def test_info_messages_logged_not_output(self, mock_datafeed, caplog):
        """INFO messages should use logger.info() and NOT call output callback."""
        # Track output callback invocations
        output_messages = []
        output_callback = output_messages.append

        # Set up a scenario that triggers INFO-level messages
        # Query should trigger "数据库中已有完整数据" or similar INFO message
        mock_datafeed.database.load_bar_data.return_value = []

        # Create request
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - timedelta(days=7)
        end = today - timedelta(days=1)

        # Mock vision client to return empty (triggers "未下载到任何新数据")
        with patch.object(
            mock_datafeed.spot_vision_client, "download_klines", return_value=None
        ):
            req = HistoryRequest(
                symbol="BTCUSDT_SPOT_BINANCE",
                exchange=Exchange.GLOBAL,
                interval=Interval.MINUTE,
                start=start,
                end=end,
            )

            # Execute with output callback
            bars = mock_datafeed.query_bar_history(req, output=output_callback)

            # Verify INFO messages were logged via logger
            assert "INFO" in caplog.text or any(
                "INFO" in record.levelname for record in caplog.records
            ), "Expected INFO message to be logged via logger.info()"

            # Verify INFO messages were NOT sent to output callback
            info_messages_in_output = [
                msg for msg in output_messages if "INFO" in msg or "信息" in msg
            ]
            assert len(info_messages_in_output) == 0, (
                f"INFO messages should NOT be sent to output callback, but found: {info_messages_in_output}"
            )

    def test_init_info_message_uses_logger(self, caplog):
        """init() INFO messages should use logger, not output callback."""
        mock_rest_client = MagicMock()
        mock_rest_client.get_exchange_info.return_value = {
            "symbols": [{"symbol": "BTCUSDT"}]
        }

        output_messages = []
        output_callback = output_messages.append

        with (
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.VisionClient",
                return_value=MagicMock(),
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.get_database",
                return_value=MagicMock(),
            ),
        ):
            df = BinanceDatafeed()
            df.init(output=output_callback)

            # Check that INFO-level messages went to logger
            info_records = [r for r in caplog.records if r.levelname == "INFO"]
            assert len(info_records) > 0, "Expected INFO messages in logger"

            # Check that "正在初始化" message did NOT go to output
            init_messages_in_output = [
                msg for msg in output_messages if "正在初始化" in msg
            ]
            assert len(init_messages_in_output) == 0, (
                "初始化消息应该通过logger.info()记录，而不是output callback"
            )


class TestWarningMessagesLoggedNotOutput:
    """Tests for WARNING message logging behavior.

    WARNING messages should be logged via logger.warning() but NOT sent to output callback.
    """

    def test_warning_messages_logged_not_output(self, mock_datafeed, caplog):
        """WARNING messages should use logger.warning() and NOT call output callback."""
        output_messages = []
        output_callback = output_messages.append

        # Trigger a warning scenario - invalid symbol
        req = HistoryRequest(
            symbol="INVALID_SYMBOL_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=datetime.now() - timedelta(days=1),
            end=datetime.now(),
        )

        bars = mock_datafeed.query_bar_history(req, output=output_callback)

        # Check logger got WARNING messages
        warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warning_records) > 0, "Expected WARNING messages in logger"

        # Verify WARNING messages did NOT go to output callback
        warning_messages_in_output = [
            msg for msg in output_messages if "WARNING" in msg or "警告" in msg
        ]
        assert len(warning_messages_in_output) == 0, (
            f"WARNING messages should NOT be sent to output callback, but found: {warning_messages_in_output}"
        )


class TestErrorMessagesDualOutput:
    """Tests for ERROR message logging behavior.

    ERROR messages should be logged via BOTH logger.error() AND output callback.
    """

    def test_error_messages_dual_output(self, mock_datafeed, caplog):
        """ERROR messages should use BOTH logger.error() AND output callback."""
        output_messages = []
        output_callback = output_messages.append

        # Mock a failure scenario - corrupted zip file
        mock_datafeed.database.load_bar_data.return_value = []
        mock_datafeed.spot_vision_client.download_klines.return_value = (
            b"not a valid zip file"
        )

        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today.replace(day=1) - timedelta(days=20)
        start = start.replace(day=1)
        end = start + timedelta(days=5)

        req = HistoryRequest(
            symbol="BTCUSDT_SPOT_BINANCE",
            exchange=Exchange.GLOBAL,
            interval=Interval.MINUTE,
            start=start,
            end=end,
        )

        bars = mock_datafeed.query_bar_history(req, output=output_callback)

        # Check logger got ERROR messages
        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(error_records) > 0, "Expected ERROR messages in logger"

        # Check output callback also got error messages
        error_messages_in_output = [
            msg
            for msg in output_messages
            if "ERROR" in msg or "错误" in msg or "失败" in msg
        ]
        assert len(error_messages_in_output) > 0, (
            "ERROR messages should ALSO be sent to output callback for visibility"
        )

    def test_init_error_uses_dual_output(self, caplog):
        """init() failure should use BOTH logger.error() AND output callback."""
        mock_rest_client = MagicMock()
        mock_rest_client.get_exchange_info.side_effect = Exception("Network error")

        output_messages = []
        output_callback = output_messages.append

        with (
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.VisionClient",
                return_value=MagicMock(),
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.BinanceRestClient",
                return_value=mock_rest_client,
            ),
            patch(
                "vnpy_crypto_binance_datafeed.datafeed.get_database",
                return_value=MagicMock(),
            ),
        ):
            df = BinanceDatafeed()
            result = df.init(output=output_callback)

            assert result is False

            # Check logger got ERROR message
            error_records = [r for r in caplog.records if r.levelname == "ERROR"]
            assert len(error_records) > 0, "Expected ERROR in logger for init failure"

            # Check output callback also got error message
            error_messages_in_output = [
                msg for msg in output_messages if "失败" in msg or "错误" in msg
            ]
            assert len(error_messages_in_output) > 0, (
                "ERROR messages should ALSO be sent to output callback"
            )


class TestHelperMethodsExist:
    """Tests verifying that the helper logging methods exist (TDD requirement).

    These tests explicitly check for the existence of _log_info, _log_warning, _log_error
    methods that should be added in the GREEN phase.
    """

    def test_log_info_method_exists(self, mock_datafeed):
        """BinanceDatafeed should have _log_info method."""
        assert hasattr(mock_datafeed, "_log_info"), (
            "_log_info method should exist for proper INFO message handling"
        )
        assert callable(getattr(mock_datafeed, "_log_info")), (
            "_log_info should be callable"
        )

    def test_log_warning_method_exists(self, mock_datafeed):
        """BinanceDatafeed should have _log_warning method."""
        assert hasattr(mock_datafeed, "_log_warning"), (
            "_log_warning method should exist for proper WARNING message handling"
        )
        assert callable(getattr(mock_datafeed, "_log_warning")), (
            "_log_warning should be callable"
        )

    def test_log_error_method_exists(self, mock_datafeed):
        """BinanceDatafeed should have _log_error method."""
        assert hasattr(mock_datafeed, "_log_error"), (
            "_log_error method should exist for proper ERROR message handling"
        )
        assert callable(getattr(mock_datafeed, "_log_error")), (
            "_log_error should be callable"
        )
