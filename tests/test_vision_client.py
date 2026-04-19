import pytest
import hashlib
from unittest.mock import MagicMock, patch
from datetime import date
import requests
from vnpy_binance_datafeed.vision_client import VisionClient
from vnpy_binance_datafeed.constant import BINANCE_VISION_URL


@pytest.fixture
def client():
    return VisionClient()


def test_download_klines_url(client):
    """Test URL construction for monthly klines."""
    with patch.object(client, "_request") as mock_request:
        mock_request.return_value = b"fake_data"

        symbol = "BTCUSDT"
        interval = "1m"
        year = 2024
        month = 1

        client.download_klines(symbol, interval, year, month)

        expected_url = f"{BINANCE_VISION_URL}/data/spot/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip"
        mock_request.assert_called_once_with(expected_url)


def test_download_daily_klines_url(client):
    """Test URL construction for daily klines."""
    with patch.object(client, "_request") as mock_request:
        mock_request.return_value = b"fake_data"

        symbol = "BTCUSDT"
        interval = "1m"
        dt = date(2024, 1, 1)

        client.download_daily_klines(symbol, interval, dt)

        expected_url = f"{BINANCE_VISION_URL}/data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-2024-01-01.zip"
        mock_request.assert_called_once_with(expected_url)


def test_verify_checksum_valid(client):
    """Test checksum verification with correct data."""
    data = b"hello world"
    checksum = hashlib.sha256(data).hexdigest()

    assert client.verify_checksum(data, checksum) is True


def test_verify_checksum_invalid(client):
    """Test checksum verification with corrupted data."""
    data = b"hello world"
    checksum = "wrong_checksum"

    assert client.verify_checksum(data, checksum) is False
    assert client.verify_checksum(None, checksum) is False
    assert client.verify_checksum(data, None) is False


def test_network_timeout(client):
    """Test timeout handling (mock)."""
    with patch.object(client.session, "get") as mock_get:
        mock_get.side_effect = requests.exceptions.Timeout("Timeout error")

        with patch("time.sleep"):  # Skip sleep to speed up tests
            result = client.download_klines("BTCUSDT", "1m", 2024, 1)

            assert result is None
            # Initial call + 3 retries = 4 calls
            assert mock_get.call_count == 4


def test_retry_on_error(client):
    """Test retry logic (mock)."""
    with patch.object(client.session, "get") as mock_get:
        # Fail twice, then succeed
        mock_response = MagicMock()
        mock_response.content = b"success_data"
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None

        mock_get.side_effect = [
            requests.exceptions.RequestException("Error 1"),
            requests.exceptions.HTTPError("Error 2"),
            mock_response,
        ]

        with patch("time.sleep") as mock_sleep:
            result = client.download_klines("BTCUSDT", "1m", 2024, 1)

            assert result == b"success_data"
            assert mock_get.call_count == 3
            assert mock_sleep.call_count == 2
            # Verify exponential backoff: 2^0=1, 2^1=2
            mock_sleep.assert_any_call(1)
            mock_sleep.assert_any_call(2)


def test_get_checksum(client):
    """Test get_checksum method."""
    with patch.object(client, "_request") as mock_request:
        # Mock checksum file content: "checksum  filename"
        mock_request.return_value = b"abc123def456  BTCUSDT-1m-2024-01.zip"

        checksum = client.get_checksum("BTCUSDT", "1m", 2024, 1)

        assert checksum == "abc123def456"

        expected_url = f"{BINANCE_VISION_URL}/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip.CHECKSUM"
        mock_request.assert_called_once_with(expected_url)


def test_get_checksum_none(client):
    """Test get_checksum when request fails."""
    with patch.object(client, "_request") as mock_request:
        mock_request.return_value = None

        checksum = client.get_checksum("BTCUSDT", "1m", 2024, 1)

        assert checksum is None
