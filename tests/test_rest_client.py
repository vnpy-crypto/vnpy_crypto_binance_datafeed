import pytest
import time
import requests
from unittest.mock import MagicMock, patch
from vnpy_binance_datafeed.rest_client import BinanceRestClient
from vnpy_binance_datafeed.constant import BINANCE_REST_URL


@pytest.fixture
def client():
    return BinanceRestClient()


def test_get_klines_url(client):
    """
    Test URL construction for get_klines.
    """
    with patch.object(client.session, "request") as mock_request:
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = []

        symbol = "BTCUSDT"
        interval = "1m"
        start_time = 1609459200000
        end_time = 1609462800000
        limit = 1000

        client.get_klines(symbol, interval, start_time, end_time, limit)

        expected_url = f"{BINANCE_REST_URL}/api/v3/klines"
        expected_params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
        }

        mock_request.assert_called_once_with(
            method="GET",
            url=expected_url,
            params=expected_params,
            timeout=client.timeout,
        )


def test_rate_limiting(client):
    """
    Test rate limit logic (mock).
    """
    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
        patch("time.time") as mock_time,
    ):
        # Mock time to be constant
        mock_time.return_value = 1000.0

        # First request
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {"serverTime": 12345}

        client.get_server_time()

        # Second request immediately after
        # requests_per_second = 10.0, so wait_time = 0.1
        client.get_server_time()

        # Check if sleep was called with correct wait time
        # wait_time = max(0.0, (1.0 / 10.0) - (1000.0 - 1000.0)) = 0.1
        mock_sleep.assert_called_with(0.1)


def test_rate_limit_backoff(client):
    """
    Test backoff on 429 (mock).
    """
    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
    ):
        # First request returns 429
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {"Retry-After": "5"}

        # Second request returns 200
        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"serverTime": 12345}

        mock_request.side_effect = [mock_429, mock_200]

        client.get_server_time()

        # Check if backoff was applied
        assert client.backoff_delay == 2.5  # 5 * 0.5 after success
        mock_sleep.assert_any_call(5.0)


def test_network_timeout(client):
    """
    Test timeout handling (mock).
    """
    import requests

    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
    ):
        # First request raises timeout
        # Second request returns 200
        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"serverTime": 12345}

        mock_request.side_effect = [requests.exceptions.Timeout("Timeout"), mock_200]

        client.get_server_time()

        # Check if retry with backoff was applied
        # Initial backoff is 1.0, then halved to 0.5 after success
        assert client.backoff_delay == 0.5
        mock_sleep.assert_any_call(1.0)


def test_rate_limit_no_retry_after(client):
    """
    Test 429 without Retry-After header.
    """
    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
    ):
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {}

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"serverTime": 12345}

        # First call: backoff becomes 1.0
        # Second call: success, backoff becomes 0.5
        mock_request.side_effect = [mock_429, mock_200]

        client.get_server_time()
        assert client.backoff_delay == 0.5
        mock_sleep.assert_any_call(1.0)


def test_ip_banned(client):
    """
    Test 418 IP banned.
    """
    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
    ):
        mock_418 = MagicMock()
        mock_418.status_code = 418

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"serverTime": 12345}

        mock_request.side_effect = [mock_418, mock_200]

        client.get_server_time()
        assert client.backoff_delay == 30.0  # 60 * 0.5
        mock_sleep.assert_any_call(60.0)


def test_network_error_max_retries(client):
    """
    Test network error max retries.
    """
    import requests

    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
    ):
        # Set initial backoff high to trigger raise
        client.backoff_delay = 40.0

        mock_request.side_effect = requests.exceptions.RequestException("Network Error")

        with pytest.raises(requests.exceptions.RequestException):
            client.get_server_time()

        # 40.0 * 2.0 = 80.0 > 60.0, so it should raise
        assert client.backoff_delay == 80.0


def test_get_klines_success(client):
    """
    Test successful get_klines with data parsing.
    """
    with (
        patch.object(client.session, "request") as mock_request,
        patch(
            "vnpy_binance_datafeed.rest_client.parse_kline_json"
        ) as mock_parse,
    ):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = [["data"]]
        mock_parse.return_value = ["parsed_data"]

        result = client.get_klines("BTCUSDT", "1m", 0, 0)

        assert result == ["parsed_data"]
        mock_parse.assert_called_once_with([["data"]])


def test_rate_limit_backoff_multiplier(client):
    """
    Test backoff multiplier when 429 and no Retry-After.
    """
    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
    ):
        client.backoff_delay = 2.0

        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {}

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"serverTime": 12345}

        mock_request.side_effect = [mock_429, mock_200]

        client.get_server_time()
        # 2.0 * 2.0 = 4.0, then halved to 2.0
        assert client.backoff_delay == 2.0
        mock_sleep.assert_any_call(4.0)


def test_other_error_raise(client):
    """
    Test other errors raise for status.
    """
    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
    ):
        mock_500 = MagicMock()
        mock_500.status_code = 500
        mock_500.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "Internal Server Error"
        )

        mock_request.return_value = mock_500

        with pytest.raises(requests.exceptions.HTTPError):
            client.get_server_time()


def test_invalid_symbol(client):
    """
    Test error handling for invalid symbol (mock).
    """
    with (
        patch.object(client.session, "request") as mock_request,
        patch("time.sleep") as mock_sleep,
    ):
        # Mock 400 Bad Request for invalid symbol
        mock_400 = MagicMock()
        mock_400.status_code = 400
        mock_400.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "Bad Request"
        )

        mock_request.return_value = mock_400

        # get_klines should return empty list on exception
        result = client.get_klines("INVALID", "1m", 0, 0)
        assert result == []


def test_get_exchange_info(client):
    """
    Test get_exchange_info.
    """
    with patch.object(client.session, "request") as mock_request:
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {"info": "test"}

        result = client.get_exchange_info()
        assert result == {"info": "test"}
