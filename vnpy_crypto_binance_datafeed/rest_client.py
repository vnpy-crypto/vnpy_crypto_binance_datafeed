import time
import requests
import logging
from typing import List, Dict, Any, Optional
from .constant import (
    BINANCE_SPOT_REST_URL,
    BINANCE_FUTURES_REST_URL,
    DEFAULT_TIMEOUT,
    MarketType,
)
from .parser import parse_kline_json


logger = logging.getLogger(__name__)


class BinanceRestClient:
    """
    Binance REST API client for datafeed.
    """

    def __init__(
        self,
        market_type: MarketType = MarketType.SPOT,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.market_type: MarketType = market_type
        self.timeout: int = timeout
        self.session: requests.Session = requests.Session()

        if self.market_type == MarketType.SPOT:
            self.base_url: str = BINANCE_SPOT_REST_URL
            self.api_prefix: str = "/api/v3"
        else:
            self.base_url: str = BINANCE_FUTURES_REST_URL
            self.api_prefix: str = "/fapi/v1"

        # Rate limiting
        self.requests_per_second: float = 10.0
        self.last_request_time: float = 0.0
        self.backoff_delay: float = 0.0

    def _request(
        self, method: str, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Send request with rate limiting and error handling.
        """
        url = f"{self.base_url}{path}"

        while True:
            # Adaptive rate limiting
            now = time.time()
            elapsed = now - self.last_request_time

            # Base wait time to maintain 10 req/s
            wait_time = max(0.0, (1.0 / self.requests_per_second) - elapsed)

            # Apply backoff if active
            if self.backoff_delay > 0:
                wait_time = max(wait_time, self.backoff_delay)

            if wait_time > 0:
                time.sleep(wait_time)

            try:
                response = self.session.request(
                    method=method, url=url, params=params, timeout=self.timeout
                )
                self.last_request_time = time.time()

                if response.status_code == 200:
                    # Success, gradually reduce backoff
                    self.backoff_delay = max(0.0, self.backoff_delay * 0.5)
                    if self.backoff_delay < 0.1:
                        self.backoff_delay = 0.0
                    return response.json()

                if response.status_code == 429:
                    # Rate limit exceeded
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        self.backoff_delay = float(retry_after)
                    else:
                        if self.backoff_delay == 0:
                            self.backoff_delay = 1.0
                        else:
                            self.backoff_delay *= 2.0

                    # Cap backoff at 60 seconds
                    self.backoff_delay = min(self.backoff_delay, 60.0)
                    continue

                if response.status_code == 418:
                    # IP banned
                    self.backoff_delay = 60.0
                    continue

                # Other errors
                response.raise_for_status()

            except requests.exceptions.RequestException:
                # Network error, retry with backoff
                if self.backoff_delay == 0:
                    self.backoff_delay = 1.0
                else:
                    self.backoff_delay *= 2.0

                if self.backoff_delay > 60.0:
                    raise

                continue

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        end_time: int,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get klines from REST API.
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
        }

        try:
            data = self._request("GET", f"{self.api_prefix}/klines", params=params)
            if not data:
                return []
            return parse_kline_json(data)
        except Exception as e:
            # Log the error for debugging purposes
            logger.warning(f"Failed to get klines: {e}")
            # Return empty list for invalid symbols or other errors as per requirement
            return []

    def get_exchange_info(self) -> Dict[str, Any]:
        """
        Get exchange info for symbol validation.
        """
        return self._request("GET", f"{self.api_prefix}/exchangeInfo")

    def get_server_time(self) -> int:
        """
        Get server time for timestamp sync.
        """
        data = self._request("GET", f"{self.api_prefix}/time")
        return data["serverTime"]
