import hashlib
import time
import logging
import requests
from datetime import date
from typing import Optional
from .constant import (
    BINANCE_VISION_URL,
    BINANCE_SPOT_VISION_PATH,
    BINANCE_FUTURES_VISION_PATH,
    DEFAULT_TIMEOUT,
    MarketType,
)


logger = logging.getLogger(__name__)


class VisionClient:
    """
    Client for downloading data from data.binance.vision.
    """

    def __init__(
        self,
        market_type: MarketType = MarketType.SPOT,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.market_type: MarketType = market_type
        self.timeout = timeout
        self.session = requests.Session()

        if self.market_type == MarketType.SPOT:
            self.vision_path: str = BINANCE_SPOT_VISION_PATH
        else:
            self.vision_path: str = BINANCE_FUTURES_VISION_PATH

    def _request(self, url: str, max_retries: int = 3) -> Optional[bytes]:
        """
        Perform HTTP GET request with retry logic.
        """
        for i in range(max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.content
            except (requests.RequestException, requests.HTTPError) as e:
                if i == max_retries:
                    # Log or raise error
                    logger.warning(
                        f"Failed to download from {url} after {max_retries} retries: {e}"
                    )
                    return None

                # Exponential backoff: 1s, 2s, 4s
                wait_time = 2**i
                time.sleep(wait_time)
        return None

    def download_klines(
        self, symbol: str, interval: str, year: int, month: int
    ) -> Optional[bytes]:
        """
        Download monthly klines.
        """
        url = f"{BINANCE_VISION_URL}/{self.vision_path}/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip"
        return self._request(url)

    def download_daily_klines(
        self, symbol: str, interval: str, dt: date
    ) -> Optional[bytes]:
        """
        Download daily klines.
        """
        url = f"{BINANCE_VISION_URL}/{self.vision_path}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{dt.strftime('%Y-%m-%d')}.zip"
        return self._request(url)

    def get_checksum(
        self, symbol: str, interval: str, year: int, month: int
    ) -> Optional[str]:
        """
        Download checksum file for monthly klines.
        """
        url = f"{BINANCE_VISION_URL}/{self.vision_path}/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip.CHECKSUM"
        data = self._request(url)
        if data:
            # Checksum file content is usually: "checksum  filename"
            content = data.decode("utf-8").strip()
            return content.split()[0]
        return None

    def verify_checksum(self, data: bytes, checksum: str) -> bool:
        """
        Verify SHA256 checksum.
        """
        if not data or not checksum:
            return False

        calculated_checksum = hashlib.sha256(data).hexdigest()
        return calculated_checksum == checksum
