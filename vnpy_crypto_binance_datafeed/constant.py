from dataclasses import dataclass
from enum import Enum
from vnpy.trader.constant import Interval

# Interval mapping (vnpy Interval ↔ Binance interval string)
# Note: vnpy's Interval enum only contains MINUTE, HOUR, DAILY, WEEKLY, TICK.
# For 5m, 15m, 30m, 4h, they are not present in the standard vnpy Interval enum.
# We map the ones that are available.
INTERVAL_VT2BINANCE: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# Supported intervals for Binance datafeed
SUPPORTED_INTERVALS: list[str] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]


class MarketType(Enum):
    """
    Market type for Binance.
    """

    SPOT = "SPOT"
    FUTURES = "FUTURES"
    SWAP = "SWAP"


@dataclass
class ParsedSymbol:
    """
    Parsed symbol result from vt_symbol.
    """

    base: str
    market_type: str
    exchange: str
    full_symbol: str


def parse_vt_symbol(vt_symbol: str) -> ParsedSymbol | None:
    """
    Parse vt_symbol in format: {BASE}_{SPOT|SWAP}_BINANCE[.GLOBAL]

    Returns ParsedSymbol on success, None on invalid format.
    """
    if not vt_symbol:
        return None

    # Remove .GLOBAL suffix if present
    clean_symbol = vt_symbol
    if clean_symbol.endswith(".GLOBAL"):
        clean_symbol = clean_symbol[:-7]

    # Split by underscore
    parts = clean_symbol.split("_")
    if len(parts) != 3:
        return None

    base, market_type_str, exchange = parts

    # Validate exchange
    if exchange != "BINANCE":
        return None

    # Validate market_type
    valid_market_types = {"SPOT", "SWAP"}
    if market_type_str not in valid_market_types:
        return None

    # Validate base is not empty
    if not base:
        return None

    return ParsedSymbol(
        base=base, market_type=market_type_str, exchange=exchange, full_symbol=vt_symbol
    )


# API endpoints for data.binance.vision
BINANCE_VISION_URL: str = "https://data.binance.vision"
BINANCE_SPOT_VISION_PATH: str = "data/spot"
BINANCE_FUTURES_VISION_PATH: str = "data/futures/usdm"

# API endpoints for REST API
BINANCE_SPOT_REST_URL: str = "https://api.binance.com"
BINANCE_FUTURES_REST_URL: str = "https://fapi.binance.com"
BINANCE_REST_URL: str = BINANCE_SPOT_REST_URL

# Default configuration values
DEFAULT_TIMEOUT: int = 30
DATA_FRESHNESS_THRESHOLD_HOURS: int = 24
