import csv
import io
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any

from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval


# UTC timezone constant (consistent with vnpy_binance gateway)
UTC_TZ = ZoneInfo("UTC")


def parse_kline_csv(data: bytes) -> List[Dict[str, Any]]:
    """
    Parse CSV from data.binance.vision
    Columns (0-indexed):
    0: Open time (milliseconds, microseconds from 2025)
    1: Open
    2: High
    3: Low
    4: Close
    5: Volume
    6: Close time
    7: Quote asset volume (turnover)
    8: Number of trades
    9: Taker buy base asset volume
    10: Taker buy quote asset volume
    11: Ignore
    """
    klines = []
    # Decode bytes to string
    content = data.decode("utf-8")
    f = io.StringIO(content)
    reader = csv.reader(f)

    for row in reader:
        if not row:
            continue

        # Some CSV files might have headers, but Binance public data usually doesn't.
        # However, we should handle cases where the first column is not a number.
        try:
            float(row[0])
        except (ValueError, IndexError):
            continue

        kline = {
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
            "turnover": float(row[7]),
            "num_trades": int(row[8]),
            "taker_buy_base_volume": float(row[9]),
            "taker_buy_quote_volume": float(row[10]),
        }
        klines.append(kline)

    return klines


def parse_kline_json(data: list) -> List[Dict[str, Any]]:
    """
    Parse JSON from REST API
    Same 12 elements as array.
    """
    klines = []
    for row in data:
        if not row or len(row) < 11:
            continue

        kline = {
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
            "turnover": float(row[7]),
            "num_trades": int(row[8]),
            "taker_buy_base_volume": float(row[9]),
            "taker_buy_quote_volume": float(row[10]),
        }
        klines.append(kline)
    return klines


def generate_datetime(timestamp: int) -> datetime:
    """
    Generate datetime from timestamp.
    Handle milliseconds and microseconds (from 2025).

    Returns UTC-aware datetime to be consistent with vnpy_binance gateway.
    """
    # 2025-01-01 00:00:00 in milliseconds is 1735689600000
    # If timestamp > 10^14, it's likely microseconds
    if timestamp > 10**14:
        dt = datetime.fromtimestamp(timestamp / 1000000, tz=UTC_TZ)
    else:
        dt = datetime.fromtimestamp(timestamp / 1000, tz=UTC_TZ)

    return dt  # UTC-aware datetime


def convert_to_bar_data(
    raw_data: Dict[str, Any], symbol: str, exchange: Exchange, interval: Interval
) -> BarData:
    """
    Convert to vnpy BarData
    """
    bar = BarData(
        symbol=symbol,
        exchange=exchange,
        datetime=generate_datetime(raw_data["open_time"]),
        interval=interval,
        volume=raw_data["volume"],
        turnover=raw_data["turnover"],
        open_price=raw_data["open"],
        high_price=raw_data["high"],
        low_price=raw_data["low"],
        close_price=raw_data["close"],
        open_interest=0,
        gateway_name="BINANCE_DATAFEED",
    )
    return bar
