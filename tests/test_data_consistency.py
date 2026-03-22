"""
Data Consistency Test Script

Compare BTCUSDT data between vnpy SQLite database and Binance API.
Generates JSON and CSV reports with detailed differences.
"""

import math
import csv
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from vnpy.trader.database import get_database
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData

from vnpy_crypto_binance_datafeed.rest_client import BinanceRestClient
from vnpy_crypto_binance_datafeed.constant import MarketType


# =============================================================================
# Module 1: Configuration and Constants
# =============================================================================

SYMBOL = "BTCUSDT_SPOT_BINANCE"
API_SYMBOL = "BTCUSDT"  # For Binance API calls
START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 3, 22)  # end is exclusive
INTERVAL = Interval.MINUTE
EXCHANGE = Exchange.GLOBAL

# Output file paths with timestamp
OUTPUT_DIR = Path(__file__).parent
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
JSON_REPORT_PATH = OUTPUT_DIR / f"consistency_report_{TIMESTAMP}.json"
CSV_REPORT_PATH = OUTPUT_DIR / f"consistency_differences_{TIMESTAMP}.csv"

# Floating point comparison tolerance
FLOAT_TOLERANCE = 1e-9

# DB_TZ constant (Asia/Shanghai) - vnpy stores datetime in this timezone
try:
    from vnpy.trader.database import DB_TZ
except ImportError:
    from zoneinfo import ZoneInfo

    DB_TZ = ZoneInfo("Asia/Shanghai")

UTC_TZ = timezone.utc


# =============================================================================
# Module 2: vnpy Data Reading
# =============================================================================


def load_vnpy_data() -> Dict[datetime, Dict[str, Any]]:
    """
    Load bar data from vnpy SQLite database.

    Returns:
        Dict mapping datetime to bar data dict (with tz-aware or naive datetime as key)
    """
    database = get_database()

    # load_bar_data parameters: symbol, exchange, interval, start, end
    bars: List[BarData] = database.load_bar_data(
        symbol=SYMBOL,
        exchange=EXCHANGE,
        interval=INTERVAL,
        start=START_DATE,
        end=END_DATE,  # end is exclusive
    )

    # Convert to dict with datetime as key
    result: Dict[datetime, Dict[str, Any]] = {}
    for bar in bars:
        # Store datetime as-is (may be naive or aware)
        dt = bar.datetime
        result[dt] = {
            "open_price": bar.open_price,
            "high_price": bar.high_price,
            "low_price": bar.low_price,
            "close_price": bar.close_price,
            "volume": bar.volume,
            "turnover": bar.turnover,
        }

    return result


# =============================================================================
# Module 3: Binance API Data Download
# =============================================================================


def load_binance_data() -> Dict[datetime, Dict[str, Any]]:
    """
    Download kline data from Binance REST API.

    Handles batched downloads (1000 items per request) and processes
    data across month boundaries.

    Returns:
        Dict mapping datetime to kline data dict
    """
    rest_client = BinanceRestClient(market_type=MarketType.SPOT)

    # Convert datetime to milliseconds timestamp
    # Binance API expects UTC milliseconds
    start_ms = int(START_DATE.timestamp() * 1000)
    end_ms = int(END_DATE.timestamp() * 1000)

    all_klines: List[Dict[str, Any]] = []

    # Batch download: 1000 items per request
    current_start = start_ms
    while current_start < end_ms:
        batch_end = min(
            current_start + (1000 * 60 * 1000), end_ms
        )  # 1000 minutes in ms

        klines = rest_client.get_klines(
            symbol=API_SYMBOL,
            interval="1m",
            start_time=current_start,
            end_time=batch_end,
            limit=1000,
        )

        if not klines:
            break

        all_klines.extend(klines)

        # Move start to continue from last kline's open_time
        last_kline = klines[-1]
        current_start = last_kline["open_time"] + (60 * 1000)  # Next minute

        # Safety check to prevent infinite loop
        if len(klines) < 1000:
            break

    # Convert to dict with datetime as key
    result: Dict[datetime, Dict[str, Any]] = {}
    for kline in all_klines:
        # Binance returns UTC timestamp in milliseconds
        open_time_ms = kline["open_time"]
        dt = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC_TZ)

        # Remove tz info to get naive UTC datetime for comparison
        dt_naive = dt.replace(tzinfo=None)

        result[dt_naive] = {
            "open_price": kline["open"],
            "high_price": kline["high"],
            "low_price": kline["low"],
            "close_price": kline["close"],
            "volume": kline["volume"],
            "turnover": kline["turnover"],
        }

    return result


# =============================================================================
# Module 4: Data Comparison Logic
# =============================================================================


def normalize_datetime(dt: datetime) -> datetime:
    """
    Normalize datetime to naive for comparison.

    The issue is that:
    - Binance API returns UTC timestamps
    - vnpy stores datetimes as Asia/Shanghai timezone

    For proper comparison, we need to convert UTC -> DB_TZ using astimezone.
    """
    if dt.tzinfo is not None:
        # Convert to DB_TZ timezone
        dt = dt.astimezone(DB_TZ)
        # Return as naive datetime
        return dt.replace(tzinfo=None)

    # If naive, assume it's UTC and convert properly
    dt_utc = dt.replace(tzinfo=UTC_TZ)
    dt_db_tz = dt_utc.astimezone(DB_TZ)
    return dt_db_tz.replace(tzinfo=None)


def compare_floats(a: float, b: float) -> bool:
    """Compare two floats with tolerance."""
    return math.isclose(a, b, rel_tol=FLOAT_TOLERANCE, abs_tol=0.0)


def compare_bars(
    vnpy_bar: Dict[str, Any], binance_bar: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compare two bar records and return differences.

    Compares fields: open_price, high_price, low_price, close_price, volume, turnover
    """
    differences: Dict[str, Dict[str, float]] = {}

    fields = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "turnover",
    ]

    for field in fields:
        vnpy_val = vnpy_bar[field]
        binance_val = binance_bar[field]

        if not compare_floats(vnpy_val, binance_val):
            differences[field] = {
                "vnpy": vnpy_val,
                "binance": binance_val,
                "diff": vnpy_val - binance_val,
                "rel_diff": (vnpy_val - binance_val) / binance_val
                if binance_val != 0
                else 0,
            }

    return differences


def compare_data(
    vnpy_data: Dict[datetime, Dict[str, Any]],
    binance_data: Dict[datetime, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Compare vnpy and binance data.

    Returns report with summary, differences, and missing entries.
    """
    # Normalize keys for matching
    # Binance uses UTC, vnpy uses DB_TZ (Asia/Shanghai)
    # When comparing, we shift binance datetime by +8 hours to match vnpy
    normalized_binance: Dict[datetime, Dict[str, Any]] = {}
    for dt, bar in binance_data.items():
        normalized_dt = normalize_datetime(dt)
        normalized_binance[normalized_dt] = bar

    # Get all timestamps
    vnpy_times = set(vnpy_data.keys())
    binance_times = set(normalized_binance.keys())

    common_times = vnpy_times & binance_times
    missing_from_vnpy = sorted(binance_times - vnpy_times)
    missing_from_binance = sorted(vnpy_times - binance_times)

    # Compare matching entries
    differences: List[Dict[str, Any]] = []
    matched_count = 0

    for dt in sorted(common_times):
        vnpy_bar = vnpy_data[dt]
        binance_bar = normalized_binance[dt]
        diffs = compare_bars(vnpy_bar, binance_bar)

        if diffs:
            differences.append(
                {
                    "datetime": dt.isoformat(),
                    "differences": diffs,
                }
            )
        else:
            matched_count += 1

    total_vnpy = len(vnpy_data)
    total_binance = len(binance_data)
    total_common = len(common_times)
    diff_count = len(differences)
    missing_vnpy_count = len(missing_from_vnpy)
    missing_binance_count = len(missing_from_binance)

    return {
        "summary": {
            "total_vnpy": total_vnpy,
            "total_binance": total_binance,
            "total_common": total_common,
            "matched_count": matched_count,
            "difference_count": diff_count,
            "missing_from_vnpy_count": missing_vnpy_count,
            "missing_from_binance_count": missing_binance_count,
        },
        "differences": differences,
        "missing_from_vnpy": [dt.isoformat() for dt in missing_from_vnpy],
        "missing_from_binance": [dt.isoformat() for dt in missing_from_binance],
    }


# =============================================================================
# Module 5: Report Generation
# =============================================================================


def generate_report(report: Dict[str, Any]) -> None:
    """
    Generate JSON and CSV reports.
    """
    # Generate JSON report
    with open(JSON_REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"JSON report saved to: {JSON_REPORT_PATH}")

    # Generate CSV report for differences
    with open(CSV_REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Header
        writer.writerow(
            [
                "datetime",
                "field",
                "vnpy_value",
                "binance_value",
                "difference",
                "relative_difference",
            ]
        )

        # Data rows
        for diff_entry in report["differences"]:
            dt = diff_entry["datetime"]
            for field, values in diff_entry["differences"].items():
                writer.writerow(
                    [
                        dt,
                        field,
                        values["vnpy"],
                        values["binance"],
                        values["diff"],
                        values["rel_diff"],
                    ]
                )

    print(f"CSV report saved to: {CSV_REPORT_PATH}")

    # Print summary to console
    summary = report["summary"]
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total vnpy records:     {summary['total_vnpy']}")
    print(f"Total Binance records:  {summary['total_binance']}")
    print(f"Common timestamps:      {summary['total_common']}")
    print(f"Matched (no diff):      {summary['matched_count']}")
    print(f"With differences:      {summary['difference_count']}")
    print(f"Missing from vnpy:     {summary['missing_from_vnpy_count']}")
    print(f"Missing from Binance:  {summary['missing_from_binance_count']}")
    print("=" * 60)


# =============================================================================
# Main Program
# =============================================================================


def main():
    print("=" * 60)
    print("Data Consistency Test - BTCUSDT")
    print("=" * 60)
    print(f"Symbol:      {SYMBOL}")
    print(f"Time Range:  {START_DATE} to {END_DATE}")
    print(f"Interval:    {INTERVAL}")
    print(f"DB TZ:       {DB_TZ}")
    print()

    # 1. Load vnpy data
    print("Loading data from vnpy database...")
    vnpy_data = load_vnpy_data()
    print(f"  Loaded {len(vnpy_data)} records from vnpy")

    # 2. Download Binance data
    print("Downloading data from Binance API...")
    binance_data = load_binance_data()
    print(f"  Downloaded {len(binance_data)} records from Binance")

    # 3. Compare data
    print("Comparing data...")
    report = compare_data(vnpy_data, binance_data)

    # 4. Generate reports
    print("Generating reports...")
    generate_report(report)

    print()
    print("Done!")


if __name__ == "__main__":
    main()
