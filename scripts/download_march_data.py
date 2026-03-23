"""Download BTCUSDT data from 2026-01-01 to 2026-03-21 to fill March gap."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import HistoryRequest
from vnpy.trader.database import get_database
from vnpy_crypto_binance_datafeed.datafeed import BinanceDatafeed


def main():
    print("=" * 60)
    print("Downloading BTCUSDT_SPOT_BINANCE data")
    print("Time range: 2026-01-01 to 2026-03-21")
    print("=" * 60)

    # Initialize datafeed
    datafeed = BinanceDatafeed()
    datafeed.init(output=print)

    # Create history request
    req = HistoryRequest(
        symbol="BTCUSDT_SPOT_BINANCE",
        exchange=Exchange.GLOBAL,
        interval=Interval.MINUTE,
        start=datetime(2026, 1, 1),
        end=datetime(2026, 3, 21, 23, 59),
    )

    # Download data (this will trigger smart fallback for March)
    print("\nStarting download...")
    bars = datafeed.query_bar_history(req, output=print)

    print(f"\nDownloaded {len(bars)} bars total")

    # Save to database
    if bars:
        database = get_database()
        database.save_bar_data(bars)
        print(f"Saved {len(bars)} bars to database")
    else:
        print("No new bars to save")

    print("\n" + "=" * 60)
    print("Download complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
