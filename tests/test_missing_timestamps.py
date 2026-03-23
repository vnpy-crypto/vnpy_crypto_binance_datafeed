"""
Investigate missing minute data points on Binance API.
Query specific timestamps to verify if data exists on Binance.
"""

from vnpy_crypto_binance_datafeed.rest_client import BinanceRestClient
from vnpy_crypto_binance_datafeed.constant import MarketType
from datetime import datetime, timezone

client = BinanceRestClient(market_type=MarketType.SPOT)

# Missing time points in UTC+8 and their UTC equivalents
missing_times = [
    ("2026-03-15 20:13:00+08:00", "2026-03-15 12:13:00 UTC", 1742049180000),
    ("2026-03-15 20:37:00+08:00", "2026-03-15 12:37:00 UTC", 1742050620000),
    ("2026-03-17 00:27:00+08:00", "2026-03-16 16:27:00 UTC", 1742155320000),
]

print("=" * 60)
print("Binance API Query for Missing Timestamps")
print("=" * 60)

for local_label, utc_label, ts in missing_times:
    print(f"\n=== Querying: {local_label} ({utc_label}) ===")
    print(f"Timestamp: {ts}")

    # Query a small window around the timestamp (3 minutes before to 3 minutes after)
    result = client.get_klines(
        symbol="BTCUSDT",
        interval="1m",
        start_time=ts - 180000,  # 3 min before
        end_time=ts + 180000,  # 3 min after
        limit=10,
    )

    print(f"Results returned: {len(result)} klines")

    # Check if the exact timestamp exists
    exact_match = None
    for kline in result:
        open_time = kline["open_time"]
        if open_time == ts:
            exact_match = kline
            break

    if exact_match:
        print(f"  [EXACT MATCH FOUND]")
        print(f"     Open:  {exact_match['open']}")
        print(f"     High:  {exact_match['high']}")
        print(f"     Low:   {exact_match['low']}")
        print(f"     Close: {exact_match['close']}")
        print(f"     Vol:   {exact_match['volume']}")
    else:
        print(f"  [NO EXACT MATCH for timestamp {ts}]")
        if result:
            print(f"  Nearest klines:")
            for kline in result[:5]:
                dt = datetime.fromtimestamp(kline["open_time"] / 1000, tz=timezone.utc)
                match_indicator = " <-- EXACT" if kline["open_time"] == ts else ""
                print(
                    f"    {dt}: open={kline['open']}, close={kline['close']}{match_indicator}"
                )

print("\n" + "=" * 60)
print("Investigation Complete")
print("=" * 60)
