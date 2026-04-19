import pytest
from datetime import datetime
from vnpy.trader.constant import Exchange, Interval
from vnpy_binance_datafeed.parser import (
    parse_kline_csv,
    parse_kline_json,
    generate_datetime,
    convert_to_bar_data,
)


def test_parse_kline_csv():
    # Sample CSV data from Binance
    # 0: Open time, 1: Open, 2: High, 3: Low, 4: Close, 5: Volume, 6: Close time, 7: Turnover, 8: Trades, 9: Taker buy base, 10: Taker buy quote, 11: Ignore
    csv_data = b"1609459200000,28923.63,28961.66,28913.12,28961.66,27.45703800,1609459259999,794487.66411928,1292,16.77719500,485390.29825708,0"

    klines = parse_kline_csv(csv_data)

    assert len(klines) == 1
    kline = klines[0]
    assert kline["open_time"] == 1609459200000
    assert kline["open"] == 28923.63
    assert kline["high"] == 28961.66
    assert kline["low"] == 28913.12
    assert kline["close"] == 28961.66
    assert kline["volume"] == 27.45703800
    assert kline["close_time"] == 1609459259999
    assert kline["turnover"] == 794487.66411928
    assert kline["num_trades"] == 1292
    assert kline["taker_buy_base_volume"] == 16.77719500
    assert kline["taker_buy_quote_volume"] == 485390.29825708


def test_parse_kline_csv_microseconds():
    # Microseconds timestamp (from 2025)
    # 1735689600000000 is 2025-01-01 00:00:00 in microseconds
    csv_data = b"1735689600000000,28923.63,28961.66,28913.12,28961.66,27.45703800,1735689659999999,794487.66411928,1292,16.77719500,485390.29825708,0"

    klines = parse_kline_csv(csv_data)

    assert len(klines) == 1
    kline = klines[0]
    assert kline["open_time"] == 1735689600000000

    dt = generate_datetime(kline["open_time"])
    expected_dt = datetime.fromtimestamp(1735689600)
    assert dt == expected_dt


def test_parse_kline_json():
    # Sample JSON data from Binance REST API
    json_data = [
        [
            1609459200000,  # Open time
            "28923.63",  # Open
            "28961.66",  # High
            "28913.12",  # Low
            "28961.66",  # Close
            "27.45703800",  # Volume
            1609459259999,  # Close time
            "794487.66411928",  # Quote asset volume
            1292,  # Number of trades
            "16.77719500",  # Taker buy base asset volume
            "485390.29825708",  # Taker buy quote asset volume
            "0",  # Ignore
        ]
    ]

    klines = parse_kline_json(json_data)

    assert len(klines) == 1
    kline = klines[0]
    assert kline["open_time"] == 1609459200000
    assert kline["open"] == 28923.63
    assert kline["high"] == 28961.66
    assert kline["low"] == 28913.12
    assert kline["close"] == 28961.66
    assert kline["volume"] == 27.45703800
    assert kline["close_time"] == 1609459259999
    assert kline["turnover"] == 794487.66411928
    assert kline["num_trades"] == 1292
    assert kline["taker_buy_base_volume"] == 16.77719500
    assert kline["taker_buy_quote_volume"] == 485390.29825708


def test_convert_to_bar_data():
    raw_data = {
        "open_time": 1609459200000,
        "open": 28923.63,
        "high": 28961.66,
        "low": 28913.12,
        "close": 28961.66,
        "volume": 27.45703800,
        "close_time": 1609459259999,
        "turnover": 794487.66411928,
        "num_trades": 1292,
        "taker_buy_base_volume": 16.77719500,
        "taker_buy_quote_volume": 485390.29825708,
    }

    symbol = "BTCUSDT"
    exchange = Exchange.GLOBAL
    interval = Interval.MINUTE

    bar = convert_to_bar_data(raw_data, symbol, exchange, interval)

    assert bar.symbol == symbol
    assert bar.exchange == exchange
    assert bar.interval == interval
    expected_dt = datetime.fromtimestamp(1609459200)
    assert bar.datetime == expected_dt
    assert bar.open_price == 28923.63
    assert bar.high_price == 28961.66
    assert bar.low_price == 28913.12
    assert bar.close_price == 28961.66
    assert bar.volume == 27.45703800
    assert bar.turnover == 794487.66411928
    assert bar.gateway_name == "BINANCE_DATAFEED"


def test_invalid_csv():
    # Empty data
    assert parse_kline_csv(b"") == []

    # Header row or invalid data
    csv_data = b"open_time,open,high,low,close\ninvalid,1,2,3,4"
    assert parse_kline_csv(csv_data) == []

    # Empty line in CSV
    csv_data = b"1609459200000,28923.63,28961.66,28913.12,28961.66,27.45703800,1609459259999,794487.66411928,1292,16.77719500,485390.29825708,0\n\n"
    klines = parse_kline_csv(csv_data)
    assert len(klines) == 1

    # Row with missing columns
    csv_data = b"1609459200000,28923.63"
    with pytest.raises(IndexError):
        parse_kline_csv(csv_data)


def test_invalid_json():
    # Empty data
    assert parse_kline_json([]) == []

    # Row with missing columns
    json_data = [[1609459200000, "28923.63"]]
    assert parse_kline_json(json_data) == []

    # Invalid data types
    json_data = [["invalid"] * 11]
    with pytest.raises(ValueError):
        parse_kline_json(json_data)
