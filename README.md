# BINANCE Datafeed for VeighNa

BINANCE datafeed module for VeighNa framework.

## Features

- Download historical K-line data from Binance
- Support both Spot and USDT-M Futures markets
- Hybrid data source: data.binance.vision (bulk) + REST API (recent)
- Automatic data deduplication and database storage

## Installation

```bash
pip install -e .
```

## Configuration

Add to your `vt_setting.json`:

```json
{
    "datafeed.name": "crypto_binance_datafeed",
    "binance.market_type": "SPOT"
}
```

## Usage

```python
from vnpy.trader.datafeed import get_datafeed
from vnpy.trader.object import HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from datetime import datetime

datafeed = get_datafeed()

req = HistoryRequest(
    symbol="BTCUSDT",
    exchange=Exchange.GLOBAL,
    interval=Interval.HOUR,
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31)
)

bars = datafeed.query_bar_history(req)
```

## License

MIT
