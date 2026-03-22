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

## Symbol Format

合约代码格式: `{原始代码}_{SPOT|SWAP}_BINANCE`

- 现货合约: `BTCUSDT_SPOT_BINANCE`
- 期货合约: `BTCUSDT_SWAP_BINANCE`

系统会自动识别市场类型并选择正确的API端点。

## Usage

```python
from vnpy.trader.datafeed import get_datafeed
from vnpy.trader.object import HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from datetime import datetime

datafeed = get_datafeed()

req = HistoryRequest(
    symbol="BTCUSDT_SPOT_BINANCE",  # 或 BTCUSDT_SWAP_BINANCE
    exchange=Exchange.GLOBAL,
    interval=Interval.HOUR,
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31)
)

bars = datafeed.query_bar_history(req)
```

## Troubleshooting

### 错误: "合约代码格式错误"

确保合约代码格式正确:
- ✅ 正确: `BTCUSDT_SPOT_BINANCE`
- ✅ 正确: `BTCUSDT_SWAP_BINANCE`
- ❌ 错误: `BTCUSDT` (缺少市场类型标识)

## License

MIT
