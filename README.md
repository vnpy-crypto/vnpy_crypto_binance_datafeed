# vnpy_binance_datafeed

[中文文档](#中文文档)

Binance historical data download module for the VeighNa framework, providing cryptocurrency K-line data support.

## Features

- **Dual Data Source Support**: Automatically selects the optimal data source
  - `data.binance.vision`: Historical data (downloaded by month, fast)
  - REST API: Recent data (real-time retrieval)
- **Smart Gap Detection**: Only downloads missing data, avoiding duplicate downloads
- **Dual Market Support**: Spot (SPOT) and Perpetual Contracts (SWAP/USDT-M Futures)
- **Multiple Time Intervals**: Supports 1m, 5m, 15m, 30m, 1h, 4h, 1d
- **Data Deduplication**: Automatically handles duplicate data

## Installation

```bash
pip install vnpy-binance-datafeed
```

For development installation:

```bash
pip install -e .
```

## Contract Code Format

| Market | Format | Example |
|--------|--------|---------|
| Spot | `{SYMBOL}_SPOT_BINANCE` | `BTCUSDT_SPOT_BINANCE` |
| Perpetual | `{SYMBOL}_SWAP_BINANCE` | `BTCUSDT_SWAP_BINANCE` |

The system automatically identifies the market type and selects the correct API endpoint.

## Supported Time Intervals

| Interval | Binance Parameter | Note |
|----------|-------------------|------|
| 1 minute | `1m` | Interval.MINUTE |
| 5 minutes | `5m` | - |
| 15 minutes | `15m` | - |
| 30 minutes | `30m` | - |
| 1 hour | `1h` | Interval.HOUR |
| 4 hours | `4h` | - |
| 1 day | `1d` | Interval.DAILY |

## Data Source Selection Strategy

The module automatically selects the optimal data source based on the requested time range:

| Time Range | Data Source | Description |
|------------|-------------|-------------|
| End time < yesterday | Vision | Historical data, batch download by month |
| Start time >= yesterday | REST API | Recent data, real-time retrieval |
| Spanning boundary | Vision + REST | Both combined, automatic merge and deduplication |

## Usage

### Configuration

#### GUI Global Configuration

Configure the datafeed in VeighNa Trader:

1. Launch VeighNa Trader
2. Click the **Configuration** menu
3. Find the **datafeed.name** configuration item
4. Enter: `binance_datafeed`
5. Click OK

> ⚠️ **Important**: If `datafeed.name` is not configured, the GUI's data management features will not be able to use this module.

#### Using via GUI

1. Launch VeighNa Trader
2. Open the **Data Manager** application
3. Click **Download Data**
4. Fill in the information:
   - Symbol: `BTCUSDT_SPOT_BINANCE`
   - Exchange: `GLOBAL`
   - Interval: Select time interval
   - Start Date: Select start date
5. Click Download

#### Using via Code

```python
from vnpy.trader.datafeed import get_datafeed
from vnpy.trader.object import HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from datetime import datetime

# Get datafeed instance
datafeed = get_datafeed()

# Create historical data request
req = HistoryRequest(
    symbol="BTCUSDT_SPOT_BINANCE",  # Spot
    # symbol="BTCUSDT_SWAP_BINANCE",  # Perpetual
    exchange=Exchange.GLOBAL,
    interval=Interval.MINUTE,
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31)
)

# Query historical data
bars = datafeed.query_bar_history(req)
print(f"Downloaded {len(bars)} bars")
```

## Incremental Update Mechanism

The module implements intelligent gap detection, only downloading missing data:

```
Request data: 2024-01-01 to 2024-03-31
    │
    ├─► Load existing data from database
    │
    ├─► Detect gaps (missing time points)
    │
    ├─► Only download missing parts
    │
    └─► Return new data (for datamanager to save)
```

## Timezone Handling

| Stage | Timezone | Description |
|-------|----------|-------------|
| User Input | DB_TZ (Asia/Shanghai) | GUI automatically adds |
| Binance API | UTC | Automatically converted during request |
| Returned Data | UTC-aware | Handled by parser |
| Database Storage | naive (actual DB_TZ) | Converted by convert_tz |

### Automatic Features

The module automatically:
- Loads Binance contract list during initialization
- Identifies market type based on contract code
- Selects the correct API endpoint

## FAQ

### Error: "Invalid contract code format"

Ensure the contract code format is correct:
- ✅ Correct: `BTCUSDT_SPOT_BINANCE`
- ✅ Correct: `ETHUSDT_SWAP_BINANCE`
- ❌ Incorrect: `BTCUSDT` (missing market type identifier)
- ❌ Incorrect: `btcusdt_spot_binance` (needs uppercase)

### Error: "Unsupported K-line interval"

Ensure you use a supported time interval: `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`

## Dependencies

- vnpy >= 3.0
- requests

## License

MIT License

## Related Links

- [VeighNa](https://github.com/vnpy/vnpy) - Quantitative trading platform
- [Binance Public Data](https://data.binance.vision/) - Binance historical data
- [Binance API](https://binance-docs.github.io/apidocs/) - Binance API documentation

---

# 中文文档

[English](#vnpy_binance_datafeed)

Binance 历史数据下载模块，为 VeighNa 框架提供加密货币 K线数据支持。

## 功能特性

- **双数据源支持**：自动选择最优数据源
  - `data.binance.vision`：历史数据（按月下载，速度快）
  - REST API：近期数据（实时获取）
- **智能缺口检测**：只下载缺失的数据，避免重复下载
- **双市场支持**：现货 (SPOT) 和永续合约 (SWAP/USDT-M Futures)
- **多时间周期**：支持 1m、5m、15m、30m、1h、4h、1d
- **数据去重**：自动处理重复数据

## 安装

```bash
pip install vnpy-binance-datafeed
```

开发安装：

```bash
pip install -e .
```

## 合约代码格式

| 市场 | 格式 | 示例 |
|------|------|------|
| 现货 | `{SYMBOL}_SPOT_BINANCE` | `BTCUSDT_SPOT_BINANCE` |
| 永续合约 | `{SYMBOL}_SWAP_BINANCE` | `BTCUSDT_SWAP_BINANCE` |

系统会自动识别市场类型并选择正确的 API 端点。

## 支持的时间周期

| 周期 | Binance 参数 | 说明 |
|------|-------------|------|
| 1分钟 | `1m` | Interval.MINUTE |
| 5分钟 | `5m` | - |
| 15分钟 | `15m` | - |
| 30分钟 | `30m` | - |
| 1小时 | `1h` | Interval.HOUR |
| 4小时 | `4h` | - |
| 1天 | `1d` | Interval.DAILY |

## 数据源选择策略

模块会根据请求的时间范围自动选择最优数据源：

| 时间范围 | 数据源 | 说明 |
|----------|--------|------|
| 结束时间 < 昨天 | Vision | 历史数据，按月批量下载 |
| 开始时间 >= 昨天 | REST API | 近期数据，实时获取 |
| 跨越边界 | Vision + REST | 两者结合，自动合并去重 |

## 使用方法

### 配置

#### GUI 全局配置

在 VeighNa Trader 中配置 datafeed：

1. 启动 VeighNa Trader
2. 点击菜单 **配置**
3. 找到 **datafeed.name** 配置项
4. 填入：`binance_datafeed`
5. 点击确定

> ⚠️ **重要**：如果不配置 `datafeed.name`，GUI 的数据管理功能将无法使用此模块。

#### 通过 GUI 使用

1. 启动 VeighNa Trader
2. 打开 **数据管理** 应用
3. 点击 **下载数据**
4. 填写信息：
   - 代码：`BTCUSDT_SPOT_BINANCE`
   - 交易所：`GLOBAL`
   - 周期：选择时间周期
   - 开始日期：选择起始日期
5. 点击下载

#### 通过代码使用

```python
from vnpy.trader.datafeed import get_datafeed
from vnpy.trader.object import HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from datetime import datetime

# 获取 datafeed 实例
datafeed = get_datafeed()

# 创建历史数据请求
req = HistoryRequest(
    symbol="BTCUSDT_SPOT_BINANCE",  # 现货
    # symbol="BTCUSDT_SWAP_BINANCE",  # 永续合约
    exchange=Exchange.GLOBAL,
    interval=Interval.MINUTE,
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31)
)

# 查询历史数据
bars = datafeed.query_bar_history(req)
print(f"下载了 {len(bars)} 根K线")
```

## 增量更新机制

模块实现了智能缺口检测，只下载缺失的数据：

```
请求数据: 2024-01-01 到 2024-03-31
    │
    ├─► 加载数据库已有数据
    │
    ├─► 检测缺口 (缺失的时间点)
    │
    ├─► 只下载缺失部分
    │
    └─► 返回新数据 (供 datamanager 保存)
```

## 时区处理

| 阶段 | 时区 | 说明 |
|------|------|------|
| 用户输入 | DB_TZ (Asia/Shanghai) | GUI 自动添加 |
| Binance API | UTC | 请求时自动转换 |
| 返回数据 | UTC-aware | parser 处理 |
| 数据库存储 | naive (实际 DB_TZ) | convert_tz 转换 |

### 自动功能

模块会自动：
- 初始化时加载 Binance 合约列表
- 根据合约代码识别市场类型
- 选择正确的 API 端点

## 常见问题

### 错误: "合约代码格式错误"

确保合约代码格式正确：
- ✅ 正确: `BTCUSDT_SPOT_BINANCE`
- ✅ 正确: `ETHUSDT_SWAP_BINANCE`
- ❌ 错误: `BTCUSDT` (缺少市场类型标识)
- ❌ 错误: `btcusdt_spot_binance` (需要大写)

### 错误: "不支持的K线周期"

确保使用支持的时间周期：`1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`

## 依赖

- vnpy >= 3.0
- requests

## 许可证

MIT License

## 相关链接

- [VeighNa](https://github.com/vnpy/vnpy) - 量化交易平台
- [Binance Public Data](https://data.binance.vision/) - Binance 历史数据
- [Binance API](https://binance-docs.github.io/apidocs/) - Binance API 文档
