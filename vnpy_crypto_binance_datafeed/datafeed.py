from datetime import datetime, timedelta, date
from typing import List, Optional, Callable, Any
import zipfile
import io

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.object import HistoryRequest, BarData
from vnpy.trader.database import get_database, BaseDatabase
from vnpy.trader.constant import Exchange, Interval

from vnpy.trader.setting import SETTINGS
from .vision_client import VisionClient
from .rest_client import BinanceRestClient
from .parser import parse_kline_csv, convert_to_bar_data
from .constant import (
    INTERVAL_VT2BINANCE,
    SUPPORTED_INTERVALS,
    MarketType,
    parse_vt_symbol,
)


class BinanceDatafeed(BaseDatafeed):
    """
    Binance datafeed for downloading history data.
    """

    def __init__(self, market_type: MarketType = None):
        """"""
        if market_type:
            self.market_type: MarketType = market_type
        else:
            market_type_str = SETTINGS.get("binance.market_type", "SPOT")
            self.market_type: MarketType = MarketType(market_type_str.upper())

        # Dual market clients
        self.spot_rest_client: BinanceRestClient = BinanceRestClient(
            market_type=MarketType.SPOT
        )
        self.swap_rest_client: BinanceRestClient = BinanceRestClient(
            market_type=MarketType.SWAP
        )
        self.spot_vision_client: VisionClient = VisionClient(
            market_type=MarketType.SPOT
        )
        self.swap_vision_client: VisionClient = VisionClient(
            market_type=MarketType.SWAP
        )

        # Backward compatibility: point to spot clients by default
        self.vision_client: VisionClient = self.spot_vision_client
        self.rest_client: BinanceRestClient = self.spot_rest_client

        self.database: BaseDatabase = get_database()
        self.inited: bool = False

        # Separate symbol sets for each market
        self.spot_symbols: set[str] = set()
        self.swap_symbols: set[str] = set()
        # Backward compatibility: combined symbols
        self.symbols: set[str] = set()

    def init(self, output: Callable = print) -> bool:
        """
        Initialize datafeed service connection.
        """
        if self.inited:
            return True

        try:
            output("正在初始化Binance数据服务...")

            # Load SPOT symbols
            output("正在加载现货合约信息...")
            spot_info = self.spot_rest_client.get_exchange_info()
            for symbol_data in spot_info.get("symbols", []):
                base_symbol = symbol_data["symbol"]
                self.spot_symbols.add(base_symbol)
                # Also add full format: BTCUSDT -> BTCUSDT_SPOT_BINANCE
                self.spot_symbols.add(f"{base_symbol}_SPOT_BINANCE")

            # Load SWAP symbols
            output("正在加载期货合约信息...")
            swap_info = self.swap_rest_client.get_exchange_info()
            for symbol_data in swap_info.get("symbols", []):
                base_symbol = symbol_data["symbol"]
                self.swap_symbols.add(base_symbol)
                self.swap_symbols.add(f"{base_symbol}_SWAP_BINANCE")

            # Update backward compat sets
            self.symbols = self.spot_symbols | self.swap_symbols

            self.inited = True
            output(
                f"Binance数据服务初始化成功，加载了 {len(self.spot_symbols)} 个现货合约和 {len(self.swap_symbols)} 个期货合约"
            )
            return True
        except Exception as e:
            output(f"Binance数据服务初始化失败: {e}")
            return False

    def query_bar_history(
        self, req: HistoryRequest, output: Callable = print
    ) -> List[BarData]:
        """
        Query history bar data.
        """
        if not self.inited:
            self.init(output)

        # Parse symbol to extract base, market_type, and full_symbol
        parsed = parse_vt_symbol(req.symbol.upper())

        if parsed is None:
            output("合约代码格式错误，应为 XXX_SPOT_BINANCE 或 XXX_SWAP_BINANCE")
            return []

        # Select clients based on market_type
        if parsed.market_type == "SPOT":
            rest_client = self.spot_rest_client
            vision_client = self.spot_vision_client
        else:  # SWAP
            rest_client = self.swap_rest_client
            vision_client = self.swap_vision_client

        # Validate symbol (strip .GLOBAL suffix for validation)
        symbol_for_validation = parsed.full_symbol.replace(".GLOBAL", "")
        if self.symbols and symbol_for_validation not in self.symbols:
            output(f"不支持的合约代码: {parsed.full_symbol}")
            return []

        # Use parsed.base for API calls
        symbol_for_api = parsed.base  # e.g., "BTCUSDT"

        # Handle None end time
        end_time: datetime = req.end if req.end else datetime.now()
        start_time: datetime = req.start
        interval: Optional[Interval] = req.interval

        if interval is None:
            output("K线周期不能为空")
            return []

        # Validate interval and get Binance interval string
        binance_interval = ""
        if interval in INTERVAL_VT2BINANCE:
            binance_interval = INTERVAL_VT2BINANCE[interval]
        elif hasattr(interval, "value") and interval.value in SUPPORTED_INTERVALS:
            binance_interval = str(interval.value)
        else:
            output(f"不支持的K线周期: {interval}")
            return []

        # Check if data already exists in database
        existing_bars = self.database.load_bar_data(
            symbol=req.symbol,
            exchange=req.exchange,
            interval=interval,
            start=start_time,
            end=end_time,
        )

        if existing_bars:
            output(f"数据库中已存在 {len(existing_bars)} 根K线数据，跳过下载")
            return existing_bars

        # Determine data source
        source = self._determine_data_source(start_time, end_time)

        bars = []
        if source == "vision":
            bars = self._download_from_vision(
                req,
                binance_interval,
                start_time,
                end_time,
                interval,
                output,
                vision_client=vision_client,
                symbol_for_api=symbol_for_api,
            )
        elif source == "rest":
            bars = self._download_from_rest(
                req,
                binance_interval,
                start_time,
                end_time,
                interval,
                output,
                rest_client=rest_client,
                symbol_for_api=symbol_for_api,
            )
        elif source == "both":
            bars_vision = self._download_from_vision(
                req,
                binance_interval,
                start_time,
                end_time,
                interval,
                output,
                vision_client=vision_client,
                symbol_for_api=symbol_for_api,
            )
            bars_rest = self._download_from_rest(
                req,
                binance_interval,
                start_time,
                end_time,
                interval,
                output,
                rest_client=rest_client,
                symbol_for_api=symbol_for_api,
            )
            # Merge and deduplicate
            bars_dict = {bar.datetime: bar for bar in bars_vision}
            for bar in bars_rest:
                bars_dict[bar.datetime] = bar
            bars = list(bars_dict.values())
            bars.sort(key=lambda x: x.datetime)

        if bars:
            self._save_to_database(bars)
            output(f"成功下载并保存 {len(bars)} 根K线数据")
        else:
            output("未下载到任何数据")

        return bars

    def _determine_data_source(self, start: datetime, end: datetime) -> str:
        """
        Choose vision vs rest
        """
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)

        if end < yesterday:
            return "vision"
        elif start >= yesterday:
            return "rest"
        else:
            return "both"

    def _download_from_vision(
        self,
        req: HistoryRequest,
        binance_interval: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        output: Callable = print,
        vision_client: Optional[VisionClient] = None,
        symbol_for_api: Optional[str] = None,
    ) -> List[BarData]:
        """
        Download from data.binance.vision
        """
        # Use provided client and symbol, or fall back to defaults (for backward compatibility)
        if vision_client is None:
            vision_client = self.vision_client
        if symbol_for_api is None:
            symbol_for_api = req.symbol.upper()

        # Keep full symbol for BarData
        symbol_for_bar = req.symbol.upper()

        start_date = start_time.date()
        end_date = end_time.date()

        current_date = start_date.replace(day=1)

        bars = []

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month

            output(f"正在从Vision下载 {symbol_for_api} {year}-{month:02d} 的数据...")

            zip_data = vision_client.download_klines(
                symbol_for_api, binance_interval, year, month
            )
            if zip_data:
                checksum = vision_client.get_checksum(
                    symbol_for_api, binance_interval, year, month
                )
                if checksum:
                    if not vision_client.verify_checksum(zip_data, checksum):
                        output(f"校验和验证失败: {year}-{month:02d}")
                        # Move to next month
                        if month == 12:
                            current_date = current_date.replace(year=year + 1, month=1)
                        else:
                            current_date = current_date.replace(month=month + 1)
                        continue

                try:
                    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
                        for filename in z.namelist():
                            if filename.endswith(".csv"):
                                csv_data = z.read(filename)
                                raw_klines = parse_kline_csv(csv_data)
                                for raw in raw_klines:
                                    bar = convert_to_bar_data(
                                        raw, symbol_for_bar, req.exchange, interval
                                    )

                                    # Use date comparison to avoid timezone issues
                                    if start_time <= bar.datetime <= end_time:
                                        bars.append(bar)
                except zipfile.BadZipFile:
                    output(f"下载的ZIP文件损坏: {year}-{month:02d}")
                except Exception as e:
                    output(f"处理ZIP文件时发生错误: {e}")
            else:
                output(f"未能下载 {year}-{month:02d} 的数据，可能尚未生成")

            # Move to next month
            if month == 12:
                current_date = current_date.replace(year=year + 1, month=1)
            else:
                current_date = current_date.replace(month=month + 1)

        return bars

    def _download_from_rest(
        self,
        req: HistoryRequest,
        binance_interval: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        output: Callable = print,
        rest_client: Optional[BinanceRestClient] = None,
        symbol_for_api: Optional[str] = None,
    ) -> List[BarData]:
        """
        Download from REST API
        """
        # Use provided client and symbol, or fall back to defaults (for backward compatibility)
        if rest_client is None:
            rest_client = self.rest_client
        if symbol_for_api is None:
            symbol_for_api = req.symbol.upper()

        # Keep full symbol for BarData
        symbol_for_bar = req.symbol.upper()

        start_ts = int(start_time.timestamp() * 1000)
        end_ts = int(end_time.timestamp() * 1000)

        bars = []
        current_start = start_ts

        while current_start < end_ts:
            output(
                f"正在从REST API下载 {symbol_for_api} 数据，起始时间: {datetime.fromtimestamp(current_start / 1000)}"
            )

            raw_klines = rest_client.get_klines(
                symbol=symbol_for_api,
                interval=binance_interval,
                start_time=current_start,
                end_time=end_ts,
                limit=1000,
            )

            if not raw_klines:
                break

            for raw in raw_klines:
                bar = convert_to_bar_data(raw, symbol_for_bar, req.exchange, interval)

                # Filter by time range
                if start_time <= bar.datetime < end_time:
                    bars.append(bar)

            # Update current_start to the last kline's open_time + 1
            last_open_time = raw_klines[-1]["open_time"]
            if current_start >= last_open_time + 1:
                break
            current_start = last_open_time + 1

        return bars

    def _save_to_database(self, bars: List[BarData]) -> None:
        """
        Save to database
        """
        if not bars:
            return

        self.database.save_bar_data(bars)
