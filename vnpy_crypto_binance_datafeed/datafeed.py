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
from .constant import INTERVAL_VT2BINANCE, SUPPORTED_INTERVALS, MarketType


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

        self.vision_client: VisionClient = VisionClient(market_type=self.market_type)
        self.rest_client: BinanceRestClient = BinanceRestClient(
            market_type=self.market_type
        )
        self.database: BaseDatabase = get_database()
        self.inited: bool = False
        self.symbols: set[str] = set()

    def init(self, output: Callable = print) -> bool:
        """
        Initialize datafeed service connection.
        """
        if self.inited:
            return True

        try:
            output("正在初始化Binance数据服务...")
            exchange_info = self.rest_client.get_exchange_info()
            for symbol_data in exchange_info.get("symbols", []):
                self.symbols.add(symbol_data["symbol"])

            self.inited = True
            output("Binance数据服务初始化成功")
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

        # Normalize symbol
        symbol = req.symbol.upper()

        # Validate symbol
        if self.symbols and symbol not in self.symbols:
            output(f"不支持的合约代码: {symbol}")
            return []

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
            symbol=symbol,
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
                req, binance_interval, start_time, end_time, interval, output
            )
        elif source == "rest":
            bars = self._download_from_rest(
                req, binance_interval, start_time, end_time, interval, output
            )
        elif source == "both":
            bars_vision = self._download_from_vision(
                req, binance_interval, start_time, end_time, interval, output
            )
            bars_rest = self._download_from_rest(
                req, binance_interval, start_time, end_time, interval, output
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
    ) -> List[BarData]:
        """
        Download from data.binance.vision
        """
        symbol = req.symbol.upper()

        start_date = start_time.date()
        end_date = end_time.date()

        current_date = start_date.replace(day=1)

        bars = []

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month

            output(f"正在从Vision下载 {symbol} {year}-{month:02d} 的数据...")

            zip_data = self.vision_client.download_klines(
                symbol, binance_interval, year, month
            )
            if zip_data:
                checksum = self.vision_client.get_checksum(
                    symbol, binance_interval, year, month
                )
                if checksum:
                    if not self.vision_client.verify_checksum(zip_data, checksum):
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
                                        raw, symbol, req.exchange, interval
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
    ) -> List[BarData]:
        """
        Download from REST API
        """
        symbol = req.symbol.upper()

        start_ts = int(start_time.timestamp() * 1000)
        end_ts = int(end_time.timestamp() * 1000)

        bars = []
        current_start = start_ts

        while current_start < end_ts:
            output(
                f"正在从REST API下载 {symbol} 数据，起始时间: {datetime.fromtimestamp(current_start / 1000)}"
            )

            raw_klines = self.rest_client.get_klines(
                symbol=symbol,
                interval=binance_interval,
                start_time=current_start,
                end_time=end_ts,
                limit=1000,
            )

            if not raw_klines:
                break

            for raw in raw_klines:
                bar = convert_to_bar_data(raw, symbol, req.exchange, interval)

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
