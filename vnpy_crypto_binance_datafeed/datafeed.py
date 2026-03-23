from datetime import datetime, timedelta, date
from typing import List, Optional, Callable, Any
import zipfile
import io

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.object import HistoryRequest, BarData
from vnpy.trader.database import get_database, BaseDatabase, DB_TZ
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.logger import logger

from vnpy.trader.setting import SETTINGS
from .vision_client import VisionClient
from .rest_client import BinanceRestClient
from .parser import parse_kline_csv, convert_to_bar_data, UTC_TZ
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

    def _log_info(self, msg: str) -> None:
        """Log INFO message (no popup)."""
        logger.info(msg)

    def _log_warning(self, msg: str) -> None:
        """Log WARNING message (no popup)."""
        logger.warning(msg)

    def _log_error(self, msg: str, output: Callable) -> None:
        """Log ERROR message AND call output (popup)."""
        logger.error(msg)
        output(msg)

    def init(self, output: Callable = print) -> bool:
        """
        Initialize datafeed service connection.
        """
        if self.inited:
            return True

        try:
            self._log_info("正在初始化Binance数据服务...")

            # Load SPOT symbols
            self._log_info("正在加载现货合约信息...")
            spot_info = self.spot_rest_client.get_exchange_info()
            for symbol_data in spot_info.get("symbols", []):
                base_symbol = symbol_data["symbol"]
                self.spot_symbols.add(base_symbol)
                # Also add full format: BTCUSDT -> BTCUSDT_SPOT_BINANCE
                self.spot_symbols.add(f"{base_symbol}_SPOT_BINANCE")

            # Load SWAP symbols
            self._log_info("正在加载期货合约信息...")
            swap_info = self.swap_rest_client.get_exchange_info()
            for symbol_data in swap_info.get("symbols", []):
                base_symbol = symbol_data["symbol"]
                self.swap_symbols.add(base_symbol)
                self.swap_symbols.add(f"{base_symbol}_SWAP_BINANCE")

            # Update backward compat sets
            self.symbols = self.spot_symbols | self.swap_symbols

            self.inited = True
            self._log_info(
                f"Binance数据服务初始化成功，加载了 {len(self.spot_symbols)} 个现货合约和 {len(self.swap_symbols)} 个期货合约"
            )
            return True
        except Exception as e:
            self._log_error(f"Binance数据服务初始化失败: {e}", output)
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
            self._log_error(
                "合约代码格式错误，应为 XXX_SPOT_BINANCE 或 XXX_SWAP_BINANCE", output
            )
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
            self._log_error(f"不支持的合约代码: {parsed.full_symbol}", output)
            return []

        # Use parsed.base for API calls
        symbol_for_api = parsed.base  # e.g., "BTCUSDT"

        # Handle None end time
        end_time: datetime = req.end if req.end else datetime.now()
        start_time: datetime = req.start
        interval: Optional[Interval] = req.interval

        if interval is None:
            self._log_error("K线周期不能为空", output)
            return []

        # Validate interval and get Binance interval string
        binance_interval = ""
        if interval in INTERVAL_VT2BINANCE:
            binance_interval = INTERVAL_VT2BINANCE[interval]
        elif hasattr(interval, "value") and interval.value in SUPPORTED_INTERVALS:
            binance_interval = str(interval.value)
        else:
            self._log_error(f"不支持的K线周期: {interval}", output)
            return []

        # Convert aware datetime to naive for SQLite query compatibility
        # SQLite stores datetime as naive strings; aware datetime causes
        # lexicographic comparison failures at boundary values
        db_start = (
            start_time.astimezone(DB_TZ).replace(tzinfo=None)
            if start_time.tzinfo
            else start_time
        )
        db_end = (
            end_time.astimezone(DB_TZ).replace(tzinfo=None)
            if end_time.tzinfo
            else end_time
        )

        # Load existing data from database
        existing_bars = self.database.load_bar_data(
            symbol=req.symbol,
            exchange=req.exchange,
            interval=interval,
            start=db_start,
            end=db_end,
        )

        # Find gaps in existing data based on requested interval
        gaps = self._find_gaps(existing_bars, start_time, end_time, interval)

        if not gaps:
            self._log_info(
                f"数据库中已有完整数据 ({len(existing_bars)} 根K线)，跳过下载"
            )
            return existing_bars

        self._log_info(f"检测到 {len(gaps)} 个数据缺口，开始补全...")

        # Download data for each gap
        all_new_bars: List[BarData] = []
        for gap_start, gap_end in gaps:
            self._log_info(f"正在补全缺口: {gap_start} 到 {gap_end}")

            # Determine data source for this gap
            source = self._determine_data_source(gap_start, gap_end)

            if source in ["vision", "both"]:
                # Unpack tuple: (bars_vision, missing_months)
                bars_vision, missing_months = self._download_from_vision(
                    req,
                    binance_interval,
                    gap_start,
                    gap_end,
                    interval,
                    output,
                    vision_client=vision_client,
                    symbol_for_api=symbol_for_api,
                )
                new_bars = bars_vision

                # Smart fallback: only download missing months via REST
                if missing_months:
                    self._log_warning(
                        f"Vision缺失 {len(missing_months)} 个月份，使用REST API补充"
                    )
                    for month_start_date, month_end_date in missing_months:
                        # Convert date to datetime for REST API
                        month_start_dt = datetime.combine(
                            month_start_date, datetime.min.time()
                        )
                        month_end_dt = datetime.combine(
                            month_end_date, datetime.max.time()
                        )

                        # Ensure timezone consistency: if gap_start has tzinfo, apply it
                        if gap_start.tzinfo is not None:
                            month_start_dt = month_start_dt.replace(
                                tzinfo=gap_start.tzinfo
                            )
                            month_end_dt = month_end_dt.replace(tzinfo=gap_start.tzinfo)

                        # Clip to gap boundaries (don't download outside requested range)
                        actual_start = max(month_start_dt, gap_start)
                        actual_end = min(month_end_dt, gap_end)

                        if actual_start < actual_end:
                            bars_rest = self._download_from_rest(
                                req,
                                binance_interval,
                                actual_start,
                                actual_end,
                                interval,
                                output,
                                rest_client=rest_client,
                                symbol_for_api=symbol_for_api,
                            )
                            new_bars.extend(bars_rest)

                    # Deduplicate by datetime (in case of overlap)
                    new_bars_dict = {bar.datetime: bar for bar in new_bars}
                    new_bars = list(new_bars_dict.values())
                    new_bars.sort(key=lambda x: x.datetime)
            elif source == "rest":
                new_bars = self._download_from_rest(
                    req,
                    binance_interval,
                    gap_start,
                    gap_end,
                    interval,
                    output,
                    rest_client=rest_client,
                    symbol_for_api=symbol_for_api,
                )
            else:
                new_bars = []

            all_new_bars.extend(new_bars)

        # Following vnpy's design pattern: datafeed only returns newly downloaded data,
        # the caller (vnpy_datamanager) is responsible for saving to database.
        if all_new_bars:
            self._log_info(f"成功下载 {len(all_new_bars)} 根K线数据")
        else:
            self._log_warning("未下载到任何新数据")
            # Return empty list to indicate no new data
            if existing_bars:
                self._log_info(f"数据库中已有完整数据 ({len(existing_bars)} 根K线)")

        return all_new_bars

    def _determine_data_source(self, start: datetime, end: datetime) -> str:
        """
        Choose vision vs rest
        """
        # If end has timezone info, use the same timezone for comparison
        if end.tzinfo is not None:
            tz = end.tzinfo
            today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
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
    ) -> tuple[List[BarData], List[tuple[date, date]]]:
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

        start_date = start_time.astimezone(UTC_TZ).date()
        end_date = end_time.astimezone(UTC_TZ).date()

        current_date = start_date.replace(day=1)

        bars = []
        missing_months: List[tuple[date, date]] = []

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month

            # Skip Vision for current month (monthly data not generated yet)
            today = date.today()
            if year == today.year and month == today.month:
                self._log_info(
                    f"跳过当前月份 {year}-{month:02d} 的 Vision 下载（月度数据尚未生成）"
                )
                month_end = self._get_last_day_of_month(current_date)
                missing_months.append((current_date, month_end))

                # Move to next month
                if month == 12:
                    current_date = current_date.replace(year=year + 1, month=1)
                else:
                    current_date = current_date.replace(month=month + 1)
                continue

            self._log_info(
                f"正在从Vision下载 {symbol_for_api} {year}-{month:02d} 的数据..."
            )

            zip_data = vision_client.download_klines(
                symbol_for_api, binance_interval, year, month
            )
            if zip_data:
                checksum = vision_client.get_checksum(
                    symbol_for_api, binance_interval, year, month
                )
                if checksum:
                    if not vision_client.verify_checksum(zip_data, checksum):
                        self._log_error(f"校验和验证失败: {year}-{month:02d}", output)
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
                    self._log_error(f"下载的ZIP文件损坏: {year}-{month:02d}", output)
                except Exception as e:
                    self._log_error(f"处理ZIP文件时发生错误: {e}", output)
            else:
                self._log_warning(f"未能下载 {year}-{month:02d} 的数据，可能尚未生成")
                month_end = self._get_last_day_of_month(current_date)
                missing_months.append((current_date, month_end))

            # Move to next month
            if month == 12:
                current_date = current_date.replace(year=year + 1, month=1)
            else:
                current_date = current_date.replace(month=month + 1)

        return bars, missing_months

    def _get_last_day_of_month(self, d: date) -> date:
        """Get the last day of the month for a given date."""
        if d.month == 12:
            return date(d.year + 1, 1, 1) - timedelta(days=1)
        else:
            return date(d.year, d.month + 1, 1) - timedelta(days=1)

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
            self._log_info(
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

    def _get_interval_delta(self, interval: Interval) -> timedelta:
        """
        Get timedelta for interval - determines gap detection granularity.
        """
        if interval == Interval.MINUTE:
            return timedelta(minutes=1)
        elif interval == Interval.HOUR:
            return timedelta(hours=1)
        elif interval == Interval.DAILY:
            return timedelta(days=1)
        else:
            return timedelta(minutes=1)  # Default

    def _find_gaps(
        self,
        existing_bars: List[BarData],
        start: datetime,
        end: datetime,
        interval: Interval,
    ) -> List[tuple[datetime, datetime]]:
        """
        Find time gaps in existing data based on the requested interval.

        Gap granularity is determined by interval:
        - MINUTE: detect missing minutes
        - HOUR: detect missing hours
        - DAILY: detect missing days

        Returns list of (gap_start, gap_end) tuples.
        """
        # Normalize timezone: add DB_TZ to naive datetimes
        # This ensures consistency with database-loaded aware datetimes
        if start.tzinfo is None:
            start = start.replace(tzinfo=DB_TZ)
        if end.tzinfo is None:
            end = end.replace(tzinfo=DB_TZ)

        if not existing_bars:
            return [(start, end)]

        # Get interval delta based on requested interval
        interval_delta = self._get_interval_delta(interval)

        # Build set of existing timestamps
        existing_times = {bar.datetime for bar in existing_bars}

        # Find gaps
        gaps: List[tuple[datetime, datetime]] = []
        current = start
        gap_start = None

        while current <= end:
            if current not in existing_times:
                if gap_start is None:
                    gap_start = current
            else:
                if gap_start is not None:
                    gaps.append((gap_start, current - interval_delta))
                    gap_start = None
            current += interval_delta

        # Handle gap at end
        if gap_start is not None:
            gaps.append((gap_start, end))

        return gaps
