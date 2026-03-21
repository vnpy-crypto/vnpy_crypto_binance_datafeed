from .datafeed import BinanceDatafeed

# vnpy 要求导出的类名必须是 Datafeed
Datafeed = BinanceDatafeed

__version__ = "2026.03.22"

__all__ = ["Datafeed", "BinanceDatafeed"]
